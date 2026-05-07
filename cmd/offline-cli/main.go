// Command offline-cli runs the full pipeline (ASR → bot → TTS) end-to-end
// against pre-recorded audio. Use it to verify provider integration
// without needing FreeSWITCH.
//
// Input formats:
//   - .wav  PCM S16LE mono 8kHz (header parsed, data extracted)
//   - .pcm  raw PCM S16LE mono 8kHz (read as-is)
//
// Output: raw PCM S16LE mono 8kHz suitable for `ffplay -f s16le -ar 8000 -ac 1`.
//
// Example:
//
//	go run ./cmd/offline-cli \
//	  -in sample.wav -out tts-out.pcm \
//	  -asr-token "$ASR_TOKEN" \
//	  -tts-key "$TTS_KEY" -voice thuyanh-north \
//	  -bot-url http://localhost:11006/api/v1/call/
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"callbot-master/internal/asr"
	"callbot-master/internal/bot"
	"callbot-master/internal/pipeline"
	"callbot-master/internal/tts"
	"callbot-master/internal/vad"
)

func main() {
	in := flag.String("in", "", "input audio file (.wav or .pcm, S16LE 8kHz mono)")
	out := flag.String("out", "tts-out.pcm", "output PCM file")
	convID := flag.String("conv", "offline-cli-001", "conversation_id")

	asrEndpoint := flag.String("asr-endpoint", "103.253.20.28:9000", "Viettel ASR gRPC addr")
	asrToken := flag.String("asr-token", os.Getenv("MASTER_ASR_TOKEN"), "ASR token (env MASTER_ASR_TOKEN)")

	ttsEndpoint := flag.String("tts-endpoint", "ws://103.253.20.27:8767", "Viettel TTS WS endpoint")
	ttsKey := flag.String("tts-key", os.Getenv("MASTER_TTS_TOKEN"), "TTS api key (env MASTER_TTS_TOKEN)")
	voice := flag.String("voice", os.Getenv("MASTER_TTS_VOICE_ID"), "voiceId")
	tempo := flag.Float64("tempo", 1.0, "TTS tempo")

	botURL := flag.String("bot-url", "http://localhost:11006/api/v1/call/", "bot endpoint")

	frameMs := flag.Int("frame-ms", 20, "PCM frame size in ms (frames pushed at this cadence to ASR)")
	pace := flag.Bool("pace", false, "pace audio in real-time (sleep frame-ms between sends)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s -in FILE [flags]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if *in == "" {
		flag.Usage()
		os.Exit(2)
	}
	pcm, err := readAudio(*in)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", *in, err)
		os.Exit(1)
	}
	fmt.Printf("[input] file=%s pcm_bytes=%d audio_ms=%d\n", *in, len(pcm), len(pcm)/16)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	asrCli, err := asr.NewViettelClient(ctx, *asrEndpoint, *asrToken)
	if err != nil {
		fmt.Fprintf(os.Stderr, "asr dial: %v\n", err)
		os.Exit(1)
	}
	defer asrCli.Close()

	ttsCli := tts.NewViettelClient(*ttsEndpoint, *ttsKey, *voice, 8000, *tempo)
	botCli := bot.NewHTTPClient(*botURL, 5*time.Second, 25*time.Second)
	vadDet := vad.NewEnergy(vad.Default())

	cfg := pipeline.Defaults()
	cfg.VoiceID = *voice
	cfg.Tempo = *tempo
	p := pipeline.New(*convID, cfg, asrCli, ttsCli, botCli, vadDet)

	src := newPacedSource(pcm, 8000, *frameMs, *pace)

	listenStart := time.Now()
	transcript, err := p.Listen(ctx, src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Listen: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[listen] ms=%d transcript=%q\n",
		time.Since(listenStart).Milliseconds(), transcript)
	if transcript == "" {
		fmt.Fprintln(os.Stderr, "empty transcript — cannot continue")
		os.Exit(1)
	}

	outFile, err := os.Create(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", *out, err)
		os.Exit(1)
	}
	defer outFile.Close()

	speakStart := time.Now()
	// Offline mode has no live audio — pass nil src so barge-in stays disabled.
	action, err := p.Speak(ctx, nil, outFile, transcript)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Speak: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[speak] ms=%d action=%s out=%s\n",
		time.Since(speakStart).Milliseconds(), action, *out)
}

// pacedSource is an AudioSource fed by a fixed PCM buffer.
type pacedSource struct{ ch chan []byte }

func (p *pacedSource) Frames() <-chan []byte { return p.ch }

func newPacedSource(pcm []byte, sampleRate, frameMs int, pace bool) *pacedSource {
	frameBytes := sampleRate * frameMs / 1000 * 2 // S16LE
	if frameBytes <= 0 {
		frameBytes = 320
	}
	ch := make(chan []byte, 8)
	go func() {
		defer close(ch)
		for i := 0; i < len(pcm); i += frameBytes {
			end := i + frameBytes
			if end > len(pcm) {
				end = len(pcm)
			}
			frame := make([]byte, end-i)
			copy(frame, pcm[i:end])
			ch <- frame
			if pace {
				time.Sleep(time.Duration(frameMs) * time.Millisecond)
			}
		}
	}()
	return &pacedSource{ch: ch}
}

// readAudio loads a .wav or .pcm file as raw S16LE 8kHz mono PCM.
func readAudio(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".pcm", ".raw":
		return data, nil
	case ".wav":
		return parseWAV(data)
	default:
		return nil, fmt.Errorf("unsupported extension %q (use .wav or .pcm)", ext)
	}
}

// parseWAV extracts S16LE 8kHz mono PCM from a standard RIFF/WAVE file.
// Tolerates extra chunks between fmt and data.
func parseWAV(data []byte) ([]byte, error) {
	if len(data) < 12 {
		return nil, errors.New("file too short to be WAV")
	}
	if string(data[0:4]) != "RIFF" || string(data[8:12]) != "WAVE" {
		return nil, errors.New("not a RIFF/WAVE file")
	}
	r := bytes.NewReader(data[12:])
	var fmtSeen bool
	var sampleRate uint32
	var bitsPerSample uint16
	var channels uint16
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil, fmt.Errorf("read chunk header: %w", err)
		}
		id := string(hdr[0:4])
		size := binary.LittleEndian.Uint32(hdr[4:8])

		switch id {
		case "fmt ":
			body := make([]byte, size)
			if _, err := io.ReadFull(r, body); err != nil {
				return nil, fmt.Errorf("read fmt: %w", err)
			}
			if size < 16 {
				return nil, fmt.Errorf("fmt chunk too small (%d)", size)
			}
			audioFormat := binary.LittleEndian.Uint16(body[0:2])
			if audioFormat != 1 {
				return nil, fmt.Errorf("only PCM (format=1) supported, got %d", audioFormat)
			}
			channels = binary.LittleEndian.Uint16(body[2:4])
			sampleRate = binary.LittleEndian.Uint32(body[4:8])
			bitsPerSample = binary.LittleEndian.Uint16(body[14:16])
			fmtSeen = true
		case "data":
			if !fmtSeen {
				return nil, errors.New("data chunk before fmt")
			}
			body := make([]byte, size)
			if _, err := io.ReadFull(r, body); err != nil {
				return nil, fmt.Errorf("read data: %w", err)
			}
			if channels != 1 {
				return nil, fmt.Errorf("expected mono, got %d channels", channels)
			}
			if sampleRate != 8000 {
				return nil, fmt.Errorf("expected 8000 Hz, got %d Hz", sampleRate)
			}
			if bitsPerSample != 16 {
				return nil, fmt.Errorf("expected 16-bit, got %d", bitsPerSample)
			}
			return body, nil
		default:
			// Skip unknown chunk (e.g. LIST, INFO).
			if _, err := r.Seek(int64(size), io.SeekCurrent); err != nil {
				return nil, fmt.Errorf("skip chunk %s: %w", id, err)
			}
		}
	}
}

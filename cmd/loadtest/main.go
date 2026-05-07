// Command loadtest drives N concurrent offline pipelines (ASR + Bot + TTS,
// no FreeSWITCH) and reports p50/p95/p99 turn latencies. Validates the
// "p99 turn < 2s" goal from CLAUDE.md without needing live SIP traffic.
//
// Each virtual call:
//   1. Speak greeting (bot greeting → TTS) — measures cold-path
//   2. Optionally Listen with the input audio file → bot turn → TTS
//   3. Repeat -turns times
//
// Usage:
//
//	go run ./cmd/loadtest -ccu 50 -turns 2 \
//	    -in sample.wav \
//	    -bot-url http://localhost:11006/api/v1/call/ \
//	    -tts-key $TTS_KEY -voice thuyanh-north \
//	    -asr-token $ASR_TOKEN
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"callbot-master/internal/asr"
	"callbot-master/internal/bot"
	"callbot-master/internal/pipeline"
	"callbot-master/internal/tts"
	"callbot-master/internal/vad"
)

func main() {
	ccu := flag.Int("ccu", 10, "concurrent virtual calls")
	turns := flag.Int("turns", 2, "turns per call (>=1; turn 0 = greeting)")
	rampMs := flag.Int("ramp-ms", 50, "stagger between call starts (ms) to avoid burst")
	in := flag.String("in", "", "input audio (.wav S16LE 8kHz mono or .pcm); empty → greeting-only")

	asrEndpoint := flag.String("asr-endpoint", "103.253.20.28:9000", "")
	asrToken := flag.String("asr-token", os.Getenv("MASTER_ASR_TOKEN"), "")
	ttsEndpoint := flag.String("tts-endpoint", "ws://103.253.20.27:8767", "")
	ttsKey := flag.String("tts-key", os.Getenv("MASTER_TTS_TOKEN"), "")
	voice := flag.String("voice", os.Getenv("MASTER_TTS_VOICE_ID"), "")
	tempo := flag.Float64("tempo", 1.0, "")
	botURL := flag.String("bot-url", "http://localhost:11006/api/v1/call/", "")

	flag.Parse()

	if *ccu < 1 || *turns < 1 {
		fmt.Fprintln(os.Stderr, "ccu and turns must be >= 1")
		os.Exit(2)
	}

	var inputPCM []byte
	if *in != "" {
		var err error
		inputPCM, err = readAudio(*in)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read input: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("[input] %s pcm_bytes=%d audio_ms=%d\n",
			*in, len(inputPCM), len(inputPCM)/16)
	} else {
		fmt.Println("[input] none — greeting-only test")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	asrClient, err := asr.NewViettelClient(ctx, *asrEndpoint, *asrToken)
	if err != nil {
		fmt.Fprintf(os.Stderr, "asr dial: %v\n", err)
		os.Exit(1)
	}
	defer asrClient.Close()
	ttsClient := tts.NewViettelClient(*ttsEndpoint, *ttsKey, *voice, 8000, *tempo)
	botClient := bot.NewHTTPClient(*botURL, 5*time.Second, 25*time.Second)

	collector := newCollector()
	var wg sync.WaitGroup
	overallStart := time.Now()

	for i := 0; i < *ccu; i++ {
		wg.Add(1)
		go runVirtualCall(ctx, &wg, i, asrClient, ttsClient, botClient,
			inputPCM, *turns, *voice, *tempo, collector)
		// Ramp: stagger the starts
		select {
		case <-time.After(time.Duration(*rampMs) * time.Millisecond):
		case <-ctx.Done():
			break
		}
	}
	wg.Wait()
	overallDur := time.Since(overallStart)

	collector.report(*ccu, *turns, overallDur)
}

// runVirtualCall simulates one call's lifetime with the loadtest collector
// recording per-stage latencies.
func runVirtualCall(
	ctx context.Context, wg *sync.WaitGroup, idx int,
	asrCli *asr.ViettelClient, ttsCli *tts.ViettelClient, botCli *bot.HTTPClient,
	inputPCM []byte, turns int, voice string, tempo float64,
	c *collector,
) {
	defer wg.Done()

	uuid := fmt.Sprintf("loadtest-%d-%d", time.Now().UnixNano(), idx)
	cfg := pipeline.Defaults()
	cfg.VoiceID = voice
	cfg.Tempo = tempo

	p := pipeline.New(uuid, cfg, asrCli, ttsCli, botCli, vad.NewEnergy(vad.Default()))
	p.Scenario = "loadtest"

	for turn := 0; turn < turns; turn++ {
		callStart := time.Now()
		var src pipeline.AudioSource
		if turn > 0 && len(inputPCM) > 0 {
			src = newReplaySource(inputPCM, cfg.SampleRate*20/1000*2)
		}

		// Drain TTS to /dev/null so we measure pipeline-bound time.
		sink := io.Discard.(io.Writer)

		_, err := p.RunTurn(ctx, src, sink)
		dur := time.Since(callStart)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			c.recordError(turn, err)
			return
		}
		c.recordTurn(turn, dur)
	}
}

// --- collector ---------------------------------------------------------------

type collector struct {
	mu      sync.Mutex
	turns   map[int][]time.Duration // turn idx → latencies
	errs    atomic.Int64
	errSeen sync.Map // string → struct{}
}

func newCollector() *collector {
	return &collector{turns: map[int][]time.Duration{}}
}

func (c *collector) recordTurn(turnIdx int, d time.Duration) {
	c.mu.Lock()
	c.turns[turnIdx] = append(c.turns[turnIdx], d)
	c.mu.Unlock()
}

func (c *collector) recordError(turnIdx int, err error) {
	c.errs.Add(1)
	c.errSeen.Store(err.Error(), struct{}{})
}

func (c *collector) report(ccu, turns int, overallDur time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Printf("\n=== loadtest report: ccu=%d turns=%d wall=%dms ===\n",
		ccu, turns, overallDur.Milliseconds())
	if c.errs.Load() > 0 {
		fmt.Printf("errors: %d\n", c.errs.Load())
		c.errSeen.Range(func(k, _ any) bool {
			fmt.Printf("  • %s\n", k)
			return true
		})
	}
	fmt.Printf("\n%-12s %6s %8s %8s %8s %8s %8s %8s\n",
		"turn", "n", "p50", "p75", "p90", "p95", "p99", "max")
	for i := 0; i < turns; i++ {
		ds := c.turns[i]
		if len(ds) == 0 {
			fmt.Printf("turn-%-7d %6d (no samples)\n", i, 0)
			continue
		}
		sort.Slice(ds, func(a, b int) bool { return ds[a] < ds[b] })
		fmt.Printf("turn-%-7d %6d %8s %8s %8s %8s %8s %8s\n",
			i, len(ds),
			fmtMs(pct(ds, 0.50)),
			fmtMs(pct(ds, 0.75)),
			fmtMs(pct(ds, 0.90)),
			fmtMs(pct(ds, 0.95)),
			fmtMs(pct(ds, 0.99)),
			fmtMs(ds[len(ds)-1]),
		)
	}

	// Aggregate across all turns
	var all []time.Duration
	for _, ds := range c.turns {
		all = append(all, ds...)
	}
	if len(all) > 0 {
		sort.Slice(all, func(a, b int) bool { return all[a] < all[b] })
		fmt.Printf("\n%-12s %6d %8s %8s %8s %8s %8s %8s\n",
			"all", len(all),
			fmtMs(pct(all, 0.50)),
			fmtMs(pct(all, 0.75)),
			fmtMs(pct(all, 0.90)),
			fmtMs(pct(all, 0.95)),
			fmtMs(pct(all, 0.99)),
			fmtMs(all[len(all)-1]),
		)
		// Pass/fail vs CLAUDE.md goal
		p99 := pct(all, 0.99)
		if p99 < 2*time.Second {
			fmt.Printf("\n✓ p99 = %s < 2s (goal met)\n", fmtMs(p99))
		} else {
			fmt.Printf("\n✗ p99 = %s ≥ 2s (goal MISSED)\n", fmtMs(p99))
		}
	}
}

// pct returns the latency at the given percentile (0..1) using
// nearest-rank. Input must already be sorted ascending.
func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func fmtMs(d time.Duration) string {
	return fmt.Sprintf("%dms", d.Milliseconds())
}

// --- audio source (memory) ---------------------------------------------------

type memSource struct{ ch chan []byte }

func (m *memSource) Frames() <-chan []byte { return m.ch }

func newReplaySource(pcm []byte, frameBytes int) *memSource {
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
			// Tiny jitter so Listen sees realistic-ish frame cadence
			// without tying total time to file length.
			time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond)
		}
	}()
	return &memSource{ch: ch}
}

// --- WAV reader -------------------------------------------------------------

func readAudio(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	switch ext := strings.ToLower(filepath.Ext(path)); ext {
	case ".pcm", ".raw":
		return data, nil
	case ".wav":
		return parseWAV(data)
	default:
		return nil, fmt.Errorf("unsupported %s", ext)
	}
}

func parseWAV(data []byte) ([]byte, error) {
	if len(data) < 12 || string(data[0:4]) != "RIFF" || string(data[8:12]) != "WAVE" {
		return nil, errors.New("not a RIFF/WAVE file")
	}
	r := bytes.NewReader(data[12:])
	var sampleRate uint32
	var channels uint16
	var bps uint16
	var fmtSeen bool
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil, fmt.Errorf("chunk header: %w", err)
		}
		id := string(hdr[0:4])
		size := binary.LittleEndian.Uint32(hdr[4:8])
		switch id {
		case "fmt ":
			body := make([]byte, size)
			if _, err := io.ReadFull(r, body); err != nil {
				return nil, err
			}
			if size < 16 || binary.LittleEndian.Uint16(body[0:2]) != 1 {
				return nil, errors.New("only PCM supported")
			}
			channels = binary.LittleEndian.Uint16(body[2:4])
			sampleRate = binary.LittleEndian.Uint32(body[4:8])
			bps = binary.LittleEndian.Uint16(body[14:16])
			fmtSeen = true
		case "data":
			if !fmtSeen {
				return nil, errors.New("data before fmt")
			}
			body := make([]byte, size)
			if _, err := io.ReadFull(r, body); err != nil {
				return nil, err
			}
			if channels != 1 || sampleRate != 8000 || bps != 16 {
				return nil, fmt.Errorf("expected 8kHz mono S16LE, got %d Hz / %d ch / %d bit",
					sampleRate, channels, bps)
			}
			return body, nil
		default:
			if _, err := r.Seek(int64(size), io.SeekCurrent); err != nil {
				return nil, err
			}
		}
	}
}

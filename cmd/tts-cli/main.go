// Command tts-cli streams text through Viettel TTS and writes the resulting
// PCM (S16LE 8kHz mono) to a file. Verify with:
//
//	ffplay -f s16le -ar 8000 -ac 1 out.pcm
//
// Splits the input on sentence delimiters (. ? ! …) so each chunk is sent
// as a separate WS message, exercising streaming-text-input path.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"callbot-master/internal/tts"
)

func main() {
	endpoint := flag.String("endpoint", "ws://103.253.20.27:8767", "Viettel TTS WS endpoint")
	apiKey := flag.String("api-key", os.Getenv("MASTER_TTS_TOKEN"), "TTS api key (env MASTER_TTS_TOKEN)")
	voice := flag.String("voice", os.Getenv("MASTER_TTS_VOICE_ID"), "voiceId (env MASTER_TTS_VOICE_ID)")
	tempo := flag.Float64("tempo", 1.0, "tempo multiplier")
	out := flag.String("out", "out.pcm", "output PCM file")
	convID := flag.String("conv", "tts-cli-001", "conversation_id")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] <text...>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	text := strings.Join(args, " ")
	sentences := splitSentences(text)
	if len(sentences) == 0 {
		sentences = []string{text}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := tts.NewViettelClient(*endpoint, *apiKey, *voice, 8000, *tempo)
	start := time.Now()
	stream, err := client.StartStream(ctx, tts.StreamOpts{
		ConversationID: *convID,
		VoiceID:        *voice,
		ResampleRate:   8000,
		Tempo:          *tempo,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start stream: %v\n", err)
		os.Exit(1)
	}
	defer stream.Close()
	fmt.Printf("[auth] %dms\n", time.Since(start).Milliseconds())

	// Push texts in a goroutine so we can drain audio in parallel.
	pushErr := make(chan error, 1)
	go func() {
		defer close(pushErr)
		for i, s := range sentences {
			eos := i == len(sentences)-1
			if err := stream.SendText(s, eos); err != nil {
				pushErr <- fmt.Errorf("send %q: %w", s, err)
				return
			}
			fmt.Printf("[sent %d/%d eos=%v] %s\n", i+1, len(sentences), eos, s)
		}
	}()

	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", *out, err)
		os.Exit(1)
	}
	defer f.Close()

	var total int
	firstFrameAt := time.Duration(0)
	for frame := range stream.AudioChan() {
		if firstFrameAt == 0 {
			firstFrameAt = time.Since(start)
		}
		if _, err := f.Write(frame); err != nil {
			fmt.Fprintf(os.Stderr, "write file: %v\n", err)
			os.Exit(1)
		}
		total += len(frame)
	}
	if err := <-pushErr; err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	durMs := total / 16 // 8000Hz * 2 bytes = 16 bytes/ms
	fmt.Printf("[done] bytes=%d audio_ms=%d first_frame=%dms total_wall=%dms file=%s\n",
		total, durMs, firstFrameAt.Milliseconds(), time.Since(start).Milliseconds(), *out)
}

// splitSentences splits on sentence delimiters and trims whitespace.
// Empty segments are dropped.
func splitSentences(s string) []string {
	var out []string
	var cur strings.Builder
	flush := func() {
		seg := strings.TrimSpace(cur.String())
		if seg != "" {
			out = append(out, seg)
		}
		cur.Reset()
	}
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		i += size
		cur.WriteRune(r)
		if isDelim(r) {
			flush()
		}
	}
	flush()
	return out
}

func isDelim(r rune) bool {
	switch r {
	case '.', '?', '!', '…', '\n':
		return true
	}
	return false
}

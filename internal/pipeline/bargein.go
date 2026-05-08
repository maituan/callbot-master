package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"callbot-master/internal/asr"
	"callbot-master/internal/audio"
)

// bargeInMonitor watches the caller's audio during a Speak turn by streaming
// it to an ASR session in parallel with the bot/TTS path. It triggers
// barge-in once the running transcript reaches a word threshold (default 3).
//
// Why ASR-driven instead of energy VAD: telephony audio (a-law decompressed,
// handset echo, ambient noise) makes raw RMS thresholding unreliable. ASR
// has linguistic awareness and is what we already trust to drive Listen's
// end-of-utterance — using the same provider for barge-in keeps a single
// detection contract.
//
// Side benefit: the captured transcript is reused as the next turn's user
// input, so a barge-in doesn't require another Listen+ASR round-trip.
type bargeInMonitor struct {
	uuid     string
	minWords int
	src      audio.Source

	stream asr.Stream

	triggered  atomic.Bool
	triggerCh  chan struct{}
	transcript atomic.Value // string — partial-at-trigger; replaced by final when it arrives

	// finalCh emits the ASR is_final transcript after barge-in fires.
	// The recv goroutine keeps reading the same ASR stream past the
	// trigger threshold so the bot's next turn gets a complete
	// transcript instead of the 3-word stub that tripped the trigger.
	// Receivers should pair this with a timeout — caller may keep
	// talking long after barge-in.
	finalCh chan string

	stopOnce sync.Once
	stopCh   chan struct{}
	done     chan struct{}
}

// newBargeInMonitor opens an ASR session bound to ctx and spawns the
// audio-pump + transcript-watcher goroutines. Caller must invoke stop()
// before discarding the monitor.
//
// minWords <=0 falls back to a sensible default (3) — short enough to feel
// responsive, long enough to ignore monosyllabic noise like "ờ" / "à".
func newBargeInMonitor(
	ctx context.Context,
	uuid string,
	asrCli asr.Client,
	src audio.Source,
	minWords int,
	cfg Config,
) (*bargeInMonitor, error) {
	if minWords <= 0 {
		minWords = 3
	}
	rate := cfg.SampleRate
	if rate == 0 {
		rate = 8000
	}
	stream, err := asrCli.StartStream(ctx, asr.StreamOpts{
		ConversationID: uuid + "-barge",
		SampleRate:     rate,
		Channels:       1,
		SingleSentence: cfg.ASRSingleSentence,
		// Stretch the silence floor so this session doesn't auto-finalize
		// while the caller is just listening to the bot. We rely on the
		// stop() call (Speak's defer) to close the stream when bot finishes.
		SilenceTimeoutMs: 60_000,
		SpeechTimeoutMs:  int(cfg.ASRSpeechTimeout / time.Millisecond),
		SpeechMaxMs:      int(cfg.ASRSpeechMax / time.Millisecond),
	})
	if err != nil {
		return nil, err
	}
	m := &bargeInMonitor{
		uuid:      uuid,
		minWords:  minWords,
		src:       src,
		stream:    stream,
		triggerCh: make(chan struct{}),
		finalCh:   make(chan string, 1), // buffered so emitter doesn't block on slow consumer
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
	go m.run(ctx)
	return m, nil
}

func (m *bargeInMonitor) run(ctx context.Context) {
	defer close(m.done)
	var wg sync.WaitGroup

	// Audio pump: drain caller frames into the barge-in ASR session. When
	// the caller is silent ASR receives silence frames; that's fine — we
	// only act on transcripts.
	wg.Add(1)
	go func() {
		defer wg.Done()
		frames := m.src.Frames()
		for {
			select {
			case <-m.stopCh:
				return
			case <-ctx.Done():
				return
			case frame, ok := <-frames:
				if !ok {
					return
				}
				if err := m.stream.SendAudio(frame); err != nil {
					if !errors.Is(err, io.EOF) && !m.triggered.Load() {
						slog.Debug("bargein send audio",
							"call_uuid", m.uuid, "err", err)
					}
					return
				}
			}
		}
	}()

	// Transcript watcher runs in two phases:
	//   1. Pre-trigger: fires triggerCh as soon as a partial reaches the
	//      word threshold. Captures that partial as a fallback transcript.
	//   2. Post-trigger: keeps reading the same ASR stream until is_final
	//      arrives, then emits the complete transcript on finalCh. This
	//      lets Speak's caller wait for the full sentence — critical now
	//      that we POST to bot only after final, not at trigger time.
	wg.Add(1)
	go func() {
		defer wg.Done()
		var lastText string
		for res := range m.stream.Recv() {
			text := strings.TrimSpace(res.Text)
			if text != "" {
				lastText = text
			}
			// Phase 1 — trigger as soon as the running partial passes the threshold.
			if !m.triggered.Load() {
				if text == "" || countWords(text) < m.minWords {
					continue
				}
				if m.triggered.CompareAndSwap(false, true) {
					m.transcript.Store(text)
					slog.Info("bargein triggered",
						"call_uuid", m.uuid,
						"words", countWords(text),
						"text", text)
					close(m.triggerCh)
				}
				// Don't return — fall through to phase 2 to keep reading.
			}
			// Phase 2 — once is_final arrives, emit the complete text and exit.
			if res.IsFinal {
				final := strings.TrimSpace(res.Text)
				if final == "" {
					// ASR finalised with empty text (silence detected after
					// trigger) — keep the partial-at-trigger as fallback.
					final = lastText
				}
				m.transcript.Store(final)
				select {
				case m.finalCh <- final:
				default:
				}
				return
			}
		}
		// Stream closed without final (cancelled, error). If we already
		// triggered, surface the latest text as the final result so the
		// caller doesn't block on finalCh forever.
		if m.triggered.Load() && lastText != "" {
			select {
			case m.finalCh <- lastText:
			default:
			}
		}
	}()

	wg.Wait()
}

// stop tears down the ASR stream + waits for the goroutines to exit.
// Idempotent.
func (m *bargeInMonitor) stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
		_ = m.stream.Close()
	})
	<-m.done
}

func (m *bargeInMonitor) triggerChan() <-chan struct{} { return m.triggerCh }

func (m *bargeInMonitor) capturedTranscript() string {
	if v := m.transcript.Load(); v != nil {
		return v.(string)
	}
	return ""
}

// waitForFinal blocks until the post-trigger watcher emits the final
// transcript or the timeout fires. On timeout the partial-at-trigger
// is returned so the call can still progress — caller is presumably
// still talking and we'd rather POST a slightly-stale transcript than
// hang forever.
//
// Should only be called after the trigger has fired (waitForFinal
// without a trigger blocks until stop).
func (m *bargeInMonitor) waitForFinal(timeout time.Duration) string {
	select {
	case t := <-m.finalCh:
		return t
	case <-time.After(timeout):
		return m.capturedTranscript()
	}
}

// bargeTriggerChan returns m.triggerChan() if m is non-nil, else a nil
// channel (which select treats as "never fires").
func bargeTriggerChan(m *bargeInMonitor) <-chan struct{} {
	if m == nil {
		return nil
	}
	return m.triggerChan()
}

// countWords counts whitespace-separated tokens in s. Works for Vietnamese
// (which is space-separated) and most Latin-script languages. Punctuation
// inside a word is ignored.
func countWords(s string) int {
	return len(strings.Fields(s))
}

package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"callbot-master/internal/audio"
	"callbot-master/internal/vad"
)

// bargeInMonitor watches audio frames during a Speak turn and signals when
// the caller starts talking. Only used when barge-in is enabled and a src
// is wired to Speak.
//
// Behavior:
//   - Sets VAD.BotSpeaking(true) at start (raised threshold so TTS echo
//     doesn't false-trigger).
//   - Reads frames from src, pushes through VAD, and retains the most
//     recent ~2 seconds in a ring buffer.
//   - On EventSpeechStart, closes TriggeredCh exactly once and stops
//     consuming src (next Listen will own src again).
//
// The monitor does NOT cancel the bot/TTS — that's the Speak loop's job
// after observing TriggeredCh. Keeps the cancellation chain explicit.
type bargeInMonitor struct {
	uuid    string
	vad     vad.Detector
	src     audio.Source
	ring    *audio.RingBuf

	triggered  atomic.Bool
	triggerCh  chan struct{}
	stopCh     chan struct{}
	stopOnce   sync.Once
	doneCh     chan struct{}
}

// newBargeInMonitor sizes the ring buffer to retain replayCapBytes of recent
// audio (recommend ~2 s of S16LE 8kHz = 32000 bytes).
func newBargeInMonitor(uuid string, v vad.Detector, src audio.Source, replayCapBytes int) *bargeInMonitor {
	if replayCapBytes <= 0 {
		replayCapBytes = 32000
	}
	return &bargeInMonitor{
		uuid:      uuid,
		vad:       v,
		src:       src,
		ring:      audio.NewRingBuf(replayCapBytes),
		triggerCh: make(chan struct{}),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// start spawns the watcher goroutine. ctx cancellation also stops it.
func (m *bargeInMonitor) start(ctx context.Context) {
	if m.vad != nil {
		m.vad.Reset()
		m.vad.SetBotSpeaking(true)
	}
	go m.run(ctx)
}

// stop signals the monitor to exit and waits for the goroutine to finish.
// Idempotent.
func (m *bargeInMonitor) stop() {
	m.stopOnce.Do(func() { close(m.stopCh) })
	<-m.doneCh
	if m.vad != nil {
		m.vad.SetBotSpeaking(false)
	}
}

// triggered returns the closed-on-fire channel; receiving = barge-in detected.
func (m *bargeInMonitor) triggerChan() <-chan struct{} { return m.triggerCh }

// fired reports whether a barge-in was detected.
func (m *bargeInMonitor) fired() bool { return m.triggered.Load() }

// snapshot returns a copy of the ring buffer's contents (oldest → newest).
// Call after stop() so the read is final.
func (m *bargeInMonitor) snapshot() []byte { return m.ring.Snapshot() }

// bargeTriggerChan returns m.triggerChan() if m is non-nil, else a nil
// channel (which select treats as "never fires"). Lets Speak's select
// statement compile without explicit nil checks.
func bargeTriggerChan(m *bargeInMonitor) <-chan struct{} {
	if m == nil {
		return nil
	}
	return m.triggerChan()
}

func (m *bargeInMonitor) run(ctx context.Context) {
	defer close(m.doneCh)
	frames := m.src.Frames()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case frame, ok := <-frames:
			if !ok {
				return
			}
			m.ring.Write(frame)
			if m.vad == nil {
				continue
			}
			if e := m.vad.Push(frame); e == vad.EventSpeechStart {
				if m.triggered.CompareAndSwap(false, true) {
					slog.Info("bargein triggered", "call_uuid", m.uuid)
					close(m.triggerCh)
				}
				return
			}
		}
	}
}

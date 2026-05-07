// Package vad defines voice-activity-detection contract.
// Default impl (energy + min-duration thresholds) lives in timeout.go (Phase 3).
package vad

// Event reports speech state transitions detected from audio frames.
type Event int

const (
	EventNone Event = iota
	EventSpeechStart
	EventSpeechEnd
)

// Detector consumes PCM S16LE 8kHz frames and emits transitions.
//
// SetBotSpeaking is used to suppress false triggers caused by TTS audio
// echoing back through the channel — when true, the detector should raise
// its sensitivity threshold rather than disabling itself entirely (we still
// want to detect barge-in).
type Detector interface {
	Push(pcm []byte) Event
	SetBotSpeaking(bool)
	Reset()
}

// MockDetector is a stub used in Phase 0 wiring + tests.
type MockDetector struct {
	Next      Event // returned by Push regardless of input
	BotSpeaks bool
	Resets    int
	Frames    int
}

func (m *MockDetector) Push(pcm []byte) Event {
	m.Frames++
	e := m.Next
	m.Next = EventNone
	return e
}

func (m *MockDetector) SetBotSpeaking(b bool) { m.BotSpeaks = b }

func (m *MockDetector) Reset() { m.Resets++ }

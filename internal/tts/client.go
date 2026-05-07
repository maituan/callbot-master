// Package tts defines the abstract text-to-speech streaming contract.
// Default impl (Viettel WS) lives in viettel_ws.go (Phase 2).
package tts

import "context"

// StreamOpts carries per-turn TTS config. VoiceID/ResampleRate/Tempo map to
// the Viettel voice_settings; other providers can ignore unknown fields.
type StreamOpts struct {
	ConversationID string
	VoiceID        string
	ResampleRate   int
	Tempo          float64
}

// Client opens a new TTS stream per turn (one stream = one bot response).
type Client interface {
	StartStream(ctx context.Context, opts StreamOpts) (Stream, error)
}

// Stream pushes text in (sentence by sentence), receives PCM S16LE 8kHz
// frames out via AudioChan. Set eos=true on the last SendText call.
// Close stops generation early and releases resources.
type Stream interface {
	SendText(s string, eos bool) error
	AudioChan() <-chan []byte
	Close() error
}

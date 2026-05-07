// Package asr defines the abstract speech-to-text streaming contract.
// Default impl (Viettel gRPC) lives in viettel_grpc.go (Phase 3).
package asr

import "context"

// StreamOpts carries per-call ASR config. ConversationID = FS call uuid.
type StreamOpts struct {
	ConversationID string
	SampleRate     int  // 8000
	Channels       int  // 1
	SingleSentence bool // hint to provider for utterance segmentation
	// Provider-specific timeouts (ms).
	SilenceTimeoutMs int
	SpeechTimeoutMs  int
	SpeechMaxMs      int
}

// Result is a partial or final transcript chunk.
type Result struct {
	Text    string
	IsFinal bool
}

// Client opens a new bidirectional ASR stream per call/turn.
type Client interface {
	StartStream(ctx context.Context, opts StreamOpts) (Stream, error)
}

// Stream pushes PCM S16LE 8kHz frames in, receives transcript Results out.
// Close releases provider resources; safe to call multiple times.
type Stream interface {
	SendAudio(pcm []byte) error
	Recv() <-chan Result
	Close() error
}

package asr

import (
	"context"
	"sync"
)

// MockClient is a no-op impl used by tests and the Phase 0 skeleton wiring.
type MockClient struct {
	OnStart func(opts StreamOpts) (Stream, error)
}

func (m *MockClient) StartStream(_ context.Context, opts StreamOpts) (Stream, error) {
	if m.OnStart != nil {
		return m.OnStart(opts)
	}
	return &MockStream{out: make(chan Result, 4)}, nil
}

type MockStream struct {
	mu          sync.Mutex
	out         chan Result
	closed      bool
	sendClosed  bool
	OnSend      func([]byte) error
	Pushed      [][]byte
	CloseSentAt int // monotonic counter of CloseSend calls; 0 = never
}

func (s *MockStream) SendAudio(pcm []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.Pushed = append(s.Pushed, pcm)
	if s.OnSend != nil {
		return s.OnSend(pcm)
	}
	return nil
}

func (s *MockStream) Recv() <-chan Result { return s.out }

// CloseSend half-closes the send side. The recv channel stays open so the
// test can keep emitting (mirrors gRPC client CloseSend semantics).
func (s *MockStream) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendClosed = true
	s.CloseSentAt++
	return nil
}

// Emit is a test helper to push a fake transcript result.
func (s *MockStream) Emit(r Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.out <- r
}

func (s *MockStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.out)
	return nil
}

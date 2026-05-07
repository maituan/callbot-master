package tts

import (
	"context"
	"sync"
)

type MockClient struct {
	OnStart func(opts StreamOpts) (Stream, error)
}

func (m *MockClient) StartStream(_ context.Context, opts StreamOpts) (Stream, error) {
	if m.OnStart != nil {
		return m.OnStart(opts)
	}
	return &MockStream{audio: make(chan []byte, 16)}, nil
}

type MockStream struct {
	mu     sync.Mutex
	audio  chan []byte
	closed bool
	Texts  []string
}

func (s *MockStream) SendText(text string, _ bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.Texts = append(s.Texts, text)
	return nil
}

func (s *MockStream) AudioChan() <-chan []byte { return s.audio }

// PushAudio is a test helper to feed fake PCM frames to the consumer.
func (s *MockStream) PushAudio(frame []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.audio <- frame
}

func (s *MockStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.audio)
	return nil
}

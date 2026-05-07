package bot

import (
	"context"
	"sync"
)

type MockClient struct {
	OnStream func(ctx context.Context, conversationID, message string) (TurnStream, error)
}

func (m *MockClient) Stream(ctx context.Context, conversationID, message string) (TurnStream, error) {
	if m.OnStream != nil {
		ts, err := m.OnStream(ctx, conversationID, message)
		if err == nil {
			watchCtx(ctx, ts)
		}
		return ts, err
	}
	ts := &MockTurnStream{
		sentences: make(chan string, 4),
		done:      make(chan struct{}),
		action:    ActionChat,
	}
	watchCtx(ctx, ts)
	return ts, nil
}

// watchCtx mirrors production HTTP-stream behavior: when the request ctx
// cancels, the stream closes — letting tests exercise barge-in's cancelBot
// path without manually calling ts.Close().
func watchCtx(ctx context.Context, ts TurnStream) {
	if ctx == nil {
		return
	}
	go func() {
		<-ctx.Done()
		_ = ts.Close()
	}()
}

type MockTurnStream struct {
	mu        sync.Mutex
	sentences chan string
	done      chan struct{} // closed by Close — mirrors HTTP stream EOF
	closed    bool
	action    Action
	err       error
}

func (s *MockTurnStream) Sentences() <-chan string { return s.sentences }

// Action blocks until Close, matching the HTTP stream's "wait for EOF" semantics.
// Tests that don't need that contract should call Close immediately after Emit.
func (s *MockTurnStream) Action() (Action, error) {
	if s.done != nil {
		<-s.done
	}
	return s.action, s.err
}

// EmitSentence is a test helper. Emit*("") + close to terminate.
func (s *MockTurnStream) EmitSentence(text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.sentences <- text
}

// SetAction sets what Action() returns once the stream is closed.
func (s *MockTurnStream) SetAction(a Action) { s.action = a }

func (s *MockTurnStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.sentences)
	if s.done != nil {
		close(s.done)
	}
	return nil
}

// Package session models a single in-flight call.
// One Session = one FS call uuid = one bot conversation_id.
package session

import (
	"context"
	"sync/atomic"
	"time"

	"callbot-master/internal/asr"
	"callbot-master/internal/bot"
	"callbot-master/internal/freeswitch"
	"callbot-master/internal/tts"
	"callbot-master/internal/vad"
)

// State enumerates the FSM states for one call.
//
//	IDLE → GREETING → LISTENING → THINKING → SPEAKING → LISTENING → ... → ENDED
//	                            ▲                  │
//	                            └── BARGE_IN ──────┘
type State int

const (
	StateIdle State = iota
	StateGreeting
	StateListening
	StateThinking
	StateSpeaking
	StateBargeIn
	StateEnded
)

func (s State) String() string {
	switch s {
	case StateIdle:
		return "IDLE"
	case StateGreeting:
		return "GREETING"
	case StateListening:
		return "LISTENING"
	case StateThinking:
		return "THINKING"
	case StateSpeaking:
		return "SPEAKING"
	case StateBargeIn:
		return "BARGE_IN"
	case StateEnded:
		return "ENDED"
	}
	return "UNKNOWN"
}

type Direction int

const (
	DirectionInbound Direction = iota
	DirectionOutbound
)

func (d Direction) String() string {
	if d == DirectionOutbound {
		return "outbound"
	}
	return "inbound"
}

// Turn is one user message + bot response, persisted at end-of-call.
type Turn struct {
	UserText  string
	BotText   string
	Action    bot.Action
	StartedAt time.Time
	EndedAt   time.Time
}

// Session holds per-call state and references to provider clients.
// All fields except State and History are immutable after Init.
type Session struct {
	UUID      string
	Direction Direction
	Scenario  string
	StartedAt time.Time

	state   atomic.Int32 // holds State
	history []Turn       // appended only by the session goroutine

	ctx    context.Context
	cancel context.CancelFunc

	// Injected providers — interfaces, not impls.
	ASR asr.Client
	TTS tts.Client
	Bot bot.Client
	VAD vad.Detector
	FS  *freeswitch.EventSocket
}

// New constructs a Session with the given root context. The caller owns
// cancellation; teardown via Cancel() will stop all sub-goroutines.
func New(parent context.Context, uuid, scenario string, dir Direction) *Session {
	ctx, cancel := context.WithCancel(parent)
	s := &Session{
		UUID:      uuid,
		Direction: dir,
		Scenario:  scenario,
		StartedAt: time.Now(),
		ctx:       ctx,
		cancel:    cancel,
	}
	s.state.Store(int32(StateIdle))
	return s
}

func (s *Session) Context() context.Context { return s.ctx }

func (s *Session) Cancel() { s.cancel() }

func (s *Session) State() State { return State(s.state.Load()) }

// Transition swaps state to next; returns the previous state.
func (s *Session) Transition(next State) State {
	return State(s.state.Swap(int32(next)))
}

func (s *Session) AppendTurn(t Turn) { s.history = append(s.history, t) }

func (s *Session) History() []Turn { return s.history }

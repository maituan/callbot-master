package session

import (
	"context"
	"sync"
	"time"
)

// Manager tracks active sessions by uuid.
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewManager() *Manager {
	return &Manager{sessions: make(map[string]*Session)}
}

func (m *Manager) Add(s *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[s.UUID] = s
}

func (m *Manager) Get(uuid string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[uuid]
	return s, ok
}

func (m *Manager) Remove(uuid string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, uuid)
}

// Count returns the number of active sessions, optionally filtered by direction.
// Pass -1 for "any".
func (m *Manager) Count(dir int) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if dir < 0 {
		return len(m.sessions)
	}
	n := 0
	for _, s := range m.sessions {
		if int(s.Direction) == dir {
			n++
		}
	}
	return n
}

// DrainAll cancels every session and waits up to timeout for them to clear.
// Returns true if all sessions ended; false on timeout (caller should force-exit).
func (m *Manager) DrainAll(timeout time.Duration) bool {
	m.mu.RLock()
	for _, s := range m.sessions {
		s.Cancel()
	}
	m.mu.RUnlock()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		m.mu.RLock()
		n := len(m.sessions)
		m.mu.RUnlock()
		if n == 0 {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// WithRoot exposes the manager's root context for child wiring (currently
// unused but kept for symmetry with future graceful-shutdown integration).
func (m *Manager) WithRoot(_ context.Context) {}

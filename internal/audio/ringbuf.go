// Package audio provides FIFO I/O and ring-buffer helpers for the audio path.
// Phase 0 ships only the ring buffer skeleton; FIFO orchestration arrives in Phase 4.
package audio

import "sync"

// RingBuf is a fixed-capacity byte ring used to retain the most-recent
// caller PCM so a barge-in can replay the missed audio into a fresh ASR
// stream. Writes never block; old bytes are overwritten when full.
//
// Thread-safe for one writer + many readers (Snapshot only).
type RingBuf struct {
	mu   sync.Mutex
	buf  []byte
	head int  // next write index
	full bool // true once we've wrapped at least once
}

// NewRingBuf allocates a ring of capacity bytes. Sample size is the caller's
// concern — for 8kHz S16LE mono, 2s ≈ 32000 bytes.
func NewRingBuf(capacity int) *RingBuf {
	return &RingBuf{buf: make([]byte, capacity)}
}

// Write copies p into the ring, overwriting the oldest bytes if needed.
func (r *RingBuf) Write(p []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for len(p) > 0 {
		n := copy(r.buf[r.head:], p)
		r.head += n
		if r.head >= len(r.buf) {
			r.head = 0
			r.full = true
		}
		p = p[n:]
	}
}

// Snapshot returns a copy of the current contents in chronological order
// (oldest → newest). Returns nil if the ring has never been written.
func (r *RingBuf) Snapshot() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.full && r.head == 0 {
		return nil
	}
	if !r.full {
		out := make([]byte, r.head)
		copy(out, r.buf[:r.head])
		return out
	}
	out := make([]byte, len(r.buf))
	n := copy(out, r.buf[r.head:])
	copy(out[n:], r.buf[:r.head])
	return out
}

// Reset clears the ring without freeing the underlying buffer.
func (r *RingBuf) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.head = 0
	r.full = false
	for i := range r.buf {
		r.buf[i] = 0
	}
}

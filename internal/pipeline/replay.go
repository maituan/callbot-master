package pipeline

import (
	"callbot-master/internal/audio"
)

// replaySource emits a leading PCM blob (chunked into frames) before
// forwarding the underlying audio.Source. Used after barge-in so the next
// Listen receives the audio captured during the bot's speaking phase
// before reading new frames from the FIFO.
type replaySource struct {
	out chan []byte
}

func newReplaySource(replay []byte, src audio.Source, frameBytes int) *replaySource {
	if frameBytes <= 0 {
		frameBytes = 320
	}
	rs := &replaySource{out: make(chan []byte, 8)}
	go rs.run(replay, src, frameBytes)
	return rs
}

func (r *replaySource) Frames() <-chan []byte { return r.out }

func (r *replaySource) run(replay []byte, src audio.Source, frameBytes int) {
	defer close(r.out)

	// Emit replay in frame-sized chunks so consumer receives the same
	// cadence it would from FIFOSource.
	for i := 0; i < len(replay); i += frameBytes {
		end := i + frameBytes
		if end > len(replay) {
			end = len(replay)
		}
		frame := make([]byte, end-i)
		copy(frame, replay[i:end])
		r.out <- frame
	}

	// Forward the rest from the underlying source.
	if src == nil {
		return
	}
	for f := range src.Frames() {
		r.out <- f
	}
}

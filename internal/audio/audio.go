// Package audio holds audio I/O contracts and FIFO helpers shared by
// pipeline + freeswitch wiring. Interfaces live here (consumed by pipeline)
// to keep the dependency graph one-way: pipeline → audio.
package audio

import "io"

// Source yields PCM S16LE 8kHz frames. Closing the channel signals EOF
// (e.g. WAV exhausted, FIFO peer closed). Buffer size is the source's choice.
type Source interface {
	Frames() <-chan []byte
}

// Sink receives TTS PCM frames in order. Errors are non-fatal — the
// pipeline logs and stops draining on the first error.
type Sink interface {
	io.Writer
}

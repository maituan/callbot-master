package audio

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
)

// FIFOSource adapts a recording FIFO into audio.Source. A reader goroutine
// pulls fixed-size frames from the FIFO and forwards them to the frames
// channel; the channel closes on EOF, error, or ctx cancellation.
type FIFOSource struct {
	uuid      string
	frames    chan []byte
	frameSize int
	f         *os.File // kept so Close can interrupt a blocked Read
	stop      context.CancelFunc
	done      chan struct{}
	closed    sync.Once
}

var _ Source = (*FIFOSource)(nil)

// NewFIFOSource opens path for read and starts a goroutine pumping frames.
// frameBytes is the chunk size handed to the pipeline; 320 bytes = 20ms @
// 8kHz S16LE which matches Viettel ASR's preferred cadence.
//
// The returned source is bound to ctx — cancelling ctx stops the reader.
// Caller must drain frames or call Close to avoid blocking the reader.
func NewFIFOSource(ctx context.Context, uuid, path string, frameBytes int) (*FIFOSource, error) {
	f, err := OpenFIFOForRead(path)
	if err != nil {
		return nil, err
	}
	if frameBytes <= 0 {
		frameBytes = 320
	}

	readCtx, cancel := context.WithCancel(ctx)
	src := &FIFOSource{
		uuid:      uuid,
		frames:    make(chan []byte, 8),
		frameSize: frameBytes,
		f:         f,
		stop:      cancel,
		done:      make(chan struct{}),
	}

	go src.read(readCtx, path)
	return src, nil
}

func (s *FIFOSource) Frames() <-chan []byte { return s.frames }

// Close stops the reader and waits for it to exit. Safe to call multiple
// times.
//
// The file descriptor is closed *before* waiting on the done channel: the
// recording FIFO is opened O_RDWR (so the initial open never blocks),
// which means a plain ctx cancel can't unblock a Read syscall already in
// flight — we hold a writer refcount of our own, so the kernel never
// surfaces EOF when FreeSWITCH closes its write end. Closing the fd here
// turns the in-flight Read into an "use of closed file" error, the read
// goroutine exits, and the deferred chain in SessionRunner.Run continues
// past us. Without this, the call would leak goroutines + leave the
// session entry stuck in the manager until process exit.
func (s *FIFOSource) Close() error {
	s.closed.Do(func() {
		s.stop()
		if s.f != nil {
			_ = s.f.Close()
		}
	})
	<-s.done
	return nil
}

func (s *FIFOSource) read(ctx context.Context, path string) {
	defer close(s.done)
	defer close(s.frames)
	// Don't double-close f here — Close() owns the fd's lifetime so a
	// blocked Read can be unblocked from outside this goroutine.

	f := s.f
	buf := make([]byte, s.frameSize)
	totalBytes := 0
	for {
		// Honor ctx cancel between reads. We don't preempt a blocked
		// Read, but ctx.Done() coexists with Close() (which closes the
		// fd, unblocking Read with err).
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := io.ReadFull(f, buf)
		if n > 0 {
			frame := make([]byte, n)
			copy(frame, buf[:n])
			select {
			case s.frames <- frame:
				totalBytes += n
			case <-ctx.Done():
				return
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				slog.Info("audio fifo source eof",
					"call_uuid", s.uuid, "path", path, "bytes", totalBytes)
				return
			}
			if errors.Is(err, os.ErrClosed) {
				return
			}
			slog.Warn("audio fifo source read error",
				"call_uuid", s.uuid, "err", err)
			return
		}
	}
}

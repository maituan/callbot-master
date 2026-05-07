package audio

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
)

// FIFOSource adapts a recording FIFO into audio.Source. A reader goroutine
// pulls fixed-size frames from the FIFO and forwards them to the frames
// channel; the channel closes on EOF, error, or ctx cancellation.
type FIFOSource struct {
	uuid      string
	frames    chan []byte
	frameSize int
	stop      context.CancelFunc
	done      chan struct{}
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
		stop:      cancel,
		done:      make(chan struct{}),
	}

	go src.read(readCtx, f, path)
	return src, nil
}

func (s *FIFOSource) Frames() <-chan []byte { return s.frames }

// Close stops the reader and waits for it to exit. Safe to call multiple
// times.
func (s *FIFOSource) Close() error {
	s.stop()
	<-s.done
	return nil
}

func (s *FIFOSource) read(ctx context.Context, f *os.File, path string) {
	defer close(s.done)
	defer close(s.frames)
	defer f.Close()

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

package audio

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
)

// FIFOSink writes PCM frames to a FIFO that FreeSWITCH is reading from
// (uuid_broadcast playback). A "broken pipe" error means the playback was
// stopped (barge-in, hangup) — translated to io.EOF so the pipeline can
// stop draining cleanly.
type FIFOSink struct {
	uuid string
	path string
	f    *os.File

	closed atomic.Bool
}

var _ Sink = (*FIFOSink)(nil)

// NewFIFOSink opens path for write. Caller must have already issued the FS
// playback command (uuid_broadcast) so FS is the reader on the other side
// — otherwise OpenFIFOForWrite blocks forever.
func NewFIFOSink(uuid, path string) (*FIFOSink, error) {
	f, err := OpenFIFOForWrite(path)
	if err != nil {
		return nil, fmt.Errorf("fifo sink open: %w", err)
	}
	return &FIFOSink{uuid: uuid, path: path, f: f}, nil
}

func (s *FIFOSink) Write(p []byte) (int, error) {
	if s.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	n, err := s.f.Write(p)
	if err != nil {
		// FS closing playback (barge-in, hangup, etc.) shows up as EPIPE.
		// Surface as EOF so the pipeline drain loop exits cleanly without
		// treating it as a hard error.
		if isBrokenPipe(err) {
			slog.Info("audio fifo sink broken pipe (playback stopped)",
				"call_uuid", s.uuid, "path", s.path)
			return n, io.EOF
		}
	}
	return n, err
}

// Close flushes and releases the FIFO. Idempotent.
func (s *FIFOSink) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}

func isBrokenPipe(err error) bool {
	if err == nil {
		return false
	}
	// syscall.EPIPE on Linux; errors.Is may not match os.PathError wrapping
	// across versions, so fall back to substring as a last resort.
	if errors.Is(err, syscallEPIPE) {
		return true
	}
	return strings.Contains(err.Error(), "broken pipe")
}

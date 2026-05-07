package audio

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// MakeFIFO creates (or replaces) a named pipe at path with mode 0666.
// Parent dir must already exist; errors out otherwise to surface mis-config.
func MakeFIFO(path string) error {
	// Remove any prior file/socket/fifo at this path; ignore not-exists.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove %s: %w", path, err)
	}
	if err := syscall.Mkfifo(path, 0666); err != nil {
		return fmt.Errorf("mkfifo %s: %w", path, err)
	}
	if err := os.Chmod(path, 0666); err != nil {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	return nil
}

// EnsureDir creates the parent directory of path with mode 0755 if missing.
func EnsureDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0755)
}

// OpenFIFOForRead opens a FIFO with O_RDWR — a kernel idiom that avoids the
// classic "open RO blocks until a writer arrives" deadlock. We never write
// to the returned handle; the RDWR flag just keeps the kernel from blocking.
//
// Use this for the recordings FIFO (FS writes, master reads).
func OpenFIFOForRead(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("open fifo rdwr %s: %w", path, err)
	}
	return f, nil
}

// OpenFIFOForWrite opens a FIFO O_WRONLY. This BLOCKS until a reader
// attaches — the caller is expected to issue uuid_broadcast (which makes
// FS open the read side) BEFORE calling this so the open completes.
//
// We deliberately do NOT use the O_RDWR keep-writer-alive trick: keeping
// a writer ref means FS's read side never sees EOF, so its playback
// module blocks on every read once our buffered audio is consumed and
// the caller hears intermittent silence whenever TTS production has a
// tiny gap. The v1 reference design uses one FRESH FIFO per utterance:
// open O_WRONLY, write all audio, close → FS sees EOF → playback ends
// cleanly. Subsequent Speak() calls create a new FIFO + new broadcast.
//
// The open is paired with a uuid_broadcast on the master side. Order:
//
//   1. mkfifo path
//   2. uuid_broadcast path        ← FS opens path O_RDONLY (queues)
//   3. OpenFIFOForWrite(path)     ← unblocks once FS attaches
//   4. write audio
//   5. Close()                    ← FS reads EOF after buffer drains
func OpenFIFOForWrite(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("open fifo wronly %s: %w", path, err)
	}
	return f, nil
}

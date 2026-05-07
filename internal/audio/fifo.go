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

// OpenFIFOForWrite opens a FIFO using the same O_RDWR kernel idiom we use
// on the read side: avoids the open-time deadlock that O_WRONLY would
// trigger when no reader is attached yet.
//
// Why not plain O_WRONLY: with O_WRONLY the open blocks until a reader
// opens the FIFO. If we issued uuid_broadcast first then opened O_WRONLY,
// FreeSWITCH's playback module — which opens the FIFO with O_NONBLOCK on
// the read side — sees zero bytes and immediately ends the playback (and
// in the process tears down the recording bug attached to the same
// channel). Opening O_RDWR ourselves keeps a refcount on the FIFO so
// FS's read side always has a writer present, and we can issue
// uuid_broadcast AFTER the open completes.
//
// We never read from the returned handle — the RDWR flag is purely for
// the kernel's writer-presence check.
func OpenFIFOForWrite(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("open fifo rdwr-write %s: %w", path, err)
	}
	return f, nil
}

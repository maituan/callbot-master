// Package recording archives the FreeSWITCH-side stereo MP3 recording
// into a persistent dir and exposes it over HTTP.
//
// Why this lives here, not in pipeline:
//   - The recording file is produced by FS's dialplan (record_session or
//     similar in mp-hotline.xml), not by our master — we only need to
//     observe it after CHANNEL_HANGUP_COMPLETE and copy it out before
//     the FS retention sweeper deletes the source dir.
//   - Decoupled from the call lifecycle: archiving runs on its own
//     goroutine after the call ends; if it fails, the call still
//     succeeds. Retries handle the FS-flush race (master may try to
//     copy before FS finishes writing the trailing bytes).
package recording

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// URLPersister is the slice of store.Postgres the archiver needs.
// Mocked in tests + keeps the recording package decoupled from pgx.
type URLPersister interface {
	SetRecordingURL(ctx context.Context, callID, url string) error
}

// Archiver moves <SourceDir>/<callID><FileExt> to a date-partitioned
// path under ArchiveDir and writes the resulting public URL into
// call_history via the Persister.
//
// All fields except FileExt are required; nil/empty values disable the
// archiver (Archive returns "", nil without doing anything).
type Archiver struct {
	SourceDir   string
	ArchiveDir  string
	URLPrefix   string // "/recordings" or "https://cdn.example.com/recordings"
	FileExt     string // ".mp3" by default
	MaxAttempts int    // copy retry count when FS hasn't finished writing
	WaitBetween time.Duration

	Persister URLPersister
}

// New returns an Archiver with sensible defaults filled in.
func New(sourceDir, archiveDir, urlPrefix, fileExt string, persister URLPersister) *Archiver {
	if fileExt == "" {
		fileExt = ".mp3"
	}
	if !strings.HasPrefix(fileExt, ".") {
		fileExt = "." + fileExt
	}
	return &Archiver{
		SourceDir:   sourceDir,
		ArchiveDir:  archiveDir,
		URLPrefix:   strings.TrimRight(urlPrefix, "/"),
		FileExt:     fileExt,
		MaxAttempts: 12, // ~24 s with 2 s waits — enough for FS post-process
		WaitBetween: 2 * time.Second,
		Persister:   persister,
	}
}

// Enabled reports whether the archiver has the minimum config to run.
// Speeds up the no-op path for ops who haven't wired recording yet.
func (a *Archiver) Enabled() bool {
	return a != nil && a.SourceDir != "" && a.ArchiveDir != ""
}

// Archive copies the FS recording for callID to the archive dir and
// returns the public URL. On the FS side the file may not be flushed
// immediately after CHANNEL_HANGUP_COMPLETE — we retry up to MaxAttempts
// with a small delay before declaring the file missing.
//
// Safe to call from multiple goroutines: it operates on per-call paths.
func (a *Archiver) Archive(ctx context.Context, callID string) (string, error) {
	if !a.Enabled() {
		return "", nil
	}
	srcPath := filepath.Join(a.SourceDir, callID+a.FileExt)

	// Wait for the source file to appear and stabilize. FS may hold
	// the fd open for a beat after hangup to flush MP3 frames.
	src, err := a.waitForFile(ctx, srcPath)
	if err != nil {
		return "", err
	}
	defer src.Close()

	now := time.Now()
	relDir := filepath.Join(now.Format("2006"), now.Format("01"), now.Format("02"))
	dstDir := filepath.Join(a.ArchiveDir, relDir)
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir archive: %w", err)
	}
	dstName := callID + a.FileExt
	dstPath := filepath.Join(dstDir, dstName)
	dst, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return "", fmt.Errorf("open archive dst: %w", err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return "", fmt.Errorf("copy: %w", err)
	}
	if err := dst.Sync(); err != nil {
		return "", fmt.Errorf("fsync: %w", err)
	}

	relURL := strings.Join([]string{
		a.URLPrefix,
		now.Format("2006"),
		now.Format("01"),
		now.Format("02"),
		dstName,
	}, "/")

	if a.Persister != nil {
		ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := a.Persister.SetRecordingURL(ctx2, callID, relURL); err != nil {
			slog.Warn("recording url persist failed",
				"call_uuid", callID, "url", relURL, "err", err)
			// don't fail the archive — the file is on disk, the URL
			// can be backfilled manually.
		}
	}

	slog.Info("recording archived",
		"call_uuid", callID,
		"src", srcPath,
		"dst", dstPath,
		"url", relURL)
	return relURL, nil
}

// waitForFile keeps trying to open the source until it appears OR the
// retry budget runs out. Returns the open file handle on success.
func (a *Archiver) waitForFile(ctx context.Context, path string) (*os.File, error) {
	for attempt := 0; attempt < a.MaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(a.WaitBetween):
			}
		}
		f, err := os.Open(path)
		if err == nil {
			// FS may still be writing — verify size is non-zero and stable.
			fi, statErr := f.Stat()
			if statErr == nil && fi.Size() > 0 {
				return f, nil
			}
			_ = f.Close()
			continue
		}
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("open source: %w", err)
		}
	}
	return nil, errors.New("source file did not appear within retry window")
}

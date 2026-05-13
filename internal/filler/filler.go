// Package filler picks short / long pre-recorded "uhm/dạ vâng/để em
// kiểm tra…" audio files to play while the bot is composing a reply.
// Hides bot first-byte latency.
//
// Layout:
//
//	{BaseDir}/{voice_id}/*.wav          ← short (back-compat with the
//	                                       original flat layout)
//	{BaseDir}/{voice_id}/long/*.wav     ← long
//
// Short scans the flat directory and skips any subdirectories so the
// pre-existing setup keeps working unchanged. Long lives in the
// `long/` subdirectory. Empty folder = silent behaviour (no filler);
// PickKind returns "" so the pipeline knows to skip cueing.
package filler

import (
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Kind picks which pool a filler request lands in.
type Kind string

const (
	KindShort Kind = "short"
	KindLong  Kind = "long"
)

// Pool resolves filler files per voice on demand. Caches the directory
// listing per voice for the lifetime of the process — operators
// hot-swap the file set with a restart, which is fine.
type Pool struct {
	BaseDir string

	mu    sync.RWMutex
	cache map[poolKey][]string // (voice_id, kind) → file paths
}

type poolKey struct {
	voice string
	kind  Kind
}

func NewPool(baseDir string) *Pool {
	return &Pool{
		BaseDir: baseDir,
		cache:   map[poolKey][]string{},
	}
}

// Pick returns a random SHORT filler path for the voice, or "" if none
// exists. Kept for callers (and tests) that pre-date the kind split.
func (p *Pool) Pick(voiceID string) string {
	return p.PickKind(voiceID, KindShort)
}

// PickKind returns a random filler path for the (voice, kind) pair. If
// kind=long but the long pool is empty, falls back to the short pool —
// the bot should never go silent because the long bank wasn't filled.
// Returns "" only when both pools are empty.
func (p *Pool) PickKind(voiceID string, kind Kind) string {
	if p == nil || p.BaseDir == "" || voiceID == "" {
		return ""
	}
	if kind == "" {
		kind = KindShort
	}
	files := p.list(voiceID, kind)
	if len(files) == 0 && kind == KindLong {
		// Long bank missing → fall back to short so we still mask latency.
		files = p.list(voiceID, KindShort)
	}
	if len(files) == 0 {
		return ""
	}
	if len(files) == 1 {
		return files[0]
	}
	return files[rand.Intn(len(files))]
}

// Count is exposed for log-on-startup so ops can confirm the dir was
// found and how many files matched. Defaults to short.
func (p *Pool) Count(voiceID string) int {
	return p.CountKind(voiceID, KindShort)
}

// CountKind reports how many filler files are available for one kind.
func (p *Pool) CountKind(voiceID string, kind Kind) int {
	if p == nil {
		return 0
	}
	if kind == "" {
		kind = KindShort
	}
	return len(p.list(voiceID, kind))
}

func (p *Pool) list(voiceID string, kind Kind) []string {
	key := poolKey{voice: voiceID, kind: kind}
	p.mu.RLock()
	if files, ok := p.cache[key]; ok {
		p.mu.RUnlock()
		return files
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if files, ok := p.cache[key]; ok {
		return files
	}
	dir := filepath.Join(p.BaseDir, voiceID)
	if kind == KindLong {
		dir = filepath.Join(dir, "long")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		p.cache[key] = nil
		return nil
	}
	var out []string
	for _, e := range entries {
		// Short reads files only — skip subdirectories so the `long/`
		// folder doesn't pollute the short pool when both coexist.
		if e.IsDir() {
			continue
		}
		name := strings.ToLower(e.Name())
		if strings.HasSuffix(name, ".wav") || strings.HasSuffix(name, ".mp3") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	p.cache[key] = out
	return out
}

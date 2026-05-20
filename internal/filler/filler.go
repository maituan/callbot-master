// Package filler picks pre-recorded reaction audio ("uhm/dạ vâng/để
// em kiểm tra…") to play while the bot is composing a reply, hiding
// bot first-byte latency.
//
// Layout — label-driven so adding/renaming intents is a data-only
// change (drop in a folder, no code edit):
//
//	{BaseDir}/{voice_id}/*.wav             ← fallback short pool (flat)
//	{BaseDir}/{voice_id}/{LABEL}/*.wav     ← per-intent, LABEL = the
//	                                          normalised intent label
//	                                          (e.g. PROCEDURE_NEW)
//
// PickLabel(voice, label) tries the label folder first and silently
// falls back to the flat pool when the folder is missing/empty — so an
// intent the ops team hasn't recorded audio for still gets a generic
// "uhm" instead of dead air. Empty folder everywhere = silent (no
// filler); the pipeline skips cueing.
package filler

import (
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Pool resolves filler files per (voice, label) on demand. Caches the
// directory listing for the lifetime of the process — operators
// hot-swap the file set with a restart, which is fine.
type Pool struct {
	BaseDir string

	mu    sync.RWMutex
	cache map[poolKey][]string // (voice_id, label) → file paths
}

type poolKey struct {
	voice string
	label string // "" = flat fallback pool
}

func NewPool(baseDir string) *Pool {
	return &Pool{
		BaseDir: baseDir,
		cache:   map[poolKey][]string{},
	}
}

// Pick returns a random file from the flat fallback pool. Kept for
// callers (and tests) that don't classify.
func (p *Pool) Pick(voiceID string) string {
	return p.PickLabel(voiceID, "")
}

// PickLabel returns a random filler path for the (voice, label) pair.
// label is the normalised intent label (UPPERCASE) or "" for the flat
// fallback. When the label folder is missing or empty, falls back to
// the flat pool so the bot never goes silent over an unrecorded
// intent. Returns "" only when both the label folder and the flat
// pool are empty.
func (p *Pool) PickLabel(voiceID, label string) string {
	if p == nil || p.BaseDir == "" || voiceID == "" {
		return ""
	}
	if label != "" {
		if files := p.list(voiceID, label); len(files) > 0 {
			return pickRandom(files)
		}
		// Label folder missing/empty → fall through to flat pool.
	}
	flat := p.list(voiceID, "")
	if len(flat) == 0 {
		return ""
	}
	return pickRandom(flat)
}

func pickRandom(files []string) string {
	if len(files) == 1 {
		return files[0]
	}
	return files[rand.Intn(len(files))]
}

// Count reports how many files the flat fallback pool has — used for
// log-on-startup sanity checks.
func (p *Pool) Count(voiceID string) int {
	if p == nil {
		return 0
	}
	return len(p.list(voiceID, ""))
}

func (p *Pool) list(voiceID, label string) []string {
	key := poolKey{voice: voiceID, label: label}
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
	if label != "" {
		dir = filepath.Join(dir, label)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		p.cache[key] = nil
		return nil
	}
	var out []string
	for _, e := range entries {
		// Skip subdirectories — for the flat (label="") pool this
		// keeps per-label folders out of the fallback set.
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

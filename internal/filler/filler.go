// Package filler picks short pre-recorded "uhm/dạ vâng" audio files to
// play while the bot is composing a reply. Hides bot first-byte latency.
//
// Layout: {BaseDir}/{voice_id}/*.wav (or .mp3). Each voice has its own
// folder so the filler matches the voice timbre. Empty folder = silent
// behaviour (no filler).
package filler

import (
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Pool resolves filler files per voice on demand. Caches the directory
// listing per voice for the lifetime of the process — operators
// hot-swap the file set with a restart, which is fine.
type Pool struct {
	BaseDir string

	mu    sync.RWMutex
	cache map[string][]string // voice_id → file paths
}

func NewPool(baseDir string) *Pool {
	return &Pool{
		BaseDir: baseDir,
		cache:   map[string][]string{},
	}
}

// Pick returns a random filler path for the voice, or "" if none exists.
// Safe for concurrent use.
func (p *Pool) Pick(voiceID string) string {
	if p == nil || p.BaseDir == "" || voiceID == "" {
		return ""
	}
	files := p.list(voiceID)
	if len(files) == 0 {
		return ""
	}
	if len(files) == 1 {
		return files[0]
	}
	// math/rand is fine here — choice doesn't need to be unpredictable.
	return files[rand.Intn(len(files))]
}

// Count is exposed for log-on-startup so ops can confirm the dir was
// found and how many files matched.
func (p *Pool) Count(voiceID string) int {
	if p == nil {
		return 0
	}
	return len(p.list(voiceID))
}

func (p *Pool) list(voiceID string) []string {
	p.mu.RLock()
	if files, ok := p.cache[voiceID]; ok {
		p.mu.RUnlock()
		return files
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if files, ok := p.cache[voiceID]; ok {
		return files
	}
	dir := filepath.Join(p.BaseDir, voiceID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		// Cache the empty result so we don't stat the FS on every call.
		p.cache[voiceID] = nil
		return nil
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := strings.ToLower(e.Name())
		if strings.HasSuffix(name, ".wav") || strings.HasSuffix(name, ".mp3") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	p.cache[voiceID] = out
	return out
}

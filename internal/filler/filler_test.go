package filler

import (
	"os"
	"path/filepath"
	"testing"
)

// touch creates an empty file, making parent dirs as needed.
func touch(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestPickLabel_PrefersLabelFolder(t *testing.T) {
	base := t.TempDir()
	voice := "thuyanh-north"
	touch(t, filepath.Join(base, voice, "uhm.wav"))                       // flat fallback
	touch(t, filepath.Join(base, voice, "PROCEDURE_NEW", "de-em-xem.wav")) // label folder

	p := NewPool(base)
	got := p.PickLabel(voice, "PROCEDURE_NEW")
	if filepath.Base(got) != "de-em-xem.wav" {
		t.Fatalf("expected label-folder file, got %q", got)
	}
}

func TestPickLabel_FallsBackToFlatWhenLabelMissing(t *testing.T) {
	base := t.TempDir()
	voice := "thuyanh-north"
	touch(t, filepath.Join(base, voice, "uhm.wav")) // only flat pool exists

	p := NewPool(base)
	// Label has no folder → fall back to flat.
	got := p.PickLabel(voice, "OFF_TOPIC")
	if filepath.Base(got) != "uhm.wav" {
		t.Fatalf("expected flat fallback, got %q", got)
	}
}

func TestPickLabel_EmptyLabelUsesFlat(t *testing.T) {
	base := t.TempDir()
	voice := "v"
	touch(t, filepath.Join(base, voice, "a.wav"))
	touch(t, filepath.Join(base, voice, "META", "b.wav"))

	p := NewPool(base)
	got := p.PickLabel(voice, "")
	if filepath.Base(got) != "a.wav" {
		t.Fatalf("empty label should use flat pool, got %q", got)
	}
}

func TestPickLabel_NothingAvailable(t *testing.T) {
	base := t.TempDir()
	p := NewPool(base)
	if got := p.PickLabel("nobody", "ANY"); got != "" {
		t.Fatalf("expected empty when no audio, got %q", got)
	}
}

func TestPickLabel_FlatSkipsSubdirs(t *testing.T) {
	base := t.TempDir()
	voice := "v"
	touch(t, filepath.Join(base, voice, "flat.wav"))
	touch(t, filepath.Join(base, voice, "CHITCHAT", "nested.wav"))

	p := NewPool(base)
	// Flat pool must NOT include nested label files.
	got := p.PickLabel(voice, "")
	if filepath.Base(got) != "flat.wav" {
		t.Fatalf("flat pool leaked a subdir file: %q", got)
	}
}

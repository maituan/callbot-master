package intent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseLabel(t *testing.T) {
	cases := []struct {
		in   string
		want Kind
		ok   bool
	}{
		{"BUSINESS", KindLong, true},
		{"CHITCHAT", KindShort, true},
		{"  BUSINESS\n", KindLong, true},
		{"\"BUSINESS\"", KindLong, true},
		{"business", KindLong, true},
		{"chitchat", KindShort, true},
		{"unknown", KindShort, false},
		{"", KindShort, false},
	}
	for _, c := range cases {
		got, err := ParseLabel(c.in)
		if got != c.want {
			t.Errorf("ParseLabel(%q): kind=%v want=%v", c.in, got, c.want)
		}
		if (err == nil) != c.ok {
			t.Errorf("ParseLabel(%q): err=%v ok=%v", c.in, err, c.ok)
		}
	}
}

func TestHTTPClient_Classify_Business(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("BUSINESS"))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	got, err := c.Classify(context.Background(), "conv-1", "anh muốn hỏi thủ tục")
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if got != KindLong {
		t.Fatalf("got %v want long", got)
	}
}

func TestHTTPClient_Classify_Chitchat(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("CHITCHAT\n"))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	got, err := c.Classify(context.Background(), "conv-1", "ờ")
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if got != KindShort {
		t.Fatalf("got %v want short", got)
	}
}

func TestHTTPClient_Classify_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte("BUSINESS"))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	got, err := c.Classify(ctx, "conv-1", "x")
	if err == nil {
		t.Fatal("expected timeout error")
	}
	// On any failure we fall back to short so the pipeline can keep
	// going without branching on the error type.
	if got != KindShort {
		t.Fatalf("fallback should be short, got %v", got)
	}
}

func TestHTTPClient_EmptyURL(t *testing.T) {
	c := NewHTTPClient("")
	got, err := c.Classify(context.Background(), "x", "y")
	if err != ErrNoIntent {
		t.Fatalf("err=%v want ErrNoIntent", err)
	}
	if got != KindShort {
		t.Fatalf("got %v want short", got)
	}
}

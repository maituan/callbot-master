package bot

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// streamWriter helps tests write chunked bot output with controlled timing.
type streamWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

func newStreamWriter(t *testing.T, w http.ResponseWriter) *streamWriter {
	t.Helper()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(200)
	f, ok := w.(http.Flusher)
	if !ok {
		t.Fatalf("ResponseWriter does not support Flusher")
	}
	return &streamWriter{w: w, flusher: f}
}

func (s *streamWriter) write(t *testing.T, chunk string) {
	t.Helper()
	if _, err := io.WriteString(s.w, chunk); err != nil {
		t.Fatalf("write chunk: %v", err)
	}
	s.flusher.Flush()
}

func TestHTTPClient_StreamsAndParsesAction(t *testing.T) {
	chunks := []string{
		"Xin chào, ", "anh chị cần gì ạ", "?",
		" Em sẵn sàng hỗ trợ.", "|CHAT",
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req callRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if req.ConversationID != "conv-1" || req.Message != "hello" {
			t.Errorf("unexpected req=%+v", req)
		}
		sw := newStreamWriter(t, w)
		for _, c := range chunks {
			sw.write(t, c)
		}
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, time.Second, 5*time.Second)
	ts, err := c.Stream(context.Background(), "conv-1", "hello")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer ts.Close()

	var got []string
	for s := range ts.Sentences() {
		got = append(got, s)
	}
	want := []string{"Xin chào, anh chị cần gì ạ?", "Em sẵn sàng hỗ trợ."}
	if !equalStrings(got, want) {
		t.Fatalf("sentences = %#v, want %#v", got, want)
	}
	a, err := ts.Action()
	if err != nil {
		t.Fatalf("action err: %v", err)
	}
	if a != ActionChat {
		t.Fatalf("action = %q, want CHAT", a)
	}
}

func TestHTTPClient_NonOKStatusReturnsErr(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, time.Second, 2*time.Second)
	_, err := c.Stream(context.Background(), "x", "y")
	if err == nil {
		t.Fatalf("expected error on 500")
	}
}

func TestHTTPClient_ContextCancelStopsReader(t *testing.T) {
	// Server streams a chunk, then hangs forever. Caller cancels via Close()
	// — Sentences() must close and Action() must return promptly with err.
	hang := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sw := newStreamWriter(t, w)
		sw.write(t, "Đang chờ.")
		<-hang // never released until test cleanup
	}))
	defer srv.Close()
	defer close(hang)

	c := NewHTTPClient(srv.URL, time.Second, 30*time.Second)
	ts, err := c.Stream(context.Background(), "x", "y")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	// Drain the first sentence so we know the stream is live, then close.
	first := <-ts.Sentences()
	if first != "Đang chờ." {
		t.Fatalf("first sentence = %q", first)
	}
	_ = ts.Close()

	// Sentences must close + Action must return within a short window.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ts.Sentences():
			if !ok {
				goto checkAction
			}
		case <-deadline:
			t.Fatal("sentences channel did not close after Close()")
		}
	}
checkAction:
	_, err = ts.Action()
	if err == nil {
		t.Fatalf("expected ctx error after Close()")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

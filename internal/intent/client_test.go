package intent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNormalizeLabel(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"PROCEDURE_NEW", "PROCEDURE_NEW"},
		{"  procedure_field_reask\n", "PROCEDURE_FIELD_REASK"},
		{"\"META\"", "META"},
		{"off_topic", "OFF_TOPIC"},
		{" 'CHITCHAT' ", "CHITCHAT"},
		{"", ""},
		{"   ", ""},
	}
	for _, c := range cases {
		if got := NormalizeLabel(c.in); got != c.want {
			t.Errorf("NormalizeLabel(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestHTTPClient_Classify_Label(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("PROCEDURE_NEW"))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	got, err := c.Classify(context.Background(), "conv-1", "thủ tục đăng ký kết hôn")
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if got != "PROCEDURE_NEW" {
		t.Fatalf("got %q want PROCEDURE_NEW", got)
	}
}

func TestHTTPClient_Classify_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte("META"))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	got, err := c.Classify(ctx, "conv-1", "x")
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if got != "" {
		t.Fatalf("on error label should be empty, got %q", got)
	}
}

func TestHTTPClient_EmptyURL(t *testing.T) {
	c := NewHTTPClient("")
	got, err := c.Classify(context.Background(), "x", "y")
	if err != ErrNoIntent {
		t.Fatalf("err=%v want ErrNoIntent", err)
	}
	if got != "" {
		t.Fatalf("got %q want empty", got)
	}
}

func TestHTTPClient_EmptyBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("   "))
	}))
	defer srv.Close()
	c := NewHTTPClient(srv.URL)
	if _, err := c.Classify(context.Background(), "x", "y"); err == nil {
		t.Fatal("expected error on empty label body")
	}
}

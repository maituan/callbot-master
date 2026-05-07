package tts

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeServer spins up a minimal Viettel-compatible WS endpoint backed by a
// caller-supplied handler. The handler runs inside one connection per dial.
type fakeServer struct {
	srv *httptest.Server
	url string
}

func newFakeServer(t *testing.T, handler func(t *testing.T, ws *websocket.Conn)) *fakeServer {
	t.Helper()
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(*http.Request) bool { return true },
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer ws.Close()
		handler(t, ws)
	}))
	return &fakeServer{srv: srv, url: "ws" + strings.TrimPrefix(srv.URL, "http")}
}

func (f *fakeServer) close() { f.srv.Close() }

func TestViettelClient_AuthAndStreamAudio(t *testing.T) {
	pcm := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06}
	pcmB64 := base64.StdEncoding.EncodeToString(pcm)

	var receivedTexts []string
	var mu sync.Mutex

	srv := newFakeServer(t, func(t *testing.T, ws *websocket.Conn) {
		// 1) Read auth, send authenticated.
		var auth map[string]any
		if err := ws.ReadJSON(&auth); err != nil {
			t.Errorf("read auth: %v", err)
			return
		}
		if _, ok := auth["voice_settings"]; !ok {
			t.Errorf("auth missing voice_settings: %#v", auth)
		}
		if err := ws.WriteJSON(map[string]any{
			"status":        "authenticated",
			"sampling_rate": 8000,
		}); err != nil {
			t.Errorf("write auth resp: %v", err)
			return
		}

		// 2) Read each SendText, emit audio chunk per text. On end_of_input,
		//    emit one more chunk with isFinal=true.
		for {
			var msg map[string]any
			if err := ws.ReadJSON(&msg); err != nil {
				return
			}
			text, _ := msg["text"].(string)
			mu.Lock()
			receivedTexts = append(receivedTexts, text)
			mu.Unlock()

			if err := ws.WriteJSON(map[string]any{
				"audio":       pcmB64,
				"sample_rate": 8000,
				"text":        text,
			}); err != nil {
				return
			}
			if eos, _ := msg["end_of_input"].(bool); eos {
				_ = ws.WriteJSON(map[string]any{
					"audio":       pcmB64,
					"isFinal":     true,
					"sample_rate": 8000,
				})
				return
			}
		}
	})
	defer srv.close()

	c := NewViettelClient(srv.url, "test-key", "voiceA", 8000, 1.0)
	stream, err := c.StartStream(context.Background(), StreamOpts{
		ConversationID: "tts-test-1",
	})
	if err != nil {
		t.Fatalf("StartStream: %v", err)
	}
	defer stream.Close()

	if err := stream.SendText("Câu một.", false); err != nil {
		t.Fatalf("send 1: %v", err)
	}
	if err := stream.SendText("Câu hai.", true); err != nil {
		t.Fatalf("send 2 eos: %v", err)
	}

	var got []byte
	for frame := range stream.AudioChan() {
		got = append(got, frame...)
	}
	wantLen := len(pcm) * 3 // 1 frame per text + 1 final
	if len(got) != wantLen {
		t.Fatalf("audio bytes = %d, want %d", len(got), wantLen)
	}

	mu.Lock()
	if len(receivedTexts) != 2 {
		t.Fatalf("server got %d texts, want 2", len(receivedTexts))
	}
	mu.Unlock()
}

func TestViettelClient_AuthRejectedReturnsErr(t *testing.T) {
	srv := newFakeServer(t, func(t *testing.T, ws *websocket.Conn) {
		var auth map[string]any
		_ = ws.ReadJSON(&auth)
		_ = ws.WriteJSON(map[string]any{
			"status": "rejected",
			"error":  "bad api key",
		})
	})
	defer srv.close()

	c := NewViettelClient(srv.url, "wrong-key", "voiceA", 8000, 1.0)
	_, err := c.StartStream(context.Background(), StreamOpts{ConversationID: "x"})
	if err == nil || !strings.Contains(err.Error(), "bad api key") {
		t.Fatalf("err = %v, want auth rejected", err)
	}
}

func TestViettelClient_CloseUnblocksReader(t *testing.T) {
	// Server hangs after auth; client Close() must close the audio channel
	// and cause the reader goroutine to exit.
	hang := make(chan struct{})
	srv := newFakeServer(t, func(t *testing.T, ws *websocket.Conn) {
		var auth map[string]any
		_ = ws.ReadJSON(&auth)
		_ = ws.WriteJSON(map[string]any{"status": "authenticated", "sampling_rate": 8000})
		<-hang
	})
	defer srv.close()
	defer close(hang)

	c := NewViettelClient(srv.url, "k", "v", 8000, 1.0)
	stream, err := c.StartStream(context.Background(), StreamOpts{ConversationID: "x"})
	if err != nil {
		t.Fatalf("StartStream: %v", err)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case _, ok := <-stream.AudioChan():
		if ok {
			t.Fatalf("audio chan should be closed, got value")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("audio chan did not close after Close()")
	}
}

func TestViettelClient_ServerErrorFrameIsSkipped(t *testing.T) {
	pcm := []byte{0xAA, 0xBB}
	pcmB64 := base64.StdEncoding.EncodeToString(pcm)

	srv := newFakeServer(t, func(t *testing.T, ws *websocket.Conn) {
		var auth map[string]any
		_ = ws.ReadJSON(&auth)
		_ = ws.WriteJSON(map[string]any{"status": "authenticated", "sampling_rate": 8000})

		var msg map[string]any
		_ = ws.ReadJSON(&msg) // SendText

		// Server emits an error frame mid-stream — client must skip it.
		_ = ws.WriteJSON(map[string]any{"error": "transient blip"})
		_ = ws.WriteJSON(map[string]any{"audio": pcmB64, "isFinal": true})
	})
	defer srv.close()

	c := NewViettelClient(srv.url, "k", "v", 8000, 1.0)
	stream, err := c.StartStream(context.Background(), StreamOpts{ConversationID: "x"})
	if err != nil {
		t.Fatalf("StartStream: %v", err)
	}
	defer stream.Close()

	if err := stream.SendText("Hello.", true); err != nil {
		t.Fatalf("SendText: %v", err)
	}

	var got []byte
	for frame := range stream.AudioChan() {
		got = append(got, frame...)
	}
	if len(got) != len(pcm) {
		t.Fatalf("audio bytes = %d, want %d", len(got), len(pcm))
	}
}

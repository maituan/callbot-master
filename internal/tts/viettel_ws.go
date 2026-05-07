package tts

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ViettelClient implements tts.Client against the Viettel TTS WebSocket
// endpoint. Wire format (port từ telecenter-v3-voice-ai-backend/internal/tts):
//
//	C → S (auth, first frame):
//	  {"text": " ", "voice_settings": {"voiceId","resample_rate","tempo"},
//	   "generator_config": {"chunk_length_schedule": [1]}, "xi_api_key": "..."}
//	S → C: {"status": "authenticated", "sampling_rate": 8000}
//
//	C → S (per sentence): {"text": "...", "end_of_input": bool}
//	S → C (per chunk): {"audio": "<b64-pcm-s16le-8k>", "isFinal": bool,
//	                    "sample_rate": 8000, "text": "...", "duration": ...}
//
//	C → S (cancel): {"reset": true}  S responds {"status": "reset"}
type ViettelClient struct {
	endpoint     string
	apiKey       string
	defaultVoice string
	resampleRate int
	tempo        float64
	dialTimeout  time.Duration
}

// NewViettelClient configures the WS dialer. resampleRate=0 → 8000 default.
func NewViettelClient(endpoint, apiKey, defaultVoice string, resampleRate int, tempo float64) *ViettelClient {
	if resampleRate == 0 {
		resampleRate = 8000
	}
	if tempo == 0 {
		tempo = 1.0
	}
	return &ViettelClient{
		endpoint:     endpoint,
		apiKey:       apiKey,
		defaultVoice: defaultVoice,
		resampleRate: resampleRate,
		tempo:        tempo,
		dialTimeout:  10 * time.Second,
	}
}

func (c *ViettelClient) StartStream(ctx context.Context, opts StreamOpts) (Stream, error) {
	voice := opts.VoiceID
	if voice == "" {
		voice = c.defaultVoice
	}
	resample := opts.ResampleRate
	if resample == 0 {
		resample = c.resampleRate
	}
	tempo := opts.Tempo
	if tempo == 0 {
		tempo = c.tempo
	}

	dialer := websocket.Dialer{HandshakeTimeout: c.dialTimeout}
	dialCtx, cancelDial := context.WithTimeout(ctx, c.dialTimeout)
	defer cancelDial()
	conn, _, err := dialer.DialContext(dialCtx, c.endpoint, http.Header{})
	if err != nil {
		return nil, fmt.Errorf("tts dial: %w", err)
	}

	auth := map[string]any{
		"text": " ",
		"voice_settings": map[string]any{
			"voiceId":       voice,
			"resample_rate": resample,
			"tempo":         tempo,
		},
		"generator_config": map[string]any{
			"chunk_length_schedule": []int{1},
		},
		"xi_api_key": c.apiKey,
	}
	if err := conn.WriteJSON(auth); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("tts auth send: %w", err)
	}

	// Read auth response. Server may send the very first audio frame as a
	// separate message right after; we only require the first message to be
	// an authenticated status.
	var authResp map[string]any
	if err := conn.ReadJSON(&authResp); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("tts auth recv: %w", err)
	}
	status, _ := authResp["status"].(string)
	if status != "authenticated" {
		errMsg, _ := authResp["error"].(string)
		_ = conn.Close()
		if errMsg == "" {
			errMsg = fmt.Sprintf("status=%v", authResp["status"])
		}
		return nil, fmt.Errorf("tts auth rejected: %s", errMsg)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	s := &viettelStream{
		ctx:            streamCtx,
		cancel:         cancel,
		conn:           conn,
		audio:          make(chan []byte, 16),
		conversationID: opts.ConversationID,
	}
	go s.read()
	go s.watchCtx()
	return s, nil
}

type viettelStream struct {
	ctx            context.Context
	cancel         context.CancelFunc
	conn           *websocket.Conn
	conversationID string

	writeMu sync.Mutex // serializes WriteJSON calls
	audio   chan []byte

	closed atomic.Bool

	// Closed exactly once when the reader exits.
	readerDone chan struct{}
	readerOnce sync.Once
}

// SendText pushes one sentence; eos=true on the last call so the server can
// emit isFinal. Calling SendText after Close is a no-op.
func (s *viettelStream) SendText(text string, eos bool) error {
	if s.closed.Load() {
		return errors.New("tts stream closed")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	msg := map[string]any{
		"text":         text,
		"end_of_input": eos,
	}
	if err := s.conn.WriteJSON(msg); err != nil {
		return fmt.Errorf("tts send text: %w", err)
	}
	return nil
}

func (s *viettelStream) AudioChan() <-chan []byte { return s.audio }

// Close terminates the WS, unblocking any in-flight SendText/Read.
// Safe to call multiple times.
func (s *viettelStream) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.cancel()
	// Best-effort polite close; ignore errors since the conn may already be torn down.
	_ = s.conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(time.Second),
	)
	_ = s.conn.Close()
	return nil
}

// watchCtx tears down the conn when the parent ctx cancels (e.g. session end
// or barge-in cancel from the pipeline).
func (s *viettelStream) watchCtx() {
	<-s.ctx.Done()
	if !s.closed.Load() {
		_ = s.conn.Close()
	}
}

func (s *viettelStream) read() {
	defer close(s.audio)
	defer s.readerOnce.Do(func() {
		if s.readerDone != nil {
			close(s.readerDone)
		}
	})

	for {
		var msg map[string]any
		if err := s.conn.ReadJSON(&msg); err != nil {
			if !s.closed.Load() {
				slog.Debug("tts read closed", "call_uuid", s.conversationID, "err", err)
			}
			return
		}

		// Server-side error frame: log, continue (server keeps the conn open).
		if errMsg, ok := msg["error"].(string); ok && errMsg != "" {
			slog.Warn("tts server error frame", "call_uuid", s.conversationID, "err", errMsg)
			continue
		}

		// Reset ack frame.
		if status, _ := msg["status"].(string); status == "reset" {
			slog.Debug("tts reset confirmed", "call_uuid", s.conversationID)
			continue
		}

		if audioB64, ok := msg["audio"].(string); ok && audioB64 != "" {
			pcm, err := base64.StdEncoding.DecodeString(audioB64)
			if err != nil {
				slog.Warn("tts audio b64 decode failed",
					"call_uuid", s.conversationID, "err", err)
			} else if len(pcm) > 0 {
				select {
				case s.audio <- pcm:
				case <-s.ctx.Done():
					return
				}
			}
		}

		if isFinal, _ := msg["isFinal"].(bool); isFinal {
			slog.Debug("tts final received", "call_uuid", s.conversationID)
			return
		}
	}
}

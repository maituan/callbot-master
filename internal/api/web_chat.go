package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"callbot-master/internal/auth"
	"callbot-master/internal/bot"
	"callbot-master/internal/store"
)

// chat handles POST /api/v1/web/chat/{token}. Body: {session_id?, message}.
// Streams plain text chunks back as the bot REST emits them; stores the
// completed turn in web_session/web_turn.
//
// Wire format mirrors the upstream bot:
//   - Content-Type: text/plain; charset=utf-8 with chunked transfer
//   - Each flushed chunk is one or more sentences as the model produces them
//   - The very last chunk carries `|<ACTION>` which we strip server-side
//     and re-emit as a trailer header `X-Bot-Action`.
//
// New sessions are minted on first message; the response carries
// `X-Session-Id` so the client can echo it on subsequent messages.
func (h *webHandler) chat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tok := strings.TrimPrefix(r.URL.Path, "/api/v1/web/chat/")
	b, _, iat, err := h.resolveToken(r.Context(), tok, auth.BotShareChannelChat)
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, err.Error())
		return
	}

	var body struct {
		SessionID string `json:"session_id,omitempty"`
		Message   string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if strings.TrimSpace(body.Message) == "" && body.SessionID != "" {
		writeJSONError(w, http.StatusBadRequest, "message required")
		return
	}

	// Resolve or create session.
	var sess *store.WebSession
	if body.SessionID != "" {
		sid, err := uuid.Parse(body.SessionID)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid session_id")
			return
		}
		sess, err = h.d.Store.GetWebSession(r.Context(), sid)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if sess == nil || sess.BotID != b.ID || sess.Channel != "chat" {
			writeJSONError(w, http.StatusNotFound, "session not found")
			return
		}
	} else {
		sess = &store.WebSession{
			BotID:     b.ID,
			TenantID:  b.TenantID,
			Channel:   "chat",
			IP:        r.RemoteAddr,
			UserAgent: r.UserAgent(),
		}
		if !iat.IsZero() {
			sess.TokenIAT = &iat
		}
		if err := h.d.Store.CreateWebSession(r.Context(), sess); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	// Build bot client. Constructed per-call (cheap HTTP transport).
	if h.d.BotFactory == nil {
		writeJSONError(w, http.StatusInternalServerError, "bot factory not configured")
		return
	}
	bc, err := h.d.BotFactory(b)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "bot client: "+err.Error())
		return
	}

	conversationID := "web-" + sess.ID.String()
	now := time.Now()

	// Persist user turn upfront so even an aborted bot stream leaves a
	// trace.
	userTurn := &store.WebTurn{
		SessionID:  sess.ID,
		Role:       "user",
		Text:       body.Message,
		ASRFinalAt: &now,
	}
	if err := h.d.Store.AppendWebTurn(r.Context(), userTurn); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "persist user turn: "+err.Error())
		return
	}

	// Open the bot stream. Use a context detached from the HTTP request
	// only for the persistence after the client disconnects — we still
	// want to write the partial bot turn to DB.
	stream, err := bc.Stream(r.Context(), conversationID, body.Message)
	if err != nil {
		_ = h.d.Store.AppendWebTurn(context.Background(), &store.WebTurn{
			SessionID: sess.ID, Role: "bot", Text: "", Action: "ERROR",
		})
		writeJSONError(w, http.StatusBadGateway, "bot stream: "+err.Error())
		return
	}
	defer stream.Close()

	// Streaming response.
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Session-Id", sess.ID.String())
	w.Header().Set("Trailer", "X-Bot-Action")

	flusher, _ := w.(http.Flusher)
	w.WriteHeader(http.StatusOK)

	var (
		acc            strings.Builder
		firstByteAt    *time.Time
	)
	for sentence := range stream.Sentences() {
		if sentence == "" {
			continue
		}
		if firstByteAt == nil {
			t := time.Now()
			firstByteAt = &t
		}
		acc.WriteString(sentence)
		if _, err := io.WriteString(w, sentence); err != nil {
			// Client disconnected — abandon the stream but still persist
			// what we got.
			break
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
	action, actionErr := stream.Action()
	doneAt := time.Now()

	// Trailer header — best-effort; some intermediaries strip trailers.
	if action != "" {
		w.Header().Set("X-Bot-Action", string(action))
	}

	// Persist bot turn (use background ctx to survive client disconnects).
	persistCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	botTurn := &store.WebTurn{
		SessionID:      sess.ID,
		Role:           "bot",
		Text:           strings.TrimSpace(acc.String()),
		BotFirstByteAt: firstByteAt,
		BotDoneAt:      &doneAt,
		Action:         string(action),
	}
	if actionErr != nil && action == "" {
		botTurn.Action = "ERROR"
	}
	if err := h.d.Store.AppendWebTurn(persistCtx, botTurn); err != nil {
		// Already streamed body; just log via writer is impossible.
		// Caller will see incomplete turn idx if they fetch session
		// later — acceptable trade-off for a chat playground.
		_ = err
	}

	// If bot signalled ENDCALL, optionally mark session ended so the
	// QC tab shows it as completed.
	if action == bot.ActionEndCall {
		_ = h.d.Store.EndWebSession(persistCtx, sess.ID, "ended", "")
	}
}

// helper: write a JSON line into the chunked body (unused for now —
// keeping for future SSE-style fallback).
func writeChunkLine(w io.Writer, prefix, line string) error {
	if _, err := fmt.Fprintf(w, "%s: %s\n", prefix, line); err != nil {
		return err
	}
	return nil
}

// errStreamClosed is returned when the bot stream ends before any chunks.
var errStreamClosed = errors.New("bot stream closed without output")

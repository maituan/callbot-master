package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"callbot-master/internal/auth"
	"callbot-master/internal/bot"
	"callbot-master/internal/store"
)

// WebStore is the slice of *store.Postgres the web playground needs.
// Bot lookups are reused from BotLookup; web_session CRUD is separate.
type WebStore interface {
	BotLookup
	CreateWebSession(ctx context.Context, s *store.WebSession) error
	EndWebSession(ctx context.Context, id uuid.UUID, status, errMsg string) error
	AppendWebTurn(ctx context.Context, t *store.WebTurn) error
	ListWebSessions(ctx context.Context, f store.WebSessionFilter) ([]*store.WebSession, error)
	GetWebSession(ctx context.Context, id uuid.UUID) (*store.WebSession, error)
}

// WebDeps wires the public web playground (chat + voice) and the
// authenticated share-mint + sessions QC endpoints.
type WebDeps struct {
	Issuer *auth.Issuer
	Store  WebStore

	// ChatTTL caps share tokens for chat (and voice) when callers don't
	// override via ttl_hours. Defaults to 7d.
	ChatTTL time.Duration

	// BotFactory builds a bot.Client given a BotConfig. Shared with the
	// session pipeline via the registry. Called fresh per chat turn.
	BotFactory func(*store.BotConfig) (bot.Client, error)

	// Voice config — same endpoint for every bot's web voice sessions.
	// The phone pipeline stays at 8 kHz; web voice runs 16 kHz for
	// noticeably better quality.
	VoiceASREndpoint string // "host:port" Viettel ASR 16k
	VoiceASRSampleRate int  // 16000
	VoiceTTSResampleRate int // 16000

	// VoiceRecordingDir, when non-empty, dumps each TTS turn's PCM as
	// WAV under <dir>/<session_id>/<idx>.wav so QC can listen back.
	VoiceRecordingDir string
}

// RegisterWeb mounts:
//
//	POST /api/v1/share/bots/{id}              — auth, mints bot-share token
//	GET  /api/v1/web/bot/{token}              — PUBLIC, bootstrap info
//	POST /api/v1/web/chat/{token}             — PUBLIC, streaming text reply
//	GET  /api/v1/web/voice/{token}            — PUBLIC, WebSocket upgrade
//	GET  /api/v1/web/sessions                 — auth, list (filter by bot/channel)
//	GET  /api/v1/web/sessions/{id}            — auth, detail incl. turns
func RegisterWeb(mux *http.ServeMux, d WebDeps) {
	if d.Issuer == nil || d.Store == nil {
		mux.HandleFunc("/api/v1/share/bots/", webDisabled)
		mux.HandleFunc("/api/v1/web/", webDisabled)
		mux.HandleFunc("/api/v1/web/sessions", webDisabled)
		return
	}
	if d.ChatTTL <= 0 {
		d.ChatTTL = 7 * 24 * time.Hour
	}
	h := &webHandler{d: d}
	mux.HandleFunc("/api/v1/share/bots/", h.mint)
	mux.HandleFunc("/api/v1/web/bot/", h.bootstrap)
	mux.HandleFunc("/api/v1/web/chat/", h.chat)
	mux.HandleFunc("/api/v1/web/voice/", h.voice)
	mux.HandleFunc("/api/v1/web/sessions", h.listSessions)
	mux.HandleFunc("/api/v1/web/sessions/", h.getSession)
}

func webDisabled(w http.ResponseWriter, _ *http.Request) {
	writeJSONError(w, http.StatusServiceUnavailable, "web playground not configured")
}

type webHandler struct{ d WebDeps }

// ── Mint share token (auth) ───────────────────────────────────────────

func (h *webHandler) mint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/share/bots/")
	if rest == "" {
		writeJSONError(w, http.StatusBadRequest, "missing bot id")
		return
	}
	botID, err := uuid.Parse(rest)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid bot id")
		return
	}
	b, err := h.d.Store.GetBotByID(r.Context(), botID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if b == nil {
		writeJSONError(w, http.StatusNotFound, "bot not found")
		return
	}
	if !id.IsPlatformAdmin() && (id.TenantID == nil || *id.TenantID != b.TenantID) {
		writeJSONError(w, http.StatusForbidden, "forbidden")
		return
	}

	var body struct {
		TTLHours int    `json:"ttl_hours,omitempty"`
		Channel  string `json:"channel,omitempty"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	ttl := h.d.ChatTTL
	if body.TTLHours > 0 {
		want := time.Duration(body.TTLHours) * time.Hour
		const max = 30 * 24 * time.Hour
		if want > max {
			want = max
		}
		ttl = want
	}
	channel := auth.BotShareChannel(body.Channel)
	if channel == "" {
		channel = auth.BotShareChannelBoth
	}

	tok, exp, err := h.d.Issuer.IssueBotShareToken(b.ID.String(), channel, ttl)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"bot_id":     b.ID.String(),
		"channel":    string(channel),
		"token":      tok,
		"expires_at": exp,
	})
}

// ── Public bootstrap ──────────────────────────────────────────────────

func (h *webHandler) bootstrap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tok := strings.TrimPrefix(r.URL.Path, "/api/v1/web/bot/")
	b, _, _, err := h.resolveToken(r.Context(), tok, "")
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, err.Error())
		return
	}
	_, channel, _, _ := h.d.Issuer.ParseBotShareToken(tok)
	writeJSON(w, http.StatusOK, map[string]any{
		"bot": map[string]any{
			"id":           b.ID.String(),
			"name":         b.Name,
			"slug":         b.Slug,
			"tenant_slug":  b.TenantSlug,
			"tts_voice_id": b.TTSVoiceID,
		},
		"channel":         string(channel),
		"chat_allowed":    auth.ChannelAllows(channel, auth.BotShareChannelChat),
		"voice_allowed":   auth.ChannelAllows(channel, auth.BotShareChannelVoice),
	})
}

// resolveToken validates token, fetches bot, and (if want != "") checks
// the channel grant covers want. Returns (bot, channel, iat, err).
func (h *webHandler) resolveToken(ctx context.Context, tok string, want auth.BotShareChannel) (*store.BotConfig, auth.BotShareChannel, time.Time, error) {
	if tok == "" {
		return nil, "", time.Time{}, errors.New("missing token")
	}
	botID, channel, iat, err := h.d.Issuer.ParseBotShareToken(tok)
	if err != nil {
		return nil, "", time.Time{}, errors.New("invalid or expired share link")
	}
	if want != "" && !auth.ChannelAllows(channel, want) {
		return nil, "", time.Time{}, fmt.Errorf("share link does not grant %s access", want)
	}
	id, err := uuid.Parse(botID)
	if err != nil {
		return nil, "", time.Time{}, errors.New("invalid token payload")
	}
	b, err := h.d.Store.GetBotByID(ctx, id)
	if err != nil {
		return nil, "", time.Time{}, err
	}
	if b == nil || !b.Enabled {
		return nil, "", time.Time{}, errors.New("bot unavailable")
	}
	return b, channel, iat, nil
}

// ── Sessions list (auth) ──────────────────────────────────────────────

func (h *webHandler) listSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	q := r.URL.Query()
	f := store.WebSessionFilter{
		Channel: q.Get("channel"),
		Status:  q.Get("status"),
	}
	if v := q.Get("bot_id"); v != "" {
		bid, err := uuid.Parse(v)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid bot_id")
			return
		}
		f.BotID = bid
	}
	if v := q.Get("limit"); v != "" {
		var n int
		_, _ = fmt.Sscanf(v, "%d", &n)
		f.Limit = n
	}
	if v := q.Get("offset"); v != "" {
		var n int
		_, _ = fmt.Sscanf(v, "%d", &n)
		f.Offset = n
	}
	if !id.IsPlatformAdmin() && id.TenantID != nil {
		f.TenantID = *id.TenantID
	}
	rows, err := h.d.Store.ListWebSessions(r.Context(), f)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(rows))
	for _, s := range rows {
		out = append(out, webSessionSummary(s))
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": out})
}

func (h *webHandler) getSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/web/sessions/")
	sid, err := uuid.Parse(rest)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid session id")
		return
	}
	s, err := h.d.Store.GetWebSession(r.Context(), sid)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if s == nil {
		writeJSONError(w, http.StatusNotFound, "session not found")
		return
	}
	if !id.IsPlatformAdmin() && (id.TenantID == nil || *id.TenantID != s.TenantID) {
		writeJSONError(w, http.StatusForbidden, "forbidden")
		return
	}
	writeJSON(w, http.StatusOK, webSessionDetail(s))
}

func webSessionSummary(s *store.WebSession) map[string]any {
	return map[string]any{
		"id":         s.ID.String(),
		"bot_id":     s.BotID.String(),
		"channel":    s.Channel,
		"started_at": s.StartedAt,
		"ended_at":   s.EndedAt,
		"status":     s.Status,
		"turns":      s.TotalTurns,
		"ip":         s.IP,
	}
}

func webSessionDetail(s *store.WebSession) map[string]any {
	turns := make([]map[string]any, 0, len(s.Turns))
	for _, t := range s.Turns {
		turns = append(turns, map[string]any{
			"id":                 t.ID.String(),
			"idx":                t.Idx,
			"role":               t.Role,
			"text":               t.Text,
			"audio_path":         t.AudioPath,
			"asr_partial_at":     t.ASRPartialAt,
			"asr_final_at":       t.ASRFinalAt,
			"bot_first_byte_at":  t.BotFirstByteAt,
			"bot_done_at":        t.BotDoneAt,
			"tts_first_audio_at": t.TTSFirstAudioAt,
			"tts_done_at":        t.TTSDoneAt,
			"action":             t.Action,
			"created_at":         t.CreatedAt,
		})
	}
	return map[string]any{
		"id":            s.ID.String(),
		"bot_id":        s.BotID.String(),
		"channel":       s.Channel,
		"started_at":    s.StartedAt,
		"ended_at":      s.EndedAt,
		"status":        s.Status,
		"total_turns":   s.TotalTurns,
		"recording_dir": s.RecordingDir,
		"ip":            s.IP,
		"user_agent":    s.UserAgent,
		"turns":         turns,
	}
}

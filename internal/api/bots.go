package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"callbot-master/internal/auth"
	"callbot-master/internal/store"
)

// BotsDeps wires the bot CRUD endpoints. Store must implement the bot
// CRUD slice; without it the routes return 503.
type BotsDeps struct {
	Store BotStore
}

// BotStore is the slice of *store.Postgres the bot API needs. Letting
// the api package depend on an interface keeps it pgx-free for tests.
type BotStore interface {
	BotLookup // GetBotByID, GetTenantBySlug, GetBotByTenantAndSlug
	ListBots(ctx context.Context, tenantID *uuid.UUID) ([]*store.BotConfig, error)
	ListTenants(ctx context.Context) ([]*store.Tenant, error)
	CreateBot(ctx context.Context, in store.BotWriteInput) (uuid.UUID, error)
	UpdateBot(ctx context.Context, id uuid.UUID, in store.BotWriteInput, expectedVersion int) error
	SoftDeleteBot(ctx context.Context, id uuid.UUID) error
	AddDID(ctx context.Context, did string, botID uuid.UUID, note string) error
	RemoveDID(ctx context.Context, did string) error
	ListDIDs(ctx context.Context, botID uuid.UUID) ([]store.DIDRecord, error)
}

// RegisterBots mounts /api/v1/bots and /api/v1/bots/{id}* on the given mux.
// Tenant routes live in tenants.go (separate handler, full CRUD).
func RegisterBots(mux *http.ServeMux, d BotsDeps) {
	if d.Store == nil {
		mux.HandleFunc("/api/v1/bots", botsDisabled)
		mux.HandleFunc("/api/v1/bots/", botsDisabled)
		return
	}
	h := &botsHandler{d: d}
	mux.HandleFunc("/api/v1/bots", h.collection)
	mux.HandleFunc("/api/v1/bots/", h.item)
}

type botsHandler struct{ d BotsDeps }

func botsDisabled(w http.ResponseWriter, _ *http.Request) {
	writeJSONError(w, http.StatusServiceUnavailable, "bot store not configured")
}

// ── Bots ──────────────────────────────────────────────────────────────

func (h *botsHandler) collection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.list(w, r)
	case http.MethodPost:
		h.create(w, r)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *botsHandler) item(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/bots/")
	if rest == "" {
		writeJSONError(w, http.StatusNotFound, "missing bot id")
		return
	}
	parts := strings.Split(rest, "/")
	id, err := uuid.Parse(parts[0])
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid bot id")
		return
	}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			h.get(w, r, id)
		case http.MethodPatch:
			h.update(w, r, id)
		case http.MethodDelete:
			h.delete(w, r, id)
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}
	if len(parts) == 2 && parts[1] == "dids" {
		switch r.Method {
		case http.MethodGet:
			h.listDIDs(w, r, id)
		case http.MethodPost:
			h.addDID(w, r, id)
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}
	if len(parts) == 3 && parts[1] == "dids" {
		// /api/v1/bots/{id}/dids/{did}
		if r.Method != http.MethodDelete {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		h.removeDID(w, r, id, parts[2])
		return
	}
	if len(parts) == 2 && parts[1] == "test" {
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		h.test(w, r, id)
		return
	}
	writeJSONError(w, http.StatusNotFound, "no such route")
}

func (h *botsHandler) list(w http.ResponseWriter, r *http.Request) {
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	scope := id.TenantID
	if id.IsPlatformAdmin() {
		scope = nil // sees every tenant
	}
	bots, err := h.d.Store.ListBots(r.Context(), scope)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "list bots: "+err.Error())
		return
	}
	out := make([]map[string]any, 0, len(bots))
	for _, b := range bots {
		out = append(out, botSummary(b))
	}
	writeJSON(w, http.StatusOK, map[string]any{"bots": out})
}

func (h *botsHandler) get(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	bot, err := h.d.Store.GetBotByID(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "lookup bot: "+err.Error())
		return
	}
	if bot == nil {
		writeJSONError(w, http.StatusNotFound, "bot not found")
		return
	}
	if !canAccessBot(r.Context(), bot) {
		writeJSONError(w, http.StatusForbidden, "forbidden")
		return
	}
	dids, err := h.d.Store.ListDIDs(r.Context(), bot.ID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "list dids: "+err.Error())
		return
	}
	body := botDetail(bot)
	body["dids"] = didListJSON(dids)
	writeJSON(w, http.StatusOK, body)
}

func (h *botsHandler) create(w http.ResponseWriter, r *http.Request) {
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	var req botWriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}

	tenantID, err := h.resolveTargetTenant(id, &req)
	if err != nil {
		writeJSONError(w, http.StatusForbidden, err.Error())
		return
	}
	in, err := req.toWriteInput(tenantID, &id.UserID, /*forceReplaceTokens*/ true)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	newID, err := h.d.Store.CreateBot(r.Context(), in)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "create bot: "+err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": newID})
}

func (h *botsHandler) update(w http.ResponseWriter, r *http.Request, botID uuid.UUID) {
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	existing, err := h.d.Store.GetBotByID(r.Context(), botID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "lookup bot: "+err.Error())
		return
	}
	if existing == nil {
		writeJSONError(w, http.StatusNotFound, "bot not found")
		return
	}
	if !canAccessBot(r.Context(), existing) {
		writeJSONError(w, http.StatusForbidden, "forbidden")
		return
	}
	var req botWriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Version == nil {
		writeJSONError(w, http.StatusBadRequest, "version is required for update")
		return
	}
	// Tenant cannot move on update.
	in, err := req.toWriteInput(existing.TenantID, &id.UserID, /*forceReplaceTokens*/ false)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.d.Store.UpdateBot(r.Context(), botID, in, *req.Version); err != nil {
		if errors.Is(err, store.ErrVersionMismatch) {
			writeJSONError(w, http.StatusConflict, "stale version — refresh the form")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "update bot: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *botsHandler) delete(w http.ResponseWriter, r *http.Request, botID uuid.UUID) {
	bot, err := h.d.Store.GetBotByID(r.Context(), botID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "lookup bot: "+err.Error())
		return
	}
	if bot == nil {
		writeJSONError(w, http.StatusNotFound, "bot not found")
		return
	}
	if !canAccessBot(r.Context(), bot) {
		writeJSONError(w, http.StatusForbidden, "forbidden")
		return
	}
	if err := h.d.Store.SoftDeleteBot(r.Context(), botID); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "delete: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ── DIDs ──────────────────────────────────────────────────────────────

func (h *botsHandler) listDIDs(w http.ResponseWriter, r *http.Request, botID uuid.UUID) {
	bot, err := h.d.Store.GetBotByID(r.Context(), botID)
	if err != nil || bot == nil {
		writeJSONError(w, http.StatusNotFound, "bot not found")
		return
	}
	if !canAccessBot(r.Context(), bot) {
		writeJSONError(w, http.StatusForbidden, "forbidden")
		return
	}
	dids, err := h.d.Store.ListDIDs(r.Context(), botID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"dids": didListJSON(dids)})
}

func (h *botsHandler) addDID(w http.ResponseWriter, r *http.Request, botID uuid.UUID) {
	bot, err := h.d.Store.GetBotByID(r.Context(), botID)
	if err != nil || bot == nil {
		writeJSONError(w, http.StatusNotFound, "bot not found")
		return
	}
	if !canAccessBot(r.Context(), bot) {
		writeJSONError(w, http.StatusForbidden, "forbidden")
		return
	}
	var body struct {
		DID  string `json:"did"`
		Note string `json:"note,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	body.DID = strings.TrimSpace(body.DID)
	if body.DID == "" {
		writeJSONError(w, http.StatusBadRequest, "did is required")
		return
	}
	if err := h.d.Store.AddDID(r.Context(), body.DID, botID, body.Note); err != nil {
		if errors.Is(err, store.ErrDIDTaken) {
			writeJSONError(w, http.StatusConflict, "DID already assigned")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"did": body.DID})
}

func (h *botsHandler) removeDID(w http.ResponseWriter, r *http.Request, botID uuid.UUID, did string) {
	bot, err := h.d.Store.GetBotByID(r.Context(), botID)
	if err != nil || bot == nil {
		writeJSONError(w, http.StatusNotFound, "bot not found")
		return
	}
	if !canAccessBot(r.Context(), bot) {
		writeJSONError(w, http.StatusForbidden, "forbidden")
		return
	}
	if err := h.d.Store.RemoveDID(r.Context(), did); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ── shared helpers ────────────────────────────────────────────────────

// canAccessBot checks tenant scope. platform_admin sees everything;
// tenant_user only sees its own tenant.
func canAccessBot(ctx context.Context, b *store.BotConfig) bool {
	id := auth.FromContext(ctx)
	if id == nil {
		return false
	}
	if id.IsPlatformAdmin() {
		return true
	}
	return id.TenantID != nil && *id.TenantID == b.TenantID
}

// resolveTargetTenant picks the tenant for a NEW bot. tenant_user can
// only create in its own tenant; platform_admin must specify a tenant
// in the request body.
func (h *botsHandler) resolveTargetTenant(id *auth.Identity, req *botWriteRequest) (uuid.UUID, error) {
	if !id.IsPlatformAdmin() {
		if id.TenantID == nil {
			return uuid.Nil, fmt.Errorf("tenant_user has no tenant")
		}
		return *id.TenantID, nil
	}
	// platform_admin
	if req.TenantID != nil {
		return *req.TenantID, nil
	}
	if req.TenantSlug != "" {
		t, err := h.d.Store.GetTenantBySlug(context.Background(), req.TenantSlug)
		if err != nil || t == nil {
			return uuid.Nil, fmt.Errorf("tenant %q not found", req.TenantSlug)
		}
		return t.ID, nil
	}
	return uuid.Nil, fmt.Errorf("admin must specify tenant_id or tenant_slug")
}

// botSummary is the row-shaped JSON for the list page (no tokens).
func botSummary(b *store.BotConfig) map[string]any {
	return map[string]any{
		"id":              b.ID,
		"tenant_id":       b.TenantID,
		"tenant_slug":     b.TenantSlug,
		"slug":            b.Slug,
		"name":            b.Name,
		"enabled":         b.Enabled,
		"bot_url":         b.BotURL,
		"asr_endpoint":    b.ASREndpoint,
		"tts_endpoint":    b.TTSEndpoint,
		"tts_voice_id":    b.TTSVoiceID,
		"bargein_enabled": b.BargeInEnabled,
		"version":         b.Version,
		"updated_at":      b.UpdatedAt,
	}
}

// botDetail is the full-form JSON for the edit page (tokens masked).
func botDetail(b *store.BotConfig) map[string]any {
	out := botSummary(b)
	out["bot_first_byte_timeout_ms"] = b.BotFirstByteTimeoutMs
	out["bot_total_timeout_ms"] = b.BotTotalTimeoutMs
	out["asr_provider"] = b.ASRProvider
	out["asr_token_mask"] = maskToken(b.ASRToken)
	out["tts_provider"] = b.TTSProvider
	out["tts_token_mask"] = maskToken(b.TTSToken)
	out["tts_tempo"] = b.TTSTempo
	out["asr_silence_timeout_sec"] = b.ASRSilenceTimeoutSec
	out["asr_speech_timeout_sec"] = b.ASRSpeechTimeoutSec
	out["asr_speech_max_sec"] = b.ASRSpeechMaxSec
	out["asr_single_sentence"] = b.ASRSingleSentence
	out["bargein_min_words"] = b.BargeInMinWords
	out["filler_enabled"] = b.FillerEnabled
	out["created_at"] = b.CreatedAt
	return out
}

// maskToken returns "" for an empty token, "••••" for a short one, or
// "••••<last4>" so the UI can show that a token is set without revealing
// the secret. Plaintext tokens never leave the server.
func maskToken(t string) string {
	if t == "" {
		return ""
	}
	if len(t) <= 4 {
		return "••••"
	}
	return "••••" + t[len(t)-4:]
}

func didListJSON(ds []store.DIDRecord) []map[string]any {
	out := make([]map[string]any, 0, len(ds))
	for _, d := range ds {
		out = append(out, map[string]any{
			"did":        d.DID,
			"note":       d.Note,
			"created_at": d.CreatedAt,
		})
	}
	return out
}

// botWriteRequest is the JSON shape for create+update. Pointers
// distinguish "field omitted" from "field set to zero value" — needed
// for the token round-trip (omitted = preserve existing).
type botWriteRequest struct {
	TenantID   *uuid.UUID `json:"tenant_id,omitempty"`
	TenantSlug string     `json:"tenant_slug,omitempty"`
	Slug       string     `json:"slug"`
	Name       string     `json:"name"`
	Enabled    *bool      `json:"enabled,omitempty"`

	BotURL                string `json:"bot_url"`
	BotFirstByteTimeoutMs int    `json:"bot_first_byte_timeout_ms"`
	BotTotalTimeoutMs     int    `json:"bot_total_timeout_ms"`

	ASRProvider string  `json:"asr_provider"`
	ASREndpoint string  `json:"asr_endpoint"`
	ASRToken    *string `json:"asr_token,omitempty"` // nil → preserve existing

	TTSProvider string  `json:"tts_provider"`
	TTSEndpoint string  `json:"tts_endpoint"`
	TTSToken    *string `json:"tts_token,omitempty"`

	TTSVoiceID           string  `json:"tts_voice_id"`
	TTSTempo             float64 `json:"tts_tempo"`
	ASRSilenceTimeoutSec int     `json:"asr_silence_timeout_sec"`
	ASRSpeechTimeoutSec  int     `json:"asr_speech_timeout_sec"`
	ASRSpeechMaxSec      int     `json:"asr_speech_max_sec"`
	ASRSingleSentence    *bool   `json:"asr_single_sentence,omitempty"`

	BargeInEnabled  *bool `json:"bargein_enabled,omitempty"`
	BargeInMinWords int   `json:"bargein_min_words"`
	FillerEnabled   *bool `json:"filler_enabled,omitempty"`

	Version *int `json:"version,omitempty"` // required on UPDATE
}

// toWriteInput converts a request to the store layer's input. forceReplaceTokens
// is true on CREATE (we always store what the user typed, even empty).
// On UPDATE, ReplaceXXXToken is set only when the field was explicitly
// present in the JSON body (asr_token != nil).
func (req *botWriteRequest) toWriteInput(tenantID uuid.UUID, actor *uuid.UUID, forceReplaceTokens bool) (store.BotWriteInput, error) {
	if req.Slug == "" || req.Name == "" || req.BotURL == "" || req.ASREndpoint == "" || req.TTSEndpoint == "" {
		return store.BotWriteInput{}, fmt.Errorf("slug, name, bot_url, asr_endpoint, tts_endpoint are required")
	}
	asrToken := ""
	replaceASR := forceReplaceTokens
	if req.ASRToken != nil {
		asrToken = *req.ASRToken
		replaceASR = true
	}
	ttsToken := ""
	replaceTTS := forceReplaceTokens
	if req.TTSToken != nil {
		ttsToken = *req.TTSToken
		replaceTTS = true
	}
	return store.BotWriteInput{
		TenantID:              tenantID,
		Slug:                  req.Slug,
		Name:                  req.Name,
		Enabled:               coalesceBool(req.Enabled, true),
		BotURL:                req.BotURL,
		BotFirstByteTimeoutMs: defaultInt(req.BotFirstByteTimeoutMs, 5000),
		BotTotalTimeoutMs:     defaultInt(req.BotTotalTimeoutMs, 25000),
		ASRProvider:           defaultStr(req.ASRProvider, "viettel"),
		ASREndpoint:           req.ASREndpoint,
		ASRToken:              asrToken,
		ReplaceASRToken:       replaceASR,
		TTSProvider:           defaultStr(req.TTSProvider, "viettel"),
		TTSEndpoint:           req.TTSEndpoint,
		TTSToken:              ttsToken,
		ReplaceTTSToken:       replaceTTS,
		TTSVoiceID:            req.TTSVoiceID,
		TTSTempo:              defaultFloat(req.TTSTempo, 1.0),
		ASRSilenceTimeoutSec:  defaultInt(req.ASRSilenceTimeoutSec, 5),
		ASRSpeechTimeoutSec:   defaultInt(req.ASRSpeechTimeoutSec, 1),
		ASRSpeechMaxSec:       defaultInt(req.ASRSpeechMaxSec, 30),
		ASRSingleSentence:     coalesceBool(req.ASRSingleSentence, true),
		BargeInEnabled:        coalesceBool(req.BargeInEnabled, true),
		BargeInMinWords:       defaultInt(req.BargeInMinWords, 3),
		FillerEnabled:         coalesceBool(req.FillerEnabled, false),
		ActorUserID:           actor,
	}, nil
}

func coalesceBool(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}
func defaultInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}
func defaultStr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
func defaultFloat(v, def float64) float64 {
	if v == 0 {
		return def
	}
	return v
}

// ── Dry-run test ───────────────────────────────────────────────────────

// test runs cheap reachability probes against the bot's configured
// endpoints and returns a per-component status map. Doesn't actually
// open ASR/TTS streams (avoids burning real provider quota); this is
// just "can the network see the host?". Real e2e probes would mean
// pumping silent PCM through ASR + sending a dummy text to TTS, which
// can be added later when the UI exposes a fuller "test call" mode.
func (h *botsHandler) test(w http.ResponseWriter, r *http.Request, botID uuid.UUID) {
	bot, err := h.d.Store.GetBotByID(r.Context(), botID)
	if err != nil || bot == nil {
		writeJSONError(w, http.StatusNotFound, "bot not found")
		return
	}
	if !canAccessBot(r.Context(), bot) {
		writeJSONError(w, http.StatusForbidden, "forbidden")
		return
	}

	type probe struct {
		Component string `json:"component"`
		Endpoint  string `json:"endpoint"`
		OK        bool   `json:"ok"`
		LatencyMs int64  `json:"latency_ms"`
		Err       string `json:"error,omitempty"`
	}
	results := []probe{
		runHTTPProbe(r.Context(), "bot", bot.BotURL),
		runTCPProbe(r.Context(), "asr", bot.ASREndpoint),
		runWSProbe(r.Context(), "tts", bot.TTSEndpoint),
	}
	overall := true
	for _, p := range results {
		if !p.OK {
			overall = false
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      overall,
		"results": results,
	})
}

// runHTTPProbe does a low-cost OPTIONS/HEAD to the bot URL — we treat
// any non-5xx response as "host is alive".
func runHTTPProbe(ctx context.Context, name, raw string) (out struct {
	Component string `json:"component"`
	Endpoint  string `json:"endpoint"`
	OK        bool   `json:"ok"`
	LatencyMs int64  `json:"latency_ms"`
	Err       string `json:"error,omitempty"`
}) {
	out.Component = name
	out.Endpoint = raw
	if raw == "" {
		out.Err = "endpoint not set"
		return
	}
	start := time.Now()
	defer func() { out.LatencyMs = time.Since(start).Milliseconds() }()

	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodHead, raw, nil)
	if err != nil {
		out.Err = err.Error()
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		out.Err = err.Error()
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		out.Err = fmt.Sprintf("http %d", resp.StatusCode)
		return
	}
	out.OK = true
	return
}

// runTCPProbe dials the host:port — used for ASR (gRPC). A successful
// dial is enough to confirm L4 reachability + token correctness can't
// be checked here without burning a real session.
func runTCPProbe(ctx context.Context, name, hostport string) (out struct {
	Component string `json:"component"`
	Endpoint  string `json:"endpoint"`
	OK        bool   `json:"ok"`
	LatencyMs int64  `json:"latency_ms"`
	Err       string `json:"error,omitempty"`
}) {
	out.Component = name
	out.Endpoint = hostport
	if hostport == "" {
		out.Err = "endpoint not set"
		return
	}
	start := time.Now()
	defer func() { out.LatencyMs = time.Since(start).Milliseconds() }()

	d := net.Dialer{Timeout: 3 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", hostport)
	if err != nil {
		out.Err = err.Error()
		return
	}
	_ = conn.Close()
	out.OK = true
	return
}

// runWSProbe checks the WS host + port (parses ws://host:port/path,
// dials the TCP layer). Doesn't perform the WS handshake — the auth
// step needs real credentials and isn't worth the noise here.
func runWSProbe(ctx context.Context, name, raw string) (out struct {
	Component string `json:"component"`
	Endpoint  string `json:"endpoint"`
	OK        bool   `json:"ok"`
	LatencyMs int64  `json:"latency_ms"`
	Err       string `json:"error,omitempty"`
}) {
	out.Component = name
	out.Endpoint = raw
	if raw == "" {
		out.Err = "endpoint not set"
		return
	}
	u, err := url.Parse(raw)
	if err != nil {
		out.Err = err.Error()
		return
	}
	hp := u.Host
	if !strings.Contains(hp, ":") {
		// supply a default port matching the scheme
		switch u.Scheme {
		case "wss", "https":
			hp += ":443"
		default:
			hp += ":80"
		}
	}
	tcp := runTCPProbe(ctx, name, hp)
	out.LatencyMs = tcp.LatencyMs
	out.OK = tcp.OK
	out.Err = tcp.Err
	return
}

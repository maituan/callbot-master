package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"callbot-master/internal/auth"
	"callbot-master/internal/pipeline"
	"callbot-master/internal/store"
)

// OriginateDeps wires the ad-hoc single-call endpoint. Skipped (503) if
// any field is nil.
type OriginateDeps struct {
	Outbound        *pipeline.OutboundHandler
	BotLookup       BotLookup
	DefaultCallerID string
	DefaultTenantSlug string
}

func RegisterOriginate(mux *http.ServeMux, d OriginateDeps) {
	if d.Outbound == nil || d.BotLookup == nil {
		mux.HandleFunc("/api/v1/calls/originate", originateDisabled)
		return
	}
	h := &originateHandler{d: d}
	mux.HandleFunc("/api/v1/calls/originate", h.handle)
}

type originateHandler struct{ d OriginateDeps }

func originateDisabled(w http.ResponseWriter, _ *http.Request) {
	writeJSONError(w, http.StatusServiceUnavailable, "outbound not configured")
}

// handle dials a single phone number with the chosen bot. Returns the
// call UUID immediately — the actual call lifecycle continues in the
// outbound handler (CHANNEL_ANSWER → SessionRunner.Run).
func (h *originateHandler) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	var body struct {
		BotID      string `json:"bot_id"`
		BotSlug    string `json:"bot_slug"`
		TenantSlug string `json:"tenant_slug"`
		Phone      string `json:"phone"`
		CallerID   string `json:"caller_id"`
		Name       string `json:"name,omitempty"`
		LeadID     string `json:"lead_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if body.Phone == "" {
		writeJSONError(w, http.StatusBadRequest, "phone is required")
		return
	}

	bot, err := h.resolveBot(r.Context(), body.BotID, body.BotSlug, body.TenantSlug)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if bot == nil {
		writeJSONError(w, http.StatusNotFound, "bot not found")
		return
	}
	// Tenant scope: tenant_user can only originate via bots in their tenant.
	if !id.IsPlatformAdmin() && (id.TenantID == nil || *id.TenantID != bot.TenantID) {
		writeJSONError(w, http.StatusForbidden, "bot not in your tenant")
		return
	}

	caller := body.CallerID
	if caller == "" {
		caller = h.d.DefaultCallerID
	}

	// Optional lead metadata travels through CustomData → RunOpts → call_history.
	custom := map[string]any{}
	if body.Name != "" {
		custom["name"] = body.Name
	}
	if body.LeadID != "" {
		custom["lead_id"] = body.LeadID
	}

	callUUID, err := h.d.Outbound.Originate(r.Context(), pipeline.OriginateOpts{
		Phone:      body.Phone,
		CallerID:   caller,
		Bot:        bot,
		CustomData: custom,
		// No OnAnswered/OnEnded — ad-hoc call, status is in call_history.
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "originate: "+err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"call_uuid":   callUUID,
		"phone":       body.Phone,
		"caller_id":   caller,
		"bot_id":      bot.ID,
		"bot_slug":    bot.Slug,
		"tenant_slug": bot.TenantSlug,
		"status":      "originated",
	})
}

func (h *originateHandler) resolveBot(ctx context.Context, botID, botSlug, tenantSlug string) (*store.BotConfig, error) {
	if botID != "" {
		id, err := uuid.Parse(botID)
		if err != nil {
			return nil, err
		}
		return h.d.BotLookup.GetBotByID(ctx, id)
	}
	if botSlug == "" {
		return nil, nil // caller must give bot_id or bot_slug
	}
	ts := tenantSlug
	if ts == "" {
		ts = h.d.DefaultTenantSlug
	}
	if ts == "" {
		return nil, nil
	}
	t, err := h.d.BotLookup.GetTenantBySlug(ctx, ts)
	if err != nil || t == nil {
		return nil, err
	}
	return h.d.BotLookup.GetBotByTenantAndSlug(ctx, t.ID, botSlug)
}

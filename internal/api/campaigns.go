package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"callbot-master/internal/campaign"
	"callbot-master/internal/store"
)

// CampaignDeps wires the API to the campaign manager + outbound originate.
// The originate function is provided by pipeline.OutboundHandler. BindFunc
// receives the manager + campaign + the resolved bot snapshot used for
// every lead in this campaign.
type CampaignDeps struct {
	Manager  *campaign.Manager
	BindFunc func(*campaign.Manager, *campaign.Campaign, *store.BotConfig) campaign.OriginateFunc
	// BotLookup resolves a bot by id or slug at campaign create time.
	// At least one of ByID / BySlug must be provided; the API returns
	// 503 otherwise.
	BotLookup BotLookup
	// DefaultBotSlug + DefaultTenantSlug let legacy callers (curl
	// scripts) skip the bot fields entirely.
	DefaultTenantSlug string
	DefaultBotSlug    string
	DefaultCallerID   string
	// MaxUploadBytes caps multipart parsing (CSV size). 32MB default.
	MaxUploadBytes int64
}

// BotLookup is the slice of store.Postgres campaign create needs.
type BotLookup interface {
	GetBotByID(ctx context.Context, id uuid.UUID) (*store.BotConfig, error)
	GetTenantBySlug(ctx context.Context, slug string) (*store.Tenant, error)
	GetBotByTenantAndSlug(ctx context.Context, tenantID uuid.UUID, slug string) (*store.BotConfig, error)
}

// RegisterCampaigns mounts /api/v1/campaigns routes on the given mux.
func RegisterCampaigns(mux *http.ServeMux, d CampaignDeps) {
	if d.MaxUploadBytes <= 0 {
		d.MaxUploadBytes = 32 << 20
	}
	if d.DefaultCallerID == "" {
		d.DefaultCallerID = "callbot"
	}
	h := &campaignHandler{d: d}

	mux.HandleFunc("/api/v1/campaigns", h.collection)
	mux.HandleFunc("/api/v1/campaigns/", h.item) // trailing slash → /api/v1/campaigns/{id}[/cancel]
}

type campaignHandler struct {
	d CampaignDeps
}

func (h *campaignHandler) collection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.list(w, r)
	case http.MethodPost:
		h.create(w, r)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// item routes /api/v1/campaigns/{id} and /api/v1/campaigns/{id}/cancel.
func (h *campaignHandler) item(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/campaigns/")
	if rest == "" {
		writeJSONError(w, http.StatusNotFound, "missing campaign id")
		return
	}
	parts := strings.SplitN(rest, "/", 2)
	id := parts[0]

	if len(parts) == 2 {
		switch parts[1] {
		case "cancel":
			if r.Method != http.MethodPost {
				writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			h.cancel(w, r, id)
			return
		default:
			writeJSONError(w, http.StatusNotFound, "unknown sub-resource")
			return
		}
	}

	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	h.get(w, r, id)
}

func (h *campaignHandler) list(w http.ResponseWriter, _ *http.Request) {
	cs := h.d.Manager.List()
	type listEntry struct {
		ID        string           `json:"id"`
		Status    string           `json:"status"`
		Scenario  string           `json:"scenario"`
		CallerID  string           `json:"caller_id"`
		CCU       int              `json:"ccu"`
		Stats     campaign.Stats   `json:"stats"`
		CreatedAt string           `json:"created_at"`
	}
	out := make([]listEntry, 0, len(cs))
	for _, c := range cs {
		out = append(out, listEntry{
			ID:        c.ID,
			Status:    c.Status,
			Scenario:  c.Scenario,
			CallerID:  c.CallerID,
			CCU:       c.CCU,
			Stats:     c.Stats(),
			CreatedAt: c.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"campaigns": out})
}

func (h *campaignHandler) get(w http.ResponseWriter, _ *http.Request, id string) {
	c := h.d.Manager.Get(id)
	if c == nil {
		writeJSONError(w, http.StatusNotFound, "campaign not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":         c.ID,
		"status":     c.Status,
		"scenario":   c.Scenario,
		"caller_id":  c.CallerID,
		"ccu":        c.CCU,
		"stats":      c.Stats(),
		"leads":      c.Leads,
		"created_at": c.CreatedAt,
	})
}

func (h *campaignHandler) cancel(w http.ResponseWriter, _ *http.Request, id string) {
	if !h.d.Manager.Cancel(id) {
		writeJSONError(w, http.StatusNotFound, "campaign not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": "canceled"})
}

func (h *campaignHandler) create(w http.ResponseWriter, r *http.Request) {
	if h.d.BindFunc == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "outbound originate not wired")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, h.d.MaxUploadBytes)
	if err := r.ParseMultipartForm(h.d.MaxUploadBytes); err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("parse multipart: %v", err))
		return
	}

	if h.d.BotLookup == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "bot lookup not wired")
		return
	}
	bot, err := h.resolveBot(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if bot == nil {
		writeJSONError(w, http.StatusNotFound, "bot not found")
		return
	}
	callerID := r.FormValue("caller_id")
	if callerID == "" {
		callerID = h.d.DefaultCallerID
	}
	ccu := 1
	if v := r.FormValue("ccu"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeJSONError(w, http.StatusBadRequest, "ccu must be a positive integer")
			return
		}
		ccu = n
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "file field is required")
		return
	}
	defer file.Close()

	leads, err := campaign.ParseCSV(file)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("parse csv: %v", err))
		return
	}
	if len(leads) == 0 {
		writeJSONError(w, http.StatusBadRequest, "CSV contained no leads")
		return
	}

	// Pre-declare so the originate closure can capture the campaign pointer
	// (needed for status callbacks). The closure runs after Create returns,
	// at which point c is assigned.
	var c *campaign.Campaign
	var bound campaign.OriginateFunc
	c = h.d.Manager.Create(context.Background(), campaign.CreateOpts{
		Scenario: bot.Slug,
		CallerID: callerID,
		CCU:      ccu,
		Leads:    leads,
	}, func(ctx context.Context, phone, cid, sc string, cd map[string]any) (string, error) {
		if bound == nil {
			bound = h.d.BindFunc(h.d.Manager, c, bot)
		}
		return bound(ctx, phone, cid, sc, cd)
	})

	writeJSON(w, http.StatusCreated, map[string]any{
		"id":          c.ID,
		"status":      c.Status,
		"total":       len(leads),
		"bot_id":      bot.ID,
		"bot_slug":    bot.Slug,
		"tenant_slug": bot.TenantSlug,
		"caller_id":   callerID,
		"ccu":         ccu,
		"created_at":  c.CreatedAt,
	})
}

// resolveBot picks the bot for a campaign create request. Precedence:
//   1. bot_id (UUID) — exact lookup
//   2. bot_slug + tenant_slug (or DefaultTenantSlug)
//   3. DefaultBotSlug under DefaultTenantSlug — legacy fallback
func (h *campaignHandler) resolveBot(r *http.Request) (*store.BotConfig, error) {
	ctx := r.Context()
	if v := r.FormValue("bot_id"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			return nil, fmt.Errorf("bot_id: %w", err)
		}
		return h.d.BotLookup.GetBotByID(ctx, id)
	}
	tenantSlug := r.FormValue("tenant_slug")
	if tenantSlug == "" {
		tenantSlug = h.d.DefaultTenantSlug
	}
	botSlug := r.FormValue("bot_slug")
	if botSlug == "" {
		// Legacy alias: campaigns used to take "scenario".
		botSlug = r.FormValue("scenario")
	}
	if botSlug == "" {
		botSlug = h.d.DefaultBotSlug
	}
	if tenantSlug == "" || botSlug == "" {
		return nil, fmt.Errorf("bot_id or (tenant_slug + bot_slug) required")
	}
	t, err := h.d.BotLookup.GetTenantBySlug(ctx, tenantSlug)
	if err != nil {
		return nil, fmt.Errorf("tenant lookup: %w", err)
	}
	if t == nil {
		return nil, fmt.Errorf("tenant %q not found", tenantSlug)
	}
	return h.d.BotLookup.GetBotByTenantAndSlug(ctx, t.ID, botSlug)
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil && !errors.Is(err, http.ErrBodyNotAllowed) {
		// Headers already sent; nothing useful to do.
		return
	}
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

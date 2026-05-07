package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"callbot-master/internal/auth"
	"callbot-master/internal/store"
)

// TenantsDeps wires the /api/v1/tenants endpoints. Mutating endpoints
// require platform_admin; read returns scoped data.
type TenantsDeps struct {
	Store   TenantStore
	Auditor AuditWriter
}

type TenantStore interface {
	ListTenants(ctx context.Context) ([]*store.Tenant, error)
	GetTenantByID(ctx context.Context, id uuid.UUID) (*store.Tenant, error)
	GetTenantBySlug(ctx context.Context, slug string) (*store.Tenant, error)
	CreateTenant(ctx context.Context, slug, name string) (uuid.UUID, error)
	UpdateTenant(ctx context.Context, id uuid.UUID, name string, enabled bool) error
	DeleteTenant(ctx context.Context, id uuid.UUID) error
}

// RegisterTenants overrides any earlier tenant route registration. We
// deliberately mount the same path here as the bots package did so the
// fuller CRUD wins; main.go is expected to call this AFTER RegisterBots.
func RegisterTenants(mux *http.ServeMux, d TenantsDeps) {
	if d.Store == nil {
		return
	}
	h := &tenantsHandler{d: d}
	mux.HandleFunc("/api/v1/tenants", h.collection)
	mux.HandleFunc("/api/v1/tenants/", h.item)
}

type tenantsHandler struct{ d TenantsDeps }

func (h *tenantsHandler) collection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.list(w, r)
	case http.MethodPost:
		h.create(w, r)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *tenantsHandler) item(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/tenants/")
	if rest == "" {
		writeJSONError(w, http.StatusNotFound, "missing tenant id")
		return
	}
	id, err := uuid.Parse(rest)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid tenant id")
		return
	}
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
}

func (h *tenantsHandler) list(w http.ResponseWriter, r *http.Request) {
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	ts, err := h.d.Store.ListTenants(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !id.IsPlatformAdmin() {
		filtered := make([]*store.Tenant, 0, 1)
		for _, t := range ts {
			if id.TenantID != nil && t.ID == *id.TenantID {
				filtered = append(filtered, t)
			}
		}
		ts = filtered
	}
	out := make([]map[string]any, 0, len(ts))
	for _, t := range ts {
		out = append(out, tenantJSON(t))
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenants": out})
}

func (h *tenantsHandler) get(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	t, err := h.d.Store.GetTenantByID(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if t == nil {
		writeJSONError(w, http.StatusNotFound, "tenant not found")
		return
	}
	caller := auth.FromContext(r.Context())
	if caller == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	if !caller.IsPlatformAdmin() && (caller.TenantID == nil || *caller.TenantID != t.ID) {
		writeJSONError(w, http.StatusForbidden, "forbidden")
		return
	}
	writeJSON(w, http.StatusOK, tenantJSON(t))
}

func (h *tenantsHandler) create(w http.ResponseWriter, r *http.Request) {
	if !requireAdmin(w, r) {
		return
	}
	var body struct {
		Slug string `json:"slug"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	body.Slug = strings.TrimSpace(body.Slug)
	body.Name = strings.TrimSpace(body.Name)
	if body.Slug == "" || body.Name == "" {
		writeJSONError(w, http.StatusBadRequest, "slug and name required")
		return
	}
	tid, err := h.d.Store.CreateTenant(r.Context(), body.Slug, body.Name)
	if err != nil {
		if errors.Is(err, store.ErrSlugTaken) {
			writeJSONError(w, http.StatusConflict, "slug already in use")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	recordAudit(h.d.Auditor, r, &tid, "tenant.create", "tenant", tid.String(), nil,
		map[string]any{"slug": body.Slug, "name": body.Name})
	writeJSON(w, http.StatusCreated, map[string]any{"id": tid, "slug": body.Slug, "name": body.Name})
}

func (h *tenantsHandler) update(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	if !requireAdmin(w, r) {
		return
	}
	var body struct {
		Name    string `json:"name"`
		Enabled *bool  `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if body.Name == "" {
		writeJSONError(w, http.StatusBadRequest, "name required")
		return
	}
	enabled := true
	if body.Enabled != nil {
		enabled = *body.Enabled
	}
	if err := h.d.Store.UpdateTenant(r.Context(), id, body.Name, enabled); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "tenant not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	recordAudit(h.d.Auditor, r, &id, "tenant.update", "tenant", id.String(), nil,
		map[string]any{"name": body.Name, "enabled": enabled})
	w.WriteHeader(http.StatusNoContent)
}

func (h *tenantsHandler) delete(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	if !requireAdmin(w, r) {
		return
	}
	if err := h.d.Store.DeleteTenant(r.Context(), id); err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeJSONError(w, http.StatusNotFound, "tenant not found")
		case errors.Is(err, store.ErrTenantHasDependents):
			writeJSONError(w, http.StatusConflict, "remove all bots and users in this tenant first")
		default:
			writeJSONError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	recordAudit(h.d.Auditor, r, &id, "tenant.delete", "tenant", id.String(), nil, nil)
	w.WriteHeader(http.StatusNoContent)
}

func tenantJSON(t *store.Tenant) map[string]any {
	return map[string]any{
		"id":         t.ID,
		"slug":       t.Slug,
		"name":       t.Name,
		"enabled":    t.Enabled,
		"created_at": t.CreatedAt,
		"updated_at": t.UpdatedAt,
	}
}

// requireAdmin writes 403 if the caller isn't platform_admin and returns
// false. Used to gate mutating endpoints.
func requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return false
	}
	if !id.IsPlatformAdmin() {
		writeJSONError(w, http.StatusForbidden, "platform admin required")
		return false
	}
	return true
}

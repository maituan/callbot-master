package api

import (
	"context"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"callbot-master/internal/auth"
	"callbot-master/internal/store"
)

// AuditDeps wires GET /api/v1/audit. Reads the audit_log table; writes
// happen via the AuditWriter helper threaded into mutate handlers.
type AuditDeps struct {
	Store AuditReader
}

type AuditReader interface {
	ListAudit(ctx context.Context, filter store.AuditFilter) ([]*store.AuditEntry, error)
}

func RegisterAudit(mux *http.ServeMux, d AuditDeps) {
	if d.Store == nil {
		return
	}
	h := &auditHandler{d: d}
	mux.HandleFunc("/api/v1/audit", h.list)
}

type auditHandler struct{ d AuditDeps }

func (h *auditHandler) list(w http.ResponseWriter, r *http.Request) {
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
	filter := store.AuditFilter{
		EntityType: q.Get("entity_type"),
		EntityID:   q.Get("entity_id"),
		Action:     q.Get("action"),
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			filter.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			filter.Offset = n
		}
	}
	if v := q.Get("since"); v != "" {
		if t, err := parseFlexTime(v); err == nil {
			filter.Since = t
		}
	}
	if v := q.Get("until"); v != "" {
		if t, err := parseFlexTime(v); err == nil {
			filter.Until = t
		}
	}
	if v := q.Get("actor_user_id"); v != "" {
		if u, err := uuid.Parse(v); err == nil {
			filter.ActorUserID = &u
		}
	}
	// Tenant scope: tenant_user is forced to its own tenant; admin can
	// optionally pass tenant_id to filter (or omit for all tenants).
	if id.IsPlatformAdmin() {
		if v := q.Get("tenant_id"); v != "" {
			if u, err := uuid.Parse(v); err == nil {
				filter.TenantID = &u
			}
		}
	} else {
		filter.TenantID = id.TenantID
	}

	rows, err := h.d.Store.ListAudit(r.Context(), filter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(rows))
	for _, e := range rows {
		out = append(out, auditJSON(e))
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": out})
}

func auditJSON(e *store.AuditEntry) map[string]any {
	out := map[string]any{
		"id":             e.ID,
		"action":         e.Action,
		"entity_type":    e.EntityType,
		"entity_id":      e.EntityID,
		"actor_username": e.ActorUsername,
		"actor_role":     e.ActorRole,
		"created_at":     e.CreatedAt,
	}
	if e.TenantID != nil {
		out["tenant_id"] = e.TenantID.String()
	}
	if e.ActorUserID != nil {
		out["actor_user_id"] = e.ActorUserID.String()
	}
	if e.RequestIP != nil {
		out["request_ip"] = e.RequestIP.String()
	}
	if len(e.Before) > 0 && string(e.Before) != "null" {
		out["before"] = string(e.Before)
	}
	if len(e.After) > 0 && string(e.After) != "null" {
		out["after"] = string(e.After)
	}
	return out
}

package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"callbot-master/internal/auth"
)

// ShareDeps wires the call-share endpoints. Both routes need the same
// JWT issuer (token signing/verifying) and the call store (to fetch
// the call body when the token is presented).
type ShareDeps struct {
	Issuer *auth.Issuer
	Store  CallReader
	// TTL — how long a freshly-minted share link is valid for. Defaults
	// to 7 days when zero.
	TTL time.Duration
}

// RegisterShare mounts:
//
//	POST /api/v1/share/calls/{id}        — auth required, mints token
//	GET  /api/v1/share/calls/{token}     — PUBLIC (router exempts it
//	                                       from the JWT middleware).
//
// The two paths look similar but are dispatched by HTTP verb. POST
// includes a UUID (call id) → mint a token for that call; GET includes
// the JWT token itself → return the call body.
func RegisterShare(mux *http.ServeMux, d ShareDeps) {
	if d.Issuer == nil || d.Store == nil {
		return
	}
	if d.TTL <= 0 {
		d.TTL = 7 * 24 * time.Hour
	}
	h := &shareHandler{d: d}
	mux.HandleFunc("/api/v1/share/calls/", h.dispatch)
}

type shareHandler struct{ d ShareDeps }

func (h *shareHandler) dispatch(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/share/calls/")
	if rest == "" {
		writeJSONError(w, http.StatusNotFound, "missing call id or token")
		return
	}
	switch r.Method {
	case http.MethodPost:
		h.mint(w, r, rest)
	case http.MethodGet:
		h.fetch(w, r, rest)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// mint generates a share token for the given call id. Auth required +
// tenant scope check.
func (h *shareHandler) mint(w http.ResponseWriter, r *http.Request, callID string) {
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	call, err := h.d.Store.Get(r.Context(), callID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if call == nil {
		writeJSONError(w, http.StatusNotFound, "call not found")
		return
	}
	// Tenant scope: tenant_user can only share calls in their tenant.
	if !id.IsPlatformAdmin() {
		if id.TenantID == nil || call.TenantID == nil || *id.TenantID != *call.TenantID {
			writeJSONError(w, http.StatusForbidden, "forbidden")
			return
		}
	}

	// Optional override of TTL via JSON body { "ttl_hours": N }. Falls
	// back to ShareDeps.TTL when missing/invalid. Capped at 30 days
	// to limit blast radius if a token leaks.
	ttl := h.d.TTL
	var body struct {
		TTLHours int `json:"ttl_hours,omitempty"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.TTLHours > 0 {
		want := time.Duration(body.TTLHours) * time.Hour
		const max = 30 * 24 * time.Hour
		if want > max {
			want = max
		}
		ttl = want
	}

	tok, exp, err := h.d.Issuer.IssueShareToken(callID, ttl)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"call_id":    callID,
		"token":      tok,
		"expires_at": exp,
	})
}

// fetch handles the public GET /api/v1/share/calls/{token}. No auth —
// possession of the token IS the authorisation.
func (h *shareHandler) fetch(w http.ResponseWriter, r *http.Request, token string) {
	callID, err := h.d.Issuer.ParseShareToken(token)
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, "invalid or expired share link")
		return
	}
	call, err := h.d.Store.Get(r.Context(), callID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if call == nil {
		writeJSONError(w, http.StatusNotFound, "call not found")
		return
	}
	writeJSON(w, http.StatusOK, call)
}

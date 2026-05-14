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

// QCDeps wires the inline QC endpoints. Store is the slice of
// *store.Postgres we depend on; auth/middleware injects identity.
type QCDeps struct {
	Store QCStore
}

// QCStore is what the API layer needs from the persistence layer.
type QCStore interface {
	CallReader
	CreateQCEvaluation(ctx context.Context, in store.QCWriteInput) (*store.QCEvaluation, error)
	GetQCEvaluationByCallID(ctx context.Context, callID string) (*store.QCEvaluation, error)
}

// RegisterQC mounts:
//
//	POST /api/v1/qc/evaluate            — submit verdict (auth + evaluator)
//	GET  /api/v1/qc/evaluations/{call}  — fetch verdict (auth, tenant-scoped)
func RegisterQC(mux *http.ServeMux, d QCDeps) {
	if d.Store == nil {
		mux.HandleFunc("/api/v1/qc/", qcDisabled)
		return
	}
	h := &qcHandler{d: d}
	mux.HandleFunc("/api/v1/qc/evaluate", h.evaluate)
	mux.HandleFunc("/api/v1/qc/evaluations/", h.getByCall)
}

func qcDisabled(w http.ResponseWriter, _ *http.Request) {
	writeJSONError(w, http.StatusServiceUnavailable, "qc store not configured")
}

type qcHandler struct{ d QCDeps }

// ── POST /api/v1/qc/evaluate ──────────────────────────────────────────

func (h *qcHandler) evaluate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	if !id.CanEvaluate() {
		writeJSONError(w, http.StatusForbidden, "not authorised to QC — ask a platform admin to enable is_evaluator on your account")
		return
	}

	var body struct {
		CallID  string `json:"call_id"`
		Verdict string `json:"verdict"`
		Reason  string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	body.CallID = strings.TrimSpace(body.CallID)
	if body.CallID == "" {
		writeJSONError(w, http.StatusBadRequest, "call_id required")
		return
	}
	if body.Verdict != "like" && body.Verdict != "dislike" {
		writeJSONError(w, http.StatusBadRequest, "verdict must be 'like' or 'dislike'")
		return
	}

	// Tenant scope: tenant_user only sees their own tenant's calls.
	call, err := h.d.Store.Get(r.Context(), body.CallID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "lookup call: "+err.Error())
		return
	}
	if call == nil {
		writeJSONError(w, http.StatusNotFound, "call not found")
		return
	}
	if !id.IsPlatformAdmin() {
		if id.TenantID == nil || call.TenantID == nil || *id.TenantID != *call.TenantID {
			writeJSONError(w, http.StatusForbidden, "forbidden")
			return
		}
	}

	ev, err := h.d.Store.CreateQCEvaluation(r.Context(), store.QCWriteInput{
		CallID:      body.CallID,
		EvaluatorID: id.UserID,
		Verdict:     body.Verdict,
		Reason:      body.Reason,
	})
	switch {
	case errors.Is(err, store.ErrQCAlreadyEvaluated):
		writeJSONError(w, http.StatusConflict, "call already evaluated")
		return
	case errors.Is(err, store.ErrQCReasonRequired):
		writeJSONError(w, http.StatusBadRequest, "dislike requires a reason of at least 10 characters")
		return
	case err != nil:
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Re-fetch with evaluator_name hydrated for the UI.
	full, _ := h.d.Store.GetQCEvaluationByCallID(r.Context(), ev.CallID)
	if full == nil {
		full = ev // fallback — shouldn't happen but harmless
	}
	writeJSON(w, http.StatusCreated, qcEvaluationJSON(full))
}

// ── GET /api/v1/qc/evaluations/{call_id} ──────────────────────────────

func (h *qcHandler) getByCall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	callID := strings.TrimPrefix(r.URL.Path, "/api/v1/qc/evaluations/")
	if callID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing call_id")
		return
	}
	call, err := h.d.Store.Get(r.Context(), callID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "lookup call: "+err.Error())
		return
	}
	if call == nil {
		writeJSONError(w, http.StatusNotFound, "call not found")
		return
	}
	if !id.IsPlatformAdmin() {
		if id.TenantID == nil || call.TenantID == nil || *id.TenantID != *call.TenantID {
			writeJSONError(w, http.StatusForbidden, "forbidden")
			return
		}
	}
	ev, err := h.d.Store.GetQCEvaluationByCallID(r.Context(), callID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if ev == nil {
		// 200 with null lets the UI distinguish "not yet evaluated"
		// from "evaluation lookup failed" without parsing an error.
		writeJSON(w, http.StatusOK, map[string]any{"evaluation": nil})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"evaluation": qcEvaluationJSON(ev)})
}

func qcEvaluationJSON(ev *store.QCEvaluation) map[string]any {
	out := map[string]any{
		"id":             ev.ID.String(),
		"call_id":        ev.CallID,
		"evaluator_id":   ev.EvaluatorID.String(),
		"evaluator_name": ev.EvaluatorName,
		"verdict":        ev.Verdict,
		"created_at":     ev.CreatedAt,
	}
	if ev.Reason != "" {
		out["reason"] = ev.Reason
	}
	// guard against the uuid.Nil tenant when error path leaves it unset
	if ev.TenantID != uuid.Nil {
		out["tenant_id"] = ev.TenantID.String()
	}
	return out
}

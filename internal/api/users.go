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

// UsersDeps wires /api/v1/users.
type UsersDeps struct {
	Store   UserStore
	Auditor AuditWriter
}

type UserStore interface {
	ListUsers(ctx context.Context, tenantID *uuid.UUID) ([]*store.User, error)
	GetUserByID(ctx context.Context, id uuid.UUID) (*store.User, error)
	GetUserByUsername(ctx context.Context, username string) (*store.User, error)
	CreateTenantUser(ctx context.Context, username, passwordHash string, tenantID uuid.UUID) (uuid.UUID, error)
	UpdateUserPassword(ctx context.Context, userID uuid.UUID, passwordHash string) error
	UpdateUserEnabled(ctx context.Context, id uuid.UUID, enabled bool) error
	SetUserEvaluator(ctx context.Context, id uuid.UUID, enabled bool) error
	SetUserBotAdmin(ctx context.Context, id uuid.UUID, enabled bool) error
	DeleteUser(ctx context.Context, id uuid.UUID) error
}

func RegisterUsers(mux *http.ServeMux, d UsersDeps) {
	if d.Store == nil {
		return
	}
	h := &usersHandler{d: d}
	mux.HandleFunc("/api/v1/users", h.collection)
	mux.HandleFunc("/api/v1/users/", h.item)
}

type usersHandler struct{ d UsersDeps }

func (h *usersHandler) collection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.list(w, r)
	case http.MethodPost:
		h.create(w, r)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *usersHandler) item(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/users/")
	if rest == "" {
		writeJSONError(w, http.StatusNotFound, "missing user id")
		return
	}
	parts := strings.Split(rest, "/")

	// /api/v1/users/me/password — self-service password change.
	if parts[0] == "me" && len(parts) == 2 && parts[1] == "password" {
		if r.Method != http.MethodPatch {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		h.changeOwnPassword(w, r)
		return
	}
	id, err := uuid.Parse(parts[0])
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid user id")
		return
	}

	if len(parts) == 2 && parts[1] == "password" {
		if r.Method != http.MethodPatch {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		h.adminResetPassword(w, r, id)
		return
	}

	switch r.Method {
	case http.MethodPatch:
		h.update(w, r, id)
	case http.MethodDelete:
		h.delete(w, r, id)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *usersHandler) list(w http.ResponseWriter, r *http.Request) {
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	scope := id.TenantID
	if id.IsPlatformAdmin() {
		scope = nil
	}
	users, err := h.d.Store.ListUsers(r.Context(), scope)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(users))
	for _, u := range users {
		out = append(out, userJSON(u))
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": out})
}

func (h *usersHandler) create(w http.ResponseWriter, r *http.Request) {
	if !requireAdmin(w, r) {
		return
	}
	var body struct {
		Username string  `json:"username"`
		Password string  `json:"password"`
		TenantID *string `json:"tenant_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	body.Username = strings.TrimSpace(body.Username)
	if body.Username == "" || body.Password == "" || body.TenantID == nil {
		writeJSONError(w, http.StatusBadRequest, "username, password, tenant_id required")
		return
	}
	tid, err := uuid.Parse(*body.TenantID)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid tenant_id")
		return
	}
	hash, err := auth.HashPassword(body.Password)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "hash: "+err.Error())
		return
	}
	uid, err := h.d.Store.CreateTenantUser(r.Context(), body.Username, hash, tid)
	if err != nil {
		// Username UNIQUE → 23505. Let's surface that nicely.
		if strings.Contains(err.Error(), "23505") {
			writeJSONError(w, http.StatusConflict, "username already taken")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	recordAudit(h.d.Auditor, r, &tid, "user.create", "user", uid.String(), nil,
		map[string]any{"username": body.Username, "tenant_id": tid})
	writeJSON(w, http.StatusCreated, map[string]any{"id": uid, "username": body.Username})
}

func (h *usersHandler) update(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	if !requireAdmin(w, r) {
		return
	}
	var body struct {
		Enabled     *bool `json:"enabled"`
		IsEvaluator *bool `json:"is_evaluator"`
		IsBotAdmin  *bool `json:"is_bot_admin"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if body.Enabled == nil && body.IsEvaluator == nil && body.IsBotAdmin == nil {
		writeJSONError(w, http.StatusBadRequest, "nothing to update (supports 'enabled', 'is_evaluator', 'is_bot_admin')")
		return
	}
	delta := map[string]any{}
	if body.Enabled != nil {
		if err := h.d.Store.UpdateUserEnabled(r.Context(), id, *body.Enabled); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeJSONError(w, http.StatusNotFound, "user not found")
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		delta["enabled"] = *body.Enabled
	}
	if body.IsEvaluator != nil {
		if err := h.d.Store.SetUserEvaluator(r.Context(), id, *body.IsEvaluator); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeJSONError(w, http.StatusNotFound, "user not found")
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		delta["is_evaluator"] = *body.IsEvaluator
	}
	if body.IsBotAdmin != nil {
		if err := h.d.Store.SetUserBotAdmin(r.Context(), id, *body.IsBotAdmin); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeJSONError(w, http.StatusNotFound, "user not found")
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		delta["is_bot_admin"] = *body.IsBotAdmin
	}
	recordAudit(h.d.Auditor, r, nil, "user.update", "user", id.String(), nil, delta)
	w.WriteHeader(http.StatusNoContent)
}

func (h *usersHandler) delete(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	if !requireAdmin(w, r) {
		return
	}
	caller := auth.FromContext(r.Context())
	if caller != nil && caller.UserID == id {
		writeJSONError(w, http.StatusBadRequest, "cannot delete yourself")
		return
	}
	if err := h.d.Store.DeleteUser(r.Context(), id); err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeJSONError(w, http.StatusNotFound, "user not found")
		case errors.Is(err, store.ErrLastAdmin):
			writeJSONError(w, http.StatusBadRequest, "cannot delete the last platform admin")
		default:
			writeJSONError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	recordAudit(h.d.Auditor, r, nil, "user.delete", "user", id.String(), nil, nil)
	w.WriteHeader(http.StatusNoContent)
}

// adminResetPassword lets a platform_admin force a password change for
// any user without knowing their current password.
func (h *usersHandler) adminResetPassword(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	if !requireAdmin(w, r) {
		return
	}
	var body struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if len(body.Password) < 6 {
		writeJSONError(w, http.StatusBadRequest, "password must be at least 6 chars")
		return
	}
	hash, err := auth.HashPassword(body.Password)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := h.d.Store.UpdateUserPassword(r.Context(), id, hash); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	recordAudit(h.d.Auditor, r, nil, "user.password_admin_reset", "user", id.String(), nil, nil)
	w.WriteHeader(http.StatusNoContent)
}

// changeOwnPassword: the authenticated user changes their own password
// after verifying the current one. Available to every role.
func (h *usersHandler) changeOwnPassword(w http.ResponseWriter, r *http.Request) {
	caller := auth.FromContext(r.Context())
	if caller == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	var body struct {
		Current string `json:"current_password"`
		New     string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if len(body.New) < 6 {
		writeJSONError(w, http.StatusBadRequest, "new password must be at least 6 chars")
		return
	}
	u, err := h.d.Store.GetUserByID(r.Context(), caller.UserID)
	if err != nil || u == nil {
		writeJSONError(w, http.StatusUnauthorized, "user not found")
		return
	}
	if !auth.VerifyPassword(u.PasswordHash, body.Current) {
		writeJSONError(w, http.StatusUnauthorized, "current password incorrect")
		return
	}
	hash, err := auth.HashPassword(body.New)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := h.d.Store.UpdateUserPassword(r.Context(), u.ID, hash); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	recordAudit(h.d.Auditor, r, u.TenantID, "user.password_self_change", "user", u.ID.String(), nil, nil)
	w.WriteHeader(http.StatusNoContent)
}

// userJSON returns the safe-to-expose shape (no password hash).
func userJSON(u *store.User) map[string]any {
	out := map[string]any{
		"id":            u.ID,
		"username":      u.Username,
		"role":          u.Role,
		"enabled":       u.Enabled,
		"is_evaluator":  u.IsEvaluator,
		"is_bot_admin":  u.IsBotAdmin,
	}
	if u.TenantID != nil {
		out["tenant_id"] = u.TenantID.String()
	}
	if u.LastLoginAt != nil {
		out["last_login_at"] = u.LastLoginAt
	}
	out["created_at"] = u.CreatedAt
	return out
}

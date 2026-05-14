package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"callbot-master/internal/auth"
	"callbot-master/internal/store"
)

// AuthDeps wires the login/logout/me endpoints. Issuer is required;
// Store provides the user lookup.
type AuthDeps struct {
	Issuer       *auth.Issuer
	Store        AuthUserStore
	CookieSecure bool
}

type AuthUserStore interface {
	GetUserByUsername(ctx context.Context, username string) (*store.User, error)
	GetUserByID(ctx context.Context, id uuid.UUID) (*store.User, error)
	MarkLogin(ctx context.Context, userID uuid.UUID)
}

func RegisterAuth(mux *http.ServeMux, d AuthDeps) {
	if d.Issuer == nil || d.Store == nil {
		mux.HandleFunc("/api/v1/auth/login", authDisabled)
		mux.HandleFunc("/api/v1/auth/logout", authDisabled)
		mux.HandleFunc("/api/v1/auth/me", authDisabled)
		return
	}
	h := &authHandler{d: d}
	mux.HandleFunc("/api/v1/auth/login", h.login)
	mux.HandleFunc("/api/v1/auth/logout", h.logout)
	mux.HandleFunc("/api/v1/auth/me", h.me)
}

type authHandler struct{ d AuthDeps }

func authDisabled(w http.ResponseWriter, _ *http.Request) {
	writeJSONError(w, http.StatusServiceUnavailable, "auth not configured (set MASTER_JWT_SECRET + admin password)")
}

func (h *authHandler) login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if body.Username == "" || body.Password == "" {
		writeJSONError(w, http.StatusBadRequest, "username and password required")
		return
	}

	u, err := h.d.Store.GetUserByUsername(r.Context(), body.Username)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "lookup user")
		return
	}
	// Constant-ish response time: do a dummy hash check even if user
	// doesn't exist, so we don't leak username existence via timing.
	if u == nil || !u.Enabled {
		_ = auth.VerifyPassword("$2a$12$invalidinvalidinvalidinvalidinvalidinvalidinvalidinvali", body.Password)
		writeJSONError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	if !auth.VerifyPassword(u.PasswordHash, body.Password) {
		writeJSONError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	token, exp, err := h.d.Issuer.Issue(u.ID, u.Username, u.Role, u.TenantID, u.IsEvaluator)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "issue token")
		return
	}
	h.d.Store.MarkLogin(r.Context(), u.ID)

	http.SetCookie(w, &http.Cookie{
		Name:     auth.CookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.d.CookieSecure,
		SameSite: http.SameSiteLaxMode,
		Expires:  exp,
	})
	writeJSON(w, http.StatusOK, identityJSON(u))
}

func (h *authHandler) logout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     auth.CookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   h.d.CookieSecure,
		SameSite: http.SameSiteLaxMode,
	})
	w.WriteHeader(http.StatusNoContent)
}

func (h *authHandler) me(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	u, err := h.d.Store.GetUserByID(r.Context(), id.UserID)
	if err != nil || u == nil {
		writeJSONError(w, http.StatusUnauthorized, "user not found")
		return
	}
	writeJSON(w, http.StatusOK, identityJSON(u))
}

// identityJSON is the safe shape of User to expose — never includes the
// password hash.
func identityJSON(u *store.User) map[string]any {
	out := map[string]any{
		"id":           u.ID,
		"username":     u.Username,
		"role":         u.Role,
		"enabled":      u.Enabled,
		"is_evaluator": u.IsEvaluator,
	}
	if u.TenantID != nil {
		out["tenant_id"] = u.TenantID.String()
	}
	if u.LastLoginAt != nil {
		out["last_login_at"] = u.LastLoginAt
	}
	return out
}


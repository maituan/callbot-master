package auth

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

// Identity is the authenticated principal extracted from the JWT.
// It rides on the request context for the lifetime of the request.
type Identity struct {
	UserID      uuid.UUID
	Username    string
	Role        string
	TenantID    *uuid.UUID // nil means platform_admin (sees all tenants)
	IsEvaluator bool       // QC gate: platform_admin always wins via CanEvaluate()
}

// IsPlatformAdmin is the load-bearing role check most handlers need.
func (i *Identity) IsPlatformAdmin() bool { return i.Role == "platform_admin" }

// CanEvaluate reports whether this identity may submit QC verdicts.
// platform_admin is always allowed; tenant_user must be opted in.
func (i *Identity) CanEvaluate() bool {
	if i == nil {
		return false
	}
	return i.IsPlatformAdmin() || i.IsEvaluator
}

type ctxKey struct{}

// FromContext returns the identity for the current request, or nil if
// the route wasn't wrapped by Middleware (or the request was unauthenticated
// and the middleware allowed it).
func FromContext(ctx context.Context) *Identity {
	v, _ := ctx.Value(ctxKey{}).(*Identity)
	return v
}

// CookieName is shared between login handler and middleware.
const CookieName = "callbot_session"

// Middleware verifies the JWT (cookie or Authorization: Bearer header)
// and injects the Identity into context. On invalid/missing token the
// request returns 401 — wrap *only* protected routes.
func Middleware(issuer *Issuer) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw := tokenFromRequest(r)
			if raw == "" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			claims, err := issuer.Parse(raw)
			if err != nil {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			id := &Identity{
				UserID:      claims.UserID,
				Username:    claims.Username,
				Role:        claims.Role,
				TenantID:    claims.TenantID,
				IsEvaluator: claims.IsEvaluator,
			}
			ctx := context.WithValue(r.Context(), ctxKey{}, id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireAdmin wraps a handler so only platform_admin gets through.
// 403 instead of 401 — caller is authenticated, just lacks privilege.
func RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := FromContext(r.Context())
		if id == nil || !id.IsPlatformAdmin() {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// tokenFromRequest checks the cookie first, then Authorization: Bearer.
// Cookie is the dominant transport for browser callers.
func tokenFromRequest(r *http.Request) string {
	if c, err := r.Cookie(CookieName); err == nil && c.Value != "" {
		return c.Value
	}
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	return ""
}

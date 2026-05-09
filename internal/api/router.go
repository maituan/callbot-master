// Package api exposes the ops HTTP surface (health, metrics, calls, campaigns).
package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"callbot-master/internal/auth"
	"callbot-master/internal/metrics"
	"callbot-master/internal/session"
)

type Router struct {
	mux       *http.ServeMux
	manager   *session.Manager
	metrics   *metrics.Collectors
	startedAt time.Time
	// corsOrigin, if non-empty, is echoed in Access-Control-Allow-Origin on
	// every response and short-circuits OPTIONS preflights with 204.
	// Set via SetCORS — empty means CORS disabled.
	corsOrigin string
	// authIssuer, if non-nil, makes the auth middleware enforce JWTs on
	// every /api/v1/* path except /api/v1/auth/login.
	authIssuer *auth.Issuer
}

// New wires /health and /metrics. metrics may be nil — the endpoint then
// returns an empty collectors response (only Go runtime + process metrics
// from a fresh registry would be missing too).
func New(mgr *session.Manager, m *metrics.Collectors) *Router {
	r := &Router{
		mux:       http.NewServeMux(),
		manager:   mgr,
		metrics:   m,
		startedAt: time.Now(),
	}
	r.mux.HandleFunc("/health", r.health)
	if m != nil {
		r.mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	} else {
		r.mux.HandleFunc("/metrics", r.metricsDisabled)
	}
	return r
}

func (r *Router) Handler() http.Handler {
	var h http.Handler = r.mux
	if r.authIssuer != nil {
		h = authChain(r.authIssuer, r.mux)
	}
	if r.corsOrigin != "" {
		h = corsMiddleware(r.corsOrigin, h)
	}
	return h
}

// SetCORS enables Access-Control-Allow-Origin on responses. Pass "" to
// disable, "*" to allow any origin, or an exact origin like
// "http://localhost:3001". Has no effect on requests already same-origin.
func (r *Router) SetCORS(origin string) { r.corsOrigin = origin }

// SetAuth enables JWT enforcement on /api/v1/* (except /api/v1/auth/login).
// Pass nil to leave the API open (dev / phase-out).
func (r *Router) SetAuth(issuer *auth.Issuer) { r.authIssuer = issuer }

// authChain wraps mux with the JWT middleware, but exempts public paths.
// Public: /health, /metrics, /recordings/*, /api/v1/auth/login.
// Everything else under /api/v1/* requires a valid token.
func authChain(issuer *auth.Issuer, mux *http.ServeMux) http.Handler {
	protected := auth.Middleware(issuer)(mux)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		// CORS preflight + public surface bypass JWT entirely.
		// Share-fetch (GET only) is public — possession of the
		// signed token IS the authorisation. Mint (POST) still
		// goes through auth.
		if r.Method == http.MethodOptions ||
			p == "/health" || p == "/metrics" ||
			strings.HasPrefix(p, "/recordings/") ||
			p == "/api/v1/auth/login" ||
			(r.Method == http.MethodGet && strings.HasPrefix(p, "/api/v1/share/calls/")) ||
			// Web playground public surfaces — token-in-URL is the auth.
			// Covers GET /api/v1/web/bot/{token}, GET /api/v1/web/voice/{token},
			// POST /api/v1/web/chat/{token}.
			strings.HasPrefix(p, "/api/v1/web/bot/") ||
			strings.HasPrefix(p, "/api/v1/web/chat/") ||
			strings.HasPrefix(p, "/api/v1/web/voice/") {
			mux.ServeHTTP(w, r)
			return
		}
		// Only enforce on the API surface — anything else (favicon, static)
		// passes through too.
		if !strings.HasPrefix(p, "/api/") {
			mux.ServeHTTP(w, r)
			return
		}
		protected.ServeHTTP(w, r)
	})
}

func corsMiddleware(origin string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Vary", "Origin")
		h.Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		// "*" cannot be combined with credentials per spec, so only set
		// the credentials header when the operator pinned an exact origin.
		if origin != "*" {
			h.Set("Access-Control-Allow-Credentials", "true")
		}
		if req.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, req)
	})
}

// Mux exposes the underlying mux so feature packages (campaigns, calls)
// can register their own routes without going through Router.
func (r *Router) Mux() *http.ServeMux { return r.mux }

// MountStaticDir serves files from dir under prefix. Used for the
// archived call recordings ("/recordings/...") so the ops UI can play
// them back. No auth — same trust model as /metrics.
func (r *Router) MountStaticDir(prefix, dir string) {
	if prefix == "" || dir == "" {
		return
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	r.mux.Handle(prefix, http.StripPrefix(prefix, http.FileServer(http.Dir(dir))))
}

func (r *Router) health(w http.ResponseWriter, _ *http.Request) {
	body := map[string]any{
		"status":         "ok",
		"uptime_seconds": int(time.Since(r.startedAt).Seconds()),
		"active_calls":   r.manager.Count(-1),
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func (r *Router) metricsDisabled(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte("# metrics disabled (no collectors registered)\n"))
}

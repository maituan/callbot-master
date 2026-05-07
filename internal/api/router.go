// Package api exposes the ops HTTP surface (health, metrics, calls, campaigns).
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"callbot-master/internal/metrics"
	"callbot-master/internal/session"
)

type Router struct {
	mux       *http.ServeMux
	manager   *session.Manager
	metrics   *metrics.Collectors
	startedAt time.Time
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

func (r *Router) Handler() http.Handler { return r.mux }

// Mux exposes the underlying mux so feature packages (campaigns, calls)
// can register their own routes without going through Router.
func (r *Router) Mux() *http.ServeMux { return r.mux }

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

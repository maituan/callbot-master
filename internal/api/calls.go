package api

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"callbot-master/internal/store"
)

// CallsDeps wires the call_history read endpoints.
type CallsDeps struct {
	Store CallReader
}

// CallReader is the read-only slice of store.Postgres the API needs.
// Lets unit tests inject a fake without pgx.
type CallReader interface {
	Get(ctx context.Context, callID string) (*store.CallRecord, error)
	List(ctx context.Context, filter store.ListFilter) ([]*store.CallRecord, error)
}

// RegisterCalls mounts /api/v1/calls + /api/v1/calls/{id} on the given mux.
func RegisterCalls(mux *http.ServeMux, d CallsDeps) {
	if d.Store == nil {
		// Without a store we mount stubs that return 503 — clearer than 404.
		mux.HandleFunc("/api/v1/calls", callsDisabled)
		mux.HandleFunc("/api/v1/calls/", callsDisabled)
		return
	}
	h := &callsHandler{d: d}
	mux.HandleFunc("/api/v1/calls", h.collection)
	mux.HandleFunc("/api/v1/calls/", h.item)
}

type callsHandler struct{ d CallsDeps }

func callsDisabled(w http.ResponseWriter, _ *http.Request) {
	writeJSONError(w, http.StatusServiceUnavailable, "call_history persistence not configured")
}

func (h *callsHandler) collection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	f := store.ListFilter{
		Phone:     q.Get("phone"),
		Scenario:  q.Get("scenario"),
		Direction: q.Get("direction"),
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeJSONError(w, http.StatusBadRequest, "limit must be a non-negative integer")
			return
		}
		f.Limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeJSONError(w, http.StatusBadRequest, "offset must be a non-negative integer")
			return
		}
		f.Offset = n
	}
	if v := q.Get("since"); v != "" {
		t, err := parseFlexTime(v)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "since must be RFC3339 (e.g. 2026-05-07T00:00:00Z)")
			return
		}
		f.Since = t
	}
	if v := q.Get("until"); v != "" {
		t, err := parseFlexTime(v)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "until must be RFC3339")
			return
		}
		f.Until = t
	}

	records, err := h.d.Store.List(r.Context(), f)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "list calls: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"calls":  records,
		"limit":  f.Limit,
		"offset": f.Offset,
	})
}

func (h *callsHandler) item(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/calls/")
	if id == "" {
		writeJSONError(w, http.StatusNotFound, "missing call id")
		return
	}
	rec, err := h.d.Store.Get(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "get call: "+err.Error())
		return
	}
	if rec == nil {
		writeJSONError(w, http.StatusNotFound, "call not found")
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

// parseFlexTime accepts the two ISO formats common from JS clients:
//   1. RFC3339          — "2026-05-08T19:50:00Z"           (Go's stdlib)
//   2. RFC3339Nano      — "2026-05-08T19:50:00.000Z"       (Date.toISOString())
// JS toISOString always emits the millisecond form; the original
// RFC3339-only parser silently rejected those, leaving filter.Since
// zero and producing puzzling empty stats. Try both layouts before
// giving up.
func parseFlexTime(v string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339Nano, v)
}

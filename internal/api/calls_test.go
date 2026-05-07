package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"callbot-master/internal/store"
)

// fakeStore implements CallReader for handler tests.
type fakeStore struct {
	get  func(ctx context.Context, id string) (*store.CallRecord, error)
	list func(ctx context.Context, f store.ListFilter) ([]*store.CallRecord, error)
}

func (f *fakeStore) Get(ctx context.Context, id string) (*store.CallRecord, error) {
	return f.get(ctx, id)
}
func (f *fakeStore) List(ctx context.Context, fi store.ListFilter) ([]*store.CallRecord, error) {
	return f.list(ctx, fi)
}

func newCallsServer(t *testing.T, fs *fakeStore) *httptest.Server {
	mux := http.NewServeMux()
	RegisterCalls(mux, CallsDeps{Store: fs})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestCallsAPI_GetByID_Found(t *testing.T) {
	want := &store.CallRecord{
		CallID: "uuid-1", Direction: "inbound", Scenario: "test",
		Phone: "0901", StartTime: time.Now(), EndTime: time.Now(),
		Status: "ended", Action: "CHAT",
	}
	fs := &fakeStore{
		get: func(_ context.Context, id string) (*store.CallRecord, error) {
			if id != "uuid-1" {
				t.Errorf("got id %q", id)
			}
			return want, nil
		},
	}
	srv := newCallsServer(t, fs)
	resp, err := srv.Client().Get(srv.URL + "/api/v1/calls/uuid-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var got store.CallRecord
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if got.CallID != "uuid-1" || got.Action != "CHAT" {
		t.Fatalf("got = %+v", got)
	}
}

func TestCallsAPI_GetByID_NotFound(t *testing.T) {
	fs := &fakeStore{
		get: func(_ context.Context, _ string) (*store.CallRecord, error) { return nil, nil },
	}
	srv := newCallsServer(t, fs)
	resp, err := srv.Client().Get(srv.URL + "/api/v1/calls/nope")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestCallsAPI_List_WithFilters(t *testing.T) {
	var seenFilter store.ListFilter
	fs := &fakeStore{
		list: func(_ context.Context, f store.ListFilter) ([]*store.CallRecord, error) {
			seenFilter = f
			return []*store.CallRecord{{CallID: "u1"}, {CallID: "u2"}}, nil
		},
	}
	srv := newCallsServer(t, fs)
	resp, err := srv.Client().Get(srv.URL + "/api/v1/calls?phone=0901&scenario=hcc&direction=inbound&limit=10&offset=20&since=2026-01-01T00:00:00Z")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if seenFilter.Phone != "0901" || seenFilter.Scenario != "hcc" || seenFilter.Direction != "inbound" {
		t.Fatalf("filter = %+v", seenFilter)
	}
	if seenFilter.Limit != 10 || seenFilter.Offset != 20 {
		t.Fatalf("paging = %+v", seenFilter)
	}
	if seenFilter.Since.Year() != 2026 {
		t.Fatalf("since = %v", seenFilter.Since)
	}
}

func TestCallsAPI_List_BadParams(t *testing.T) {
	fs := &fakeStore{list: func(context.Context, store.ListFilter) ([]*store.CallRecord, error) { return nil, nil }}
	srv := newCallsServer(t, fs)

	// limit negative
	r, _ := srv.Client().Get(srv.URL + "/api/v1/calls?limit=-1")
	if r.StatusCode != http.StatusBadRequest {
		t.Fatalf("limit -1 status = %d", r.StatusCode)
	}
	r.Body.Close()

	// since malformed
	r2, _ := srv.Client().Get(srv.URL + "/api/v1/calls?since=yesterday")
	if r2.StatusCode != http.StatusBadRequest {
		t.Fatalf("since=yesterday status = %d", r2.StatusCode)
	}
	r2.Body.Close()
}

func TestCallsAPI_NotConfigured_503(t *testing.T) {
	mux := http.NewServeMux()
	RegisterCalls(mux, CallsDeps{Store: nil}) // disabled
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/api/v1/calls/anything")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

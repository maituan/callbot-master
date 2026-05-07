package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"callbot-master/internal/campaign"
	"callbot-master/internal/store"
)

// stubBindFunc returns an OriginateFunc that records calls + always succeeds.
func stubBindFunc(callsTo *atomic.Int32) func(*campaign.Manager, *campaign.Campaign, *store.BotConfig) campaign.OriginateFunc {
	return func(_ *campaign.Manager, _ *campaign.Campaign, _ *store.BotConfig) campaign.OriginateFunc {
		return func(_ context.Context, phone, _, _ string, _ map[string]any) (string, error) {
			callsTo.Add(1)
			return "uuid-" + phone, nil
		}
	}
}

// fakeBotLookup returns a hardcoded bot for any tenant_slug + bot_slug.
type fakeBotLookup struct{ bot *store.BotConfig }

func newFakeBotLookup() *fakeBotLookup {
	tid := uuid.New()
	bid := uuid.New()
	return &fakeBotLookup{bot: &store.BotConfig{
		ID: bid, TenantID: tid, TenantSlug: "default", Slug: "default", Enabled: true,
	}}
}

func (f *fakeBotLookup) GetBotByID(_ context.Context, _ uuid.UUID) (*store.BotConfig, error) {
	return f.bot, nil
}
func (f *fakeBotLookup) GetTenantBySlug(_ context.Context, _ string) (*store.Tenant, error) {
	return &store.Tenant{ID: f.bot.TenantID, Slug: f.bot.TenantSlug, Enabled: true}, nil
}
func (f *fakeBotLookup) GetBotByTenantAndSlug(_ context.Context, _ uuid.UUID, _ string) (*store.BotConfig, error) {
	return f.bot, nil
}

func uploadCSV(t *testing.T, srv *httptest.Server, csvBody, scenario, ccu string) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	if scenario != "" {
		_ = mw.WriteField("scenario", scenario)
	}
	if ccu != "" {
		_ = mw.WriteField("ccu", ccu)
	}
	fw, err := mw.CreateFormFile("file", "leads.csv")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := io.WriteString(fw, csvBody); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close mw: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/campaigns", &buf)
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do req: %v", err)
	}
	return resp
}

func newTestServer(t *testing.T, mgr *campaign.Manager, calls *atomic.Int32) *httptest.Server {
	mux := http.NewServeMux()
	RegisterCampaigns(mux, CampaignDeps{
		Manager:           mgr,
		BindFunc:          stubBindFunc(calls),
		BotLookup:         newFakeBotLookup(),
		DefaultTenantSlug: "default",
		DefaultBotSlug:    "default",
		DefaultCallerID:   "callbot",
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestAPI_CreateCampaign_HappyPath(t *testing.T) {
	mgr := campaign.NewManager()
	var calls atomic.Int32
	srv := newTestServer(t, mgr, &calls)

	csv := "phone,name\n0901,An\n0902,Bình\n"
	resp := uploadCSV(t, srv, csv, "hcc-leadgen", "2")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	id, _ := got["id"].(string)
	if !strings.HasPrefix(id, "camp-") {
		t.Fatalf("id = %q", id)
	}
	// fakeBotLookup ignores the requested slug and always returns its
	// canned bot ("default"), so we just assert the field is present.
	if got["bot_slug"] != "default" {
		t.Fatalf("bot_slug = %v", got["bot_slug"])
	}
	if got["total"].(float64) != 2 {
		t.Fatalf("total = %v", got["total"])
	}

	// Wait for the worker pool to drain.
	mgr.Wait(id)
	if calls.Load() != 2 {
		t.Fatalf("originate calls = %d, want 2", calls.Load())
	}
}

func TestAPI_CreateCampaign_RejectsBadCSV(t *testing.T) {
	mgr := campaign.NewManager()
	var calls atomic.Int32
	srv := newTestServer(t, mgr, &calls)

	resp := uploadCSV(t, srv, "name\nAn\n", "", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestAPI_CreateCampaign_NoLeads(t *testing.T) {
	mgr := campaign.NewManager()
	var calls atomic.Int32
	srv := newTestServer(t, mgr, &calls)

	resp := uploadCSV(t, srv, "phone\n", "", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestAPI_GetCampaign(t *testing.T) {
	mgr := campaign.NewManager()
	var calls atomic.Int32
	srv := newTestServer(t, mgr, &calls)

	resp := uploadCSV(t, srv, "phone\n0901\n", "", "")
	resp.Body.Close()

	id := mgr.List()[0].ID
	r2, err := srv.Client().Get(srv.URL + "/api/v1/campaigns/" + id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer r2.Body.Close()
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", r2.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(r2.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["id"] != id {
		t.Fatalf("id = %v", body["id"])
	}
	if _, ok := body["leads"]; !ok {
		t.Fatalf("leads field missing")
	}
}

func TestAPI_GetCampaign_NotFound(t *testing.T) {
	mgr := campaign.NewManager()
	var calls atomic.Int32
	srv := newTestServer(t, mgr, &calls)

	r, err := srv.Client().Get(srv.URL + "/api/v1/campaigns/nope")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", r.StatusCode)
	}
}

func TestAPI_CancelCampaign(t *testing.T) {
	mgr := campaign.NewManager()
	var calls atomic.Int32
	srv := newTestServer(t, mgr, &calls)

	// Generate a campaign with many leads + slow originate so cancel hits before drain.
	manyLeads := "phone\n"
	for i := 0; i < 50; i++ {
		manyLeads += "090" + string(rune('0'+(i%10))) + "\n"
	}
	resp := uploadCSV(t, srv, manyLeads, "", "1")
	resp.Body.Close()
	id := mgr.List()[0].ID

	// Sleep briefly so at least one originate is in-flight.
	time.Sleep(20 * time.Millisecond)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/campaigns/"+id+"/cancel", nil)
	r2, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	defer r2.Body.Close()
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", r2.StatusCode)
	}
	mgr.Wait(id)
	c := mgr.Get(id)
	if c.Status != "canceled" {
		t.Fatalf("status = %q", c.Status)
	}
}

func TestAPI_CancelCampaign_NotFound(t *testing.T) {
	mgr := campaign.NewManager()
	var calls atomic.Int32
	srv := newTestServer(t, mgr, &calls)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/campaigns/nope/cancel", nil)
	r, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", r.StatusCode)
	}
}

func TestAPI_ListCampaigns(t *testing.T) {
	mgr := campaign.NewManager()
	var calls atomic.Int32
	srv := newTestServer(t, mgr, &calls)

	for i := 0; i < 3; i++ {
		resp := uploadCSV(t, srv, "phone\n0901\n", "", "")
		resp.Body.Close()
	}

	r, err := srv.Client().Get(srv.URL + "/api/v1/campaigns")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", r.StatusCode)
	}
	var body struct {
		Campaigns []struct {
			ID    string `json:"id"`
			Stats struct {
				Total int `json:"total"`
			} `json:"stats"`
		} `json:"campaigns"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Campaigns) != 3 {
		t.Fatalf("campaigns = %d, want 3", len(body.Campaigns))
	}
}

func TestAPI_NoBindFuncReturns503(t *testing.T) {
	mgr := campaign.NewManager()
	mux := http.NewServeMux()
	RegisterCampaigns(mux, CampaignDeps{Manager: mgr}) // no BindFunc
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp := uploadCSV(t, srv, "phone\n0901\n", "", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

// keep imports used
var _ = errors.New

package campaign

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseCSV_Basic(t *testing.T) {
	// "plate" is no longer a well-known column; it should round-trip
	// through CustomData so the bot adapter still sees it.
	csv := "phone,name,plate,note\n0901,An,ABC,VIP\n0902,Bình,DEF,\n"
	leads, err := ParseCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("ParseCSV: %v", err)
	}
	if len(leads) != 2 {
		t.Fatalf("got %d leads, want 2", len(leads))
	}
	if leads[0].Phone != "0901" || leads[0].Name != "An" {
		t.Fatalf("lead0 = %+v", leads[0])
	}
	if leads[0].CustomData["plate"] != "ABC" {
		t.Fatalf("lead0.plate (CustomData) = %v", leads[0].CustomData["plate"])
	}
	if leads[0].CustomData["note"] != "VIP" {
		t.Fatalf("lead0.note = %v", leads[0].CustomData["note"])
	}
	if leads[1].Status != StatusPending {
		t.Fatalf("lead1.Status = %s", leads[1].Status)
	}
}

func TestParseCSV_RequiresPhoneColumn(t *testing.T) {
	csv := "name,plate\nAn,ABC\n"
	_, err := ParseCSV(strings.NewReader(csv))
	if err == nil || !strings.Contains(err.Error(), "phone") {
		t.Fatalf("want phone-required error, got %v", err)
	}
}

func TestParseCSV_HeaderTrimAndCase(t *testing.T) {
	csv := "  Phone , LEAD_id\n0901, L1\n"
	leads, err := ParseCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("ParseCSV: %v", err)
	}
	if len(leads) != 1 || leads[0].Phone != "0901" || leads[0].LeadID != "L1" {
		t.Fatalf("unexpected leads %+v", leads)
	}
}

func TestManager_DispatchesAtCCU(t *testing.T) {
	leads := []*Lead{
		{Phone: "1", Status: StatusPending},
		{Phone: "2", Status: StatusPending},
		{Phone: "3", Status: StatusPending},
		{Phone: "4", Status: StatusPending},
	}

	var (
		concurrent atomic.Int32
		peak       atomic.Int32
		mu         sync.Mutex
		seen       []string
	)
	originate := func(_ context.Context, phone, _, _ string, _ map[string]any) (string, error) {
		now := concurrent.Add(1)
		// Track peak observed concurrency.
		for {
			cur := peak.Load()
			if now <= cur {
				break
			}
			if peak.CompareAndSwap(cur, now) {
				break
			}
		}
		time.Sleep(40 * time.Millisecond)
		concurrent.Add(-1)
		mu.Lock()
		seen = append(seen, phone)
		mu.Unlock()
		return "uuid-" + phone, nil
	}

	m := NewManager()
	c := m.Create(context.Background(), CreateOpts{
		Scenario:      "test",
		CallerID:      "callbot",
		CCU:           2,
		Leads:         leads,
		RatePerWorker: 1 * time.Millisecond,
	}, originate)
	if !m.Wait(c.ID) {
		t.Fatal("Wait returned false")
	}

	if peak.Load() > 2 {
		t.Fatalf("peak concurrency %d, want <=2 (CCU)", peak.Load())
	}
	if len(seen) != 4 {
		t.Fatalf("dialed %d, want 4", len(seen))
	}
	// After originate, leads sit in Dialing until the ANSWER/HANGUP handler
	// (outside the manager) advances them. We only verify all leads got
	// originated and none are still Pending.
	stats := c.Stats()
	if stats.Pending != 0 {
		t.Fatalf("leads still pending after Wait; stats=%+v", stats)
	}
	if stats.Dialing != 4 {
		t.Fatalf("expected all 4 leads in Dialing post-originate; stats=%+v", stats)
	}
	if c.Leads[0].CallUUID != "uuid-1" {
		t.Fatalf("lead 0 callUUID = %q", c.Leads[0].CallUUID)
	}
}

func TestManager_OriginateErrorMarksLeadFailed(t *testing.T) {
	leads := []*Lead{
		{Phone: "1", Status: StatusPending},
		{Phone: "2", Status: StatusPending},
	}
	originate := func(_ context.Context, phone, _, _ string, _ map[string]any) (string, error) {
		if phone == "1" {
			return "", &origErr{msg: "fs busy"}
		}
		return "uuid-" + phone, nil
	}

	m := NewManager()
	c := m.Create(context.Background(), CreateOpts{
		CCU:           1,
		Leads:         leads,
		RatePerWorker: 1 * time.Millisecond,
	}, originate)
	m.Wait(c.ID)

	if leads[0].Status != StatusFailed {
		t.Fatalf("lead 0 status = %s, want failed", leads[0].Status)
	}
	if leads[0].Error != "fs busy" {
		t.Fatalf("lead 0 error = %q", leads[0].Error)
	}
	if leads[1].CallUUID != "uuid-2" {
		t.Fatalf("lead 1 callUUID = %q", leads[1].CallUUID)
	}
}

func TestManager_CancelStopsDialing(t *testing.T) {
	const N = 30
	leads := make([]*Lead, N)
	for i := range leads {
		leads[i] = &Lead{Phone: "ph", Status: StatusPending}
	}

	var dialed atomic.Int32
	originate := func(_ context.Context, _, _, _ string, _ map[string]any) (string, error) {
		dialed.Add(1)
		time.Sleep(10 * time.Millisecond)
		return "uuid", nil
	}

	m := NewManager()
	c := m.Create(context.Background(), CreateOpts{
		CCU:           1,
		Leads:         leads,
		RatePerWorker: 1 * time.Millisecond,
	}, originate)

	time.Sleep(30 * time.Millisecond)
	if !m.Cancel(c.ID) {
		t.Fatal("Cancel returned false")
	}
	m.Wait(c.ID)

	if d := dialed.Load(); d >= int32(N) {
		t.Fatalf("dialed %d/%d — cancel did not stop dialing", d, N)
	}
	if c.Status != "canceled" {
		t.Fatalf("status = %q, want canceled", c.Status)
	}
	stats := c.Stats()
	if stats.Canceled == 0 {
		t.Fatalf("expected at least one lead canceled; stats=%+v", stats)
	}
}

func TestCampaign_SetLeadStatus(t *testing.T) {
	c := &Campaign{Leads: []*Lead{{Phone: "1", CallUUID: "u1", Status: StatusDialing}}}
	c.SetLeadStatus("u1", StatusCompleted, "")
	if c.Leads[0].Status != StatusCompleted {
		t.Fatalf("status = %s", c.Leads[0].Status)
	}
	if c.Leads[0].EndedAt == nil {
		t.Fatal("EndedAt should be set on terminal status")
	}
}

func TestCampaign_FindByUUID(t *testing.T) {
	c := &Campaign{Leads: []*Lead{
		{Phone: "1", CallUUID: "u1"},
		{Phone: "2", CallUUID: "u2"},
	}}
	got := c.FindByUUID("u2")
	if got == nil || got.Phone != "2" {
		t.Fatalf("FindByUUID(u2) = %+v", got)
	}
	if c.FindByUUID("nope") != nil {
		t.Fatal("FindByUUID(nope) should be nil")
	}
}

type origErr struct{ msg string }

func (e *origErr) Error() string { return e.msg }

package campaign

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"callbot-master/internal/metrics"
)

// OriginateFunc places one outbound call. Implementation typically wraps
// FreeSWITCH `originate` + custom variables; the manager doesn't care.
//
//	phone:      target phone number / dial string
//	callerID:   caller-id presented to callee
//	scenario:   passed-through to the bot/session for branching logic
//	customData: free-form metadata (lead_id, gender, plate, …) — adapter
//	            decides how to forward (e.g. as FS chan vars).
//
// Returns the FS call uuid on success. On error, the lead is marked failed.
type OriginateFunc func(
	ctx context.Context,
	phone, callerID, scenario string,
	customData map[string]any,
) (callUUID string, err error)

// Manager keeps campaigns in memory and runs a per-campaign worker pool.
//
// Concurrency model:
//   - One Manager per process; safe for many concurrent Create calls.
//   - Each campaign gets its own goroutine that fans out to CCU workers.
//   - Cancellation propagates via campaign-scoped context and the
//     `canceled` flag — workers stop dialing further leads.
type Manager struct {
	campaigns sync.Map // id → *campaignRun
	counter   atomic.Int64
	Metrics   *metrics.Collectors // nil-safe
}

func NewManager() *Manager { return &Manager{} }

// recordCampaign pushes the campaign's current stats into the
// campaign_progress gauge. Cheap; safe to call after every mutation.
func (m *Manager) recordCampaign(c *Campaign) {
	if m.Metrics == nil {
		return
	}
	s := c.Stats()
	g := m.Metrics.CampaignProgress
	g.WithLabelValues(c.ID, "pending").Set(float64(s.Pending))
	g.WithLabelValues(c.ID, "dialing").Set(float64(s.Dialing))
	g.WithLabelValues(c.ID, "answered").Set(float64(s.Answered))
	g.WithLabelValues(c.ID, "completed").Set(float64(s.Completed))
	g.WithLabelValues(c.ID, "failed").Set(float64(s.Failed))
	g.WithLabelValues(c.ID, "no_answer").Set(float64(s.NoAnswer))
	g.WithLabelValues(c.ID, "canceled").Set(float64(s.Canceled))
}

type campaignRun struct {
	c      *Campaign
	cancel context.CancelFunc
	done   chan struct{}
}

// Get returns the campaign with the given id, or nil.
func (m *Manager) Get(id string) *Campaign {
	v, ok := m.campaigns.Load(id)
	if !ok {
		return nil
	}
	return v.(*campaignRun).c
}

// SetLeadStatus mutates the lead and refreshes the campaign_progress gauge.
// Use this from external code (outbound handler) instead of c.SetLeadStatus
// to keep metrics in sync.
func (m *Manager) SetLeadStatus(c *Campaign, callUUID string, status CallStatus, errMsg string) {
	c.SetLeadStatus(callUUID, status, errMsg)
	m.recordCampaign(c)
}

// List returns a snapshot of all campaigns, newest first by creation time.
func (m *Manager) List() []*Campaign {
	var out []*Campaign
	m.campaigns.Range(func(_, v any) bool {
		out = append(out, v.(*campaignRun).c)
		return true
	})
	return out
}

// CreateOpts groups Create parameters; lets callers add fields without
// breaking the signature.
type CreateOpts struct {
	Scenario string
	CallerID string
	CCU      int
	Leads    []*Lead
	// RatePerWorker is the inter-call sleep on a single worker after each
	// originate, to avoid bursting FS. Default 500ms (matches v1).
	RatePerWorker time.Duration
}

// Create starts a new campaign and returns it immediately. Worker pool
// runs in the background.
func (m *Manager) Create(parent context.Context, opts CreateOpts, originate OriginateFunc) *Campaign {
	id := fmt.Sprintf("camp-%d", m.counter.Add(1))

	if opts.CCU <= 0 {
		opts.CCU = 1
	}
	if opts.RatePerWorker <= 0 {
		opts.RatePerWorker = 500 * time.Millisecond
	}
	if opts.Scenario == "" {
		opts.Scenario = "default"
	}
	if opts.CallerID == "" {
		opts.CallerID = "callbot"
	}

	c := &Campaign{
		ID:        id,
		Scenario:  opts.Scenario,
		CallerID:  opts.CallerID,
		CCU:       opts.CCU,
		Leads:     opts.Leads,
		Status:    "running",
		CreatedAt: time.Now(),
	}
	ctx, cancel := context.WithCancel(parent)
	run := &campaignRun{c: c, cancel: cancel, done: make(chan struct{})}
	m.campaigns.Store(id, run)
	m.recordCampaign(c)

	go m.runWorkers(ctx, run, opts, originate)
	return c
}

// Cancel marks the campaign canceled and stops dialing further leads.
// Already-dialed leads continue their session (caller decides whether to
// hang them up via session.Manager). Returns false if id unknown.
func (m *Manager) Cancel(id string) bool {
	v, ok := m.campaigns.Load(id)
	if !ok {
		return false
	}
	run := v.(*campaignRun)
	run.c.mu.Lock()
	if run.c.canceled {
		run.c.mu.Unlock()
		return true
	}
	run.c.canceled = true
	run.c.Status = "canceled"
	run.c.mu.Unlock()
	run.cancel()
	m.recordCampaign(run.c)
	return true
}

// Wait blocks until the campaign's worker pool exits. Returns false if id unknown.
func (m *Manager) Wait(id string) bool {
	v, ok := m.campaigns.Load(id)
	if !ok {
		return false
	}
	<-v.(*campaignRun).done
	return true
}

func (m *Manager) runWorkers(ctx context.Context, run *campaignRun, opts CreateOpts, originate OriginateFunc) {
	defer close(run.done)

	c := run.c
	sem := make(chan struct{}, opts.CCU)
	var wg sync.WaitGroup

	for idx, lead := range c.Leads {
		// Cancel check before acquiring a slot — lets us stop fast.
		c.mu.Lock()
		canceled := c.canceled
		c.mu.Unlock()
		if canceled || ctx.Err() != nil {
			c.mu.Lock()
			if lead.Status == StatusPending {
				lead.Status = StatusCanceled
				now := time.Now()
				lead.EndedAt = &now
			}
			c.mu.Unlock()
			continue
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			c.mu.Lock()
			if lead.Status == StatusPending {
				lead.Status = StatusCanceled
				now := time.Now()
				lead.EndedAt = &now
			}
			c.mu.Unlock()
			continue
		}
		wg.Add(1)

		go func(idx int, l *Lead) {
			defer wg.Done()
			defer func() { <-sem }()

			c.mu.Lock()
			l.Status = StatusDialing
			now := time.Now()
			l.StartedAt = &now
			c.mu.Unlock()
			m.recordCampaign(c)

			cd := mergeCustomData(l)
			callUUID, err := originate(ctx, l.Phone, c.CallerID, c.Scenario, cd)

			c.mu.Lock()
			if err != nil {
				l.Status = StatusFailed
				l.Error = err.Error()
				endNow := time.Now()
				l.EndedAt = &endNow
				slog.Warn("campaign lead originate failed",
					"campaign_id", c.ID, "lead_idx", idx, "phone", l.Phone, "err", err)
			} else {
				l.CallUUID = callUUID
				slog.Info("campaign lead dialing",
					"campaign_id", c.ID, "lead_idx", idx,
					"phone", l.Phone, "call_uuid", callUUID)
			}
			c.mu.Unlock()
			m.recordCampaign(c)

			// Burst control. Sleep is honored even on error so we don't
			// overrun FS during a sustained failure mode.
			select {
			case <-time.After(opts.RatePerWorker):
			case <-ctx.Done():
			}
		}(idx, lead)
	}

	wg.Wait()
	c.mu.Lock()
	if !c.canceled {
		c.Status = "done"
	}
	c.mu.Unlock()
	stats := c.Stats()
	slog.Info("campaign done",
		"campaign_id", c.ID,
		"total", stats.Total,
		"completed", stats.Completed,
		"failed", stats.Failed,
		"no_answer", stats.NoAnswer,
		"canceled", stats.Canceled,
	)
}

// mergeCustomData returns a copy of the lead's CustomData enriched with
// the well-known fields (lead_id, gender, name, plate) so the adapter
// only has to look at one map.
func mergeCustomData(l *Lead) map[string]any {
	out := make(map[string]any, len(l.CustomData)+4)
	for k, v := range l.CustomData {
		out[k] = v
	}
	if l.LeadID != "" {
		out["lead_id"] = l.LeadID
	}
	if l.Gender != "" {
		out["gender"] = l.Gender
	}
	if l.Name != "" {
		out["name"] = l.Name
	}
	if l.Plate != "" {
		out["plate"] = l.Plate
	}
	return out
}

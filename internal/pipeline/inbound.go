package pipeline

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/fiorix/go-eventsocket/eventsocket"

	"callbot-master/internal/session"
)

// InboundDeps wires the inbound handler. Runner is shared with outbound
// (one per process); inbound-specific knobs live alongside.
type InboundDeps struct {
	Runner    *SessionRunner
	DID       string
	PickupSLA time.Duration
	Scenario  string
}

// InboundHandler subscribes to CHANNEL_PARK and runs an inbound session
// per matching call. Multiple sessions run concurrently; state per call
// lives in SessionRunner.Sessions.
type InboundHandler struct {
	d         InboundDeps
	rootCtx   context.Context
	wg        sync.WaitGroup
	startOnce sync.Once
}

// NewInboundHandler binds the handler to a root context so graceful
// shutdown (parent cancel → drain) cascades to in-flight calls.
func NewInboundHandler(rootCtx context.Context, d InboundDeps) *InboundHandler {
	if d.PickupSLA <= 0 {
		d.PickupSLA = 30 * time.Second
	}
	return &InboundHandler{d: d, rootCtx: rootCtx}
}

// Register attaches the PARK handler. Call once.
func (h *InboundHandler) Register() {
	h.startOnce.Do(func() {
		h.d.Runner.ESL.RegisterHandler("CHANNEL_PARK", h.onPark)
		slog.Info("inbound handler registered",
			"did", h.d.DID, "pickup_sla", h.d.PickupSLA.String())
	})
}

// Wait blocks until every spawned inbound session goroutine has exited.
// Pair with Sessions.DrainAll for graceful shutdown.
func (h *InboundHandler) Wait() { h.wg.Wait() }

func (h *InboundHandler) onPark(ev *eventsocket.Event) {
	uuid := ev.Get("Unique-Id")
	dest := pickInboundDest(ev)
	caller := ev.Get("Caller-Caller-Id-Number")

	if uuid == "" {
		return
	}
	if dest != h.d.DID {
		slog.Debug("inbound park ignored (DID mismatch)",
			"call_uuid", uuid, "dest", dest, "want", h.d.DID)
		return
	}
	if _, exists := h.d.Runner.Sessions.Get(uuid); exists {
		slog.Debug("inbound park duplicate (session already running)",
			"call_uuid", uuid)
		return
	}

	startedAt := time.Now()
	slog.Info("inbound park accepted",
		"call_uuid", uuid, "from", caller, "did", dest)

	// Spawn the session in a goroutine — never block the ESL loop.
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		opts := RunOpts{
			UUID:        uuid,
			Caller:      caller,
			Scenario:    h.d.Scenario,
			Direction:   session.DirectionInbound,
			NeedsAnswer: true,
			StartedAt:   startedAt,
		}
		// Log SLA breach if our setup runs past PickupSLA.
		go func() {
			select {
			case <-time.After(h.d.PickupSLA):
				if _, ok := h.d.Runner.Sessions.Get(uuid); !ok {
					return
				}
				slog.Warn("pickup SLA exceeded",
					"call_uuid", uuid, "sla", h.d.PickupSLA.String())
			case <-h.rootCtx.Done():
			}
		}()
		if err := h.d.Runner.Run(h.rootCtx, opts); err != nil {
			slog.Error("inbound session ended with error",
				"call_uuid", uuid, "err", err)
		}
	}()
}

// pickInboundDest extracts the destination number from a CHANNEL_PARK event.
// FS exposes several variants depending on dialplan + SIP profile; we try
// the most reliable ones in order.
func pickInboundDest(ev *eventsocket.Event) string {
	candidates := []string{
		"Caller-Destination-Number",
		"variable_sip_to_user",
		"variable_destination_number",
		"variable_dialed_extension",
	}
	for _, k := range candidates {
		if v := ev.Get(k); v != "" {
			return v
		}
	}
	return ""
}

func cleanupFile(path string, logger *slog.Logger) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		logger.Debug("fifo cleanup", "path", path, "err", err)
	}
}

func ms(start time.Time) int64 { return time.Since(start).Milliseconds() }

package pipeline

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/fiorix/go-eventsocket/eventsocket"

	"callbot-master/internal/registry"
	"callbot-master/internal/session"
	"callbot-master/internal/store"
)

// BotResolver looks up a bot by inbound DID. nil result = no route.
type BotResolver interface {
	GetBotByDID(ctx context.Context, did string) (*store.BotConfig, error)
}

// InboundDeps wires the inbound handler. Runner is shared with outbound
// (one per process); per-call provider construction goes through Registry.
type InboundDeps struct {
	Runner    *SessionRunner
	Resolver  BotResolver       // nil → 503: every PARK is rejected
	Registry  *registry.Registry
	PickupSLA time.Duration
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
		slog.Info("inbound handler registered (DID-driven routing)",
			"pickup_sla", h.d.PickupSLA.String())
	})
}

// Wait blocks until every spawned inbound session goroutine has exited.
func (h *InboundHandler) Wait() { h.wg.Wait() }

func (h *InboundHandler) onPark(ev *eventsocket.Event) {
	uuid := ev.Get("Unique-Id")
	dest := pickInboundDest(ev)
	caller := ev.Get("Caller-Caller-Id-Number")

	if uuid == "" {
		return
	}
	if h.d.Resolver == nil || h.d.Registry == nil {
		slog.Warn("inbound park dropped (resolver/registry not wired)",
			"call_uuid", uuid, "did", dest)
		return
	}
	if _, exists := h.d.Runner.Sessions.Get(uuid); exists {
		slog.Debug("inbound park duplicate", "call_uuid", uuid)
		return
	}

	// DB lookup happens on the ESL goroutine; fast (indexed PK on did).
	// If ever measurably slow we'd cache, but a single PK lookup is
	// cheaper than the goroutine spawn that follows.
	lookupCtx, cancel := context.WithTimeout(h.rootCtx, 2*time.Second)
	bot, err := h.d.Resolver.GetBotByDID(lookupCtx, dest)
	cancel()
	if err != nil {
		slog.Error("inbound bot lookup failed",
			"call_uuid", uuid, "did", dest, "err", err)
		return
	}
	if bot == nil {
		// Demoted to debug: when v2 shares an ESL with another master/bridge,
		// every PARK from the other process flows through here. The "this
		// DID isn't ours" path fires constantly and isn't actionable.
		slog.Debug("inbound park skipped (no bot for DID)",
			"call_uuid", uuid, "did", dest)
		return
	}

	providers, err := h.d.Registry.For(h.rootCtx, bot)
	if err != nil {
		slog.Error("inbound provider build failed",
			"call_uuid", uuid, "bot_id", bot.ID, "err", err)
		return
	}

	startedAt := time.Now()
	slog.Info("inbound park accepted",
		"call_uuid", uuid, "from", caller, "did", dest,
		"tenant", bot.TenantSlug, "bot", bot.Slug)

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		opts := RunOpts{
			UUID:        uuid,
			Caller:      caller,
			Scenario:    bot.Slug, // scenario used for metrics labels + history
			Direction:   session.DirectionInbound,
			NeedsAnswer: true,
			StartedAt:   startedAt,
			Bot:         bot,
			ASR:         providers.ASR,
			TTS:         providers.TTS,
			BotImpl:     providers.Bot,
		}
		if err := h.d.Runner.Run(h.rootCtx, opts); err != nil {
			slog.Error("inbound session ended with error",
				"call_uuid", uuid, "err", err)
		}
	}()
}

// pickInboundDest extracts the destination number from a CHANNEL_PARK event.
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

package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/fiorix/go-eventsocket/eventsocket"

	"callbot-master/internal/asr"
	"callbot-master/internal/bot"
	"callbot-master/internal/campaign"
	"callbot-master/internal/metrics"
	"callbot-master/internal/registry"
	"callbot-master/internal/session"
	"callbot-master/internal/store"
	"callbot-master/internal/tts"
)

// OutboundDeps wires the outbound handler.
type OutboundDeps struct {
	Runner   *SessionRunner
	Registry *registry.Registry // resolves providers from BotConfig
}

// outboundMetrics is a small accessor — we read Metrics off the runner so
// callers don't have to wire it twice.
func (h *OutboundHandler) outboundMetrics() *metrics.Collectors {
	if h.d.Runner == nil {
		return nil
	}
	return h.d.Runner.Metrics
}

// MakeCampaignOriginateFunc adapts the handler's Originate to the signature
// the campaign manager expects, routing status updates through the manager
// so campaign_progress metrics stay in sync.
//
// Bot is captured at campaign-create time, not at originate time, so a
// running campaign keeps using the bot config it was started with even
// if an admin edits the bot row in the meantime.
func (h *OutboundHandler) MakeCampaignOriginateFuncWithManager(mgr *campaign.Manager, c *campaign.Campaign, b *store.BotConfig) campaign.OriginateFunc {
	return func(ctx context.Context, phone, callerID, scenario string, cd map[string]any) (string, error) {
		return h.Originate(ctx, OriginateOpts{
			Phone:      phone,
			CallerID:   callerID,
			Bot:        b,
			CustomData: cd,
			OnAnswered: func(uuid string) {
				mgr.SetLeadStatus(c, uuid, campaign.StatusAnswered, "")
			},
			OnEnded: func(uuid string, status campaign.CallStatus, errMsg string) {
				mgr.SetLeadStatus(c, uuid, status, errMsg)
			},
		})
	}
}

// pendingCall holds the metadata of an originated call until the callee
// answers (CHANNEL_ANSWER) — at which point we hand off to SessionRunner.
// Bot config + providers are captured here at Originate time so a campaign
// running in parallel cannot have its bot config swapped mid-flight if
// an admin edits it.
type pendingCall struct {
	uuid       string
	phone      string
	scenario   string
	customData map[string]any
	originated time.Time
	// Resolved at Originate; used at ANSWER time.
	bot        *store.BotConfig
	asrClient  asr.Client
	ttsClient  tts.Client
	botClient  bot.Client
	// Optional callbacks for campaign integration.
	onAnswered func(uuid string)
	onEnded    func(uuid string, status campaign.CallStatus, errMsg string)
	// Filled when ANSWER fires; used by HANGUP path to mark COMPLETED vs FAILED.
	wasAnswered bool
}

// OutboundHandler manages outbound dialing: Originate API used by
// campaign workers, ANSWER handler that picks up answered calls, and
// HANGUP handler that cleans up + reports campaign status.
type OutboundHandler struct {
	d         OutboundDeps
	rootCtx   context.Context
	pending   sync.Map // uuid → *pendingCall
	wg        sync.WaitGroup
	startOnce sync.Once
}

// NewOutboundHandler binds to a root context so SIGTERM cascades.
func NewOutboundHandler(rootCtx context.Context, d OutboundDeps) *OutboundHandler {
	return &OutboundHandler{d: d, rootCtx: rootCtx}
}

// Register attaches CHANNEL_ANSWER + CHANNEL_HANGUP handlers. Call once.
func (h *OutboundHandler) Register() {
	h.startOnce.Do(func() {
		h.d.Runner.ESL.RegisterHandler("CHANNEL_ANSWER", h.onAnswer)
		h.d.Runner.ESL.RegisterHandler("CHANNEL_HANGUP", h.onHangup)
		slog.Info("outbound handler registered")
	})
}

// Wait blocks until every spawned outbound session goroutine has exited.
func (h *OutboundHandler) Wait() { h.wg.Wait() }

// OriginateOpts groups the fields needed to place an outbound call.
// onAnswered/onEnded are optional hooks the campaign manager uses to keep
// lead status in sync with FS reality.
//
// Bot is required: the campaign manager resolves it before calling
// Originate, so this handler doesn't need to know about the DB.
type OriginateOpts struct {
	Phone      string
	CallerID   string
	Bot        *store.BotConfig
	CustomData map[string]any
	OnAnswered func(uuid string)
	OnEnded    func(uuid string, status campaign.CallStatus, errMsg string)
}

// Originate places one outbound call via FreeSWITCH. The returned UUID is
// pre-generated (origination_uuid) and registered as pending — when
// CHANNEL_ANSWER fires for it, the runner picks up automatically.
//
// If the originate API call fails, the UUID is dropped from pending and
// onEnded(StatusFailed) is invoked synchronously.
func (h *OutboundHandler) Originate(ctx context.Context, opts OriginateOpts) (string, error) {
	if opts.Bot == nil {
		return "", fmt.Errorf("originate: bot config required")
	}
	if h.d.Registry == nil {
		return "", fmt.Errorf("originate: registry not wired")
	}
	providers, err := h.d.Registry.For(ctx, opts.Bot)
	if err != nil {
		return "", fmt.Errorf("resolve providers: %w", err)
	}
	uuid := genUUID()
	pc := &pendingCall{
		uuid:       uuid,
		phone:      opts.Phone,
		scenario:   opts.Bot.Slug,
		customData: opts.CustomData,
		originated: time.Now(),
		bot:        opts.Bot,
		asrClient:  providers.ASR,
		ttsClient:  providers.TTS,
		botClient:  providers.Bot,
		onAnswered: opts.OnAnswered,
		onEnded:    opts.OnEnded,
	}
	h.pending.Store(uuid, pc)

	caller := opts.CallerID
	if caller == "" {
		caller = "callbot"
	}
	scenario := opts.Bot.Slug
	// Prepend the bot's outbound prefix (carrier-routing key in the FS
	// dialplan) when set and the user-supplied phone doesn't already
	// start with it. Lets ops type clean numbers (0971…) on the form
	// while the dialplan keeps its `^3323(\d{10})$` regex working.
	dial := opts.Phone
	if opts.Bot.OutboundPrefix != "" && !strings.HasPrefix(dial, opts.Bot.OutboundPrefix) {
		dial = opts.Bot.OutboundPrefix + dial
	}
	pc.phone = dial // dialed form is what FS will report; metrics + history align
	// "bot id" arg in v1's Originate signature was a custom var; we keep
	// the slot for compatibility but don't rely on it.
	if err := h.d.Runner.ESL.Originate(uuid, dial, caller, "bot-"+uuid, scenario); err != nil {
		h.pending.Delete(uuid)
		if m := h.outboundMetrics(); m != nil {
			m.OriginateTotal.WithLabelValues("error").Inc()
		}
		if opts.OnEnded != nil {
			opts.OnEnded(uuid, campaign.StatusFailed, err.Error())
		}
		return "", err
	}
	if m := h.outboundMetrics(); m != nil {
		m.OriginateTotal.WithLabelValues("ok").Inc()
	}
	slog.Info("outbound originate sent",
		"call_uuid", uuid, "phone", opts.Phone,
		"caller_id", caller, "scenario", scenario)
	return uuid, nil
}

func (h *OutboundHandler) onAnswer(ev *eventsocket.Event) {
	uuid := ev.Get("Unique-Id")
	if uuid == "" {
		return
	}
	v, ok := h.pending.Load(uuid)
	if !ok {
		return
	}
	pc := v.(*pendingCall)
	pc.wasAnswered = true

	if _, dup := h.d.Runner.Sessions.Get(uuid); dup {
		slog.Debug("outbound answer duplicate (session already running)",
			"call_uuid", uuid)
		return
	}

	startedAt := time.Now()
	slog.Info("outbound answer accepted",
		"call_uuid", uuid, "phone", pc.phone,
		"dial_to_answer_ms", startedAt.Sub(pc.originated).Milliseconds())
	if pc.onAnswered != nil {
		pc.onAnswered(uuid)
	}

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		opts := RunOpts{
			UUID:        uuid,
			Caller:      pc.phone,
			Phone:       pc.phone,
			Scenario:    pc.scenario,
			Direction:   session.DirectionOutbound,
			NeedsAnswer: false, // callee already answered
			StartedAt:   startedAt,
			Bot:         pc.bot,
			ASR:         pc.asrClient,
			TTS:         pc.ttsClient,
			BotImpl:     pc.botClient,
			LeadID:      strFromMap(pc.customData, "lead_id"),
			Gender:      strFromMap(pc.customData, "gender"),
			Name:        strFromMap(pc.customData, "name"),
			Plate:       strFromMap(pc.customData, "plate"),
			OnEnd: func() {
				if pc.onEnded != nil {
					pc.onEnded(uuid, campaign.StatusCompleted, "")
				}
				h.pending.Delete(uuid)
			},
		}
		if err := h.d.Runner.Run(h.rootCtx, opts); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			slog.Error("outbound session ended with error",
				"call_uuid", uuid, "err", err)
		}
	}()
}

func (h *OutboundHandler) onHangup(ev *eventsocket.Event) {
	uuid := ev.Get("Unique-Id")
	if uuid == "" {
		return
	}
	cause := ev.Get("Hangup-Cause")

	// Active session? Cancel it; OnEnd handles status reporting.
	if sess, ok := h.d.Runner.Sessions.Get(uuid); ok {
		slog.Info("outbound hangup cancels active session",
			"call_uuid", uuid, "cause", cause)
		sess.Cancel()
		return
	}

	// Pending but never answered → no_answer / failed.
	v, ok := h.pending.LoadAndDelete(uuid)
	if !ok {
		return
	}
	pc := v.(*pendingCall)
	if pc.wasAnswered {
		// Edge: ANSWER fired but session goroutine hasn't started yet —
		// rare race; treat as completed since pickup happened.
		if pc.onEnded != nil {
			pc.onEnded(uuid, campaign.StatusCompleted, "")
		}
		return
	}
	status := classifyHangup(cause)
	slog.Info("outbound hangup before answer",
		"call_uuid", uuid, "cause", cause, "lead_status", status)
	if pc.onEnded != nil {
		pc.onEnded(uuid, status, cause)
	}
}

// classifyHangup maps an FS Hangup-Cause to a campaign lead status.
// See https://wiki.freeswitch.org/wiki/Hangup_Causes.
func classifyHangup(cause string) campaign.CallStatus {
	switch cause {
	case "NO_ANSWER", "ALLOTTED_TIMEOUT", "USER_NOT_REGISTERED":
		return campaign.StatusNoAnswer
	case "CALL_REJECTED", "USER_BUSY", "DESTINATION_OUT_OF_ORDER",
		"NORMAL_TEMPORARY_FAILURE", "NETWORK_OUT_OF_ORDER":
		return campaign.StatusFailed
	case "":
		return campaign.StatusFailed
	default:
		return campaign.StatusFailed
	}
}

// MakeCampaignOriginateFunc adapts the handler's Originate to the signature
// the campaign manager expects.
func (h *OutboundHandler) MakeCampaignOriginateFunc(c *campaign.Campaign, b *store.BotConfig) campaign.OriginateFunc {
	return func(ctx context.Context, phone, callerID, scenario string, cd map[string]any) (string, error) {
		return h.Originate(ctx, OriginateOpts{
			Phone:      phone,
			CallerID:   callerID,
			Bot:        b,
			CustomData: cd,
			OnAnswered: func(uuid string) {
				c.SetLeadStatus(uuid, campaign.StatusAnswered, "")
			},
			OnEnded: func(uuid string, status campaign.CallStatus, errMsg string) {
				c.SetLeadStatus(uuid, status, errMsg)
			},
		})
	}
}

// strFromMap pulls a string value from a custom-data map, returning "" if
// the key is missing or the value isn't a string.
func strFromMap(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// genUUID generates a v4 UUID without external deps.
func genUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Astronomically unlikely; fall back to a time-based marker so we
		// never return an empty string to the caller.
		return fmt.Sprintf("uuid-fallback-%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]))
}

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"callbot-master/internal/asr"
	"callbot-master/internal/audio"
	"callbot-master/internal/bot"
	"callbot-master/internal/freeswitch"
	"callbot-master/internal/metrics"
	"callbot-master/internal/session"
	"callbot-master/internal/store"
	"callbot-master/internal/tts"
	"callbot-master/internal/vad"
)

// SessionRunner owns the shared call lifecycle: answer (if needed) → mkfifo
// → uuid_record → uuid_broadcast → greeting + turn loop → cleanup.
//
// Both inbound (PARK pickup) and outbound (campaign-originated, ANSWER
// pickup) flows construct one of these and call Run.
type SessionRunner struct {
	ESL          *freeswitch.EventSocket
	Sessions     *session.Manager
	ASR          asr.Client
	TTS          tts.Client
	Bot          bot.Client
	NewVAD       func() vad.Detector
	RecordingDir string
	TTSDir       string
	FrameBytes   int
	PipelineCfg  Config
	BargeIn       bool                // toggle barge-in detection during Speak
	BargeMinWords int                 // word threshold; 0 → use Pipeline default (3)
	Metrics       *metrics.Collectors // nil-safe
	Store        CallStore           // nil-safe — when set, every call is persisted at end of Run
}

// CallStore is the minimal slice of store.Postgres SessionRunner needs.
// Decoupled so unit tests can mock without dragging in pgx.
type CallStore interface {
	Insert(ctx context.Context, r *store.CallRecord) error
}

// RunOpts describes one call's setup decisions.
type RunOpts struct {
	UUID        string
	Caller      string // caller-id number (logging only)
	Phone       string // for call_history persistence — defaults to Caller if empty
	Scenario    string
	Direction   session.Direction
	NeedsAnswer bool      // inbound (parked) → true; outbound (callee picked up) → false
	StartedAt   time.Time // event-time of the trigger (PARK or ANSWER) for SLA tracking
	OnEnd       func()    // optional callback at end-of-call (campaign status update, etc.)
	// Lead metadata for outbound campaigns; persisted into call_history.
	LeadID string
	Gender string
	Name   string
	Plate  string
}

// Run blocks until the call ends (ENDCALL, hangup, error). Returns error
// only on infrastructure failures; ENDCALL is a normal exit.
func (r *SessionRunner) Run(parent context.Context, opts RunOpts) (retErr error) {
	if opts.StartedAt.IsZero() {
		opts.StartedAt = time.Now()
	}
	if r.FrameBytes <= 0 {
		r.FrameBytes = 320
	}
	if opts.Phone == "" {
		opts.Phone = opts.Caller
	}

	// p is assigned later (after FIFO + ESL setup); the persist closure
	// reads it after Run returns so we capture the post-call history.
	var p *Pipeline

	// Persist call_history at end-of-call. nil-safe via closure capture.
	defer func() {
		if r.Store == nil {
			return
		}
		rec := buildCallRecord(opts, p, retErr, time.Now())
		// Use a fresh ctx — parent may be canceled (graceful shutdown).
		insertCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.Store.Insert(insertCtx, rec); err != nil {
			slog.Warn("call_history insert failed",
				"call_uuid", opts.UUID, "err", err)
		}
	}()

	// Root call span. Children: pipeline.greeting → pipeline.turn[N], each
	// of which contains asr.listen / bot.turn / tts.turn.
	ctx, span := tracer.Start(parent,
		"call."+opts.Direction.String(),
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String(attrCallUUID, opts.UUID),
			attribute.String(attrDirection, opts.Direction.String()),
			attribute.String(attrScenario, opts.Scenario),
			attribute.String(attrCaller, opts.Caller),
		),
	)
	defer span.End()

	sess := session.New(ctx, opts.UUID, opts.Scenario, opts.Direction)
	r.Sessions.Add(sess)
	defer r.Sessions.Remove(opts.UUID)
	defer sess.Cancel()
	if opts.OnEnd != nil {
		defer opts.OnEnd()
	}

	// active_calls gauge — bracket the whole call lifetime.
	if r.Metrics != nil {
		r.Metrics.ActiveCalls.WithLabelValues(opts.Direction.String()).Inc()
		defer r.Metrics.ActiveCalls.WithLabelValues(opts.Direction.String()).Dec()
	}

	logger := slog.With(
		"call_uuid", opts.UUID,
		"direction", opts.Direction.String(),
		"scenario", opts.Scenario,
		"caller", opts.Caller,
	)
	logger.Info("session start")

	if opts.NeedsAnswer {
		if err := r.ESL.Answer(opts.UUID); err != nil {
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("answer: %w", err)
		}
		addEventMs(span, "answered", ms(opts.StartedAt))
		logger.Debug("answered", "answered_ms", ms(opts.StartedAt))
	}

	recordPath := filepath.Join(r.RecordingDir, opts.UUID+".raw")
	ttsPath := filepath.Join(r.TTSDir, opts.UUID+".raw")
	if err := audio.EnsureDir(recordPath); err != nil {
		return fmt.Errorf("ensure record dir: %w", err)
	}
	if err := audio.EnsureDir(ttsPath); err != nil {
		return fmt.Errorf("ensure tts dir: %w", err)
	}
	if err := audio.MakeFIFO(recordPath); err != nil {
		return fmt.Errorf("mkfifo recording: %w", err)
	}
	defer cleanupFile(recordPath, logger)
	if err := audio.MakeFIFO(ttsPath); err != nil {
		return fmt.Errorf("mkfifo tts: %w", err)
	}
	defer cleanupFile(ttsPath, logger)

	src, err := audio.NewFIFOSource(sess.Context(), opts.UUID, recordPath, r.FrameBytes)
	if err != nil {
		return fmt.Errorf("fifo source: %w", err)
	}
	defer src.Close()

	if err := r.ESL.StartRecording(opts.UUID, recordPath); err != nil {
		return fmt.Errorf("start recording: %w", err)
	}
	defer func() {
		if err := r.ESL.StopRecording(opts.UUID, recordPath); err != nil {
			logger.Warn("stop recording failed", "err", err)
		}
	}()

	// Open the TTS sink BEFORE issuing uuid_broadcast. FreeSWITCH's
	// playback module opens the FIFO read-side with O_NONBLOCK; if we
	// haven't attached a writer yet it sees EOF and immediately ends the
	// playback (and tears down the recording bug as a side-effect). Our
	// O_RDWR open keeps a writer refcount so FS always sees a peer.
	sink, err := audio.NewFIFOSink(opts.UUID, ttsPath)
	if err != nil {
		return fmt.Errorf("fifo sink: %w", err)
	}
	defer sink.Close()

	if err := r.ESL.PlayAudio(opts.UUID, ttsPath); err != nil {
		return fmt.Errorf("play audio: %w", err)
	}
	addEventMs(span, "audio_path_established", ms(opts.StartedAt))
	logger.Info("audio path established",
		"setup_ms", ms(opts.StartedAt),
		"record_fifo", recordPath,
		"tts_fifo", ttsPath)

	pCfg := r.PipelineCfg
	if pCfg.SampleRate == 0 {
		pCfg = Defaults()
	}
	var v vad.Detector
	if r.NewVAD != nil {
		v = r.NewVAD()
	}
	p = New(opts.UUID, pCfg, r.ASR, r.TTS, r.Bot, v)
	p.Scenario = opts.Scenario
	p.Metrics = r.Metrics
	p.BargeIn = r.BargeIn
	p.BargeMinWords = r.BargeMinWords
	p.ESL = r.ESL // satisfies bargeESL interface

	// Greeting turn (empty user message → bot decides based on history).
	// We pass src so Speak can drain the recording FIFO during the 5+ s
	// of greeting playback. Otherwise the next Listen burst-reads the
	// accumulated backlog (mostly bot's own echo) and ASR finalizes it
	// as garbage, eating one whole turn.
	greetingStart := time.Now()
	greetCtx, greetSpan := tracer.Start(sess.Context(), "pipeline.greeting",
		trace.WithAttributes(
			attribute.String(attrTurnKind, "greeting"),
			attribute.Int(attrTurnIdx, 0),
			attribute.String(attrCallUUID, opts.UUID),
		),
	)
	cont, err := p.RunGreeting(greetCtx, src, sink)
	greetSpan.End()
	if r.Metrics != nil {
		r.Metrics.TurnDuration.WithLabelValues("greeting", opts.Scenario).
			Observe(time.Since(greetingStart).Seconds())
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("session canceled during greeting")
			return nil
		}
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("greeting turn: %w", err)
	}
	logger.Info("greeting done",
		"greet_ms", ms(greetingStart),
		"continue", cont)

	turnIdx := 1
	for cont {
		select {
		case <-sess.Context().Done():
			logger.Info("session canceled mid-loop")
			return nil
		default:
		}
		turnStart := time.Now()
		turnCtx, turnSpan := tracer.Start(sess.Context(), "pipeline.turn",
			trace.WithAttributes(
				attribute.String(attrTurnKind, "turn"),
				attribute.Int(attrTurnIdx, turnIdx),
				attribute.String(attrCallUUID, opts.UUID),
			),
		)
		var turnErr error
		cont, turnErr = p.RunTurn(turnCtx, src, sink)
		turnSpan.End()
		if r.Metrics != nil {
			r.Metrics.TurnDuration.WithLabelValues("turn", opts.Scenario).
				Observe(time.Since(turnStart).Seconds())
		}
		if turnErr != nil {
			if errors.Is(turnErr, context.Canceled) {
				logger.Info("session canceled mid-turn", "turn", turnIdx)
				return nil
			}
			turnSpan.SetStatus(codes.Error, turnErr.Error())
			span.SetStatus(codes.Error, turnErr.Error())
			logger.Error("turn failed", "turn", turnIdx, "err", turnErr)
			return turnErr
		}
		logger.Info("turn done",
			"turn", turnIdx,
			"turn_ms", ms(turnStart),
			"continue", cont)
		turnIdx++
	}
	span.SetAttributes(attribute.Int("call.turns", turnIdx-1))

	// Bot signaled ENDCALL — ask FS to hang up.
	if err := r.ESL.EndCall(opts.UUID); err != nil {
		logger.Warn("uuid_kill failed", "err", err)
	}
	logger.Info("session end",
		"total_ms", ms(opts.StartedAt),
		"turns", turnIdx-1)
	return nil
}

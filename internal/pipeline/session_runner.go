package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fiorix/go-eventsocket/eventsocket"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"callbot-master/internal/asr"
	"callbot-master/internal/audio"
	"callbot-master/internal/bot"
	"callbot-master/internal/freeswitch"
	"callbot-master/internal/metrics"
	"callbot-master/internal/recording"
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
	Archiver     *recording.Archiver // nil-safe — copies FS recording to persistent dir post-hangup

	// playbackStops maps a FIFO path → *playbackHandle for the in-flight
	// utterance on that path. We key by FULL PATH (not uuid) because FS
	// emits PLAYBACK_STOP with the file path in Application-Data, and a
	// stale PLAYBACK_STOP from a previous utterance must not signal the
	// next one. signalDone is sync.Once-guarded so duplicate events or
	// timeout-then-late-event sequences don't panic.
	playbackStops sync.Map // path → *playbackHandle
	registerOnce  sync.Once
}

// RegisterESLHandlers wires the PLAYBACK_STOP listener that drives Speak's
// post-audio drain. Idempotent — safe to call multiple times.
func (r *SessionRunner) RegisterESLHandlers() {
	r.registerOnce.Do(func() {
		r.ESL.RegisterHandler("PLAYBACK_STOP", r.onPlaybackStop)
	})
}

func (r *SessionRunner) onPlaybackStop(ev *eventsocket.Event) {
	// Match by FILE PATH, not call uuid. Multiple utterances on the same
	// channel produce multiple PLAYBACK_STOP events and we need to know
	// which one finished. FS sets Application-Data to the playback path
	// (and "Playback-File-Path" on some versions); we try both.
	path := ev.Get("Application-Data")
	if path == "" {
		path = ev.Get("Playback-File-Path")
	}
	if path == "" {
		return
	}
	v, ok := r.playbackStops.LoadAndDelete(path)
	if !ok {
		return
	}
	if h, ok := v.(*playbackHandle); ok {
		h.signalDone()
	}
}

// makePlaybackOpener returns a closure that produces a fresh per-utterance
// audio sink. Invoked once per Speak via Pipeline.PlaybackOpen.
//
// Lifecycle of one utterance:
//   1. mkfifo /<TTSDir>/<uuid>-<N>.raw      (unique name per utterance)
//   2. uuid_broadcast <uuid> <path>         (FS opens read side, queues read)
//   3. open <path> O_WRONLY                  (blocks until FS attaches)
//   4. master writes audio frames
//   5. closer.Close() closes the fd → FS reads EOF, ends playback,
//      and the FIFO file is os.Remove()'d.
func (r *SessionRunner) makePlaybackOpener(uuid string, logger *slog.Logger) PlaybackOpener {
	var counter int64
	return func(ctx context.Context) (io.WriteCloser, error) {
		n := atomic.AddInt64(&counter, 1)
		path := filepath.Join(r.TTSDir, fmt.Sprintf("%s-%d.raw", uuid, n))
		if err := audio.MakeFIFO(path); err != nil {
			return nil, fmt.Errorf("mkfifo %s: %w", path, err)
		}

		// Build the handle first so we can register it under the FIFO
		// path BEFORE issuing uuid_broadcast — otherwise a fast FS
		// could race PLAYBACK_STOP ahead of our Store.
		h := &playbackHandle{
			path:   path,
			uuid:   uuid,
			logger: logger,
			done:   make(chan struct{}),
			runner: r,
		}
		r.playbackStops.Store(path, h)

		// uuid_broadcast must happen BEFORE OpenFIFOForWrite so FS attaches
		// the read side and the open(O_WRONLY) unblocks.
		if err := r.ESL.PlayAudio(uuid, path); err != nil {
			r.playbackStops.Delete(path)
			_ = os.Remove(path)
			return nil, fmt.Errorf("uuid_broadcast: %w", err)
		}
		f, err := audio.OpenFIFOForWrite(path)
		if err != nil {
			r.playbackStops.Delete(path)
			_ = os.Remove(path)
			return nil, err
		}
		h.f = f
		logger.Debug("playback opened",
			"call_uuid", uuid, "path", path, "utt", n)
		return h, nil
	}
}

// playbackHandle wraps an *os.File so Close cleans up the FIFO file too.
// Done() returns a channel closed by the global PLAYBACK_STOP handler when
// FreeSWITCH actually finishes playing this utterance (not when we finish
// writing — those moments are ~1 s apart due to the kernel pipe buffer +
// FS's internal jitter buffer).
type playbackHandle struct {
	f        *os.File
	path     string
	uuid     string
	logger   *slog.Logger
	done     chan struct{}
	doneOnce sync.Once
	runner   *SessionRunner
	closed   atomicBool
}

// Done returns the playback-complete signal; reading from a closed chan
// is non-blocking, so callers can use it in a select with a timeout.
func (h *playbackHandle) Done() <-chan struct{} { return h.done }

// signalDone closes h.done exactly once. Safe to call from the
// PLAYBACK_STOP handler and from Close().
func (h *playbackHandle) signalDone() {
	h.doneOnce.Do(func() { close(h.done) })
}

type atomicBool struct{ v int32 }

func (a *atomicBool) cas(old, new bool) bool {
	from, to := int32(0), int32(1)
	if old {
		from = 1
	}
	if !new {
		to = 0
	}
	return atomic.CompareAndSwapInt32(&a.v, from, to)
}

func (h *playbackHandle) Write(p []byte) (int, error) { return h.f.Write(p) }

func (h *playbackHandle) Close() error {
	if !h.closed.cas(false, true) {
		return nil
	}
	// Close the FIFO write side so FS reads EOF and can finish its
	// playback. We do NOT remove the playbackStops entry here — that
	// belongs to the PLAYBACK_STOP handler, which needs to find this
	// handle to signal Done(). If PLAYBACK_STOP never arrives the
	// caller's select fires its timeout branch instead.
	closeErr := h.f.Close()
	if err := os.Remove(h.path); err != nil && !os.IsNotExist(err) {
		h.logger.Debug("playback fifo cleanup failed",
			"call_uuid", h.uuid, "path", h.path, "err", err)
	}
	return closeErr
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

	// Archive the FS-side recording out-of-band. Runs in its own
	// goroutine because the source MP3 typically isn't flushed for a
	// few seconds after CHANNEL_HANGUP_COMPLETE — we don't want to
	// hold up the rest of the cleanup waiting on disk I/O. Persists
	// the resulting URL into call_history (the goroutine outlives
	// Run, which is fine — Persister handles its own ctx).
	if r.Archiver != nil && r.Archiver.Enabled() {
		defer func(uuid string) {
			go func() {
				archiveCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				if _, err := r.Archiver.Archive(archiveCtx, uuid); err != nil {
					slog.Warn("recording archive failed",
						"call_uuid", uuid, "err", err)
				}
			}()
		}(opts.UUID)
	}

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
	if err := audio.EnsureDir(recordPath); err != nil {
		return fmt.Errorf("ensure record dir: %w", err)
	}
	if err := os.MkdirAll(r.TTSDir, 0o755); err != nil {
		return fmt.Errorf("ensure tts dir: %w", err)
	}
	if err := audio.MakeFIFO(recordPath); err != nil {
		return fmt.Errorf("mkfifo recording: %w", err)
	}
	defer cleanupFile(recordPath, logger)

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

	// TTS playback uses a FRESH FIFO per utterance — see PlaybackOpener.
	// The session-level setup keeps only the recording FIFO; each Speak
	// builds its own audio sink via Pipeline.PlaybackOpen, signals EOF
	// when done, and removes the file. This matches the v1 model and
	// prevents FS's playback from blocking on an empty long-lived FIFO
	// (the reason the caller heard "...em có thể hỗ <silence> trợ gì..."
	// before this refactor).
	playbackOpen := r.makePlaybackOpener(opts.UUID, logger)
	addEventMs(span, "audio_path_established", ms(opts.StartedAt))
	logger.Info("audio path established",
		"setup_ms", ms(opts.StartedAt),
		"record_fifo", recordPath,
		"tts_dir", r.TTSDir)

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
	p.PlaybackOpen = playbackOpen

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
	// sink=nil → Pipeline.PlaybackOpen creates a fresh per-utterance FIFO.
	cont, err := p.RunGreeting(greetCtx, src, nil)
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
		cont, turnErr = p.RunTurn(turnCtx, src, nil)
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

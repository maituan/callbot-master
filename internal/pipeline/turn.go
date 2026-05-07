// Package pipeline orchestrates one turn of caller↔bot interaction:
//
//	Listen:  audio frames → VAD + ASR → final transcript
//	Speak:   transcript  → bot stream → TTS stream → audio frames
//
// One Pipeline instance is bound to a single Session (= one FS call uuid).
// Live (FS) and offline (file/CLI) modes share the same code path; only
// the AudioSource/Sink implementations differ.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"callbot-master/internal/asr"
	"callbot-master/internal/audio"
	"callbot-master/internal/bot"
	"callbot-master/internal/metrics"
	"callbot-master/internal/tts"
	"callbot-master/internal/vad"
)

// AudioSource and AudioSink are aliased here for backwards compat with
// existing call sites; the canonical definitions live in package audio.
type AudioSource = audio.Source
type AudioSink = audio.Sink

// PlaybackOpener creates the per-utterance audio sink. SessionRunner
// implements this by issuing mkfifo + uuid_broadcast + open(O_WRONLY)
// and returning a handle whose Close() removes the FIFO file. Speak
// calls it once per call.
type PlaybackOpener func(ctx context.Context) (io.WriteCloser, error)

// Config carries per-turn knobs that aren't already on the provider opts.
//
// ASR timeouts (SilenceTimeout / SpeechTimeout / SpeechMax) are forwarded
// to the provider via asr.StreamOpts; the meanings match Viettel STT —
// see config.ASRConfig for the full doc.
type Config struct {
	SampleRate     int
	VoiceID        string
	Tempo          float64
	ResampleRate   int
	SilentTimeout  time.Duration // master-side safety cap when src never EOFs (hard upper bound)
	FirstByteTotal time.Duration // soft alert threshold (logged, doesn't error)

	ASRSilenceTimeout time.Duration // pre-speech silence → empty IsFinal
	ASRSpeechTimeout  time.Duration // post-speech trailing silence → text IsFinal
	ASRSpeechMax      time.Duration // hard cap on a single utterance
}

// Defaults returns a config suitable for the HCC scenario at 8kHz.
func Defaults() Config {
	return Config{
		SampleRate:        8000,
		ResampleRate:      8000,
		Tempo:             1.0,
		SilentTimeout:     30 * time.Second,
		FirstByteTotal:    5 * time.Second,
		ASRSilenceTimeout: 5 * time.Second,
		ASRSpeechTimeout:  800 * time.Millisecond,
		ASRSpeechMax:      30 * time.Second,
	}
}

// Pipeline holds the per-call provider clients + a per-turn config.
type Pipeline struct {
	UUID     string
	Cfg      Config
	Scenario string // for metrics labels

	ASR asr.Client
	TTS tts.Client
	Bot bot.Client
	VAD vad.Detector

	// ESL is required when BargeIn is enabled — Speak issues uuid_break
	// to stop FS playback on barge-in. nil-safe (best-effort).
	ESL bargeESL

	// BargeIn enables the in-Speak ASR monitor. When false, Speak doesn't
	// read from src at all — caller's audio is buffered in the FIFO until
	// Listen resumes (we drain it during Speak to avoid stale-audio bleed).
	BargeIn bool

	// BargeMinWords is the threshold of words in a running transcript that
	// triggers barge-in. Default 3 — short enough to be responsive, long
	// enough to ignore monosyllabic backchannel ("ờ", "vâng").
	BargeMinWords int

	Metrics *metrics.Collectors // nil-safe

	// pendingTranscript carries the user's text captured by the barge-in
	// monitor. RunTurn consumes it on the next iteration to skip Listen
	// entirely (the transcript is already in hand).
	pendingTranscript string

	// PlaybackOpen produces a fresh sink per utterance: SessionRunner
	// wires this to mkfifo + uuid_broadcast + open, with cleanup on
	// Close(). Speak invokes it once per call so every turn gets its own
	// FIFO file. Required for live FS calls; offline/tests pass a sink
	// directly via the SinkOverride below.
	PlaybackOpen PlaybackOpener

	// SinkOverride bypasses PlaybackOpen — used by offline-cli + tests
	// where the sink is just an in-memory buffer or file.
	SinkOverride AudioSink

	// Per-turn outputs of the most recent Speak — read by RunTurn to build
	// TurnRecord entries for the persistent call history.
	lastBotText  string
	lastBargedIn bool

	// history accumulates all turns of this call. Read by SessionRunner at
	// end of Run via History().
	history []TurnRecord
}

// TurnRecord captures one Listen→Speak cycle for persistence. The greeting
// turn has UserText == "".
type TurnRecord struct {
	Index     int
	UserText  string
	BotText   string
	Action    bot.Action
	StartedAt time.Time
	EndedAt   time.Time
	BargedIn  bool
}

// History returns the accumulated turn records (chronological).
func (p *Pipeline) History() []TurnRecord {
	out := make([]TurnRecord, len(p.history))
	copy(out, p.history)
	return out
}

// bargeESL is the minimal slice of *freeswitch.EventSocket that pipeline
// needs for barge-in. Hidden behind an interface so unit tests can mock it
// without dragging in the freeswitch package.
type bargeESL interface {
	StopPlayback(uuid string) error
}

func New(uuid string, cfg Config, a asr.Client, t tts.Client, b bot.Client, v vad.Detector) *Pipeline {
	return &Pipeline{UUID: uuid, Cfg: cfg, ASR: a, TTS: t, Bot: b, VAD: v}
}

// resolveSink picks the writer for one Speak call. Priority order:
//   1. SinkOverride (offline-cli / tests)
//   2. Explicit sinkArg if non-nil (legacy callers / tests)
//   3. PlaybackOpen factory (live FS path)
//
// The second return is the closer to invoke at end of Speak. nil means
// "no per-utterance teardown needed" (the caller owns sink lifetime).
func (p *Pipeline) resolveSink(ctx context.Context, sinkArg AudioSink) (AudioSink, io.Closer, error) {
	if p.SinkOverride != nil {
		return p.SinkOverride, nil, nil
	}
	if sinkArg != nil {
		return sinkArg, nil, nil
	}
	if p.PlaybackOpen == nil {
		return nil, nil, errors.New("no sink: PlaybackOpen and SinkOverride both nil")
	}
	wc, err := p.PlaybackOpen(ctx)
	if err != nil {
		return nil, nil, err
	}
	return wc, wc, nil
}

// frameBytes returns the per-frame byte size implied by Config (S16LE).
// 320 bytes default = 20 ms @ 8 kHz.
func (p *Pipeline) frameBytes() int {
	if p.Cfg.SampleRate <= 0 {
		return 320
	}
	// 20 ms frame default.
	return p.Cfg.SampleRate * 20 / 1000 * 2
}

// Listen streams audio frames from src to ASR and returns the final
// transcript Viettel STT flushes. End-of-utterance detection is delegated
// to the provider:
//
//   - speech_timeout: post-speech trailing silence → IsFinal with text
//   - silence_timeout: caller never spoke → IsFinal with empty text
//
// Master used to layer an energy-based VAD on top of this to "cut sooner",
// but RMS thresholding on telephony audio (a-law decompressed, handset
// echo, ambient noise) is brittle and the ASR side already does this with
// linguistic awareness. We let the provider drive end-of-utterance.
//
// VAD is still wired through Pipeline for the barge-in path inside Speak —
// just not used here.
//
// Stops when:
//   - ASR emits a Result with IsFinal=true (text or empty), OR
//   - src closes its channel (offline / FIFO peer disconnect), OR
//   - ctx cancels, OR
//   - cfg.SilentTimeout elapses (master-side safety cap, > ASR timeouts).
//
// Returns ("", nil) on empty IsFinal — caller decides whether to re-listen
// or end the call.
func (p *Pipeline) Listen(ctx context.Context, src AudioSource) (string, error) {
	ctx, span := tracer.Start(ctx, "asr.listen", trace.WithAttributes(callAttrs(p.UUID)...))
	defer span.End()

	stream, err := p.ASR.StartStream(ctx, asr.StreamOpts{
		ConversationID:   p.UUID,
		SampleRate:       p.Cfg.SampleRate,
		Channels:         1,
		SingleSentence:   true,
		SilenceTimeoutMs: int(p.Cfg.ASRSilenceTimeout / time.Millisecond),
		SpeechTimeoutMs:  int(p.Cfg.ASRSpeechTimeout / time.Millisecond),
		SpeechMaxMs:      int(p.Cfg.ASRSpeechMax / time.Millisecond),
	})
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		incASRErr(p.Metrics, "stream_open")
		return "", fmt.Errorf("asr start: %w", err)
	}
	defer stream.Close()

	frames := src.Frames()
	startedAt := time.Now()
	gotFirstPartial := false
	timeout := time.NewTimer(p.Cfg.SilentTimeout)
	defer timeout.Stop()

	closer, _ := stream.(interface{ CloseSend() error })
	closeSend := func() {
		// Half-close the send side so the server can flush its final
		// transcript. If the impl doesn't support CloseSend (mocks/tests),
		// we leave the stream alone — the deferred stream.Close() at the
		// end of Listen handles full teardown. Crucially do NOT call
		// Close() here: that would kill the recv channel before drain.
		if closer != nil {
			_ = closer.CloseSend()
		}
	}

	final := ""
	srcDrained := false
	gotFinal := false

readLoop:
	for {
		select {
		case <-ctx.Done():
			span.SetStatus(codes.Error, "ctx canceled")
			return "", ctx.Err()

		case <-timeout.C:
			slog.Warn("pipeline.listen silent timeout",
				"call_uuid", p.UUID,
				"after", time.Since(startedAt))
			addEventMs(span, "silent_timeout", time.Since(startedAt).Milliseconds())
			closeSend()
			break readLoop

		case frame, ok := <-frames:
			if !ok {
				srcDrained = true
				addEventMs(span, "src_eof", time.Since(startedAt).Milliseconds())
				closeSend()
				break readLoop
			}
			if err := stream.SendAudio(frame); err != nil {
				if errors.Is(err, io.EOF) {
					break readLoop
				}
				span.SetStatus(codes.Error, err.Error())
				return "", fmt.Errorf("asr send: %w", err)
			}

		case res, ok := <-stream.Recv():
			if !ok {
				break readLoop
			}
			if res.Text != "" {
				final = res.Text
				if !gotFirstPartial {
					gotFirstPartial = true
					addEventMs(span, "first_partial",
						time.Since(startedAt).Milliseconds(),
						attribute.Int("text_len", len(res.Text)))
					recordStage(p.Metrics, "asr.first_partial", p.Scenario,
						time.Since(startedAt).Seconds())
				}
			}
			if res.IsFinal {
				gotFinal = true
				break readLoop
			}
		}
	}

	// drainLoop only runs if we DIDN'T already get IsFinal in readLoop —
	// e.g., we exited because src EOF'd or VAD/timeout fired and we
	// CloseSend'd, in which case the server still owes us a final
	// transcript. Once IsFinal arrives in readLoop the server has
	// nothing more to say, so spinning here only adds latency
	// (previously cost up to 2 s per turn).
	if !gotFinal {
		drainCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
	drainLoop:
		for {
			select {
			case res, ok := <-stream.Recv():
				if !ok {
					break drainLoop
				}
				if res.Text != "" {
					final = res.Text
				}
				if res.IsFinal {
					break drainLoop
				}
			case <-drainCtx.Done():
				break drainLoop
			}
		}
	}

	final = strings.TrimSpace(final)
	totalMs := time.Since(startedAt)
	span.SetAttributes(
		attribute.Int("transcript.len", len(final)),
		attribute.Bool("src.eof", srcDrained),
	)
	addEventMs(span, "final_transcript", totalMs.Milliseconds(),
		attribute.Int("text_len", len(final)))
	recordStage(p.Metrics, "asr.final", p.Scenario, totalMs.Seconds())
	slog.Info("pipeline.listen done",
		"call_uuid", p.UUID,
		"transcript", final,
		"asr_ms", totalMs.Milliseconds(),
		"src_eof", srcDrained,
	)
	return final, nil
}

// Speak runs one bot turn: open bot stream, push every flushed sentence to
// TTS, drain TTS audio frames into sink. Returns the bot-reported action.
//
// message="" triggers the bot's greeting path (the bot decides based on
// conversation history, not on us).
//
// When src is non-nil and p.BargeIn is true, a barge-in monitor reads audio
// during bot speaking; on detected user speech, the bot stream + TTS are
// canceled, FS playback is broken, and Speak returns ActionChat early.
// The captured transcript is stashed in p.pendingTranscript for RunTurn
// to feed into the next Speak directly (no follow-up Listen needed).
func (p *Pipeline) Speak(ctx context.Context, src AudioSource, sinkArg AudioSink, message string) (bot.Action, error) {
	startedAt := time.Now()

	// Resolve the per-utterance sink. Live FS path: open via PlaybackOpen
	// (mkfifo + uuid_broadcast + O_WRONLY). Offline/tests: use SinkOverride
	// or the explicit sinkArg.
	sink, sinkCloser, err := p.resolveSink(ctx, sinkArg)
	if err != nil {
		return "", fmt.Errorf("playback open: %w", err)
	}
	if sinkCloser != nil {
		defer sinkCloser.Close()
	}

	// bot.turn covers the LLM call (request → action). Lifetime extends past
	// the sentence pump because Action() blocks until the HTTP stream closes.
	botCtx, botSpan := tracer.Start(ctx, "bot.turn", trace.WithAttributes(
		callAttrs(p.UUID)...,
	))
	// botCtx must be cancellable on barge-in to abort the HTTP stream.
	botCtx, cancelBot := context.WithCancel(botCtx)
	defer cancelBot()
	botSpan.SetAttributes(attribute.Int("message.len", len(message)))
	defer botSpan.End()

	// Spawn the barge-in monitor BEFORE bot/tts setup so the ASR session
	// is ingesting frames the moment the caller can hear the bot. Monitor
	// lifetime is bound to Speak's defer chain.
	//
	// When barge-in is off but src is wired, we still drain frames into
	// /dev/null — leaving them in the FIFO would pile up TTS-echo audio
	// and corrupt the next Listen with stale data ("bot repeats the same
	// line" failure mode).
	var monitor *bargeInMonitor
	bargeEnabled := p.BargeIn && src != nil && p.ASR != nil
	if bargeEnabled {
		m, err := newBargeInMonitor(ctx, p.UUID, p.ASR, src, p.BargeMinWords, p.Cfg)
		if err != nil {
			slog.Warn("bargein monitor init failed; falling back to drain-only",
				"call_uuid", p.UUID, "err", err)
		} else {
			monitor = m
			defer monitor.stop()
		}
	}
	if monitor == nil && src != nil {
		drainStop := make(chan struct{})
		drainDone := make(chan struct{})
		go func() {
			defer close(drainDone)
			for {
				select {
				case <-drainStop:
					return
				case _, ok := <-src.Frames():
					if !ok {
						return
					}
				}
			}
		}()
		defer func() {
			close(drainStop)
			<-drainDone
		}()
	}

	turnStream, err := p.Bot.Stream(botCtx, p.UUID, message)
	if err != nil {
		botSpan.SetStatus(codes.Error, err.Error())
		incBotErr(p.Metrics, "stream_open")
		return "", fmt.Errorf("bot stream: %w", err)
	}
	defer turnStream.Close()

	// tts.turn covers the WS lifecycle (auth → final audio frame).
	ttsCtx, ttsSpan := tracer.Start(ctx, "tts.turn", trace.WithAttributes(
		callAttrs(p.UUID)...,
	))
	defer ttsSpan.End()

	ttsStream, err := p.TTS.StartStream(ttsCtx, tts.StreamOpts{
		ConversationID: p.UUID,
		VoiceID:        p.Cfg.VoiceID,
		ResampleRate:   p.Cfg.ResampleRate,
		Tempo:          p.Cfg.Tempo,
	})
	if err != nil {
		ttsSpan.SetStatus(codes.Error, err.Error())
		incTTSErr(p.Metrics, "stream_open")
		return "", fmt.Errorf("tts start: %w", err)
	}
	defer ttsStream.Close()

	// Drain TTS audio in parallel with sentence push so we don't block
	// upstream sentence flushing on slow sink writes.
	audioErr := make(chan error, 1)
	var firstAudioAt time.Duration
	var bytesOut int
	go func() {
		defer close(audioErr)
		for frame := range ttsStream.AudioChan() {
			if firstAudioAt == 0 {
				firstAudioAt = time.Since(startedAt)
				addEventMs(ttsSpan, "first_audio", firstAudioAt.Milliseconds())
				recordStage(p.Metrics, "tts.first_audio", p.Scenario, firstAudioAt.Seconds())
			}
			if _, err := sink.Write(frame); err != nil {
				audioErr <- fmt.Errorf("sink write: %w", err)
				return
			}
			bytesOut += len(frame)
		}
	}()

	// Pump sentences from bot → TTS in a goroutine so the main flow can
	// race the sentence stream against barge-in detection. Without this,
	// barge-in would only be observable after the bot finished streaming.
	var (
		firstSentAt   time.Duration
		sentenceCount int
		sentencesDone = make(chan struct{})
		botTextBuf    strings.Builder // accumulates sentences for call history
		botTextMu     sync.Mutex
	)
	go func() {
		defer close(sentencesDone)
		var prev string
		var havePrev bool
		for s := range turnStream.Sentences() {
			botTextMu.Lock()
			if botTextBuf.Len() > 0 {
				botTextBuf.WriteByte(' ')
			}
			botTextBuf.WriteString(s)
			botTextMu.Unlock()
			if firstSentAt == 0 {
				firstSentAt = time.Since(startedAt)
				addEventMs(botSpan, "first_sentence", firstSentAt.Milliseconds(),
					attribute.Int("text_len", len(s)))
				recordStage(p.Metrics, "bot.first_sentence", p.Scenario, firstSentAt.Seconds())
			}
			if havePrev {
				if err := ttsStream.SendText(prev, false); err != nil {
					slog.Warn("tts send failed", "call_uuid", p.UUID, "err", err)
					return
				}
			}
			prev = s
			havePrev = true
			sentenceCount++
		}
		if havePrev {
			if err := ttsStream.SendText(prev, true); err != nil {
				slog.Warn("tts send eos failed", "call_uuid", p.UUID, "err", err)
			}
		} else {
			if err := ttsStream.SendText("", true); err != nil {
				slog.Warn("tts empty send eos failed", "call_uuid", p.UUID, "err", err)
			}
		}
	}()

	// Race: bot.Action() blocks until HTTP stream closes; barge-in trigger
	// must short-circuit. Sentence pump signals when it's done.
	type actionResult struct {
		action bot.Action
		err    error
	}
	actionCh := make(chan actionResult, 1)
	go func() {
		a, e := turnStream.Action()
		actionCh <- actionResult{action: a, err: e}
	}()

	var action bot.Action
	var botErr error
	bargeFired := false

	// triggerBargeIn collapses the barge-in chain (close TTS, uuid_break,
	// cancel bot) and metric increment into one place. Returns true if
	// this call actually fired (CompareAndSwap on bargeFired).
	triggerBargeIn := func() {
		if bargeFired {
			return
		}
		bargeFired = true
		botSpan.AddEvent("barge_in")
		_ = ttsStream.Close()
		if p.ESL != nil {
			if err := p.ESL.StopPlayback(p.UUID); err != nil {
				slog.Warn("uuid_break failed", "call_uuid", p.UUID, "err", err)
			}
		}
		cancelBot()
		if p.Metrics != nil {
			p.Metrics.BargeInTotal.WithLabelValues(p.Scenario).Inc()
		}
	}

	select {
	case ar := <-actionCh:
		action, botErr = ar.action, ar.err
		// Bot HTTP stream may finish much earlier than FS audio playback
		// (text streams in <1 s, FS plays the resulting 14 s of audio at
		// realtime). The barge-in monitor could fire DURING the playback
		// drain even after actionCh has fired. Check non-blocking; if
		// the trigger already happened, run the chain now.
		select {
		case <-bargeTriggerChan(monitor):
			triggerBargeIn()
		default:
		}
	case <-bargeTriggerChan(monitor):
		triggerBargeIn()
		ar := <-actionCh
		action, botErr = ar.action, ar.err
		if errors.Is(botErr, context.Canceled) {
			botErr = nil
		}
	}
	// Drain the sentence pump goroutine so we don't leak it.
	<-sentencesDone

	if botErr != nil {
		botSpan.SetStatus(codes.Error, botErr.Error())
		incBotErr(p.Metrics, "stream_read")
		return "", fmt.Errorf("bot action: %w", botErr)
	}
	botTotal := time.Since(startedAt)
	botSpan.SetAttributes(
		attribute.String(attrAction, string(action)),
		attribute.Int("bot.sentences", sentenceCount),
		attribute.Bool("bot.barged_in", bargeFired),
	)
	recordStage(p.Metrics, "bot.total", p.Scenario, botTotal.Seconds())

	// On barge-in, force action to CHAT (caller wants to talk) and stash
	// the captured transcript for RunTurn to feed into the next Speak call
	// — no follow-up Listen is needed. ENDCALL is preserved if somehow
	// the bot finished and emitted ENDCALL before we triggered.
	if bargeFired && monitor != nil {
		monitor.stop()
		p.pendingTranscript = monitor.capturedTranscript()
		botSpan.SetAttributes(
			attribute.Int("barge_in.transcript_words", countWords(p.pendingTranscript)),
		)
		if action != bot.ActionEndCall {
			action = bot.ActionChat
		}
	}

	// Wait for TTS to drain. We don't add a hard deadline beyond ctx because
	// callers usually wrap with a turn-level timeout.
	if err := <-audioErr; err != nil {
		slog.Warn("pipeline.speak audio drain", "call_uuid", p.UUID, "err", err)
		ttsSpan.RecordError(err)
		incTTSErr(p.Metrics, "audio_drain")
	}
	ttsSpan.SetAttributes(attribute.Int("audio.bytes", bytesOut))
	recordStage(p.Metrics, "tts.total", p.Scenario, time.Since(startedAt).Seconds())

	// Wait for FS to actually finish playing the audio before returning.
	// FreeSWITCH only emits PLAYBACK_STOP once the kernel pipe buffer +
	// its own jitter buffer have fully drained — this is a more accurate
	// signal than computing audio_bytes/16 ms (which overshoots by ~1 s
	// because of those buffers).
	//
	// We close the sink first so FS reads EOF and finishes promptly.
	// Skipped on barge-in (uuid_break already short-circuited playback)
	// and when no sinkCloser was created (offline / explicit-sink path).
	//
	// Important: barge-in can ALSO fire DURING this wait — bot text is
	// done streaming but FS still has 10+ s of audio queued, and the
	// caller might decide to interrupt mid-playback. Race the barge-in
	// trigger against the PLAYBACK_STOP signal.
	if !bargeFired && sinkCloser != nil {
		_ = sinkCloser.Close()
		sinkCloser = nil // already closed; defer below is a no-op
		if h, ok := sink.(interface{ Done() <-chan struct{} }); ok {
			waitStart := time.Now()
			audioDur := time.Duration(bytesOut/16) * time.Millisecond
			// Bound the wait: PLAYBACK_STOP should fire well within
			// audioDur + 5s. Beyond that we assume the event was lost
			// and proceed rather than block the call forever.
			deadline := audioDur + 5*time.Second
			select {
			case <-h.Done():
				ttsSpan.AddEvent("playback_stop_received",
					trace.WithAttributes(
						attribute.Int64("wait_ms", time.Since(waitStart).Milliseconds())))
			case <-bargeTriggerChan(monitor):
				triggerBargeIn()
				ttsSpan.AddEvent("barge_in_during_drain",
					trace.WithAttributes(
						attribute.Int64("wait_ms", time.Since(waitStart).Milliseconds())))
			case <-time.After(deadline):
				slog.Warn("playback_stop wait timed out",
					"call_uuid", p.UUID,
					"deadline_ms", deadline.Milliseconds())
				ttsSpan.AddEvent("playback_stop_timeout")
			case <-ctx.Done():
				return action, ctx.Err()
			}
		}
	}

	// Stash for RunTurn → call history. botTextMu held to avoid racing
	// with the sentence-pump goroutine (which exited at sentencesDone but
	// the buffer is still touched by the closure scope).
	botTextMu.Lock()
	p.lastBotText = botTextBuf.String()
	botTextMu.Unlock()
	p.lastBargedIn = bargeFired

	slog.Info("pipeline.speak done",
		"call_uuid", p.UUID,
		"action", string(action),
		"first_sentence_ms", firstSentAt.Milliseconds(),
		"first_audio_ms", firstAudioAt.Milliseconds(),
		"audio_bytes", bytesOut,
		"total_ms", time.Since(startedAt).Milliseconds(),
	)
	return action, nil
}

// RunTurn = Listen → Speak, returning continueCall=false on ENDCALL.
// For the very first (greeting) turn, pass nil src and "" message.
//
// Each invocation appends one TurnRecord to p.history with the transcript,
// concatenated bot text, action, and barge-in flag — used by SessionRunner
// to persist call_history at end of call.
//
// Empty-transcript handling: a non-greeting turn that produces no
// transcript (caller silent or only echoed audio) does NOT call the bot.
// Calling the bot with "" makes it emit the greeting fallback every turn,
// which manifests as "the bot keeps repeating the same line." We loop
// back to Listen so the next caller utterance gets a real reply.
//
// If a barge-in fired in the previous Speak, the captured transcript is
// already in p.pendingTranscript — skip Listen and feed it straight to
// Speak.
func (p *Pipeline) RunTurn(ctx context.Context, src AudioSource, sink AudioSink) (bool, error) {
	turnStart := time.Now()
	var transcript string
	switch {
	case p.pendingTranscript != "":
		transcript = p.pendingTranscript
		p.pendingTranscript = ""
		slog.Info("pipeline.turn using barge-in transcript",
			"call_uuid", p.UUID, "transcript", transcript)
	case src != nil:
		t, err := p.Listen(ctx, src)
		if err != nil {
			return false, err
		}
		transcript = t
		if transcript == "" {
			slog.Info("pipeline.turn skipping bot (empty transcript)",
				"call_uuid", p.UUID)
			return true, nil
		}
	}

	return p.speakAndRecord(ctx, src, sink, transcript, turnStart)
}

// RunGreeting plays the bot's first message (no caller input) and records
// the turn into history. The src is forwarded to Speak so the recording
// FIFO is drained during playback — without this, Speak finishes (5+ s of
// caller-side silence + echo accumulate in the FIFO), and the next Listen
// reads that backlog as a burst that ASR finalizes as garbage in <3 s.
func (p *Pipeline) RunGreeting(ctx context.Context, src AudioSource, sink AudioSink) (bool, error) {
	turnStart := time.Now()
	return p.speakAndRecord(ctx, src, sink, "", turnStart)
}

// speakAndRecord is the shared tail of RunTurn / RunGreeting: invoke
// Speak, append the resulting TurnRecord, translate ENDCALL into
// continueCall=false. Centralizing it keeps the history bookkeeping in
// one place.
func (p *Pipeline) speakAndRecord(ctx context.Context, src AudioSource, sink AudioSink, transcript string, turnStart time.Time) (bool, error) {
	action, err := p.Speak(ctx, src, sink, transcript)
	if err != nil {
		return false, err
	}
	p.history = append(p.history, TurnRecord{
		Index:     len(p.history),
		UserText:  transcript,
		BotText:   p.lastBotText,
		Action:    action,
		StartedAt: turnStart,
		EndedAt:   time.Now(),
		BargedIn:  p.lastBargedIn,
	})
	if action == bot.ActionEndCall {
		return false, nil
	}
	return true, nil
}

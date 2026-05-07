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

// Config carries per-turn knobs that aren't already on the provider opts.
type Config struct {
	SampleRate     int
	VoiceID        string
	Tempo          float64
	ResampleRate   int
	SilentTimeout  time.Duration // safety cap when src never EOFs and VAD never fires
	FirstByteTotal time.Duration // soft alert threshold (logged, doesn't error)
}

// Defaults returns a config suitable for the HCC scenario at 8kHz.
func Defaults() Config {
	return Config{
		SampleRate:     8000,
		ResampleRate:   8000,
		Tempo:          1.0,
		SilentTimeout:  30 * time.Second,
		FirstByteTotal: 5 * time.Second,
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

	// BargeIn enables the in-Speak monitor. When false, Speak doesn't read
	// from src at all — caller's audio is buffered in the FIFO until Listen
	// resumes.
	BargeIn bool

	Metrics *metrics.Collectors // nil-safe

	// pendingReplay holds caller audio captured during a barge-in to be
	// replayed into the next Listen call. Cleared by Listen.
	pendingReplay []byte

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

// frameBytes returns the per-frame byte size implied by Config (S16LE).
// 320 bytes default = 20 ms @ 8 kHz.
func (p *Pipeline) frameBytes() int {
	if p.Cfg.SampleRate <= 0 {
		return 320
	}
	// 20 ms frame default.
	return p.Cfg.SampleRate * 20 / 1000 * 2
}

// Listen reads audio frames from src, pushes them to ASR (+VAD), and
// returns the final transcript. Stops when:
//
//   - VAD signals SpeechEnd (live mode), OR
//   - src closes its channel (offline / FIFO peer disconnect), OR
//   - ASR emits a Result with IsFinal=true, OR
//   - ctx cancels, OR
//   - cfg.SilentTimeout elapses with no audio.
//
// Returns ("", nil) if no transcript was produced — caller decides whether
// to retry or end the call.
func (p *Pipeline) Listen(ctx context.Context, src AudioSource) (string, error) {
	ctx, span := tracer.Start(ctx, "asr.listen", trace.WithAttributes(callAttrs(p.UUID)...))
	defer span.End()

	// If a barge-in left audio behind, prepend it before reading more from src.
	if len(p.pendingReplay) > 0 {
		replay := p.pendingReplay
		p.pendingReplay = nil
		span.SetAttributes(attribute.Int("replay.bytes", len(replay)))
		src = newReplaySource(replay, src, p.frameBytes())
	}

	if p.VAD != nil {
		p.VAD.Reset()
		p.VAD.SetBotSpeaking(false)
	}

	stream, err := p.ASR.StartStream(ctx, asr.StreamOpts{
		ConversationID:   p.UUID,
		SampleRate:       p.Cfg.SampleRate,
		Channels:         1,
		SingleSentence:   true,
		SilenceTimeoutMs: 800,
		SpeechMaxMs:      30000,
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
	speechEnded := false

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
			if p.VAD != nil {
				if e := p.VAD.Push(frame); e == vad.EventSpeechEnd {
					speechEnded = true
				}
			}
			if err := stream.SendAudio(frame); err != nil {
				if errors.Is(err, io.EOF) {
					break readLoop
				}
				span.SetStatus(codes.Error, err.Error())
				return "", fmt.Errorf("asr send: %w", err)
			}
			if speechEnded {
				addEventMs(span, "vad_speech_end", time.Since(startedAt).Milliseconds())
				closeSend()
				break readLoop
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
				break readLoop
			}
		}
	}

	// Drain any remaining results post-CloseSend so we capture the final
	// transcript the server flushes.
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

	final = strings.TrimSpace(final)
	totalMs := time.Since(startedAt)
	span.SetAttributes(
		attribute.Int("transcript.len", len(final)),
		attribute.Bool("src.eof", srcDrained),
		attribute.Bool("vad.speech_end", speechEnded),
	)
	addEventMs(span, "final_transcript", totalMs.Milliseconds(),
		attribute.Int("text_len", len(final)))
	recordStage(p.Metrics, "asr.final", p.Scenario, totalMs.Seconds())
	slog.Info("pipeline.listen done",
		"call_uuid", p.UUID,
		"transcript", final,
		"asr_ms", time.Since(startedAt).Milliseconds(),
		"src_eof", srcDrained,
		"vad_end", speechEnded,
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
// The captured audio is stashed in p.pendingReplay for the next Listen.
func (p *Pipeline) Speak(ctx context.Context, src AudioSource, sink AudioSink, message string) (bot.Action, error) {
	startedAt := time.Now()

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

	// Spawn the barge-in monitor BEFORE bot/tts setup so VAD's BotSpeaking
	// flag is set while audio is still being TTS'd. Monitor lifecycle is
	// bound to Speak's defer chain.
	var monitor *bargeInMonitor
	bargeEnabled := p.BargeIn && src != nil && p.VAD != nil
	if bargeEnabled {
		monitor = newBargeInMonitor(p.UUID, p.VAD, src, 32000) // ~2s @ 8k S16LE
		monitor.start(ctx)
		defer monitor.stop()
	} else {
		if p.VAD != nil {
			// Mark bot-speaking so the next Listen.Reset is a clean slate.
			p.VAD.SetBotSpeaking(true)
			defer p.VAD.SetBotSpeaking(false)
		}
		// Drain caller audio while we're speaking so it doesn't pile up in
		// the FIFO. Otherwise the next Listen reads stale frames (bot's
		// own TTS echoed back through the handset) — ASR finalizes them
		// as empty transcripts and the bot keeps replying to silence,
		// creating the "bot repeats the same line" loop. We discard here
		// because barge-in is off; live VAD-driven interrupt is the
		// barge-in path above.
		if src != nil {
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
	select {
	case ar := <-actionCh:
		action, botErr = ar.action, ar.err
	case <-bargeTriggerChan(monitor):
		bargeFired = true
		botSpan.AddEvent("barge_in")
		_ = ttsStream.Close() // stop audio synthesis
		if p.ESL != nil {
			if err := p.ESL.StopPlayback(p.UUID); err != nil {
				slog.Warn("uuid_break failed", "call_uuid", p.UUID, "err", err)
			}
		}
		cancelBot() // abort HTTP request → Action() + sentence pump return
		ar := <-actionCh
		action, botErr = ar.action, ar.err
		// On barge-in, the bot ctx was canceled by us — this isn't a real error.
		if errors.Is(botErr, context.Canceled) {
			botErr = nil
		}
		if p.Metrics != nil {
			p.Metrics.BargeInTotal.WithLabelValues(p.Scenario).Inc()
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
	// the captured audio for the next Listen call. ENDCALL is preserved if
	// somehow the bot finished and emitted ENDCALL before we triggered.
	if bargeFired {
		monitor.stop() // ensure ring is stable
		p.pendingReplay = monitor.snapshot()
		botSpan.SetAttributes(attribute.Int("barge_in.replay_bytes", len(p.pendingReplay)))
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
func (p *Pipeline) RunTurn(ctx context.Context, src AudioSource, sink AudioSink) (bool, error) {
	turnStart := time.Now()
	var transcript string
	if src != nil {
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

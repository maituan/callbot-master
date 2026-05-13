package api

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"callbot-master/internal/asr"
	"callbot-master/internal/auth"
	"callbot-master/internal/bot"
	"callbot-master/internal/intent"
	"callbot-master/internal/metrics"
	"callbot-master/internal/store"
	"callbot-master/internal/tts"
)

// voice handles GET /api/v1/web/voice/{token} — upgrades to a WebSocket
// and runs a continuous-listen voice loop with barge-in. Wire format:
//
//	C → S:
//	  {type:"hello"}                                  (first frame)
//	  {type:"audio", pcm:"<b64 PCM S16LE 16kHz>"}     (20ms frames)
//	  {type:"end"}                                    (visitor closed)
//
//	S → C:
//	  {type:"session",  id, voice_id}
//	  {type:"state",    state:"listening"|"thinking"|"speaking"|"ended"}
//	  {type:"asr_partial", text}
//	  {type:"asr_final",   text}
//	  {type:"bot_text",    text}                       (one sentence)
//	  {type:"bot_done",    action}
//	  {type:"tts_audio",   pcm:"<b64 PCM S16LE 16kHz>"}
//	  {type:"interrupt"}                                (barge-in fired)
//	  {type:"error",       code, message}
func (h *webHandler) voice(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tok := strings.TrimPrefix(r.URL.Path, "/api/v1/web/voice/")
	b, _, iat, err := h.resolveToken(r.Context(), tok, auth.BotShareChannelVoice)
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, err.Error())
		return
	}

	upgrader := websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin:     func(r *http.Request) bool { return true }, // share-token IS the auth
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrader already wrote a response.
		return
	}

	// Create web_session row.
	sess := &store.WebSession{
		BotID:     b.ID,
		TenantID:  b.TenantID,
		Channel:   "voice",
		IP:        r.RemoteAddr,
		UserAgent: r.UserAgent(),
	}
	if !iat.IsZero() {
		sess.TokenIAT = &iat
	}
	if h.d.VoiceRecordingDir != "" {
		sess.RecordingDir = filepath.Join(h.d.VoiceRecordingDir, sess.BotID.String())
	}
	if err := h.d.Store.CreateWebSession(r.Context(), sess); err != nil {
		_ = sendWS(conn, wsFrame{Type: "error", Code: "session_create", Message: err.Error()})
		_ = conn.Close()
		return
	}

	// Per-session recording dir, materialised after we have an id.
	if sess.RecordingDir != "" {
		sess.RecordingDir = filepath.Join(sess.RecordingDir, sess.ID.String())
		_ = os.MkdirAll(sess.RecordingDir, 0o755)
	}

	logger := slog.With("web_session", sess.ID.String(), "bot", b.Slug, "channel", "voice")
	logger.Info("voice session started")

	endStatus := "ended"
	endErr := ""
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.d.Store.EndWebSession(ctx, sess.ID, endStatus, endErr); err != nil {
			logger.Warn("end session", "err", err)
		}
		_ = conn.Close()
		logger.Info("voice session ended", "status", endStatus)
	}()

	// Build providers — ASR endpoint overridden for 16k web voice;
	// TTS uses bot's endpoint but with web resample rate.
	asrEndpoint := h.d.VoiceASREndpoint
	asrRate := h.d.VoiceASRSampleRate
	if asrEndpoint == "" {
		asrEndpoint = b.ASREndpoint
	}
	if asrRate == 0 {
		asrRate = 16000
	}
	ttsResample := h.d.VoiceTTSResampleRate
	if ttsResample == 0 {
		ttsResample = 16000
	}

	asrCli, err := asr.NewViettelClient(r.Context(), asrEndpoint, b.ASRToken)
	if err != nil {
		_ = sendWS(conn, wsFrame{Type: "error", Code: "asr_dial", Message: err.Error()})
		endStatus, endErr = "error", "asr dial: "+err.Error()
		return
	}
	defer asrCli.Close()

	ttsCli := tts.NewViettelClient(b.TTSEndpoint, b.TTSToken, b.TTSVoiceID, ttsResample, b.TTSTempo)
	if h.d.BotFactory == nil {
		_ = sendWS(conn, wsFrame{Type: "error", Code: "config", Message: "bot factory not configured"})
		endStatus, endErr = "error", "no bot factory"
		return
	}
	botCli, err := h.d.BotFactory(b)
	if err != nil {
		_ = sendWS(conn, wsFrame{Type: "error", Code: "bot_init", Message: err.Error()})
		endStatus, endErr = "error", err.Error()
		return
	}

	// Hello + session.
	if err := sendWS(conn, wsFrame{Type: "session", SessionID: sess.ID.String(), VoiceID: b.TTSVoiceID}); err != nil {
		return
	}

	state := &voiceState{
		sess:        sess,
		bot:         b,
		conn:        conn,
		asrCli:      asrCli,
		ttsCli:      ttsCli,
		botCli:      botCli,
		store:       h.d.Store,
		logger:      logger,
		asrRate:     asrRate,
		ttsRate:     ttsResample,
		bargeMin:    b.BargeInMinWords,
		bargeOn:     b.BargeInEnabled,
		recordDir:   sess.RecordingDir,
		fillerDir:   h.d.VoiceFillerDir,
		metrics:     h.d.Metrics,
	}

	if err := state.run(r.Context()); err != nil {
		logger.Warn("voice loop", "err", err)
		_ = sendWS(conn, wsFrame{Type: "error", Code: "loop", Message: err.Error()})
		endStatus, endErr = "error", err.Error()
		return
	}
}

// voiceState drives the per-session loop. One state instance lives for
// one upgraded connection.
type voiceState struct {
	sess   *store.WebSession
	bot    *store.BotConfig
	conn   *websocket.Conn
	store  WebStore
	logger *slog.Logger

	asrCli  *asr.ViettelClient
	ttsCli  *tts.ViettelClient
	botCli  bot.Client
	asrRate int
	ttsRate int

	bargeOn  bool
	bargeMin int

	recordDir string
	fillerDir string

	metrics *metrics.Collectors

	writeMu sync.Mutex // serialises sendWS — gorilla disallows concurrent writes
}

// run executes greeting → loop until visitor closes or bot says ENDCALL.
func (s *voiceState) run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	// Audio in/out plumbing.
	audioCh := make(chan []byte, 64)        // browser → ASR
	stopRead := make(chan struct{})
	go s.readLoop(ctx, audioCh, stopRead)
	defer func() { <-stopRead }()

	turnIdx := 0

	// Greeting first — empty user message triggers bot's hardcoded greeting.
	if err := s.sendState("thinking"); err != nil {
		return err
	}
	turnIdx++
	if err := s.runBotTurn(ctx, audioCh, turnIdx, "" /* no user message */, false); err != nil {
		return err
	}

	for {
		// LISTENING — start ASR stream, drain audio frames, watch for final.
		if err := s.sendState("listening"); err != nil {
			return err
		}
		userText, err := s.listen(ctx, audioCh)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, errClientClosed) {
				return nil
			}
			return err
		}
		if userText == "" {
			// Silence timeout — keep listening.
			continue
		}

		// Persist user turn.
		now := time.Now()
		_ = s.store.AppendWebTurn(ctx, &store.WebTurn{
			SessionID:  s.sess.ID,
			Role:       "user",
			Text:       userText,
			ASRFinalAt: &now,
		})

		// THINKING + bot turn.
		if err := s.sendState("thinking"); err != nil {
			return err
		}
		turnIdx++
		if err := s.runBotTurn(ctx, audioCh, turnIdx, userText, false); err != nil {
			return err
		}
		// runBotTurn sets state to "speaking" internally and emits
		// bot_done with action. Loop continues to LISTENING — unless
		// runBotTurn returned errEndCall.
	}
}

var errEndCall = errors.New("bot signalled ENDCALL")
var errClientClosed = errors.New("client closed")

// listen runs one ASR stream until a non-empty final transcript arrives
// or the context is cancelled. Ignores partials at the WS level (emits
// asr_partial frames for UI animation).
func (s *voiceState) listen(ctx context.Context, audioCh <-chan []byte) (string, error) {
	stream, err := s.asrCli.StartStream(ctx, asr.StreamOpts{
		ConversationID:   "web-" + s.sess.ID.String(),
		SampleRate:       s.asrRate,
		Channels:         1,
		SingleSentence:   s.bot.ASRSingleSentence,
		SilenceTimeoutMs: int(s.bot.ASRSilenceTimeoutSec * 1000),
		SpeechTimeoutMs:  int(s.bot.ASRSpeechTimeoutSec * 1000),
		SpeechMaxMs:      int(s.bot.ASRSpeechMaxSec * 1000),
	})
	if err != nil {
		return "", fmt.Errorf("asr start: %w", err)
	}
	defer stream.Close()

	pumpDone := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				pumpDone <- ctx.Err()
				return
			case pcm, ok := <-audioCh:
				if !ok {
					pumpDone <- errClientClosed
					return
				}
				if err := stream.SendAudio(pcm); err != nil {
					pumpDone <- err
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case err := <-pumpDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				return "", err
			}
		case res, ok := <-stream.Recv():
			if !ok {
				return "", nil
			}
			text := strings.TrimSpace(res.Text)
			if !res.IsFinal {
				if text != "" {
					_ = s.sendWS(wsFrame{Type: "asr_partial", Text: text})
				}
				continue
			}
			if text != "" {
				_ = s.sendWS(wsFrame{Type: "asr_final", Text: text})
			}
			return text, nil
		}
	}
}

// runBotTurn streams the bot reply, pipes sentences into TTS, forwards
// audio to the browser. Concurrent ASR monitor for barge-in: while
// SPEAKING we keep an ASR stream open; if it returns a final with
// >= BargeMinWords, we cancel TTS, send "interrupt", and treat the
// transcript as the next user turn.
//
// forceShortFiller bypasses the intent classify for this turn and
// forces a short filler — used when the previous turn was barged
// before any TTS audio reached the browser, so the user is in the
// middle of speaking and an intent roundtrip would just add latency.
//
// Returns errEndCall if action was ENDCALL.
func (s *voiceState) runBotTurn(ctx context.Context, audioCh <-chan []byte, turnIdx int, userText string, forceShortFiller bool) error {
	turnCtx, cancelTurn := context.WithCancel(ctx)
	defer cancelTurn()

	// Filler: while waiting for the bot's first sentence, blast a
	// random pre-recorded "uhm" / "dạ vâng" so the gap doesn't feel
	// dead. Skipped on greeting turns since user hasn't spoken yet.
	// In hybrid mode we classify the transcript first; BUSINESS picks
	// from <voice>/long/, CHITCHAT or any failure falls back to flat
	// short pool. forceShortFiller skips the classify entirely.
	if userText != "" && s.fillerDir != "" && s.bot.TTSVoiceID != "" {
		go s.playFiller(turnCtx, userText, forceShortFiller)
	}

	// Bot stream.
	conversationID := "web-" + s.sess.ID.String()
	botStream, err := s.botCli.Stream(turnCtx, conversationID, userText)
	if err != nil {
		return fmt.Errorf("bot stream: %w", err)
	}
	defer botStream.Close()

	// TTS stream.
	ttsStream, err := s.ttsCli.StartStream(turnCtx, tts.StreamOpts{
		ConversationID: conversationID,
		VoiceID:        s.bot.TTSVoiceID,
		ResampleRate:   s.ttsRate,
		Tempo:          s.bot.TTSTempo,
	})
	if err != nil {
		return fmt.Errorf("tts stream: %w", err)
	}
	defer ttsStream.Close()

	// WAV recorder for QC.
	var wavPath string
	var wavWriter *wavRecorder
	if s.recordDir != "" {
		wavPath = fmt.Sprintf("%d.wav", turnIdx)
		w, werr := newWavRecorder(filepath.Join(s.recordDir, wavPath), s.ttsRate)
		if werr != nil {
			s.logger.Warn("open wav", "err", werr)
		} else {
			wavWriter = w
			defer wavWriter.Close()
		}
	}

	// Goroutine: forward TTS PCM frames out as WS messages + WAV.
	ttsDone := make(chan struct{})
	var firstAudioAt atomic.Pointer[time.Time]
	stateSpeaking := atomic.Bool{}
	go func() {
		defer close(ttsDone)
		for frame := range ttsStream.AudioChan() {
			if firstAudioAt.Load() == nil {
				now := time.Now()
				firstAudioAt.Store(&now)
				if !stateSpeaking.Swap(true) {
					_ = s.sendState("speaking")
				}
			}
			_ = s.sendWS(wsFrame{Type: "tts_audio", PCM: base64.StdEncoding.EncodeToString(frame), SampleRate: s.ttsRate})
			if wavWriter != nil {
				_, _ = wavWriter.Write(frame)
			}
		}
	}()

	// Goroutine: barge-in monitor. Continuously reads visitor audio
	// during SPEAKING, runs against a fresh ASR stream. On a final
	// transcript with >= bargeMin words, cancels turnCtx so TTS + bot
	// stream collapse, then re-uses transcript as next user turn.
	var bargedTranscript atomic.Value
	bargeStop := make(chan struct{})
	if s.bargeOn {
		// bargeMonitor cancels turnCtx itself when it captures a
		// long-enough utterance, collapsing TTS + bot streams so the
		// user doesn't have to wait for the bot to finish.
		go s.bargeMonitor(turnCtx, cancelTurn, audioCh, &bargedTranscript, bargeStop)
	} else {
		close(bargeStop)
	}

	// Pipe bot sentences into TTS.
	var firstByteAt *time.Time
	var acc strings.Builder
	for sentence := range botStream.Sentences() {
		if firstByteAt == nil {
			t := time.Now()
			firstByteAt = &t
		}
		clean := strings.TrimSpace(sentence)
		if clean == "" {
			continue
		}
		acc.WriteString(clean)
		acc.WriteString(" ")
		_ = s.sendWS(wsFrame{Type: "bot_text", Text: clean})
		if err := ttsStream.SendText(clean, false); err != nil {
			s.logger.Warn("tts send", "err", err)
			break
		}
	}
	_ = ttsStream.SendText("", true) // EOS

	action, _ := botStream.Action()

	// Wait for TTS to drain (or barge-in to cancel us).
	select {
	case <-ttsDone:
	case <-turnCtx.Done():
	}

	if s.bargeOn {
		<-bargeStop
	}

	doneAt := time.Now()
	_ = s.sendWS(wsFrame{Type: "bot_done", Action: string(action)})

	// Persist bot turn.
	persistCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bt := &store.WebTurn{
		SessionID:      s.sess.ID,
		Role:           "bot",
		Text:           strings.TrimSpace(acc.String()),
		AudioPath:      wavPath,
		BotFirstByteAt: firstByteAt,
		BotDoneAt:      &doneAt,
		Action:         string(action),
	}
	if p := firstAudioAt.Load(); p != nil {
		bt.TTSFirstAudioAt = p
	}
	bt.TTSDoneAt = &doneAt
	_ = s.store.AppendWebTurn(persistCtx, bt)

	// Barge-in transcript? Persist as the next user turn and short-circuit
	// — caller's loop will see it via state.pending.
	if v := bargedTranscript.Load(); v != nil {
		txt, _ := v.(string)
		if txt != "" {
			_ = s.sendWS(wsFrame{Type: "interrupt"})
			now := time.Now()
			_ = s.store.AppendWebTurn(persistCtx, &store.WebTurn{
				SessionID:  s.sess.ID,
				Role:       "user",
				Text:       txt,
				ASRFinalAt: &now,
			})
			// Treat barge-in as the start of the next user turn — emit
			// THINKING and run bot reply directly. If this turn never
			// played any TTS audio (firstAudioAt nil), force short
			// filler on the recursive turn since the visitor is still
			// mid-thought and another intent call would just stall.
			if err := s.sendState("thinking"); err != nil {
				return err
			}
			prevSilent := firstAudioAt.Load() == nil
			return s.runBotTurn(ctx, audioCh, turnIdx+1, txt, prevSilent)
		}
	}

	if action == bot.ActionEndCall {
		_ = s.sendState("ended")
		return errEndCall
	}
	return nil
}

// playFiller streams a random filler WAV (PCM16 mono 16 kHz) for the
// bot's voice. Layout:
//
//   <fillerDir>/<voice_id>/*.wav        short pool (flat, back-compat)
//   <fillerDir>/<voice_id>/long/*.wav   long pool (hybrid mode only)
//
// In hybrid mode the user transcript is sent to the bot's intent
// endpoint; BUSINESS → long, CHITCHAT or any failure → short. Sent as
// tts_audio frames so the browser plays them through the same queue
// as real TTS — real TTS naturally queues after.
//
// File format assumed: canonical RIFF/WAVE with 44-byte header.
// forceShort skips intent classify and uses short directly — see
// runBotTurn for when that's true (continuation after silent barge-in).
func (s *voiceState) playFiller(ctx context.Context, transcript string, forceShort bool) {
	var kind intent.Kind
	if forceShort {
		kind = intent.KindShort
	} else {
		kind = s.resolveFillerKind(ctx, transcript)
	}

	dir := filepath.Join(s.fillerDir, s.bot.TTSVoiceID)
	if kind == intent.KindLong {
		dir = filepath.Join(dir, "long")
	}
	wavs := scanWavs(dir)
	if len(wavs) == 0 && kind == intent.KindLong {
		// Long pool missing → fall back to short so the bot never
		// goes silent waiting for first-sentence.
		dir = filepath.Join(s.fillerDir, s.bot.TTSVoiceID)
		wavs = scanWavs(dir)
	}
	if len(wavs) == 0 {
		return
	}
	pick := wavs[int(time.Now().UnixNano())%len(wavs)]
	path := filepath.Join(dir, pick)
	body, err := os.ReadFile(path)
	if err != nil || len(body) <= 44 {
		s.logger.Debug("filler read", "path", path, "err", err)
		return
	}
	pcm := body[44:]
	// Stream in 20 ms chunks @ 16 kHz mono 16-bit = 640 bytes per frame.
	const chunkBytes = 640
	for i := 0; i < len(pcm); i += chunkBytes {
		select {
		case <-ctx.Done():
			return
		default:
		}
		end := i + chunkBytes
		if end > len(pcm) {
			end = len(pcm)
		}
		_ = s.sendWS(wsFrame{
			Type:       "tts_audio",
			PCM:        base64.StdEncoding.EncodeToString(pcm[i:end]),
			SampleRate: s.ttsRate,
		})
	}
}

// resolveFillerKind asks the intent endpoint when the bot opted into
// hybrid mode. Errors / timeout / non-hybrid mode all collapse to
// short — the pipeline should never block on intent.
func (s *voiceState) resolveFillerKind(ctx context.Context, transcript string) intent.Kind {
	if s.bot.FillerMode != "hybrid" || s.bot.FillerIntentURL == "" || transcript == "" {
		return intent.KindShort
	}
	timeout := time.Duration(s.bot.FillerIntentTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 1500 * time.Millisecond
	}
	c := intent.NewHTTPClient(s.bot.FillerIntentURL)
	classifyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	started := time.Now()
	kind, err := c.Classify(classifyCtx, "web-"+s.sess.ID.String(), transcript)
	if s.metrics != nil {
		s.metrics.IntentClassifyDuration.WithLabelValues("web").
			Observe(time.Since(started).Seconds())
		outcome := "fallback"
		switch {
		case err != nil:
			outcome = "fallback"
		case kind == intent.KindLong:
			outcome = "business"
		case kind == intent.KindShort:
			outcome = "chitchat"
		}
		s.metrics.IntentClassifyTotal.WithLabelValues("web", outcome).Inc()
	}
	if err != nil {
		s.logger.Debug("intent classify", "err", err)
		return intent.KindShort
	}
	return kind
}

// scanWavs returns base-names of *.wav files at dir, excluding subdirs.
// Returns nil on any error.
func scanWavs(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) < 4 {
			continue
		}
		ext := name[len(name)-4:]
		if ext != ".wav" && ext != ".WAV" {
			continue
		}
		out = append(out, name)
	}
	return out
}

// bargeMonitor opens an ASR stream parallel to TTS playback. When a
// final transcript with enough words shows up, stores it via atomic
// and cancels its parent ctx so the surrounding turn collapses.
func (s *voiceState) bargeMonitor(ctx context.Context, cancel context.CancelFunc, audioCh <-chan []byte, out *atomic.Value, done chan<- struct{}) {
	defer close(done)
	stream, err := s.asrCli.StartStream(ctx, asr.StreamOpts{
		ConversationID:   "web-barge-" + s.sess.ID.String(),
		SampleRate:       s.asrRate,
		Channels:         1,
		SingleSentence:   true,
		SilenceTimeoutMs: 60000,
		SpeechTimeoutMs:  int(s.bot.ASRSpeechTimeoutSec * 1000),
		SpeechMaxMs:      int(s.bot.ASRSpeechMaxSec * 1000),
	})
	if err != nil {
		s.logger.Debug("barge asr start", "err", err)
		return
	}
	defer stream.Close()

	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		for {
			select {
			case <-ctx.Done():
				return
			case pcm, ok := <-audioCh:
				if !ok {
					return
				}
				if err := stream.SendAudio(pcm); err != nil {
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case res, ok := <-stream.Recv():
			if !ok {
				return
			}
			text := strings.TrimSpace(res.Text)
			if !res.IsFinal || text == "" {
				continue
			}
			words := len(strings.Fields(text))
			if words < s.bargeMin {
				continue
			}
			out.Store(text)
			cancel() // collapses turnCtx → TTS + bot stream stop draining
			return
		}
	}
}

// readLoop pumps inbound WS messages. JSON frames go through dispatch;
// raw audio frames push into audioCh. A "bye" or close terminates.
func (s *voiceState) readLoop(ctx context.Context, audioCh chan<- []byte, done chan<- struct{}) {
	defer close(done)
	defer close(audioCh)
	s.conn.SetReadLimit(1 << 20) // 1MB max frame
	_ = s.conn.SetReadDeadline(time.Time{}) // no idle timeout — visitor may pause mic

	for {
		_, msg, err := s.conn.ReadMessage()
		if err != nil {
			return
		}
		var f wsFrame
		if jsonErr := json.Unmarshal(msg, &f); jsonErr != nil {
			continue
		}
		switch f.Type {
		case "hello":
			// no-op; session frame already sent on upgrade.
		case "audio":
			if f.PCM == "" {
				continue
			}
			pcm, decErr := base64.StdEncoding.DecodeString(f.PCM)
			if decErr != nil {
				continue
			}
			select {
			case audioCh <- pcm:
			case <-ctx.Done():
				return
			default:
				// Drop frame if backpressured — better than blocking the
				// reader and queuing latency.
			}
		case "end", "bye":
			return
		}
	}
}

// wsFrame is the shared envelope for both directions.
type wsFrame struct {
	Type       string `json:"type"`
	SessionID  string `json:"session_id,omitempty"`
	State      string `json:"state,omitempty"`
	VoiceID    string `json:"voice_id,omitempty"`
	Text       string `json:"text,omitempty"`
	Action     string `json:"action,omitempty"`
	PCM        string `json:"pcm,omitempty"`
	SampleRate int    `json:"sample_rate,omitempty"`
	Code       string `json:"code,omitempty"`
	Message    string `json:"message,omitempty"`
}

func (s *voiceState) sendState(state string) error {
	return s.sendWS(wsFrame{Type: "state", State: state})
}

func (s *voiceState) sendWS(f wsFrame) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.conn.WriteJSON(f)
}

func sendWS(conn *websocket.Conn, f wsFrame) error {
	return conn.WriteJSON(f)
}

// wavRecorder writes a minimal RIFF/WAVE header + PCM frames. Header is
// patched on Close so partial files are still valid (most players
// tolerate header-mismatch lengths but it's tidier to fix it).
type wavRecorder struct {
	f          *os.File
	sampleRate int
	bytesWritten uint32
}

func newWavRecorder(path string, sampleRate int) (*wavRecorder, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := &wavRecorder{f: f, sampleRate: sampleRate}
	// Reserve 44 bytes for header.
	hdr := make([]byte, 44)
	if _, err := f.Write(hdr); err != nil {
		_ = f.Close()
		return nil, err
	}
	return w, nil
}

func (w *wavRecorder) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.bytesWritten += uint32(n)
	return n, err
}

func (w *wavRecorder) Close() error {
	if w == nil || w.f == nil {
		return nil
	}
	defer func() { _ = w.f.Close(); w.f = nil }()
	// Patch header in place.
	if _, err := w.f.Seek(0, 0); err != nil {
		return err
	}
	const channels = 1
	const bitsPerSample = 16
	byteRate := uint32(w.sampleRate * channels * bitsPerSample / 8)
	blockAlign := uint16(channels * bitsPerSample / 8)
	hdr := make([]byte, 44)
	copy(hdr[0:4], []byte("RIFF"))
	binary.LittleEndian.PutUint32(hdr[4:8], 36+w.bytesWritten)
	copy(hdr[8:12], []byte("WAVE"))
	copy(hdr[12:16], []byte("fmt "))
	binary.LittleEndian.PutUint32(hdr[16:20], 16)
	binary.LittleEndian.PutUint16(hdr[20:22], 1) // PCM
	binary.LittleEndian.PutUint16(hdr[22:24], channels)
	binary.LittleEndian.PutUint32(hdr[24:28], uint32(w.sampleRate))
	binary.LittleEndian.PutUint32(hdr[28:32], byteRate)
	binary.LittleEndian.PutUint16(hdr[32:34], blockAlign)
	binary.LittleEndian.PutUint16(hdr[34:36], bitsPerSample)
	copy(hdr[36:40], []byte("data"))
	binary.LittleEndian.PutUint32(hdr[40:44], w.bytesWritten)
	_, err := w.f.Write(hdr)
	return err
}

var _ uuid.UUID // keep uuid import used in case helper stubs are removed

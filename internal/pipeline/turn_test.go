package pipeline

import (
	"bytes"
	"context"
	"testing"
	"time"

	"callbot-master/internal/asr"
	"callbot-master/internal/bot"
	"callbot-master/internal/tts"
)

// chanSource is a test-only AudioSource backed by a chan.
type chanSource struct{ ch chan []byte }

func (c *chanSource) Frames() <-chan []byte { return c.ch }

// asrFinal builds an asr.Result with IsFinal=true. Convenience for barge-in
// tests that emit a single final transcript.
func asrFinal(text string) asr.Result { return asr.Result{Text: text, IsFinal: true} }

// newASRMock returns an ASR client whose every StartStream returns the same
// pre-built MockStream. The test drives that stream via Emit/Close.
func newASRMock() (*asr.MockClient, *asr.MockStream) {
	bootstrap := &asr.MockClient{}
	s, _ := bootstrap.StartStream(context.Background(), asr.StreamOpts{})
	stream := s.(*asr.MockStream)
	return &asr.MockClient{
		OnStart: func(_ asr.StreamOpts) (asr.Stream, error) { return stream, nil },
	}, stream
}

func newTTSMock() (*tts.MockClient, *tts.MockStream) {
	bootstrap := &tts.MockClient{}
	s, _ := bootstrap.StartStream(context.Background(), tts.StreamOpts{})
	stream := s.(*tts.MockStream)
	return &tts.MockClient{
		OnStart: func(_ tts.StreamOpts) (tts.Stream, error) { return stream, nil },
	}, stream
}

func newBotMock(action bot.Action) (*bot.MockClient, *bot.MockTurnStream) {
	bootstrap := &bot.MockClient{}
	ts, _ := bootstrap.Stream(context.Background(), "x", "")
	turn := ts.(*bot.MockTurnStream)
	turn.SetAction(action)
	return &bot.MockClient{
		OnStream: func(_ context.Context, _, _ string) (bot.TurnStream, error) { return turn, nil },
	}, turn
}

func TestPipeline_Listen_ReturnsFinalTranscript(t *testing.T) {
	asrCli, asrStream := newASRMock()
	src := &chanSource{ch: make(chan []byte, 4)}
	src.ch <- make([]byte, 320)
	src.ch <- make([]byte, 320)
	close(src.ch) // EOF triggers CloseSend on the stream

	cfg := Defaults()
	cfg.SilentTimeout = 2 * time.Second
	p := New("call-1", cfg, asrCli, nil, nil, nil)

	// Feed transcripts after src EOF; CloseSend leaves recv open so these arrive.
	go func() {
		time.Sleep(20 * time.Millisecond)
		asrStream.Emit(asr.Result{Text: "xin chào", IsFinal: false})
		asrStream.Emit(asr.Result{Text: "xin chào em là callbot", IsFinal: true})
	}()

	got, err := p.Listen(context.Background(), src)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if got != "xin chào em là callbot" {
		t.Fatalf("transcript = %q", got)
	}
	if len(asrStream.Pushed) != 2 {
		t.Fatalf("ASR got %d frames, want 2", len(asrStream.Pushed))
	}
	if asrStream.CloseSentAt == 0 {
		t.Fatal("expected CloseSend to be called when src EOFs")
	}
}

func TestPipeline_Speak_PushesSentencesAndDrainsAudio(t *testing.T) {
	botCli, turn := newBotMock(bot.ActionChat)
	ttsCli, ttsStream := newTTSMock()

	go func() {
		time.Sleep(5 * time.Millisecond)
		turn.EmitSentence("Xin chào.")
		turn.EmitSentence("Em là callbot.")
		turn.Close()
		// Push audio after sentences flushed.
		time.Sleep(5 * time.Millisecond)
		ttsStream.PushAudio([]byte{0x01, 0x02, 0x03, 0x04})
		ttsStream.PushAudio([]byte{0x05, 0x06})
		ttsStream.Close()
	}()

	cfg := Defaults()
	p := New("call-1", cfg, nil, ttsCli, botCli, nil)
	var sink bytes.Buffer
	action, err := p.Speak(context.Background(), nil, &sink, "hello")
	if err != nil {
		t.Fatalf("Speak: %v", err)
	}
	if action != bot.ActionChat {
		t.Fatalf("action = %q, want CHAT", action)
	}
	if sink.Len() != 6 {
		t.Fatalf("sink bytes = %d, want 6", sink.Len())
	}
	if len(ttsStream.Texts) != 2 {
		t.Fatalf("TTS got %d texts, want 2", len(ttsStream.Texts))
	}
	if ttsStream.Texts[0] != "Xin chào." || ttsStream.Texts[1] != "Em là callbot." {
		t.Fatalf("TTS texts = %#v", ttsStream.Texts)
	}
}

func TestPipeline_Speak_EndcallActionPropagates(t *testing.T) {
	botCli, turn := newBotMock(bot.ActionEndCall)
	ttsCli, ttsStream := newTTSMock()

	go func() {
		time.Sleep(5 * time.Millisecond)
		turn.EmitSentence("Cảm ơn anh chị.")
		turn.Close()
		ttsStream.Close()
	}()

	cfg := Defaults()
	p := New("call-1", cfg, nil, ttsCli, botCli, nil)
	var sink bytes.Buffer
	action, err := p.Speak(context.Background(), nil, &sink, "tôi xong rồi")
	if err != nil {
		t.Fatalf("Speak: %v", err)
	}
	if action != bot.ActionEndCall {
		t.Fatalf("action = %q, want ENDCALL", action)
	}
}

func TestPipeline_RunTurn_GreetingPathSkipsListen(t *testing.T) {
	var seenMsg string
	botCli, turn := newBotMock(bot.ActionEndCall)
	botCli.OnStream = func(_ context.Context, _, msg string) (bot.TurnStream, error) {
		seenMsg = msg
		return turn, nil
	}
	ttsCli, ttsStream := newTTSMock()

	go func() {
		time.Sleep(5 * time.Millisecond)
		turn.EmitSentence("Xin chào.")
		turn.Close()
		ttsStream.Close()
	}()

	cfg := Defaults()
	p := New("call-1", cfg, nil, ttsCli, botCli, nil)
	var sink bytes.Buffer
	cont, err := p.RunTurn(context.Background(), nil, &sink)
	if err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	if cont {
		t.Fatalf("ENDCALL must return continue=false")
	}
	if seenMsg != "" {
		t.Fatalf("greeting must call bot with empty message, got %q", seenMsg)
	}
}

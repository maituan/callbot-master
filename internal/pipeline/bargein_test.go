package pipeline

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"callbot-master/internal/asr"
	"callbot-master/internal/bot"
)

// fakeESL records uuid_break calls for barge-in verification.
type fakeESL struct{ stops atomic.Int32 }

func (f *fakeESL) StopPlayback(uuid string) error {
	f.stops.Add(1)
	return nil
}

func TestSpeak_BargeIn_TriggersOnNWordsAndCapturesTranscript(t *testing.T) {
	asrCli, asrStream := newASRMock()
	botCli, turn := newBotMock(bot.ActionChat)
	ttsCli, ttsStream := newTTSMock()
	esl := &fakeESL{}

	cfg := Defaults()
	p := New("call-1", cfg, asrCli, ttsCli, botCli, nil)
	p.BargeIn = true
	p.BargeMinWords = 3
	p.ESL = esl
	p.Scenario = "test"

	// Bot emits one sentence then HOLDs so the action select races against
	// barge-in.
	go func() {
		time.Sleep(5 * time.Millisecond)
		turn.EmitSentence("Em xin chào, anh chị cần gì ạ?")
	}()

	// Feed audio frames so the monitor's audio pump runs; the actual
	// bytes don't matter for the mock ASR.
	src := &chanSource{ch: make(chan []byte, 8)}
	for i := 0; i < 4; i++ {
		src.ch <- bytes.Repeat([]byte{0x10, 0x20}, 160)
	}

	// Two-word partial first → MUST NOT trigger. Then 4-word → trigger.
	go func() {
		time.Sleep(15 * time.Millisecond)
		asrStream.Emit(asr.Result{Text: "tôi muốn"}) // 2 words → ignored
		time.Sleep(10 * time.Millisecond)
		asrStream.Emit(asr.Result{Text: "tôi muốn cấp đổi căn cước"}) // 5 → trigger
	}()

	// Push some TTS audio so we have something in flight when barge-in
	// closes the stream.
	go func() {
		time.Sleep(10 * time.Millisecond)
		ttsStream.PushAudio(bytes.Repeat([]byte{0x01}, 100))
	}()

	var sink bytes.Buffer
	action, err := p.Speak(context.Background(), src, &sink, "")
	if err != nil {
		t.Fatalf("Speak: %v", err)
	}
	if action != bot.ActionChat {
		t.Fatalf("action = %q, want CHAT", action)
	}
	if esl.stops.Load() == 0 {
		t.Fatal("expected uuid_break (StopPlayback) to be called on barge-in")
	}
	if p.pendingTranscript != "tôi muốn cấp đổi căn cước" {
		t.Fatalf("pendingTranscript = %q", p.pendingTranscript)
	}
}

func TestSpeak_BargeIn_BelowThresholdNoTrigger(t *testing.T) {
	asrCli, asrStream := newASRMock()
	botCli, turn := newBotMock(bot.ActionChat)
	ttsCli, ttsStream := newTTSMock()
	esl := &fakeESL{}

	cfg := Defaults()
	p := New("call-1", cfg, asrCli, ttsCli, botCli, nil)
	p.BargeIn = true
	p.BargeMinWords = 3
	p.ESL = esl

	src := &chanSource{ch: make(chan []byte, 4)}
	src.ch <- bytes.Repeat([]byte{0x10}, 320)

	// Only 2 words emitted — must not trigger. Bot finishes normally.
	go func() {
		time.Sleep(5 * time.Millisecond)
		asrStream.Emit(asr.Result{Text: "vâng dạ"})
		time.Sleep(5 * time.Millisecond)
		turn.EmitSentence("Em hiểu rồi.")
		turn.Close()
		ttsStream.PushAudio([]byte{0x01, 0x02})
		ttsStream.Close()
	}()

	var sink bytes.Buffer
	action, err := p.Speak(context.Background(), src, &sink, "alo")
	if err != nil {
		t.Fatalf("Speak: %v", err)
	}
	if action != bot.ActionChat {
		t.Fatalf("action = %q", action)
	}
	if esl.stops.Load() != 0 {
		t.Fatalf("uuid_break should NOT fire below word threshold (got %d)", esl.stops.Load())
	}
	if p.pendingTranscript != "" {
		t.Fatalf("pendingTranscript should be empty when below threshold, got %q", p.pendingTranscript)
	}
}

func TestSpeak_NoBargeIn_DrainsSrc(t *testing.T) {
	// When BargeIn=false, Speak still drains src (so audio doesn't pile
	// up between turns). The frame should be consumed.
	botCli, turn := newBotMock(bot.ActionChat)
	ttsCli, ttsStream := newTTSMock()

	cfg := Defaults()
	p := New("call-1", cfg, nil, ttsCli, botCli, nil)
	p.BargeIn = false

	go func() {
		time.Sleep(5 * time.Millisecond)
		turn.EmitSentence("Hello.")
		turn.Close()
		ttsStream.PushAudio([]byte{0x01, 0x02})
		ttsStream.Close()
	}()

	src := &chanSource{ch: make(chan []byte, 4)}
	src.ch <- bytes.Repeat([]byte{0x33}, 320)

	var sink bytes.Buffer
	action, err := p.Speak(context.Background(), src, &sink, "msg")
	if err != nil {
		t.Fatalf("Speak: %v", err)
	}
	if action != bot.ActionChat {
		t.Fatalf("action = %q", action)
	}
	select {
	case _, ok := <-src.ch:
		if ok {
			t.Fatal("expected src to be drained by Speak")
		}
	default:
		// closed/empty is fine
	}
}

func TestRunTurn_UsesPendingTranscriptInsteadOfListen(t *testing.T) {
	asrCli, _ := newASRMock()
	botCli, turn := newBotMock(bot.ActionChat)
	ttsCli, ttsStream := newTTSMock()

	cfg := Defaults()
	p := New("call-1", cfg, asrCli, ttsCli, botCli, nil)
	p.pendingTranscript = "tôi muốn cấp đổi căn cước"

	var seenMsg string
	botCli.OnStream = func(_ context.Context, _, msg string) (bot.TurnStream, error) {
		seenMsg = msg
		return turn, nil
	}

	go func() {
		time.Sleep(5 * time.Millisecond)
		turn.EmitSentence("Dạ, anh chị muốn cấp đổi.")
		turn.Close()
		ttsStream.Close()
	}()

	src := &chanSource{ch: make(chan []byte, 1)}
	close(src.ch)

	var sink bytes.Buffer
	cont, err := p.RunTurn(context.Background(), src, &sink)
	if err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	if !cont {
		t.Fatal("RunTurn should continue after CHAT")
	}
	if seenMsg != "tôi muốn cấp đổi căn cước" {
		t.Fatalf("bot called with %q, want pending transcript", seenMsg)
	}
	if p.pendingTranscript != "" {
		t.Fatalf("pendingTranscript should be cleared, got %q", p.pendingTranscript)
	}
}

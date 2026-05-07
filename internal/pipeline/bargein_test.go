package pipeline

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"callbot-master/internal/bot"
	"callbot-master/internal/vad"
)

// fakeESL records uuid_break calls for barge-in verification.
type fakeESL struct{ stops atomic.Int32 }

func (f *fakeESL) StopPlayback(uuid string) error {
	f.stops.Add(1)
	return nil
}

func TestSpeak_BargeIn_TriggersCancelAndReplay(t *testing.T) {
	botCli, turn := newBotMock(bot.ActionChat)
	ttsCli, ttsStream := newTTSMock()
	esl := &fakeESL{}

	cfg := Defaults()
	p := New("call-1", cfg, nil, ttsCli, botCli, &vad.MockDetector{})
	p.BargeIn = true
	p.ESL = esl
	p.Scenario = "test"

	// Drive bot to emit one sentence then HOLD (don't close turn) so the
	// barge-in race window is open.
	go func() {
		time.Sleep(5 * time.Millisecond)
		turn.EmitSentence("Em xin chào, anh chị cần gì ạ?")
		// don't close turn until barge-in cancels the bot ctx
	}()

	// Audio source we drive frame-by-frame so we can arm VAD between frames.
	src := &chanSource{ch: make(chan []byte, 8)}
	vadDet := p.VAD.(*vad.MockDetector)

	// Feed: 2 quiet frames (ringbuf gets data), arm VAD, 1 barge-in frame.
	go func() {
		frame := func() []byte { return bytes.Repeat([]byte{0x10, 0x20}, 160) }
		src.ch <- frame()
		src.ch <- frame()
		time.Sleep(20 * time.Millisecond) // let monitor consume above frames
		vadDet.Next = vad.EventSpeechStart
		src.ch <- frame() // this Push returns SpeechStart → barge-in fires
	}()

	// Push a TTS audio frame so we have something in flight when the
	// barge-in chain calls ttsStream.Close().
	go func() {
		time.Sleep(10 * time.Millisecond)
		ttsStream.PushAudio(bytes.Repeat([]byte{0x01}, 100))
	}()

	var sink bytes.Buffer
	action, err := p.Speak(context.Background(), src, &sink, "test message")
	if err != nil {
		t.Fatalf("Speak: %v", err)
	}
	if action != bot.ActionChat {
		t.Fatalf("action = %q, want CHAT (barge-in forces CHAT)", action)
	}
	if esl.stops.Load() == 0 {
		t.Fatal("expected uuid_break (StopPlayback) to be called on barge-in")
	}
	if len(p.pendingReplay) == 0 {
		t.Fatal("expected pendingReplay to be set after barge-in")
	}
}

func TestSpeak_NoBargeIn_NoMonitorWhenDisabled(t *testing.T) {
	// When BargeIn=false, src is ignored — no monitor goroutine, no
	// frames consumed by the pipeline during Speak.
	botCli, turn := newBotMock(bot.ActionChat)
	ttsCli, ttsStream := newTTSMock()

	cfg := Defaults()
	p := New("call-1", cfg, nil, ttsCli, botCli, &vad.MockDetector{})
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
	if len(p.pendingReplay) != 0 {
		t.Fatal("pendingReplay should be empty when barge-in disabled")
	}

	// src should still have its frame (not consumed).
	select {
	case f := <-src.ch:
		if len(f) != 320 {
			t.Fatalf("unexpected frame size %d", len(f))
		}
	default:
		t.Fatal("src frame was unexpectedly consumed")
	}
}

func TestListen_ReplayPrependedFromPendingReplay(t *testing.T) {
	asrCli, asrStream := newASRMock()

	cfg := Defaults()
	cfg.SilentTimeout = 2 * time.Second
	p := New("call-1", cfg, asrCli, nil, nil, nil)
	p.pendingReplay = bytes.Repeat([]byte{0xAA, 0xBB}, 200) // 400 bytes

	src := &chanSource{ch: make(chan []byte, 2)}
	src.ch <- bytes.Repeat([]byte{0xCC}, 320)
	close(src.ch)

	go func() {
		time.Sleep(20 * time.Millisecond)
		asrStream.Emit(asrFinal("đã nghe"))
	}()

	got, err := p.Listen(context.Background(), src)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if got != "đã nghe" {
		t.Fatalf("transcript = %q", got)
	}
	if len(p.pendingReplay) != 0 {
		t.Fatal("pendingReplay should be cleared after Listen consumes it")
	}
	// At minimum the replay frames + the src frame should have been pushed.
	if len(asrStream.Pushed) < 2 {
		t.Fatalf("ASR pushed %d frames, expected at least 2", len(asrStream.Pushed))
	}
}

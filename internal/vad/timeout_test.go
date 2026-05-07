package vad

import (
	"encoding/binary"
	"testing"
	"time"
)

// makeFrame builds an N-ms S16LE 8kHz frame with the given constant amplitude.
func makeFrame(ms int, amp int16) []byte {
	samples := 8000 * ms / 1000
	buf := make([]byte, samples*2)
	for i := 0; i < samples; i++ {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(amp))
	}
	return buf
}

func pushFrames(d *EnergyDetector, totalMs, frameMs int, amp int16) []Event {
	var events []Event
	frames := totalMs / frameMs
	for i := 0; i < frames; i++ {
		e := d.Push(makeFrame(frameMs, amp))
		if e != EventNone {
			events = append(events, e)
		}
	}
	return events
}

func TestEnergy_SilenceProducesNoEvent(t *testing.T) {
	d := NewEnergy(Default())
	got := pushFrames(d, 1000, 20, 50) // amp 50 ≈ noise floor, below threshold 500
	if len(got) != 0 {
		t.Fatalf("got events %v, want none", got)
	}
}

func TestEnergy_LoudFor200msTriggersSpeechStart(t *testing.T) {
	d := NewEnergy(Default())
	// 180ms loud: not enough.
	if got := pushFrames(d, 180, 20, 3000); len(got) != 0 {
		t.Fatalf("at 180ms got %v, want none", got)
	}
	// One more 20ms frame crosses MinSpeechDur=200ms threshold.
	e := d.Push(makeFrame(20, 3000))
	if e != EventSpeechStart {
		t.Fatalf("at 200ms got %v, want SpeechStart", e)
	}
}

func TestEnergy_LoudThenSilence_TriggersSpeechEnd(t *testing.T) {
	d := NewEnergy(Default())
	// Bring it into speech state.
	if got := pushFrames(d, 220, 20, 3000); len(got) != 1 || got[0] != EventSpeechStart {
		t.Fatalf("expected SpeechStart, got %v", got)
	}
	// 580ms quiet — below MinSilenceDur=600ms, no event yet.
	if got := pushFrames(d, 580, 20, 30); len(got) != 0 {
		t.Fatalf("at 580ms quiet got %v, want none", got)
	}
	// One more frame → SpeechEnd.
	e := d.Push(makeFrame(20, 30))
	if e != EventSpeechEnd {
		t.Fatalf("at 600ms quiet got %v, want SpeechEnd", e)
	}
}

func TestEnergy_BriefLoudIsIgnored(t *testing.T) {
	d := NewEnergy(Default())
	// 100ms loud is below MinSpeechDur=200ms — should not trigger and
	// counter should reset on the following quiet.
	if got := pushFrames(d, 100, 20, 3000); len(got) != 0 {
		t.Fatalf("100ms loud got %v, want none", got)
	}
	// 500ms quiet, then another 100ms loud — still no trigger because the
	// loud counter resets between bursts.
	pushFrames(d, 500, 20, 30)
	if got := pushFrames(d, 100, 20, 3000); len(got) != 0 {
		t.Fatalf("after quiet+brief loud got %v, want none", got)
	}
}

func TestEnergy_BotSpeakingRaisesThreshold(t *testing.T) {
	cfg := Default()
	cfg.BotSpeakingMul = 10.0 // make it very strict
	d := NewEnergy(cfg)
	d.SetBotSpeaking(true)
	// Amplitude 3000 normally triggers, but with 10x multiplier the
	// threshold becomes 5000 → should NOT trigger.
	if got := pushFrames(d, 400, 20, 3000); len(got) != 0 {
		t.Fatalf("loud-while-bot got %v, want none (suppressed)", got)
	}
	// Bot stops speaking → same audio level should now trigger.
	d.SetBotSpeaking(false)
	got := pushFrames(d, 400, 20, 3000)
	if len(got) != 1 || got[0] != EventSpeechStart {
		t.Fatalf("after bot stops got %v, want one SpeechStart", got)
	}
}

func TestEnergy_ResetClearsState(t *testing.T) {
	d := NewEnergy(Default())
	pushFrames(d, 300, 20, 3000) // → SpeechStart
	d.Reset()
	// After reset, 100ms loud should NOT immediately end-trigger because
	// we're back in pre-speech state.
	if got := pushFrames(d, 100, 20, 3000); len(got) != 0 {
		t.Fatalf("after reset, 100ms loud got %v, want none", got)
	}
}

func TestPcmRMS_KnownValues(t *testing.T) {
	// Constant amp 1000 → RMS = 1000.
	if got := pcmRMS(makeFrame(20, 1000)); got < 999 || got > 1001 {
		t.Fatalf("RMS(1000) = %v, want ~1000", got)
	}
	if got := pcmRMS(makeFrame(20, 0)); got != 0 {
		t.Fatalf("RMS(0) = %v, want 0", got)
	}
}

func TestFrameDuration_8kHz(t *testing.T) {
	if got := frameDuration(640, 8000); got != 40*time.Millisecond {
		t.Fatalf("duration(640@8k) = %v, want 40ms", got)
	}
	if got := frameDuration(320, 8000); got != 20*time.Millisecond {
		t.Fatalf("duration(320@8k) = %v, want 20ms", got)
	}
}

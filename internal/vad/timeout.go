package vad

import (
	"encoding/binary"
	"math"
	"sync"
	"time"
)

// Config tunes the energy-based detector. Defaults suit 8kHz S16LE mono.
//
// Detection rule (CLAUDE.md "barge-in must-have"):
//   - SpeechStart fires once continuous loud audio has lasted MinSpeechDur.
//   - SpeechEnd fires once continuous quiet has lasted MinSilenceDur after
//     a prior SpeechStart.
//   - When BotSpeaking=true, EnergyThreshold is multiplied by BotSpeakingMul
//     to suppress false triggers from echoed TTS audio.
type Config struct {
	SampleRate      int
	EnergyThreshold float64       // RMS amplitude above which a frame counts as "loud"
	MinSpeechDur    time.Duration // continuous loud time → SpeechStart
	MinSilenceDur   time.Duration // continuous quiet time after start → SpeechEnd
	BotSpeakingMul  float64       // threshold multiplier while bot is speaking
}

// Default returns a sensible config for 8kHz S16LE phone audio.
// EnergyThreshold of 500 RMS is well above background noise floor (~50-150)
// while still catching normal speech (~1500-3000).
func Default() Config {
	return Config{
		SampleRate:      8000,
		EnergyThreshold: 500.0,
		MinSpeechDur:    200 * time.Millisecond,
		MinSilenceDur:   600 * time.Millisecond,
		BotSpeakingMul:  3.0,
	}
}

// EnergyDetector implements vad.Detector using a frame-RMS energy threshold
// plus min-duration hysteresis. Not safe for concurrent Push; one detector
// per session/call.
type EnergyDetector struct {
	cfg Config

	mu        sync.Mutex
	botSpeaks bool

	// FSM:
	//   - !inSpeech: counting loud time toward SpeechStart
	//   - inSpeech:  counting quiet time toward SpeechEnd
	inSpeech bool
	loudDur  time.Duration
	quietDur time.Duration
}

// NewEnergy returns a detector. Pass Default() for sensible defaults.
func NewEnergy(cfg Config) *EnergyDetector {
	if cfg.SampleRate == 0 {
		cfg.SampleRate = 8000
	}
	if cfg.BotSpeakingMul == 0 {
		cfg.BotSpeakingMul = 1.0
	}
	return &EnergyDetector{cfg: cfg}
}

// Push consumes one PCM frame (S16LE) and returns Event according to the FSM.
// Most calls return EventNone; transitions return EventSpeechStart/SpeechEnd
// exactly once per crossing.
func (d *EnergyDetector) Push(pcm []byte) Event {
	d.mu.Lock()
	defer d.mu.Unlock()

	dur := frameDuration(len(pcm), d.cfg.SampleRate)
	if dur == 0 {
		return EventNone
	}
	rms := pcmRMS(pcm)
	threshold := d.cfg.EnergyThreshold
	if d.botSpeaks {
		threshold *= d.cfg.BotSpeakingMul
	}

	loud := rms >= threshold

	if !d.inSpeech {
		if loud {
			d.loudDur += dur
			d.quietDur = 0
			if d.loudDur >= d.cfg.MinSpeechDur {
				d.inSpeech = true
				d.loudDur = 0
				return EventSpeechStart
			}
		} else {
			d.loudDur = 0
		}
		return EventNone
	}

	// inSpeech == true
	if !loud {
		d.quietDur += dur
		d.loudDur = 0
		if d.quietDur >= d.cfg.MinSilenceDur {
			d.inSpeech = false
			d.quietDur = 0
			return EventSpeechEnd
		}
	} else {
		d.quietDur = 0
	}
	return EventNone
}

func (d *EnergyDetector) SetBotSpeaking(b bool) {
	d.mu.Lock()
	d.botSpeaks = b
	d.mu.Unlock()
}

func (d *EnergyDetector) Reset() {
	d.mu.Lock()
	d.inSpeech = false
	d.loudDur = 0
	d.quietDur = 0
	d.mu.Unlock()
}

// frameDuration computes the wall duration of a PCM S16LE frame.
func frameDuration(byteLen, sampleRate int) time.Duration {
	if sampleRate <= 0 || byteLen < 2 {
		return 0
	}
	samples := byteLen / 2
	return time.Duration(samples) * time.Second / time.Duration(sampleRate)
}

// pcmRMS computes the root-mean-square amplitude of an S16LE PCM frame.
// Returns 0 for empty/invalid input.
func pcmRMS(pcm []byte) float64 {
	if len(pcm) < 2 {
		return 0
	}
	n := len(pcm) / 2
	var sumSq float64
	for i := 0; i < n; i++ {
		s := int16(binary.LittleEndian.Uint16(pcm[i*2 : i*2+2]))
		f := float64(s)
		sumSq += f * f
	}
	return math.Sqrt(sumSq / float64(n))
}

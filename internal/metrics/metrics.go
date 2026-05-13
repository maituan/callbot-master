// Package metrics owns the Prometheus collectors for callbot-master.
//
// All collectors are pre-registered on the package's own *Registry so the
// /metrics endpoint can serve them via promhttp without touching the
// global default registry — keeps tests independent and avoids leaking
// process-wide collectors when running multiple instances under test.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Collectors aggregates every metric the master exposes. One instance per
// process; pass it to packages that need to record (pipeline, campaign).
type Collectors struct {
	Registry *prometheus.Registry

	ActiveCalls      *prometheus.GaugeVec
	TurnDuration     *prometheus.HistogramVec
	StageLatency     *prometheus.HistogramVec
	BargeInTotal     *prometheus.CounterVec
	BotErrorTotal    *prometheus.CounterVec
	ASRErrorTotal    *prometheus.CounterVec
	TTSErrorTotal    *prometheus.CounterVec
	CampaignProgress *prometheus.GaugeVec
	OriginateTotal   *prometheus.CounterVec

	// IntentClassifyDuration tracks the end-to-end intent classification
	// latency: master sends transcript, awaits BUSINESS/CHITCHAT label,
	// resolves filler kind. Watch p95/p99 — if it creeps near the
	// FillerIntentTimeout the filler effectively becomes short-only.
	IntentClassifyDuration *prometheus.HistogramVec

	// IntentClassifyTotal counts intent classify results by outcome:
	// kind ∈ business | chitchat | fallback (any error/timeout/unknown).
	IntentClassifyTotal *prometheus.CounterVec
}

// New builds and registers all collectors. Standard process/Go runtime
// collectors are also added so /metrics shows GC, goroutines, fds, etc.
func New() *Collectors {
	reg := prometheus.NewRegistry()

	c := &Collectors{
		Registry: reg,
		ActiveCalls: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "master",
			Name:      "active_calls",
			Help:      "Number of in-flight calls grouped by direction.",
		}, []string{"direction"}),
		TurnDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "master",
			Name:      "turn_duration_seconds",
			Help:      "End-to-end duration of one turn (Listen + Speak), greeting included.",
			Buckets:   []float64{0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 8, 13, 21},
		}, []string{"kind", "scenario"}), // kind = "greeting" | "turn"
		StageLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "master",
			Name:      "stage_latency_seconds",
			Help:      "Per-stage latency: asr.first_partial, asr.final, bot.first_sentence, bot.total, tts.first_audio, tts.total.",
			Buckets:   []float64{0.05, 0.1, 0.2, 0.4, 0.7, 1, 1.5, 2.5, 4, 7, 12},
		}, []string{"stage", "scenario"}),
		BargeInTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "master",
			Name:      "barge_in_total",
			Help:      "Number of caller barge-in events that interrupted the bot.",
		}, []string{"scenario"}),
		BotErrorTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "master",
			Name:      "bot_error_total",
			Help:      "Bot HTTP/stream errors.",
		}, []string{"kind"}), // kind = "stream_open" | "stream_read" | "ctx_canceled" | "fallback"
		ASRErrorTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "master",
			Name:      "asr_error_total",
			Help:      "ASR gRPC stream errors.",
		}, []string{"kind"}),
		TTSErrorTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "master",
			Name:      "tts_error_total",
			Help:      "TTS WebSocket errors.",
		}, []string{"kind"}),
		CampaignProgress: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "master",
			Name:      "campaign_progress",
			Help:      "Per-campaign lead-status counters (pending/dialing/answered/completed/failed/no_answer/canceled).",
		}, []string{"campaign_id", "status"}),
		OriginateTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "master",
			Name:      "originate_total",
			Help:      "Outbound originate attempts grouped by outcome (ok|error).",
		}, []string{"result"}),
		IntentClassifyDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "master",
			Name:      "intent_classify_duration_seconds",
			Help:      "Intent classification latency per request (filler hybrid mode).",
			Buckets:   []float64{0.05, 0.1, 0.2, 0.4, 0.7, 1, 1.5, 2.5, 4},
		}, []string{"channel"}), // channel = phone | web
		IntentClassifyTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "master",
			Name:      "intent_classify_total",
			Help:      "Intent classify outcomes grouped by kind (business|chitchat|fallback).",
		}, []string{"channel", "outcome"}),
	}

	reg.MustRegister(
		c.ActiveCalls,
		c.TurnDuration,
		c.StageLatency,
		c.BargeInTotal,
		c.BotErrorTotal,
		c.ASRErrorTotal,
		c.TTSErrorTotal,
		c.CampaignProgress,
		c.OriginateTotal,
		c.IntentClassifyDuration,
		c.IntentClassifyTotal,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return c
}

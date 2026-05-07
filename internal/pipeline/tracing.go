package pipeline

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"callbot-master/internal/metrics"
)

// recordStage observes a single stage latency in the histogram if metrics
// are wired; nil-safe.
func recordStage(c *metrics.Collectors, stage, scenario string, durSec float64) {
	if c == nil {
		return
	}
	c.StageLatency.WithLabelValues(stage, scenario).Observe(durSec)
}

// incCounter is a nil-safe shortcut for *prometheus.CounterVec.WithLabelValues(...).Inc().
func incBotErr(c *metrics.Collectors, kind string) {
	if c == nil {
		return
	}
	c.BotErrorTotal.WithLabelValues(kind).Inc()
}

func incASRErr(c *metrics.Collectors, kind string) {
	if c == nil {
		return
	}
	c.ASRErrorTotal.WithLabelValues(kind).Inc()
}

func incTTSErr(c *metrics.Collectors, kind string) {
	if c == nil {
		return
	}
	c.TTSErrorTotal.WithLabelValues(kind).Inc()
}

// tracer is the package-level otel tracer. Cheap when tracing is disabled
// (no-op provider) — safe to use unconditionally.
var tracer = otel.Tracer("callbot-master/pipeline")

// Common attribute keys, hoisted for consistency across spans.
const (
	attrCallUUID  = "call.uuid"
	attrDirection = "call.direction"
	attrScenario  = "call.scenario"
	attrCaller    = "call.caller"
	attrTurnIdx   = "pipeline.turn_idx"
	attrTurnKind  = "pipeline.turn_kind" // "greeting" | "turn"
	attrAction    = "bot.action"
)

func callAttrs(uuid string) []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String(attrCallUUID, uuid)}
}

// addEventMs records a span event with an "elapsed_ms" attribute, the
// canonical way we capture per-event latency throughout a span.
func addEventMs(span trace.Span, name string, elapsedMs int64, extra ...attribute.KeyValue) {
	attrs := append([]attribute.KeyValue{attribute.Int64("elapsed_ms", elapsedMs)}, extra...)
	span.AddEvent(name, trace.WithAttributes(attrs...))
}

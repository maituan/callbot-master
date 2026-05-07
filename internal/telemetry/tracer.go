// Package telemetry initializes OpenTelemetry tracing for callbot-master.
//
// Usage:
//
//	shutdown, err := telemetry.Init(ctx, telemetry.Config{
//	    Endpoint:    "tempo:4317",
//	    ServiceName: "callbot-master",
//	    Insecure:    true,
//	})
//	defer shutdown(context.Background())
//
// When Endpoint is empty Init installs a no-op tracer provider — the
// application code keeps calling otel.Tracer(...) but spans are dropped at
// near-zero cost.
package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// Config carries the runtime knobs. Endpoint empty → no-op tracer.
type Config struct {
	Endpoint    string // OTLP gRPC, e.g. "tempo:4317"
	ServiceName string // resource attr; default "callbot-master"
	Insecure    bool   // plaintext gRPC for local dev
}

// ShutdownFunc flushes pending spans and tears down the exporter.
type ShutdownFunc func(context.Context) error

// noopShutdown is returned when tracing is disabled — keeps callers from
// having to nil-check.
func noopShutdown(_ context.Context) error { return nil }

// Init wires the global tracer provider. If Endpoint is empty, returns a
// no-op shutdown — application code can still call otel.Tracer(...) safely.
func Init(ctx context.Context, cfg Config) (ShutdownFunc, error) {
	if cfg.Endpoint == "" {
		return noopShutdown, nil
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "callbot-master"
	}

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	exporter, err := otlptrace.New(dialCtx, otlptracegrpc.NewClient(opts...))
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
		resource.WithProcess(),
		resource.WithHost(),
		resource.WithFromEnv(), // honor OTEL_RESOURCE_ATTRIBUTES
	)
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(2*time.Second),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

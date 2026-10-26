// Package telemetry configures traces for local development.
// The default exporter writes spans to stdout. It does not dial a collector.
package telemetry

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const TracerName = "github.com/chrisuzor/fleetpilot"

// Setup installs a tracer provider.
// OTEL_SDK_DISABLED=true or OTEL_TRACES_EXPORTER=none leaves tracing off.
// Any other value, including an empty OTEL_TRACES_EXPORTER, writes spans to stdout.
// The returned function flushes and shuts the provider down.
func Setup(context.Context) (func(context.Context) error, error) {
	mode := exporterMode()
	if sdkDisabled() || mode == "none" {
		slog.Info("traces", "exporter", "none")
		return func(context.Context) error { return nil }, nil
	}
	if mode != "console" {
		slog.Info("traces", "exporter", "console", "ignored", mode)
	} else {
		slog.Info("traces", "exporter", "console")
	}
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)),
		sdktrace.WithResource(resource.NewSchemaless(
			attribute.String("service.name", serviceName()),
		)),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return provider.Shutdown, nil
}

// Inject returns the W3C traceparent for the current span, or empty.
func Inject(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// Extract returns a context whose remote parent is traceParent.
// An empty value returns ctx unchanged.
func Extract(ctx context.Context, traceParent string) context.Context {
	if traceParent == "" {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{
		"traceparent": traceParent,
	})
}

func exporterMode() string {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_EXPORTER"))) {
	case "", "console", "stdout":
		return "console"
	case "none":
		return "none"
	default:
		return strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_EXPORTER")))
	}
}

func sdkDisabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED"))) {
	case "true", "1":
		return true
	default:
		return false
	}
}

func serviceName() string {
	if name := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); name != "" {
		return name
	}
	return "fleetpilot"
}

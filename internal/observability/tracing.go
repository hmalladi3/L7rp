package observability

import (
	"context"
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TracingConfig parameterizes OpenTelemetry tracing initialization.
//
// Endpoint is the OTLP gRPC collector address (e.g., "localhost:4317"). An
// empty endpoint disables export; the proxy still creates spans via a no-op
// tracer so handler code stays free of nil checks, but nothing is shipped.
//
// SampleRatio is parent-based: requests carrying an inbound traceparent
// inherit their parent's decision; requests without are sampled at this
// ratio. Defaults to 0.01 (1%) when zero or negative.
type TracingConfig struct {
	Endpoint    string
	ServiceName string
	SampleRatio float64
	Insecure    bool
}

const tracerName = "github.com/harimalladi/l7rp"

// SetupTracing initializes the global TracerProvider and propagator. Returns
// a shutdown function the caller should defer to flush pending spans on exit.
//
// On Endpoint=="" the tracer is a no-op: spans are accepted (so handler code
// can call Start unconditionally) but nothing is exported. This is the
// "tracing disabled" path.
func SetupTracing(ctx context.Context, cfg TracingConfig) (func(context.Context) error, error) {
	// Always install the W3C TraceContext + Baggage propagator. Even when
	// tracing is disabled, propagation should still work — operators may want
	// to forward incoming traceparent headers to upstreams without sampling
	// locally.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if cfg.Endpoint == "" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = "l7rp"
	}
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(serviceName)))
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}

	ratio := cfg.SampleRatio
	switch {
	case ratio <= 0:
		ratio = 0.01
	case ratio > 1:
		ratio = 1
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer returns the proxy's tracer. Safe to call before SetupTracing — the
// returned tracer is a thin wrapper over the global provider, which is the
// no-op provider until SetupTracing installs a real one.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// ExtractRequestContext pulls W3C / Baggage trace context from request
// headers into the request context. Use at the listener boundary so root
// spans inherit any caller-side context.
func ExtractRequestContext(r *http.Request) context.Context {
	return otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
}

// InjectRequestContext writes W3C / Baggage trace context onto outbound
// request headers. Use just before dispatching to an upstream so the trace
// chain stays connected.
func InjectRequestContext(ctx context.Context, r *http.Request) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(r.Header))
}

// Standard span attributes — exported so callers can use a consistent
// vocabulary without depending on the proxy-internal package.
//
// The proxy-specific attributes (proxy.route, proxy.pool, proxy.upstream)
// supplement the OpenTelemetry HTTP semantic conventions; both are recorded
// so generic OTel-aware dashboards and proxy-specific ones can both work.
var (
	AttrRoute    = attribute.Key("proxy.route")
	AttrPool     = attribute.Key("proxy.pool")
	AttrUpstream = attribute.Key("proxy.upstream")
	AttrAttempt  = attribute.Key("proxy.retry_attempt")
	AttrHedge    = attribute.Key("proxy.hedge_fired")
)

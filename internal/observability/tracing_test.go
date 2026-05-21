package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// installRecorder swaps in an in-memory span recorder for the duration of a
// test. SetupTracing wires the global TracerProvider; recordedSpans() reads
// them out at the end.
func installRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prior := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	t.Cleanup(func() { otel.SetTracerProvider(prior) })
	return rec
}

func TestTracing_SetupDisabledIsNoop(t *testing.T) {
	shutdown, err := SetupTracing(context.Background(), TracingConfig{})
	if err != nil {
		t.Fatalf("SetupTracing: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	// Calling Tracer().Start should succeed without panic when disabled.
	_, span := Tracer().Start(context.Background(), "noop-test")
	span.End()
}

func TestTracing_TracerCreatesSpan(t *testing.T) {
	rec := installRecorder(t)

	_, span := Tracer().Start(context.Background(), "proxy.request")
	span.End()

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if spans[0].Name() != "proxy.request" {
		t.Errorf("span name = %q, want %q", spans[0].Name(), "proxy.request")
	}
}

// TestTracing_ExtractsInboundTraceParent verifies that a request carrying a
// W3C traceparent header is honored as the parent of the local span.
func TestTracing_ExtractsInboundTraceParent(t *testing.T) {
	rec := installRecorder(t)

	// Construct a request with a valid W3C traceparent.
	r := httptest.NewRequest("GET", "/", nil)
	parentTrace := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	r.Header.Set("traceparent", parentTrace)

	ctx := ExtractRequestContext(r)
	_, span := Tracer().Start(ctx, "proxy.request")
	span.End()

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	// The span should be linked to the parent trace ID encoded in the
	// inbound traceparent.
	got := spans[0].SpanContext().TraceID().String()
	wantTraceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	if got != wantTraceID {
		t.Errorf("trace ID = %q, want %q (inherited from traceparent)", got, wantTraceID)
	}
}

// TestTracing_InjectsTraceParentOutbound verifies that an outbound request
// has a traceparent header set when the current context has an active span.
func TestTracing_InjectsTraceParentOutbound(t *testing.T) {
	installRecorder(t)

	ctx, span := Tracer().Start(context.Background(), "proxy.upstream.request")
	defer span.End()

	outboundReq, _ := http.NewRequest("GET", "http://upstream", nil)
	InjectRequestContext(ctx, outboundReq)

	if outboundReq.Header.Get("traceparent") == "" {
		t.Error("outbound request missing traceparent header")
	}
}

// TestTracing_PropagationDisabledStillWorks confirms that propagation is
// installed even when tracing export is disabled, so the proxy can forward
// inbound traceparent to upstreams without sampling locally.
func TestTracing_PropagationDisabledStillWorks(t *testing.T) {
	shutdown, err := SetupTracing(context.Background(), TracingConfig{}) // disabled
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	ctx := ExtractRequestContext(r)

	outbound, _ := http.NewRequest("GET", "http://x", nil)
	InjectRequestContext(ctx, outbound)
	if outbound.Header.Get("traceparent") == "" {
		t.Error("traceparent should propagate even when export is disabled")
	}
}

func TestTracing_SampleRatioClamps(t *testing.T) {
	// We can't observe sampling behavior without a real exporter, but at
	// minimum SetupTracing must not blow up on out-of-range values.
	for _, ratio := range []float64{-1.0, 0.0, 0.5, 1.0, 2.0} {
		shutdown, err := SetupTracing(context.Background(), TracingConfig{
			Endpoint:    "",
			SampleRatio: ratio,
		})
		if err != nil {
			t.Errorf("ratio=%v: %v", ratio, err)
		}
		if shutdown != nil {
			_ = shutdown(context.Background())
		}
	}
}

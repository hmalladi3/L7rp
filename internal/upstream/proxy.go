// Package upstream implements the terminal handler in every route's middleware
// chain: pick an upstream from the route's pool, dispatch the request, stream
// the response back. The proxy deliberately does not use httputil.ReverseProxy
// so retry / outcome / inflight bookkeeping stays visible at the call site.
package upstream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/harimalladi/l7rp/internal/lb"
	"github.com/harimalladi/l7rp/internal/middleware"
	"github.com/harimalladi/l7rp/internal/observability"
)

// Proxy is the terminal http.Handler that sits at the end of each route's
// middleware chain. It picks an upstream via the route's Selector, writes the
// chosen upstream to the retry middleware's AttemptOutcome (if present),
// dispatches the request via http.RoundTripper, reports the outcome to the
// upstream's circuit breaker, and streams the response back to the client.
type Proxy struct {
	poolName        string
	selector        lb.Selector
	transport       http.RoundTripper
	metrics         *observability.Metrics
	passiveRecorder func(*lb.Upstream, time.Duration, bool)
}

// NewProxy constructs a Proxy over the given selector. The transport defaults
// to http.DefaultTransport when nil; metrics may be nil to disable reporting.
func NewProxy(poolName string, selector lb.Selector, transport http.RoundTripper, metrics *observability.Metrics) *Proxy {
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &Proxy{poolName: poolName, selector: selector, transport: transport, metrics: metrics}
}

// WithPassiveRecorder wires a callback that the proxy invokes after each
// completed request (except cancellations) with the upstream, observed
// latency, and an isError flag. Used to feed the health monitor's passive
// EWMA scoring. Returns the receiver for chaining at construction sites.
func (p *Proxy) WithPassiveRecorder(fn func(*lb.Upstream, time.Duration, bool)) *Proxy {
	p.passiveRecorder = fn
	return p
}

// ServeHTTP implements the proxy lifecycle. The error paths surface back to
// the retry middleware via AttemptOutcome.Err so retries can fire.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	info := middleware.AttemptFromContext(r.Context())
	outcome := middleware.OutcomeFromContext(r.Context())

	u, err := p.selector.Pick(r.Context(), r, lb.PickHint{Exclude: info.Exclude})
	if err != nil {
		if outcome != nil {
			outcome.Err = err
		}
		writeError(w, http.StatusServiceUnavailable, "no eligible upstream")
		return
	}

	if outcome != nil {
		outcome.Chosen = u
	}

	upstreamLabel := u.URL.String()

	// Reserve a probe slot in case the breaker is HalfOpen. In Closed state
	// Reserve always returns true; this is the gate that prevents a flood of
	// requests during probe phase.
	if !u.Breaker.Reserve() {
		writeError(w, http.StatusServiceUnavailable, "upstream breaker open")
		return
	}

	// WebSocket dispatch happens after Reserve but before inflight counting
	// (the WS path manages inflight itself for the lifetime of the long-lived
	// session).
	if isWebSocketUpgrade(r) {
		p.serveWebSocket(w, r, u)
		return
	}

	u.InFlight.Add(1)
	if p.metrics != nil {
		p.metrics.UpstreamInflight.WithLabelValues(p.poolName, upstreamLabel).Inc()
	}
	defer func() {
		u.InFlight.Add(-1)
		if p.metrics != nil {
			p.metrics.UpstreamInflight.WithLabelValues(p.poolName, upstreamLabel).Dec()
		}
	}()

	// Open a client-kind child span around the upstream call. The no-op
	// tracer (installed when tracing is disabled) makes this essentially
	// free. W3C traceparent is injected on the outbound request so the
	// upstream sees the same trace.
	spanCtx, span := observability.Tracer().Start(r.Context(), "proxy.upstream.request",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("url.path", r.URL.Path),
			observability.AttrPool.String(p.poolName),
			observability.AttrUpstream.String(upstreamLabel),
		),
	)
	defer span.End()

	upReq := buildUpstreamRequest(r, u)
	upReq = upReq.WithContext(spanCtx)
	observability.InjectRequestContext(spanCtx, upReq)

	resp, err := p.transport.RoundTrip(upReq)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		// Distinguish three error cases:
		//   1. Deadline exceeded — the route's total timeout fired. Surface
		//      504 to operators looking at access logs. Treated as cancellation
		//      so the breaker doesn't blame the upstream for our budget.
		//   2. Other context cancel — the caller (retry/hedge or downstream
		//      client) gave up. Status 499-style, but Go's net/http has no
		//      499, so we fall through to 502 — the connection back to the
		//      downstream client is usually already broken anyway.
		//   3. Real upstream error — connection refused, TLS failure, etc.
		ctxErr := r.Context().Err()
		canceled := ctxErr != nil
		timedOut := errors.Is(ctxErr, context.DeadlineExceeded)
		if canceled {
			u.Breaker.Record(lb.OutcomeCancellation)
		} else {
			u.Breaker.Record(lb.OutcomeFailure)
		}
		if outcome != nil {
			outcome.Err = err
		}
		latency := time.Since(start)
		p.metrics.ObserveUpstream(p.poolName, upstreamLabel, 0, latency)
		if p.passiveRecorder != nil && !canceled {
			p.passiveRecorder(u, latency, true)
		}
		switch {
		case timedOut:
			writeError(w, http.StatusGatewayTimeout, "gateway timeout")
		default:
			writeError(w, http.StatusBadGateway, "upstream error")
		}
		return
	}
	defer resp.Body.Close()

	span.SetAttributes(attribute.String("http.response.status_code", strconv.Itoa(resp.StatusCode)))

	// 5xx responses are upstream failures; 4xx is the client's problem.
	isUpstreamErr := resp.StatusCode >= 500
	if isUpstreamErr {
		u.Breaker.Record(lb.OutcomeFailure)
		span.SetStatus(codes.Error, "upstream 5xx")
	} else {
		u.Breaker.Record(lb.OutcomeSuccess)
	}

	latency := time.Since(start)
	p.metrics.ObserveUpstream(p.poolName, upstreamLabel, resp.StatusCode, latency)
	if p.passiveRecorder != nil {
		p.passiveRecorder(u, latency, isUpstreamErr)
	}
	copyResponse(w, resp)
}

// buildUpstreamRequest produces a clone of r retargeted at u. Hop-by-hop
// header stripping and X-Forwarded-* augmentation are deferred to the
// header-transform middleware; this function focuses on URL retargeting.
func buildUpstreamRequest(r *http.Request, u *lb.Upstream) *http.Request {
	req := r.Clone(r.Context())
	req.URL.Scheme = u.URL.Scheme
	req.URL.Host = u.URL.Host
	if u.URL.Path != "" && u.URL.Path != "/" {
		req.URL.Path = u.URL.Path + req.URL.Path
	}
	req.Host = u.URL.Host
	req.RequestURI = "" // required by http.Transport.RoundTrip
	return req
}

// copyResponse streams the upstream response to the downstream writer.
func copyResponse(w http.ResponseWriter, resp *http.Response) {
	for k, vs := range resp.Header {
		w.Header()[k] = vs
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(msg + "\n"))
}

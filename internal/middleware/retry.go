package middleware

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/harimalladi/l7rp/internal/lb"
)

// RetryConfig parameterizes the Retry middleware.
type RetryConfig struct {
	// MaxAttempts is the total number of attempts (1 = no retry).
	MaxAttempts int

	// RetryMethods is the set of methods eligible for retry (uppercase).
	// Default: GET, HEAD, PUT, DELETE.
	RetryMethods []string

	// RetryOn is the set of status codes considered retryable. Default:
	// 502, 503, 504. Connection errors propagated via AttemptOutcome.Err are
	// always retryable when method allows.
	RetryOn []int

	// MaxReplayableBody caps the request body size we'll buffer for replay.
	// Requests with larger bodies are not retried. Default: 64 KiB.
	MaxReplayableBody int64

	// HedgeAfter is the fixed duration after which a hedged duplicate is
	// dispatched. Set to 0 to disable hedging.
	HedgeAfter time.Duration

	// HedgeAfterPercentile is reserved for percentile-based hedge thresholds.
	// Not implemented in v1; if non-zero, HedgeAfter is used instead.
	HedgeAfterPercentile float64

	// HedgeableMethods lists methods for which hedging is allowed. Default:
	// GET, HEAD.
	HedgeableMethods []string
}

// Retry returns a middleware that retries failed upstream attempts (per
// RetryConfig) and dispatches hedged duplicates when an attempt exceeds the
// hedge threshold.
//
// The middleware does not directly call the selector. Instead, it threads a
// per-attempt context that carries (1) the AttemptInfo with the Exclude list
// for the next attempt's selector, and (2) a pointer to an AttemptOutcome
// struct that the downstream upstream-proxy writes when it picks an upstream.
// On a retry, the retry middleware reads AttemptOutcome.Chosen and appends it
// to the next attempt's Exclude hint.
func Retry(cfg RetryConfig) Middleware {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	return func(next http.Handler) http.Handler {
		return &retryHandler{cfg: cfg, next: next}
	}
}

type retryHandler struct {
	cfg  RetryConfig
	next http.Handler
}

func (h *retryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	bodyBuf, bodyTooLarge := bufferReplayBody(req, h.cfg.MaxReplayableBody)
	methodAllowsRetry := stringInSlice(h.cfg.RetryMethods, req.Method) && !bodyTooLarge
	methodAllowsHedge := stringInSlice(h.cfg.HedgeableMethods, req.Method) && h.cfg.HedgeAfter > 0 && !bodyTooLarge

	if methodAllowsHedge && h.cfg.MaxAttempts >= 2 {
		h.serveHedged(w, req, bodyBuf)
		return
	}

	maxAttempts := h.cfg.MaxAttempts
	if !methodAllowsRetry {
		maxAttempts = 1
	}
	h.serveSequential(w, req, bodyBuf, maxAttempts)
}

func (h *retryHandler) serveSequential(w http.ResponseWriter, req *http.Request, bodyBuf []byte, maxAttempts int) {
	var exclude []*lb.Upstream
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		res := h.dispatch(req, attempt, exclude, false, bodyBuf)
		if attempt == maxAttempts || !h.shouldRetry(res) {
			res.rec.flushTo(w)
			return
		}
		if res.outcome.Chosen != nil {
			exclude = append(exclude, res.outcome.Chosen)
		}
	}
}

func (h *retryHandler) serveHedged(w http.ResponseWriter, req *http.Request, bodyBuf []byte) {
	// Per-request cancellation context for primary and hedge.
	parentCtx := req.Context()
	primaryCtx, primaryCancel := context.WithCancel(parentCtx)
	defer primaryCancel()
	primaryReq := req.WithContext(primaryCtx)

	primaryDone := make(chan attemptResult, 1)
	go func() {
		primaryDone <- h.dispatch(primaryReq, 1, nil, false, bodyBuf)
	}()

	timer := time.NewTimer(h.cfg.HedgeAfter)
	defer timer.Stop()

	select {
	case res := <-primaryDone:
		// Primary finished before hedge threshold.
		res.rec.flushTo(w)
		return
	case <-timer.C:
		// Fall through: fire hedge.
	}

	hedgeCtx, hedgeCancel := context.WithCancel(parentCtx)
	defer hedgeCancel()
	hedgeReq := req.WithContext(hedgeCtx)

	// Hedge knows nothing about the primary's chosen upstream yet (primary
	// hasn't returned); use a copy-on-demand approach — the upstream-proxy
	// is expected to honor the Exclude that's known at dispatch time, plus
	// The same upstream may be picked again when only one is eligible.
	hedgeDone := make(chan attemptResult, 1)
	go func() {
		hedgeDone <- h.dispatch(hedgeReq, 2, nil, true, bodyBuf)
	}()

	// First to complete wins; cancel the other.
	select {
	case res := <-primaryDone:
		hedgeCancel()
		// Drain hedge to avoid goroutine leak.
		<-hedgeDone
		res.rec.flushTo(w)
	case res := <-hedgeDone:
		primaryCancel()
		<-primaryDone
		res.rec.flushTo(w)
	}
}

type attemptResult struct {
	rec     *bufferedResponse
	outcome *AttemptOutcome
}

func (h *retryHandler) dispatch(req *http.Request, attempt int, exclude []*lb.Upstream, isHedge bool, bodyBuf []byte) attemptResult {
	info := AttemptInfo{Number: attempt, Exclude: exclude, IsHedge: isHedge}
	outcome := &AttemptOutcome{}

	ctx := WithAttempt(req.Context(), info)
	ctx = context.WithValue(ctx, attemptOutcomeKey{}, outcome)

	attemptReq := req.Clone(ctx)
	if bodyBuf != nil {
		attemptReq.Body = io.NopCloser(bytes.NewReader(bodyBuf))
	}

	rec := newBufferedResponse()

	func() {
		defer func() {
			if rcv := recover(); rcv != nil {
				outcome.Err = fmt.Errorf("handler panic: %v", rcv)
				if rec.statusCode == 0 {
					rec.statusCode = http.StatusBadGateway
				}
			}
		}()
		h.next.ServeHTTP(rec, attemptReq)
	}()

	return attemptResult{rec: rec, outcome: outcome}
}

func (h *retryHandler) shouldRetry(res attemptResult) bool {
	if res.outcome.Err != nil {
		return true
	}
	for _, code := range h.cfg.RetryOn {
		if res.rec.statusCode == code {
			return true
		}
	}
	return false
}

// AttemptInfo is installed on the request context by the Retry middleware and
// read by the upstream-proxy. It carries the per-attempt state that the
// selector needs to honor (the Exclude list) and that observability needs to
// label its outputs (the attempt number, whether this is a hedge).
type AttemptInfo struct {
	Number   int            // 1-indexed attempt number
	Exclude  []*lb.Upstream // upstreams to exclude on this attempt
	IsHedge  bool           // true if this attempt is a hedged duplicate
	Original *lb.Upstream   // the original attempt's upstream, when IsHedge
}

type attemptInfoKey struct{}

// AttemptFromContext extracts the current attempt info, returning a zero-but-
// initialized AttemptInfo when none is installed (the first/only attempt in
// chains without Retry).
func AttemptFromContext(ctx context.Context) AttemptInfo {
	v, ok := ctx.Value(attemptInfoKey{}).(AttemptInfo)
	if !ok || v.Number == 0 {
		v.Number = 1
	}
	return v
}

// WithAttempt returns a context carrying the given AttemptInfo. Used by Retry
// to thread per-attempt state to the upstream-proxy.
func WithAttempt(ctx context.Context, info AttemptInfo) context.Context {
	return context.WithValue(ctx, attemptInfoKey{}, info)
}

// AttemptOutcome carries per-attempt result data from the upstream-proxy back
// to the retry middleware. The upstream-proxy writes Chosen (the picked
// upstream) and optionally Err (connection error etc.); the retry middleware
// reads these to populate the next attempt's Exclude list and to decide
// retry-eligibility.
type AttemptOutcome struct {
	Chosen *lb.Upstream
	Err    error
}

type attemptOutcomeKey struct{}

// OutcomeFromContext returns the active *AttemptOutcome installed by the
// retry middleware. Downstream handlers (the upstream-proxy) write to its
// fields; returns nil when no retry middleware is in the chain.
func OutcomeFromContext(ctx context.Context) *AttemptOutcome {
	v, _ := ctx.Value(attemptOutcomeKey{}).(*AttemptOutcome)
	return v
}

// bufferedResponse is an http.ResponseWriter that captures headers, status,
// and body in memory. The retry middleware uses it to inspect attempt outcomes
// before deciding to forward or retry; the final attempt's response is flushed
// to the real ResponseWriter.
//
// Streaming responses (SSE, large bodies) are buffered too — operators should
// either disable retry for streaming routes or accept the memory cost.
type bufferedResponse struct {
	headers    http.Header
	statusCode int
	body       bytes.Buffer
}

func newBufferedResponse() *bufferedResponse {
	return &bufferedResponse{headers: make(http.Header)}
}

func (b *bufferedResponse) Header() http.Header { return b.headers }

func (b *bufferedResponse) WriteHeader(code int) {
	if b.statusCode == 0 {
		b.statusCode = code
	}
}

func (b *bufferedResponse) Write(p []byte) (int, error) {
	if b.statusCode == 0 {
		b.statusCode = http.StatusOK
	}
	return b.body.Write(p)
}

func (b *bufferedResponse) flushTo(w http.ResponseWriter) {
	for k, vs := range b.headers {
		w.Header()[k] = vs
	}
	code := b.statusCode
	if code == 0 {
		code = http.StatusOK
	}
	w.WriteHeader(code)
	_, _ = w.Write(b.body.Bytes())
}

// bufferReplayBody reads the request body up to maxSize+1 bytes. Returns the
// buffered bytes and a tooLarge flag set when the body exceeded maxSize. The
// caller is responsible for restoring req.Body when needed; the original
// reader is closed here.
func bufferReplayBody(req *http.Request, maxSize int64) (buf []byte, tooLarge bool) {
	if req.Body == nil || req.Body == http.NoBody {
		return nil, false
	}
	if maxSize <= 0 {
		// No buffering configured. Drain to avoid leaking the connection but
		// don't keep the bytes.
		_ = req.Body.Close()
		return nil, false
	}

	limited := io.LimitReader(req.Body, maxSize+1)
	data, err := io.ReadAll(limited)
	_ = req.Body.Close()
	if err != nil {
		return nil, false
	}
	if int64(len(data)) > maxSize {
		return data, true
	}
	return data, false
}

func stringInSlice(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

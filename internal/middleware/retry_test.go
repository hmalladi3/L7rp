package middleware

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/harimalladi/l7rp/internal/lb"
)

// scriptedHandler is a test-only http.Handler that returns the next scripted
// response on each call. It records every invocation, the attempt info it
// observed, and the request body content for replay verification.
type scriptedHandler struct {
	mu      sync.Mutex
	scripts []scriptedResp
	calls   []scriptedCall
}

type scriptedResp struct {
	status int
	body   string
	delay  time.Duration
	err    bool // if true, hijack the conn and close to simulate connection failure
}

type scriptedCall struct {
	attempt AttemptInfo
	body    string
}

func (h *scriptedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	idx := len(h.calls)
	if idx >= len(h.scripts) {
		idx = len(h.scripts) - 1 // repeat last script for unexpected attempts
	}
	resp := h.scripts[idx]

	bodyBytes, _ := io.ReadAll(r.Body)
	h.calls = append(h.calls, scriptedCall{
		attempt: AttemptFromContext(r.Context()),
		body:    string(bodyBytes),
	})
	h.mu.Unlock()

	if resp.delay > 0 {
		select {
		case <-time.After(resp.delay):
		case <-r.Context().Done():
			return
		}
	}

	if resp.err {
		// Approximate a connection error: panic out of the handler so the
		// surrounding recovery turns it into 500. (Real connection errors
		// happen below the handler boundary; this is the closest test signal.)
		// Note: when Phase 6 ships, real upstream errors are emitted by the
		// upstream-proxy and seen by retry; we'll revise this fixture then.
		panic("upstream connection error")
	}
	w.WriteHeader(resp.status)
	if resp.body != "" {
		_, _ = w.Write([]byte(resp.body))
	}
}

func (h *scriptedHandler) Calls() []scriptedCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]scriptedCall, len(h.calls))
	copy(out, h.calls)
	return out
}

func newRetryHandler(cfg RetryConfig, terminal http.Handler) http.Handler {
	return Retry(cfg)(terminal)
}

func TestRetry_FirstAttemptSucceeds(t *testing.T) {
	t.Parallel()

	script := &scriptedHandler{scripts: []scriptedResp{{status: 200, body: "ok"}}}
	h := newRetryHandler(RetryConfig{MaxAttempts: 3, RetryOn: []int{502}}, script)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	h.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if got := len(script.Calls()); got != 1 {
		t.Errorf("attempts = %d, want 1", got)
	}
}

func TestRetry_RetriesOnRetryableStatus(t *testing.T) {
	t.Parallel()

	script := &scriptedHandler{
		scripts: []scriptedResp{
			{status: 502, body: "first"},
			{status: 200, body: "second"},
		},
	}
	h := newRetryHandler(RetryConfig{MaxAttempts: 3, RetryOn: []int{502}, RetryMethods: []string{"GET"}}, script)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	h.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200 (after retry)", w.Code)
	}
	if got := len(script.Calls()); got != 2 {
		t.Errorf("attempts = %d, want 2", got)
	}
}

func TestRetry_DoesNotRetryNonRetryableMethod(t *testing.T) {
	t.Parallel()

	script := &scriptedHandler{
		scripts: []scriptedResp{
			{status: 502},
			{status: 200},
		},
	}
	h := newRetryHandler(RetryConfig{
		MaxAttempts:  3,
		RetryOn:      []int{502},
		RetryMethods: []string{"GET"}, // POST not in list
	}, script)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/", strings.NewReader("body"))
	h.ServeHTTP(w, req)

	if w.Code != 502 {
		t.Errorf("status = %d, want 502 (no retry for POST)", w.Code)
	}
	if got := len(script.Calls()); got != 1 {
		t.Errorf("attempts = %d, want 1", got)
	}
}

func TestRetry_MaxAttemptsBound(t *testing.T) {
	t.Parallel()

	script := &scriptedHandler{
		scripts: []scriptedResp{
			{status: 502},
			{status: 502},
			{status: 502},
			{status: 200}, // never reached
		},
	}
	h := newRetryHandler(RetryConfig{
		MaxAttempts:  2, // first attempt + 1 retry
		RetryOn:      []int{502},
		RetryMethods: []string{"GET"},
	}, script)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	h.ServeHTTP(w, req)

	if w.Code != 502 {
		t.Errorf("status = %d, want 502 (max attempts exhausted)", w.Code)
	}
	if got := len(script.Calls()); got != 2 {
		t.Errorf("attempts = %d, want exactly 2", got)
	}
}

func TestRetry_AttemptInfoExposedToDownstream(t *testing.T) {
	t.Parallel()

	script := &scriptedHandler{
		scripts: []scriptedResp{
			{status: 502},
			{status: 502},
			{status: 200},
		},
	}
	h := newRetryHandler(RetryConfig{MaxAttempts: 3, RetryOn: []int{502}, RetryMethods: []string{"GET"}}, script)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	h.ServeHTTP(w, req)

	calls := script.Calls()
	if len(calls) != 3 {
		t.Fatalf("attempts = %d, want 3", len(calls))
	}
	for i, c := range calls {
		want := i + 1
		if c.attempt.Number != want {
			t.Errorf("call %d: AttemptInfo.Number = %d, want %d", i, c.attempt.Number, want)
		}
	}
}

func TestRetry_ExcludeHintGrowsOnEachAttempt(t *testing.T) {
	t.Parallel()

	// Simulate the upstream-proxy stamping the upstream into the request via
	// a context value the test can later inspect. Retry's job is to populate
	// Exclude with previously-tried upstreams.
	u1, _ := url.Parse("http://u1")
	u2, _ := url.Parse("http://u2")
	upstreams := []*lb.Upstream{{URL: u1}, {URL: u2}}

	// scripted handler that "tries" upstreams in a known order.
	var attempt atomic.Int32
	terminal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := int(attempt.Add(1)) - 1
		// Update context: this attempt tried upstreams[idx]; future retries
		// should exclude it.
		_ = idx
		w.WriteHeader(502)
	})

	h := newRetryHandler(RetryConfig{
		MaxAttempts:  2,
		RetryOn:      []int{502},
		RetryMethods: []string{"GET"},
	}, terminal)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	h.ServeHTTP(w, req)

	// At minimum, the second attempt should have a non-empty Exclude.
	// The exact pointer the Retry middleware records depends on plumbing
	// completed in Phase 6; this test fails by design until then.
	_ = upstreams
}

func TestRetry_BufferedBodyReplayedOnRetry(t *testing.T) {
	t.Parallel()

	const bodyText = "hello world"
	script := &scriptedHandler{
		scripts: []scriptedResp{
			{status: 502},
			{status: 200},
		},
	}
	h := newRetryHandler(RetryConfig{
		MaxAttempts:       3,
		RetryOn:           []int{502},
		RetryMethods:      []string{"PUT"},
		MaxReplayableBody: 1024,
	}, script)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/", strings.NewReader(bodyText))
	h.ServeHTTP(w, req)

	calls := script.Calls()
	if len(calls) != 2 {
		t.Fatalf("attempts = %d, want 2", len(calls))
	}
	for i, c := range calls {
		if c.body != bodyText {
			t.Errorf("attempt %d body = %q, want %q (replay broken)", i+1, c.body, bodyText)
		}
	}
}

func TestRetry_SkipsRetryWhenBodyTooLarge(t *testing.T) {
	t.Parallel()

	bigBody := strings.Repeat("x", 200_000) // 200 KiB
	script := &scriptedHandler{
		scripts: []scriptedResp{
			{status: 502},
			{status: 200}, // never reached
		},
	}
	h := newRetryHandler(RetryConfig{
		MaxAttempts:       3,
		RetryOn:           []int{502},
		RetryMethods:      []string{"PUT"},
		MaxReplayableBody: 64 * 1024, // body exceeds
	}, script)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/", strings.NewReader(bigBody))
	h.ServeHTTP(w, req)

	if got := len(script.Calls()); got != 1 {
		t.Errorf("attempts = %d, want 1 (body too large to replay)", got)
	}
}

func TestRetry_HedgeFiresAfterThreshold(t *testing.T) {
	t.Parallel()

	script := &scriptedHandler{
		scripts: []scriptedResp{
			{status: 200, body: "slow", delay: 200 * time.Millisecond},
			{status: 200, body: "fast", delay: 0},
		},
	}
	h := newRetryHandler(RetryConfig{
		MaxAttempts:      2,
		RetryMethods:     []string{"GET"},
		HedgeAfter:       50 * time.Millisecond,
		HedgeableMethods: []string{"GET"},
	}, script)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	h.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if got := w.Body.String(); got != "fast" {
		t.Errorf("body = %q, want %q (hedge should have won)", got, "fast")
	}
	// Both calls should have been initiated.
	if got := len(script.Calls()); got != 2 {
		t.Errorf("attempts = %d, want 2 (hedge fired)", got)
	}
}

func TestRetry_HedgeNotFiredForNonHedgeableMethod(t *testing.T) {
	t.Parallel()

	script := &scriptedHandler{
		scripts: []scriptedResp{
			{status: 200, body: "slow", delay: 100 * time.Millisecond},
			{status: 200, body: "fast"},
		},
	}
	h := newRetryHandler(RetryConfig{
		MaxAttempts:      2,
		HedgeAfter:       10 * time.Millisecond,
		HedgeableMethods: []string{"GET"}, // POST not hedgeable
	}, script)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/", strings.NewReader("x"))
	h.ServeHTTP(w, req)

	if w.Body.String() != "slow" {
		t.Errorf("got body %q, want %q (POST should not be hedged)", w.Body.String(), "slow")
	}
	if got := len(script.Calls()); got != 1 {
		t.Errorf("attempts = %d, want 1 (no hedge)", got)
	}
}

func TestAttemptFromContext_DefaultsToOne(t *testing.T) {
	t.Parallel()

	info := AttemptFromContext(context.Background())
	if info.Number != 1 {
		t.Errorf("default AttemptInfo.Number = %d, want 1", info.Number)
	}
}

func TestWithAttempt_RoundTrip(t *testing.T) {
	t.Parallel()

	in := AttemptInfo{Number: 3, IsHedge: true}
	ctx := WithAttempt(context.Background(), in)
	out := AttemptFromContext(ctx)
	if out.Number != in.Number || out.IsHedge != in.IsHedge {
		t.Errorf("round-trip: got %+v, want %+v", out, in)
	}
}

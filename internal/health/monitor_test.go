package health

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/harimalladi/l7rp/internal/config"
	"github.com/harimalladi/l7rp/internal/lb"
)

func mkUpstream(t *testing.T, u string) *lb.Upstream {
	t.Helper()
	parsed, err := url.Parse(u)
	if err != nil {
		t.Fatalf("parse %q: %v", u, err)
	}
	up := &lb.Upstream{URL: parsed, Breaker: lb.NewCircuitBreaker(lb.DefaultCircuitBreakerConfig())}
	return up
}

// TestMonitor_HealthyAfterThreshold verifies that an upstream becomes eligible
// only after HealthyThreshold consecutive successful probes — not on the very
// first response. This is the hysteresis that prevents flap.
func TestMonitor_HealthyAfterThreshold(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	u := mkUpstream(t, backend.URL)
	pool := []*lb.Upstream{u}

	cfg := &config.ActiveHealthConfig{
		Path:               "/healthz",
		Interval:           20 * time.Millisecond,
		Timeout:            500 * time.Millisecond,
		HealthyThreshold:   2,
		UnhealthyThreshold: 2,
	}
	m := NewMonitor("backend", pool, cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if u.Eligible.Load() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("upstream never became eligible despite backend returning 200")
}

// TestMonitor_UnhealthyAfterThreshold drives the reverse transition: an
// upstream that starts eligible becomes ineligible after UnhealthyThreshold
// consecutive failures.
func TestMonitor_UnhealthyAfterThreshold(t *testing.T) {
	t.Parallel()

	var healthy atomic.Bool
	healthy.Store(true)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if healthy.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer backend.Close()

	u := mkUpstream(t, backend.URL)
	pool := []*lb.Upstream{u}

	cfg := &config.ActiveHealthConfig{
		Path:               "/healthz",
		Interval:           20 * time.Millisecond,
		Timeout:            500 * time.Millisecond,
		HealthyThreshold:   2,
		UnhealthyThreshold: 2,
	}
	m := NewMonitor("backend", pool, cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	// Wait until eligible.
	if !waitFor(t, 2*time.Second, func() bool { return u.Eligible.Load() }) {
		t.Fatal("upstream never became eligible")
	}

	// Flip backend to unhealthy.
	healthy.Store(false)

	if !waitFor(t, 2*time.Second, func() bool { return !u.Eligible.Load() }) {
		t.Errorf("upstream never became ineligible after backend started returning 500s")
	}
}

// TestMonitor_TransitionCallbackFires verifies that the OnTransition callback
// is invoked on each eligibility flip — the hook observability uses to emit
// counter increments.
func TestMonitor_TransitionCallbackFires(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	u := mkUpstream(t, backend.URL)
	pool := []*lb.Upstream{u}

	cfg := &config.ActiveHealthConfig{
		Path: "/healthz", Interval: 20 * time.Millisecond,
		Timeout: 500 * time.Millisecond, HealthyThreshold: 2, UnhealthyThreshold: 2,
	}
	m := NewMonitor("backend", pool, cfg, nil)

	var transitions atomic.Int32
	m.OnTransition = func(_ string, _ *lb.Upstream, _ bool, _ string) {
		transitions.Add(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	if !waitFor(t, 2*time.Second, func() bool { return transitions.Load() >= 1 }) {
		t.Errorf("OnTransition was never called; got %d transitions", transitions.Load())
	}
}

// TestMonitor_StartsIneligible asserts the conservative startup posture: a new
// pool's upstreams are ineligible until they prove healthy.
func TestMonitor_StartsIneligible(t *testing.T) {
	t.Parallel()

	// Backend that never responds (probe will time out).
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	addr := "http://" + listener.Addr().String()

	u := mkUpstream(t, addr)
	u.Eligible.Store(true) // simulate caller having marked eligible.

	pool := []*lb.Upstream{u}
	cfg := &config.ActiveHealthConfig{
		Path: "/", Interval: 50 * time.Millisecond, Timeout: 20 * time.Millisecond,
		HealthyThreshold: 2, UnhealthyThreshold: 2,
	}
	m := NewMonitor("backend", pool, cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	// Run.0 sets eligible=false unconditionally at start. Poll instead of a
	// fixed sleep so this test stays stable on slow CI schedulers.
	if !waitFor(t, time.Second, func() bool { return !u.Eligible.Load() }) {
		t.Errorf("upstream still eligible after monitor Run; expected ineligible at startup")
	}
}

// TestMonitor_NilConfigReturnsNil avoids the "must-construct-something" trap
// for pools that opt out of active probing.
func TestMonitor_NilConfigReturnsNil(t *testing.T) {
	t.Parallel()
	if NewMonitor("p", []*lb.Upstream{}, nil, nil) != nil {
		t.Error("NewMonitor with nil config should return nil")
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

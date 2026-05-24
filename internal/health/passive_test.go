package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/harimalladi/l7rp/internal/config"
	"github.com/harimalladi/l7rp/internal/lb"
)

func mkPassiveMon(t *testing.T, threshold float64) (*Monitor, *lb.Upstream) {
	t.Helper()
	u, _ := url.Parse("http://test")
	upstream := &lb.Upstream{URL: u, Breaker: lb.NewCircuitBreaker(lb.DefaultCircuitBreakerConfig())}
	upstream.Eligible.Store(true)

	m := NewMonitor("p", []*lb.Upstream{upstream},
		&config.ActiveHealthConfig{
			Path: "/", Interval: time.Second, Timeout: 100 * time.Millisecond,
			HealthyThreshold: 2, UnhealthyThreshold: 3,
		},
		&config.PassiveHealthConfig{
			ErrorThreshold: threshold,
			HalfLife:       100 * time.Millisecond,
		}, nil)
	return m, upstream
}

func TestPassive_NoOpWhenDisabled(t *testing.T) {
	t.Parallel()

	u, _ := url.Parse("http://test")
	upstream := &lb.Upstream{URL: u, Breaker: lb.NewCircuitBreaker(lb.DefaultCircuitBreakerConfig())}
	upstream.Eligible.Store(true)

	// Active config but no passive.
	m := NewMonitor("p", []*lb.Upstream{upstream},
		&config.ActiveHealthConfig{Path: "/", Interval: time.Second, Timeout: 100 * time.Millisecond},
		nil, nil)

	// Hammer with errors — without passive config, eligibility shouldn't change.
	for i := 0; i < 100; i++ {
		m.RecordOutcome(upstream, 10*time.Millisecond, true)
	}
	if !upstream.Eligible.Load() {
		t.Error("eligibility flipped despite passive=nil")
	}
}

// TestPassive_AccumulatesErrEWMAAndTransitions drives a stream of error
// outcomes until errEWMA crosses the threshold, then checks that the
// upstream became ineligible and the transition callback fired.
func TestPassive_AccumulatesErrEWMAAndTransitions(t *testing.T) {
	t.Parallel()

	m, upstream := mkPassiveMon(t, 0.5)

	var transitions atomic.Int32
	var lastReason string
	m.OnTransition = func(_ string, _ *lb.Upstream, eligible bool, reason string) {
		if !eligible {
			transitions.Add(1)
			lastReason = reason
		}
	}

	// Spaced 200ms apart (>= half-life) → α ≈ 0.86 per sample. After ~5 errors
	// the EWMA crosses 0.5 comfortably.
	now := time.Unix(1_700_000_000, 0)
	m.now = func() time.Time {
		now = now.Add(200 * time.Millisecond)
		return now
	}

	for i := 0; i < 10; i++ {
		m.RecordOutcome(upstream, 50*time.Millisecond, true)
		if !upstream.Eligible.Load() {
			break
		}
	}

	if upstream.Eligible.Load() {
		t.Errorf("upstream still eligible after sustained errors")
	}
	if transitions.Load() == 0 {
		t.Error("OnTransition was not called")
	}
	if lastReason != "passive_threshold_breached" {
		t.Errorf("transition reason = %q, want %q", lastReason, "passive_threshold_breached")
	}
}

// TestPassive_SuccessKeepsEligibility ensures a stream of successes doesn't
// trip the threshold.
func TestPassive_SuccessKeepsEligibility(t *testing.T) {
	t.Parallel()

	m, upstream := mkPassiveMon(t, 0.5)

	for i := 0; i < 100; i++ {
		m.RecordOutcome(upstream, 10*time.Millisecond, false)
	}
	if !upstream.Eligible.Load() {
		t.Error("eligibility flipped on pure-success stream")
	}
}

// TestPassive_OneWayValve documents the load-bearing semantic: passive scoring
// can take an upstream out, but only active probes restore it. We drive
// passive to ineligible, then send a stream of successes; eligibility must
// stay false.
func TestPassive_OneWayValve(t *testing.T) {
	t.Parallel()

	m, upstream := mkPassiveMon(t, 0.5)

	// Bump time so each update has a meaningful α.
	now := time.Unix(1_700_000_000, 0)
	m.now = func() time.Time {
		now = now.Add(200 * time.Millisecond)
		return now
	}

	// Drive to ineligible via errors.
	for i := 0; i < 20 && upstream.Eligible.Load(); i++ {
		m.RecordOutcome(upstream, 10*time.Millisecond, true)
	}
	if upstream.Eligible.Load() {
		t.Fatal("setup: passive should have marked ineligible")
	}

	// Now hammer successes — eligibility must stay false. The one-way valve
	// is enforced by the `u.Eligible.Load()` guard in passive's transition
	// path: passive only writes false→nothing, never false→true.
	for i := 0; i < 100; i++ {
		m.RecordOutcome(upstream, 10*time.Millisecond, false)
	}
	if upstream.Eligible.Load() {
		t.Error("passive scoring restored eligibility on its own; the one-way valve is broken")
	}
}

// TestPassive_ResetsActiveCountersOnTrip ensures that when passive marks an
// upstream ineligible, the active-probe counters reset — recovery then
// requires healthy_threshold consecutive probe successes from a clean slate
// rather than the upstream re-eligibling on whatever active count happens to
// have accumulated.
func TestPassive_ResetsActiveCountersOnTrip(t *testing.T) {
	t.Parallel()

	m, upstream := mkPassiveMon(t, 0.5)

	// Pre-load some active-probe successes.
	state := m.states[0]
	state.mu.Lock()
	state.successes = 5
	state.mu.Unlock()

	now := time.Unix(1_700_000_000, 0)
	m.now = func() time.Time {
		now = now.Add(200 * time.Millisecond)
		return now
	}
	for i := 0; i < 20 && upstream.Eligible.Load(); i++ {
		m.RecordOutcome(upstream, 10*time.Millisecond, true)
	}

	state.mu.Lock()
	successes := state.successes
	failures := state.failures
	state.mu.Unlock()
	if successes != 0 {
		t.Errorf("active successes = %d, want 0 (reset on passive trip)", successes)
	}
	if failures != 0 {
		t.Errorf("active failures = %d, want 0 (reset on passive trip)", failures)
	}
}

// TestPassive_AlphaDecaysWithTime verifies the time-weighted nature of the
// EWMA: when an upstream recovers (after an error spike), errors spaced far
// apart from successes decay faster — α grows with Δt, so each success pulls
// the EWMA more strongly toward zero.
//
// Setup: seed both monitors with an error spike, then drive successes at
// different clock cadences. The slow-clock monitor's EWMA decays further
// toward 0 because each success has more weight.
func TestPassive_AlphaDecaysWithTime(t *testing.T) {
	t.Parallel()

	mFast, uFast := mkPassiveMon(t, 0.99) // threshold high so we never trip
	mSlow, uSlow := mkPassiveMon(t, 0.99)

	// Wire clocks before any RecordOutcome calls so all observations use
	// deterministic times.
	tFast := time.Unix(1_700_000_000, 0)
	mFast.now = func() time.Time {
		tFast = tFast.Add(10 * time.Millisecond)
		return tFast
	}
	tSlow := time.Unix(1_700_000_000, 0)
	mSlow.now = func() time.Time {
		tSlow = tSlow.Add(time.Second)
		return tSlow
	}

	// Seed both with an error spike (errEWMA → 1.0 on first observation).
	mFast.RecordOutcome(uFast, 10*time.Millisecond, true)
	mSlow.RecordOutcome(uSlow, 10*time.Millisecond, true)

	// Now drive a series of successes. Slow clock's larger Δt gives larger α,
	// pulling errEWMA harder toward 0.
	for i := 0; i < 5; i++ {
		mFast.RecordOutcome(uFast, 10*time.Millisecond, false)
		mSlow.RecordOutcome(uSlow, 10*time.Millisecond, false)
	}

	fastState := mFast.states[0]
	slowState := mSlow.states[0]
	fastState.mu.Lock()
	slowState.mu.Lock()
	fastE := fastState.errEWMA
	slowE := slowState.errEWMA
	fastState.mu.Unlock()
	slowState.mu.Unlock()

	if slowE >= fastE {
		t.Errorf("slow-clock EWMA (%.3f) should be lower than fast-clock EWMA (%.3f): larger Δt → larger α → successes pull errEWMA toward 0 faster", slowE, fastE)
	}
}

// TestPassive_ActiveRecoveryAfterPassiveTrip drives the full lifecycle:
// passive marks ineligible, then active probes recover the upstream after
// healthy_threshold successful probes.
func TestPassive_ActiveRecoveryAfterPassiveTrip(t *testing.T) {
	t.Parallel()

	// Backend that's controllable via a flag.
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

	u, _ := url.Parse(backend.URL)
	upstream := &lb.Upstream{URL: u, Breaker: lb.NewCircuitBreaker(lb.DefaultCircuitBreakerConfig())}
	upstream.Eligible.Store(true)

	m := NewMonitor("p", []*lb.Upstream{upstream},
		&config.ActiveHealthConfig{
			Path:               "/",
			Interval:           50 * time.Millisecond,
			Timeout:            200 * time.Millisecond,
			HealthyThreshold:   2,
			UnhealthyThreshold: 3,
		},
		&config.PassiveHealthConfig{
			ErrorThreshold: 0.5,
			HalfLife:       100 * time.Millisecond,
		}, nil)

	// Trip via passive.
	tickClock := time.Unix(1_700_000_000, 0)
	m.now = func() time.Time {
		tickClock = tickClock.Add(200 * time.Millisecond)
		return tickClock
	}
	for i := 0; i < 20 && upstream.Eligible.Load(); i++ {
		m.RecordOutcome(upstream, 10*time.Millisecond, true)
	}
	if upstream.Eligible.Load() {
		t.Fatal("setup: should be passive-ineligible")
	}

	// Switch monitor back to real time so active probes work normally,
	// then run the monitor and confirm it restores eligibility.
	m.now = time.Now
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if upstream.Eligible.Load() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("upstream was not re-eligibled by active probes after passive trip")
}

package lb

import (
	"sync"
	"testing"
	"time"
)

func newTestBreaker(now func() time.Time) *CircuitBreaker {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	if now != nil {
		cb.now = now
	}
	return cb
}

func TestCB_InitialStateIsClosed(t *testing.T) {
	t.Parallel()
	cb := newTestBreaker(nil)
	if got := cb.State(); got != BreakerClosed {
		t.Errorf("initial state = %v, want Closed", got)
	}
}

func TestCB_MinObservationsGatesTrip(t *testing.T) {
	t.Parallel()
	cb := newTestBreaker(nil)

	for i := 0; i < cb.cfg.MinObservations-1; i++ {
		cb.Record(OutcomeFailure)
	}
	if got := cb.State(); got != BreakerClosed {
		t.Errorf("after %d failures (below MinObservations=%d): state = %v, want Closed",
			cb.cfg.MinObservations-1, cb.cfg.MinObservations, got)
	}

	cb.Record(OutcomeFailure) // crosses MinObservations
	if got := cb.State(); got != BreakerOpen {
		t.Errorf("after %d failures: state = %v, want Open", cb.cfg.MinObservations, got)
	}
}

// FailureRatio is strict-greater-than: at exactly the threshold, the breaker
// stays Closed.
func TestCB_FailureRatioBoundary(t *testing.T) {
	t.Parallel()
	cb := newTestBreaker(nil)
	cfg := cb.cfg

	// 10 fail / 10 success: ratio 0.5 at MinObservations boundary (20 obs).
	for i := 0; i < 10; i++ {
		cb.Record(OutcomeFailure)
		cb.Record(OutcomeSuccess)
	}
	if got := cb.State(); got != BreakerClosed {
		t.Errorf("at exact FailureRatio=%.2f: state = %v, want Closed (strict >)", cfg.FailureRatio, got)
	}

	cb.Record(OutcomeFailure) // tips ratio above 0.5
	if got := cb.State(); got != BreakerOpen {
		t.Errorf("after one more failure: state = %v, want Open", got)
	}
}

func TestCB_OpenToHalfOpenAfterDuration(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Unix(1000, 0))
	cb := newTestBreaker(clock.Now)

	for i := 0; i < 25; i++ {
		cb.Record(OutcomeFailure)
	}
	if cb.State() != BreakerOpen {
		t.Fatal("setup: breaker should be Open")
	}

	clock.Advance(cb.cfg.OpenMin - time.Millisecond)
	if got := cb.State(); got != BreakerOpen {
		t.Errorf("just before OpenMin elapses: state = %v, want Open", got)
	}

	clock.Advance(2 * time.Millisecond) // now past OpenMin
	if got := cb.State(); got != BreakerHalfOpen {
		t.Errorf("after OpenMin elapses: state = %v, want HalfOpen", got)
	}
}

func TestCB_HalfOpenAllSuccessClosesBreaker(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Unix(1000, 0))
	cb := newTestBreaker(clock.Now)

	for i := 0; i < 25; i++ {
		cb.Record(OutcomeFailure)
	}
	clock.Advance(cb.cfg.OpenMin + time.Millisecond)
	if cb.State() != BreakerHalfOpen {
		t.Fatal("setup")
	}

	for i := 0; i < cb.cfg.HalfOpenProbes; i++ {
		cb.Record(OutcomeSuccess)
	}
	if got := cb.State(); got != BreakerClosed {
		t.Errorf("after %d half-open successes: state = %v, want Closed", cb.cfg.HalfOpenProbes, got)
	}
}

func TestCB_HalfOpenAnyFailureReOpens(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Unix(1000, 0))
	cb := newTestBreaker(clock.Now)

	for i := 0; i < 25; i++ {
		cb.Record(OutcomeFailure)
	}
	clock.Advance(cb.cfg.OpenMin + time.Millisecond)
	if cb.State() != BreakerHalfOpen {
		t.Fatal("setup")
	}

	cb.Record(OutcomeSuccess) // first probe ok
	cb.Record(OutcomeFailure) // second probe fails — re-open immediately
	if got := cb.State(); got != BreakerOpen {
		t.Errorf("after half-open failure: state = %v, want Open", got)
	}
}

func TestCB_OpenDurationDoublesOnReopen(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Unix(1000, 0))
	cb := newTestBreaker(clock.Now)
	cfg := cb.cfg

	// First trip.
	for i := 0; i < 25; i++ {
		cb.Record(OutcomeFailure)
	}
	if got := cb.RemainingOpen(); got != cfg.OpenMin {
		t.Fatalf("first open: remaining = %v, want %v", got, cfg.OpenMin)
	}

	// Half-open, then fail → re-open with doubled duration.
	clock.Advance(cfg.OpenMin + time.Millisecond)
	cb.Record(OutcomeFailure)
	if cb.State() != BreakerOpen {
		t.Fatal("setup: should be re-opened")
	}
	if got := cb.RemainingOpen(); got != 2*cfg.OpenMin {
		t.Errorf("second open: remaining = %v, want %v (doubled)", got, 2*cfg.OpenMin)
	}
}

func TestCB_OpenDurationCappedAtMax(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Unix(1000, 0))
	cb := newTestBreaker(clock.Now)
	cfg := cb.cfg

	duration := cfg.OpenMin
	for duration < cfg.OpenMax {
		for i := 0; i < 25; i++ {
			cb.Record(OutcomeFailure)
		}
		clock.Advance(duration + time.Millisecond)
		cb.Record(OutcomeFailure) // half-open fails → re-open
		duration *= 2
	}

	// Now do another cycle; the duration should cap at OpenMax.
	for i := 0; i < 25; i++ {
		cb.Record(OutcomeFailure)
	}
	clock.Advance(cfg.OpenMax + time.Millisecond)
	cb.Record(OutcomeFailure)

	if got := cb.RemainingOpen(); got > cfg.OpenMax {
		t.Errorf("remaining = %v exceeds OpenMax = %v", got, cfg.OpenMax)
	}
}

// Cancellations are not failures and additionally don't count
// toward MinObservations — they should be completely invisible to the
// state machine.
func TestCB_CancellationIsIgnored(t *testing.T) {
	t.Parallel()
	cb := newTestBreaker(nil)

	for i := 0; i < 1000; i++ {
		cb.Record(OutcomeCancellation)
	}
	if got := cb.State(); got != BreakerClosed {
		t.Errorf("after 1000 cancellations: state = %v, want Closed", got)
	}

	// 19 fail + 1 succeed = 20 real observations, ratio 0.95 > 0.5 → trip.
	for i := 0; i < 19; i++ {
		cb.Record(OutcomeFailure)
	}
	cb.Record(OutcomeSuccess)
	if got := cb.State(); got != BreakerOpen {
		t.Errorf("after real observations crossing thresholds: state = %v, want Open", got)
	}
}

func TestCB_RemainingOpenIsZeroWhenNotOpen(t *testing.T) {
	t.Parallel()
	cb := newTestBreaker(nil)
	if got := cb.RemainingOpen(); got != 0 {
		t.Errorf("RemainingOpen on Closed = %v, want 0", got)
	}
}

func TestCB_ReserveInClosedAlwaysAdmits(t *testing.T) {
	t.Parallel()
	cb := newTestBreaker(nil)
	for i := 0; i < 100; i++ {
		if !cb.Reserve() {
			t.Errorf("Reserve in Closed refused (attempt %d)", i)
		}
	}
}

func TestCB_ReserveInOpenRefuses(t *testing.T) {
	t.Parallel()
	cb := newTestBreaker(nil)
	for i := 0; i < 25; i++ {
		cb.Record(OutcomeFailure)
	}
	if cb.State() != BreakerOpen {
		t.Fatal("setup")
	}
	if cb.Reserve() {
		t.Error("Reserve in Open admitted; expected refuse")
	}
}

func TestCB_ReserveInHalfOpenLimitsProbes(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Unix(1000, 0))
	cb := newTestBreaker(clock.Now)
	cfg := cb.cfg

	for i := 0; i < 25; i++ {
		cb.Record(OutcomeFailure)
	}
	clock.Advance(cfg.OpenMin + time.Millisecond)
	if cb.State() != BreakerHalfOpen {
		t.Fatal("setup")
	}

	admitted := 0
	for i := 0; i < cfg.HalfOpenProbes*3; i++ {
		if cb.Reserve() {
			admitted++
		}
	}
	if admitted != cfg.HalfOpenProbes {
		t.Errorf("admitted %d / 3×%d attempts; want exactly HalfOpenProbes = %d",
			admitted, cfg.HalfOpenProbes, cfg.HalfOpenProbes)
	}
}

func TestCB_ConcurrentRecordIsRaceClean(t *testing.T) {
	t.Parallel()
	cb := newTestBreaker(nil)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				switch (id + i) % 3 {
				case 0:
					cb.Record(OutcomeFailure)
				case 1:
					cb.Record(OutcomeSuccess)
				case 2:
					cb.Record(OutcomeCancellation)
				}
				_ = cb.State()
				_ = cb.Reserve()
			}
		}(w)
	}
	wg.Wait()
}

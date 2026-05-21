package lb

import (
	"sync"
	"time"
)

// BreakerState enumerates the three states of the circuit breaker.
type BreakerState int8

const (
	BreakerClosed BreakerState = iota
	BreakerOpen
	BreakerHalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case BreakerClosed:
		return "closed"
	case BreakerOpen:
		return "open"
	case BreakerHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// BreakerOutcome classifies the result of an upstream attempt for breaker
// accounting. Cancellations are explicitly distinct from failures — a request
// canceled because a hedged competitor finished first is not the upstream's
// fault and must not contribute to tripping the breaker.
type BreakerOutcome int8

const (
	OutcomeSuccess BreakerOutcome = iota
	OutcomeFailure
	OutcomeCancellation
)

// CircuitBreakerConfig parameterizes the trip / reset behavior. Zero values are
// not valid; callers should start from DefaultCircuitBreakerConfig and modify.
type CircuitBreakerConfig struct {
	// FailureRatio is the threshold above which the breaker trips closed→open.
	// In (0, 1].
	FailureRatio float64

	// MinObservations is the floor on the number of recent samples required
	// before the breaker can trip. Prevents a single failure from tripping a
	// low-traffic upstream (1/1 = 100%).
	MinObservations int

	// WindowDuration bounds the sliding window in wall time.
	WindowDuration time.Duration

	// WindowSize bounds the sliding window in request count. The window is the
	// intersection: most-recent WindowSize samples AND samples within
	// WindowDuration of now.
	WindowSize int

	// OpenMin is the duration the breaker stays Open after the first trip.
	OpenMin time.Duration

	// OpenMax caps the doubled Open duration after repeated trip cycles.
	OpenMax time.Duration

	// HalfOpenProbes is the maximum number of selections admitted during the
	// HalfOpen phase. If all succeed, the breaker closes; if any fails, the
	// breaker re-opens with doubled OpenDuration.
	HalfOpenProbes int
}

// DefaultCircuitBreakerConfig returns the v1 defaults documented in mid.md.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureRatio:    0.5,
		MinObservations: 20,
		WindowDuration:  30 * time.Second,
		WindowSize:      100,
		OpenMin:         time.Second,
		OpenMax:         60 * time.Second,
		HalfOpenProbes:  5,
	}
}

// CircuitBreaker is a per-(pool, upstream) state machine. It is consulted by
// the selector's eligibility filter and updated by the upstream-proxy with the
// outcome of each upstream attempt.
//
// The breaker is goroutine-safe; methods serialize on an internal mutex. The
// hot path on State() is one mutex acquisition and a small switch, which is
// acceptable since State() is called O(eligible upstreams) per pick — at most
// a handful per request.
type CircuitBreaker struct {
	cfg CircuitBreakerConfig
	now func() time.Time // injectable for deterministic tests

	mu              sync.Mutex
	state           BreakerState
	openedAt        time.Time
	openDuration    time.Duration
	probesAdmitted  int             // incremented by Reserve in HalfOpen
	probesSucceeded int             // incremented by Record(success) in HalfOpen
	samples         []breakerSample // sliding window in Closed
}

type breakerSample struct {
	failure bool
	at      time.Time
}

// NewCircuitBreaker constructs a breaker in the Closed state.
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		cfg: cfg,
		now: time.Now,
	}
}

// State returns the current state, advancing Open → HalfOpen when the open
// duration has elapsed.
func (cb *CircuitBreaker) State() BreakerState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.advanceLocked()
	return cb.state
}

// advanceLocked promotes Open → HalfOpen when the open duration has elapsed.
// Must be called with cb.mu held.
func (cb *CircuitBreaker) advanceLocked() {
	if cb.state != BreakerOpen {
		return
	}
	if cb.now().Sub(cb.openedAt) >= cb.openDuration {
		cb.state = BreakerHalfOpen
		cb.probesAdmitted = 0
		cb.probesSucceeded = 0
	}
}

// Record reports the outcome of an upstream attempt. Cancellations are ignored
// for state-transition purposes — they don't count toward the failure ratio
// nor toward MinObservations.
func (cb *CircuitBreaker) Record(outcome BreakerOutcome) {
	if outcome == OutcomeCancellation {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.advanceLocked()

	now := cb.now()
	isFailure := outcome == OutcomeFailure

	switch cb.state {
	case BreakerClosed:
		cb.samples = append(cb.samples, breakerSample{failure: isFailure, at: now})
		cb.trimWindowLocked(now)
		if cb.shouldTripLocked() {
			cb.openDuration = cb.cfg.OpenMin
			cb.openedAt = now
			cb.state = BreakerOpen
			cb.samples = cb.samples[:0]
		}

	case BreakerHalfOpen:
		if isFailure {
			// Re-open with doubled duration, capped at OpenMax.
			newDur := cb.openDuration * 2
			if newDur > cb.cfg.OpenMax {
				newDur = cb.cfg.OpenMax
			}
			cb.openDuration = newDur
			cb.openedAt = now
			cb.state = BreakerOpen
			cb.probesAdmitted = 0
			cb.probesSucceeded = 0
			return
		}
		cb.probesSucceeded++
		if cb.probesSucceeded >= cb.cfg.HalfOpenProbes {
			cb.state = BreakerClosed
			cb.openDuration = cb.cfg.OpenMin
			cb.probesAdmitted = 0
			cb.probesSucceeded = 0
			cb.samples = cb.samples[:0]
		}

	case BreakerOpen:
		// Stray Record while Open — Reserve should have refused upstream.
		// Tolerate but do nothing.
	}
}

// trimWindowLocked drops samples older than the wall-clock window and caps the
// total sample count. Must be called with cb.mu held.
func (cb *CircuitBreaker) trimWindowLocked(now time.Time) {
	cutoff := now.Add(-cb.cfg.WindowDuration)
	i := 0
	for i < len(cb.samples) && cb.samples[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		cb.samples = cb.samples[i:]
	}
	if len(cb.samples) > cb.cfg.WindowSize {
		cb.samples = cb.samples[len(cb.samples)-cb.cfg.WindowSize:]
	}
}

func (cb *CircuitBreaker) shouldTripLocked() bool {
	if len(cb.samples) < cb.cfg.MinObservations {
		return false
	}
	failures := 0
	for _, s := range cb.samples {
		if s.failure {
			failures++
		}
	}
	ratio := float64(failures) / float64(len(cb.samples))
	return ratio > cb.cfg.FailureRatio
}

// Reserve attempts to admit a selection. Returns false when the breaker is
// Open (and not yet eligible to transition to HalfOpen) or when HalfOpen probe
// slots are exhausted.
func (cb *CircuitBreaker) Reserve() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.advanceLocked()

	switch cb.state {
	case BreakerClosed:
		return true
	case BreakerOpen:
		return false
	case BreakerHalfOpen:
		if cb.probesAdmitted < cb.cfg.HalfOpenProbes {
			cb.probesAdmitted++
			return true
		}
		return false
	}
	return false
}

// RemainingOpen returns the time until the breaker exits Open, or 0 if the
// breaker is not Open.
func (cb *CircuitBreaker) RemainingOpen() time.Duration {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state != BreakerOpen {
		return 0
	}
	remaining := cb.openDuration - cb.now().Sub(cb.openedAt)
	if remaining < 0 {
		return 0
	}
	return remaining
}

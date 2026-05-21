// Package lb implements load-balancing algorithms over a pool of HTTP upstreams.
//
// The package defines the Upstream value (the unit of selection), the Selector
// interface that every algorithm satisfies, and the five algorithms that ship
// in v1: round-robin, weighted round-robin, least-connections, power-of-two-
// choices with EWMA latency scoring, and consistent hashing with bounded loads.
package lb

import (
	"math"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// Upstream is a single backend server within a pool.
//
// Ownership: each mutable field has exactly one writer, documented below.
// Readers may load from any goroutine; consistency is per-field (the struct
// as a whole is not atomic — callers that need a coherent multi-field snapshot
// should reach for the algorithm-specific methods that read fields once).
//
//   - Eligible: written by the pool's health-monitor goroutine.
//   - InFlight: incremented by the upstream client at dispatch and decremented
//     at completion (including cancellation).
//   - LatencyEWMA, LastUpdate: written by the upstream client on each outcome
//     via compare-and-swap from a single observation goroutine per call.
//   - Breaker: a state machine driven by outcome reports.
type Upstream struct {
	URL    *url.URL
	Weight int

	Eligible    atomic.Bool
	InFlight    atomic.Int64
	LatencyEWMA atomic.Uint64 // observed latency EWMA, encoded as nanoseconds
	LastUpdate  atomic.Int64  // wall-clock nanos of the last LatencyEWMA write
	Breaker     *CircuitBreaker

	// ewmaMu serializes write paths that span LatencyEWMA + LastUpdate. Reads
	// remain lock-free via atomic loads — readers tolerate seeing a freshly
	// updated EWMA next to a slightly older LastUpdate, since the only consumer
	// of LastUpdate is the writer itself.
	ewmaMu sync.Mutex
}

// RecordLatency updates LatencyEWMA using a time-weighted alpha:
//
//	α = 1 - exp(-Δt / halfLife)
//
// where Δt is the wall-clock interval since the previous update. This makes
// the EWMA decay in real time rather than in sample count — a slow upstream's
// past latency loses weight on the same time scale regardless of how many
// requests have arrived in the meantime.
//
// The first observation seeds the EWMA directly (no prior value to blend).
func (u *Upstream) RecordLatency(now time.Time, latency, halfLife time.Duration) {
	u.ewmaMu.Lock()
	defer u.ewmaMu.Unlock()

	obs := uint64(latency.Nanoseconds())
	last := u.LastUpdate.Load()
	if last == 0 {
		u.LatencyEWMA.Store(obs)
		u.LastUpdate.Store(now.UnixNano())
		return
	}

	dt := now.UnixNano() - last
	if dt < 0 {
		dt = 0
	}
	alpha := 1 - math.Exp(-float64(dt)/float64(halfLife))
	old := u.LatencyEWMA.Load()
	newEWMA := uint64(alpha*float64(obs) + (1-alpha)*float64(old))
	u.LatencyEWMA.Store(newEWMA)
	u.LastUpdate.Store(now.UnixNano())
}

// EligibleForSelection is the fast filter used at the top of every Selector.Pick.
// It returns true when the upstream is currently healthy and its circuit breaker
// is not Open. HalfOpen upstreams are returned as eligible — the algorithm picks
// them like any other, and the call site reserves a probe slot via the breaker
// before committing the pick.
func (u *Upstream) EligibleForSelection() bool {
	return u.Eligible.Load() && u.Breaker.State() != BreakerOpen
}

// LatencyNanos returns the current EWMA latency observation in nanoseconds.
// Used by P2C scoring and by per-route hedge thresholds when expressed as a
// percentile of recent observed latency.
func (u *Upstream) LatencyNanos() uint64 {
	return u.LatencyEWMA.Load()
}

package lb

import (
	"context"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"
)

// P2C implements the power-of-two-choices selector with EWMA latency scoring.
//
// On each Pick, two distinct eligible upstreams are sampled uniformly at random
// and the one with the lower load score is returned. The load score is:
//
//	score(u) = LatencyEWMA(u) × (1 + InFlight(u))
//
// This formulation penalizes both slow upstreams (high LatencyEWMA) and
// overloaded upstreams (high InFlight); either factor alone is insufficient.
//
// EWMA updates use a time-weighted alpha:
//
//	α = 1 - exp(-Δt / HalfLife)
//
// where Δt is the wall-clock interval since the upstream's previous update.
// This handles irregular update intervals correctly — a fixed-α update would
// decay by sample count rather than wall time, biasing toward high-traffic
// upstreams.
type P2C struct {
	pool     []*Upstream
	halfLife time.Duration

	// now is injectable for tests that need deterministic time-weighted EWMA
	// behavior. Production constructions use time.Now.
	now func() time.Time

	mu  sync.Mutex
	rng *rand.Rand
}

// NewP2C constructs a P2C selector over the given pool. HalfLife controls the
// EWMA half-life — the time over which an upstream's observed latency
// contributes 50% of its score. A reasonable default is 5 seconds.
func NewP2C(pool []*Upstream, halfLife time.Duration) *P2C {
	seed := uint64(time.Now().UnixNano())
	return &P2C{
		pool:     pool,
		halfLife: halfLife,
		now:      time.Now,
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}
}

// Pick selects one upstream from the pool according to the P2C algorithm.
func (p *P2C) Pick(ctx context.Context, req *http.Request, hint PickHint) (*Upstream, error) {
	eligible, release := eligibleSet(p.pool, hint)
	defer release()

	switch len(eligible) {
	case 0:
		return nil, ErrNoEligibleUpstream
	case 1:
		return eligible[0], nil
	}

	// Sample two distinct indices uniformly at random.
	p.mu.Lock()
	i := p.rng.IntN(len(eligible))
	j := p.rng.IntN(len(eligible) - 1)
	p.mu.Unlock()
	if j >= i {
		j++ // shift to make distinct from i
	}

	a, b := eligible[i], eligible[j]
	if score(a) <= score(b) {
		return a, nil
	}
	return b, nil
}

// RecordOutcome updates an upstream's LatencyEWMA from an observed latency.
// Called by the upstream client after each completed request (success or error).
func (p *P2C) RecordOutcome(u *Upstream, latency time.Duration) {
	u.RecordLatency(p.now(), latency, p.halfLife)
}

// score is the load-score function. Exported via this package-local helper so
// tests can verify the formula without inspecting Pick's internals.
func score(u *Upstream) uint64 {
	return u.LatencyNanos() * uint64(1+u.InFlight.Load())
}

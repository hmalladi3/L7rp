package lb

import (
	"context"
	"net/http"
	"sync/atomic"
)

// RoundRobin distributes picks across the eligible set by incrementing a single
// counter modulo the eligible-set size.
//
// The simplest selector and the one to choose when determinism matters more
// than reactivity to upstream load. Under sustained heterogeneity (one slow
// upstream), every upstream still gets its 1/N share — the slow one degrades
// proportionally. Use P2C if that's a problem.
type RoundRobin struct {
	pool    []*Upstream
	counter atomic.Uint64
}

// NewRoundRobin constructs a round-robin selector over the given pool.
func NewRoundRobin(pool []*Upstream) *RoundRobin {
	return &RoundRobin{pool: pool}
}

// Pick implements Selector.
func (r *RoundRobin) Pick(ctx context.Context, req *http.Request, hint PickHint) (*Upstream, error) {
	eligible, release := eligibleSet(r.pool, hint)
	defer release()

	switch len(eligible) {
	case 0:
		return nil, ErrNoEligibleUpstream
	case 1:
		return eligible[0], nil
	}

	n := uint64(len(eligible))
	idx := r.counter.Add(1) % n
	return eligible[idx], nil
}

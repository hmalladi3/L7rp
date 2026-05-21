package lb

import (
	"context"
	"net/http"
	"sync"
)

// WeightedRoundRobin distributes picks proportionally to per-upstream Weight
// using the smooth weighted round-robin algorithm from nginx.
//
// Naive expansion-by-weight (place each upstream weight times into a flat
// rotation) produces bursts of consecutive picks on heavy upstreams. The
// smooth variant interleaves: weights 5:1:1 yield the pattern A A B A C A A
// rather than A A A A A B C. This is the distribution operators actually
// expect when they reach for weighted RR.
//
// The algorithm maintains a per-upstream current_weight that grows by
// effective_weight each round; the upstream with the highest current_weight
// is selected and has the sum of effective weights subtracted from its
// current_weight. This is the same scheme published in nginx's blog and used
// by HAProxy.
type WeightedRoundRobin struct {
	pool []*Upstream

	mu sync.Mutex
	// cw is the smooth-WRR current_weight per upstream, indexed parallel to pool.
	cw []int64
}

// NewWeightedRoundRobin constructs a WRR selector over the given pool. The
// selector reads Upstream.Weight; weights ≤ 0 mean the upstream is skipped.
func NewWeightedRoundRobin(pool []*Upstream) *WeightedRoundRobin {
	return &WeightedRoundRobin{
		pool: pool,
		cw:   make([]int64, len(pool)),
	}
}

// Pick implements Selector.
func (w *WeightedRoundRobin) Pick(ctx context.Context, req *http.Request, hint PickHint) (*Upstream, error) {
	eligible, release := eligibleSet(w.pool, hint)
	defer release()

	switch len(eligible) {
	case 0:
		return nil, ErrNoEligibleUpstream
	case 1:
		// Skip the smooth-WRR update if there's only one candidate.
		return eligible[0], nil
	}

	// Smooth-WRR requires the per-upstream current_weight slot; map each
	// eligible upstream to its index in w.pool for lookup. With small pools
	// (≤ a few dozen) the inner loop is faster than maintaining a map.
	w.mu.Lock()
	defer w.mu.Unlock()

	var (
		sumWeight  int64
		bestUp     *Upstream
		bestPoolIx int
	)
	for _, u := range eligible {
		if u.Weight <= 0 {
			continue
		}
		idx := w.indexOf(u)
		if idx < 0 {
			continue
		}
		sumWeight += int64(u.Weight)
		w.cw[idx] += int64(u.Weight)
		if bestUp == nil || w.cw[idx] > w.cw[bestPoolIx] {
			bestUp = u
			bestPoolIx = idx
		}
	}
	if bestUp == nil {
		// No upstream had positive weight.
		return nil, ErrNoEligibleUpstream
	}
	w.cw[bestPoolIx] -= sumWeight
	return bestUp, nil
}

// indexOf returns the position of u in w.pool, or -1 if not found.
func (w *WeightedRoundRobin) indexOf(u *Upstream) int {
	for i, p := range w.pool {
		if p == u {
			return i
		}
	}
	return -1
}

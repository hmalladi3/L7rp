package lb

import (
	"context"
	"errors"
	"net/http"
	"sync"
)

// ErrNoEligibleUpstream is returned by Selector.Pick when no candidate in the
// pool satisfies the eligibility predicate (after applying the PickHint).
//
// Callers translate this to a 503 Service Unavailable response with a
// Retry-After header derived from circuit-breaker state.
var ErrNoEligibleUpstream = errors.New("lb: no eligible upstream")

// Selector picks one Upstream from a pool given the request context. Each of
// the five algorithms (RoundRobin, WeightedRoundRobin, LeastConnections, P2C,
// ConsistentHashBounded) implements this interface.
//
// Implementations are goroutine-safe. The Pick method is called on the request
// hot path and is expected to be allocation-light (the pool slice is read,
// not copied; intermediate eligibility slices come from a sync.Pool inside the
// package).
type Selector interface {
	Pick(ctx context.Context, req *http.Request, hint PickHint) (*Upstream, error)
}

// PickHint carries per-request advice from middleware to the selector. The
// retry/hedge middleware populates Exclude with upstreams that have already
// been tried (or are currently in flight as the original of a hedged pair).
type PickHint struct {
	// Exclude lists upstreams to skip during selection. Nil and empty slices
	// are equivalent.
	Exclude []*Upstream
}

// eligibleSet computes the eligible candidates for selection: the subset of
// pool that satisfies EligibleForSelection and is not in hint.Exclude.
//
// The returned slice is borrowed from a package-internal sync.Pool; callers
// must invoke release() when done (typically via defer). The pool keeps the
// hot path allocation-free at steady state.
func eligibleSet(pool []*Upstream, hint PickHint) (eligible []*Upstream, release func()) {
	sp := eligibleSlicePool.Get().(*[]*Upstream)
	*sp = (*sp)[:0]
	for _, u := range pool {
		if !u.EligibleForSelection() {
			continue
		}
		if contains(hint.Exclude, u) {
			continue
		}
		*sp = append(*sp, u)
	}
	return *sp, func() {
		*sp = (*sp)[:0]
		eligibleSlicePool.Put(sp)
	}
}

var eligibleSlicePool = sync.Pool{
	New: func() any {
		s := make([]*Upstream, 0, 32)
		return &s
	},
}

// contains reports whether u is in the (typically small) exclude list. Linear
// scan is correct here — Exclude is bounded by the retry middleware's
// max_attempts.
func contains(list []*Upstream, u *Upstream) bool {
	for _, x := range list {
		if x == u {
			return true
		}
	}
	return false
}

package lb

import (
	"context"
	"math"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"
)

// LeastConnections picks the eligible upstream with the lowest in-flight
// request count. Ties are broken by uniform random choice — the documented
// failure mode of the algorithm is a concurrent stampede onto the
// "currently-least-loaded" upstream when many picks observe the same minimum.
// Random tie-breaking softens this; for bursty workloads, prefer P2C.
type LeastConnections struct {
	pool []*Upstream

	mu  sync.Mutex
	rng *rand.Rand
}

// NewLeastConnections constructs an LC selector over the given pool.
func NewLeastConnections(pool []*Upstream) *LeastConnections {
	seed := uint64(time.Now().UnixNano())
	return &LeastConnections{
		pool: pool,
		rng:  rand.New(rand.NewPCG(seed, seed^0xdeadbeef)),
	}
}

// Pick implements Selector.
func (l *LeastConnections) Pick(ctx context.Context, req *http.Request, hint PickHint) (*Upstream, error) {
	eligible, release := eligibleSet(l.pool, hint)
	defer release()

	switch len(eligible) {
	case 0:
		return nil, ErrNoEligibleUpstream
	case 1:
		return eligible[0], nil
	}

	// One pass for the minimum, one pass for ties, then random tie-break.
	minInflight := int64(math.MaxInt64)
	for _, u := range eligible {
		if v := u.InFlight.Load(); v < minInflight {
			minInflight = v
		}
	}
	tieCount := 0
	for _, u := range eligible {
		if u.InFlight.Load() == minInflight {
			tieCount++
		}
	}
	if tieCount == 1 {
		for _, u := range eligible {
			if u.InFlight.Load() == minInflight {
				return u, nil
			}
		}
	}

	l.mu.Lock()
	pick := l.rng.IntN(tieCount)
	l.mu.Unlock()

	seen := 0
	for _, u := range eligible {
		if u.InFlight.Load() != minInflight {
			continue
		}
		if seen == pick {
			return u, nil
		}
		seen++
	}
	// Unreachable in normal execution.
	return eligible[0], nil
}

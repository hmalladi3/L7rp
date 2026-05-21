package lb

import (
	"context"
	"math"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestP2C_EligibilityFilter(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	pool[1].Eligible.Store(false)

	p := NewP2C(pool, 5*time.Second)
	req := httptest.NewRequest("GET", "/", nil)

	picks := make(map[*Upstream]int)
	for i := 0; i < 2000; i++ {
		u, err := p.Pick(context.Background(), req, PickHint{})
		if err != nil {
			t.Fatalf("Pick: %v", err)
		}
		picks[u]++
	}

	if picks[pool[1]] != 0 {
		t.Errorf("ineligible upstream picked %d times; want 0", picks[pool[1]])
	}
	if picks[pool[0]]+picks[pool[2]] != 2000 {
		t.Errorf("picks not fully absorbed by eligible upstreams: %d", picks[pool[0]]+picks[pool[2]])
	}
}

func TestP2C_NoEligibleReturnsTypedError(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	for _, u := range pool {
		u.Eligible.Store(false)
	}

	p := NewP2C(pool, time.Second)
	req := httptest.NewRequest("GET", "/", nil)

	got, err := p.Pick(context.Background(), req, PickHint{})
	if err != ErrNoEligibleUpstream {
		t.Errorf("err = %v, want ErrNoEligibleUpstream", err)
	}
	if got != nil {
		t.Errorf("upstream = %v, want nil", got)
	}
}

func TestP2C_SingleEligibleSkipsAlgorithm(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 4)
	for i := 1; i < len(pool); i++ {
		pool[i].Eligible.Store(false)
	}

	p := NewP2C(pool, time.Second)
	req := httptest.NewRequest("GET", "/", nil)

	for i := 0; i < 100; i++ {
		got, err := p.Pick(context.Background(), req, PickHint{})
		if err != nil {
			t.Fatalf("Pick: %v", err)
		}
		if got != pool[0] {
			t.Fatalf("single-eligible: got %v, want pool[0]", got.URL)
		}
	}
}

func TestP2C_ExcludeHint(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 2)
	p := NewP2C(pool, time.Second)
	req := httptest.NewRequest("GET", "/", nil)

	for i := 0; i < 200; i++ {
		got, err := p.Pick(context.Background(), req, PickHint{Exclude: []*Upstream{pool[0]}})
		if err != nil {
			t.Fatalf("Pick: %v", err)
		}
		if got != pool[1] {
			t.Fatalf("excluding pool[0]: got %v, want pool[1]", got.URL)
		}
	}
}

func TestP2C_ScoreFormula(t *testing.T) {
	t.Parallel()

	u := testUpstream(t, "http://u")
	cases := []struct {
		ewma     uint64
		inflight int64
		want     uint64
	}{
		{0, 0, 0},
		{100, 0, 100},
		{100, 1, 200},
		{100, 3, 400},
		{1_000_000, 10, 11_000_000},
	}
	for _, c := range cases {
		u.LatencyEWMA.Store(c.ewma)
		u.InFlight.Store(c.inflight)
		if got := score(u); got != c.want {
			t.Errorf("score(ewma=%d, inflight=%d) = %d, want %d", c.ewma, c.inflight, got, c.want)
		}
	}
}

// When every upstream has identical scores, P2C should distribute picks
// roughly uniformly. With two random samples, the probability of any
// upstream being picked converges to 1/N for large N (the algorithm is
// uniform when scores are equal).
func TestP2C_DistributionUniformWhenEqual(t *testing.T) {
	t.Parallel()

	const (
		n         = 4
		picks     = 40_000
		tolerance = 0.08 // 8% allowance for sampling variance
	)

	pool := testPool(t, n)
	p := NewP2C(pool, time.Second)
	req := httptest.NewRequest("GET", "/", nil)

	counts := make(map[*Upstream]int)
	for i := 0; i < picks; i++ {
		u, err := p.Pick(context.Background(), req, PickHint{})
		if err != nil {
			t.Fatal(err)
		}
		counts[u]++
	}

	expected := picks / n
	for _, u := range pool {
		c := counts[u]
		diff := math.Abs(float64(c-expected)) / float64(expected)
		if diff > tolerance {
			t.Errorf("upstream %s: %d picks (expected ~%d, off by %.1f%%; tolerance %.1f%%)",
				u.URL, c, expected, diff*100, tolerance*100)
		}
	}
}

// P2C with two upstreams always samples both (sampling without replacement);
// the lower-score upstream wins every time. This is a hard guarantee, not a
// distributional one.
func TestP2C_TwoUpstreamsAlwaysPicksLowerScore(t *testing.T) {
	t.Parallel()

	fast := testUpstream(t, "http://fast")
	slow := testUpstream(t, "http://slow")
	fast.LatencyEWMA.Store(uint64(time.Millisecond))
	slow.LatencyEWMA.Store(uint64(100 * time.Millisecond))

	p := NewP2C([]*Upstream{fast, slow}, time.Second)
	req := httptest.NewRequest("GET", "/", nil)

	const picks = 5000
	fastCount := 0
	for i := 0; i < picks; i++ {
		u, err := p.Pick(context.Background(), req, PickHint{})
		if err != nil {
			t.Fatal(err)
		}
		if u == fast {
			fastCount++
		}
	}
	if fastCount != picks {
		t.Errorf("fast upstream picked %d / %d (expected all — two-upstream P2C is deterministic)", fastCount, picks)
	}
}

// With three upstreams, the slow one should still get *some* picks (when both
// random samples land on the same two faster ones, then the slow one isn't
// sampled at all). But it should be a clear minority.
func TestP2C_ThreeUpstreamsSkewsAgainstSlow(t *testing.T) {
	t.Parallel()

	fast := testUpstream(t, "http://fast")
	mid := testUpstream(t, "http://mid")
	slow := testUpstream(t, "http://slow")
	fast.LatencyEWMA.Store(uint64(time.Millisecond))
	mid.LatencyEWMA.Store(uint64(10 * time.Millisecond))
	slow.LatencyEWMA.Store(uint64(100 * time.Millisecond))

	p := NewP2C([]*Upstream{fast, mid, slow}, time.Second)
	req := httptest.NewRequest("GET", "/", nil)

	const picks = 30_000
	counts := map[*Upstream]int{}
	for i := 0; i < picks; i++ {
		u, _ := p.Pick(context.Background(), req, PickHint{})
		counts[u]++
	}

	// Slow should be picked roughly when both random samples avoid it AND mid > slow,
	// or one sample includes slow AND the other sample > slow. The former is
	// impossible (mid < slow always); the latter requires the non-slow sample to be
	// even slower, which can't happen. So slow gets picked only when both samples
	// land on slow — which can't happen (distinct samples). Slow gets 0 picks.
	if counts[slow] != 0 {
		t.Errorf("slow upstream picked %d times; expected 0 (sampling-without-replacement guarantees slow never wins)", counts[slow])
	}
	if counts[fast] < counts[mid] {
		t.Errorf("fast (%d) < mid (%d); expected fast to dominate", counts[fast], counts[mid])
	}
}

// EWMA decay is time-weighted: alpha is a function of the wall-clock interval
// since the previous update, so two updates spaced apart by Δt pull the EWMA
// further toward the new value than two updates at the same instant.
//
// We drive a mock clock through P2C.now to exercise the formula
// deterministically; without an injectable clock this test would rely on
// time.Sleep, which is flaky in CI.
func TestP2C_EWMAIsTimeWeighted(t *testing.T) {
	t.Parallel()

	clock := newMockClock(time.Unix(1_700_000_000, 0)) // recent epoch
	u := testUpstream(t, "http://u")
	p := NewP2C([]*Upstream{u}, 100*time.Millisecond)
	p.now = clock.Now

	// First observation seeds the EWMA at 10ms.
	p.RecordOutcome(u, 10*time.Millisecond)
	first := u.LatencyEWMA.Load()
	if first != uint64(10*time.Millisecond) {
		t.Fatalf("first EWMA = %d, want %d (seed)", first, uint64(10*time.Millisecond))
	}

	// Same instant: Δt = 0, α = 0, the second observation should be ignored.
	p.RecordOutcome(u, 1*time.Second)
	atZero := u.LatencyEWMA.Load()
	if atZero != first {
		t.Errorf("Δt=0 update: EWMA = %d, want %d (α should be 0)", atZero, first)
	}

	// Advance well past half-life. α → 1; EWMA pulled close to the new obs.
	clock.Advance(500 * time.Millisecond)
	p.RecordOutcome(u, 1*time.Second)
	afterAdvance := u.LatencyEWMA.Load()
	if afterAdvance <= atZero {
		t.Errorf("after advancing 500ms (>>half-life=100ms): EWMA = %d, want > %d", afterAdvance, atZero)
	}
	// Sanity: should be much closer to 1s than to 10ms.
	if afterAdvance < uint64(800*time.Millisecond) {
		t.Errorf("after advancing 5×half-life: EWMA = %d ns, expected close to %d ns", afterAdvance, uint64(time.Second))
	}
}

func TestP2C_RaceClean(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 5)
	p := NewP2C(pool, time.Second)
	req := httptest.NewRequest("GET", "/", nil)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				u, err := p.Pick(context.Background(), req, PickHint{})
				if err != nil {
					t.Errorf("Pick: %v", err)
					return
				}
				u.InFlight.Add(1)
				p.RecordOutcome(u, time.Millisecond)
				u.InFlight.Add(-1)
			}
		}()
	}
	wg.Wait()
}

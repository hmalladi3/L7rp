package lb

import (
	"testing"
)

func TestEligibleSet_FiltersIneligible(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 5)
	pool[1].Eligible.Store(false)
	pool[3].Eligible.Store(false)

	got, release := eligibleSet(pool, PickHint{})
	defer release()

	want := []*Upstream{pool[0], pool[2], pool[4]}
	if len(got) != len(want) {
		t.Fatalf("eligible count = %d, want %d", len(got), len(want))
	}
	for i, u := range want {
		if got[i] != u {
			t.Errorf("eligible[%d] = %s, want %s", i, got[i].URL, u.URL)
		}
	}
}

func TestEligibleSet_HonorsExcludeHint(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 4)

	got, release := eligibleSet(pool, PickHint{Exclude: []*Upstream{pool[0], pool[2]}})
	defer release()

	want := []*Upstream{pool[1], pool[3]}
	if len(got) != len(want) {
		t.Fatalf("eligible count = %d, want %d", len(got), len(want))
	}
	for i, u := range want {
		if got[i] != u {
			t.Errorf("eligible[%d] = %s, want %s", i, got[i].URL, u.URL)
		}
	}
}

func TestEligibleSet_FiltersBreakerOpen(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)

	// Trip pool[1]'s breaker.
	for i := 0; i < 25; i++ {
		pool[1].Breaker.Record(OutcomeFailure)
	}
	if pool[1].Breaker.State() != BreakerOpen {
		t.Fatal("setup: pool[1] breaker should be Open")
	}

	got, release := eligibleSet(pool, PickHint{})
	defer release()

	for _, u := range got {
		if u == pool[1] {
			t.Error("breaker-Open upstream included in eligible set")
		}
	}
}

func TestEligibleSet_AllIneligibleReturnsEmpty(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	for _, u := range pool {
		u.Eligible.Store(false)
	}

	got, release := eligibleSet(pool, PickHint{})
	defer release()

	if len(got) != 0 {
		t.Errorf("expected empty eligible set; got %d", len(got))
	}
}

func TestEligibleForSelection_OpenBreakerMakesIneligible(t *testing.T) {
	t.Parallel()

	u := testUpstream(t, "http://u")
	if !u.EligibleForSelection() {
		t.Fatal("setup: should be eligible")
	}

	for i := 0; i < 25; i++ {
		u.Breaker.Record(OutcomeFailure)
	}
	if u.EligibleForSelection() {
		t.Error("breaker Open but EligibleForSelection returned true")
	}
}

func TestEligibleForSelection_FlagOnlyDriven(t *testing.T) {
	t.Parallel()

	u := testUpstream(t, "http://u")
	u.Eligible.Store(false)

	if u.EligibleForSelection() {
		t.Error("Eligible flag false but EligibleForSelection returned true")
	}
}

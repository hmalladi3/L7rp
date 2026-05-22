package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTimeout_PassesThroughWhenInnerFinishesInTime(t *testing.T) {
	t.Parallel()

	h := Timeout(200 * time.Millisecond)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if got := w.Body.String(); got != "ok" {
		t.Errorf("body = %q, want %q", got, "ok")
	}
}

func TestTimeout_PropagatesDeadlineToInnerHandler(t *testing.T) {
	t.Parallel()

	const budget = 80 * time.Millisecond
	dl := make(chan time.Time, 1)
	h := Timeout(budget)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if d, ok := r.Context().Deadline(); ok {
			dl <- d
		} else {
			t.Errorf("no deadline propagated to inner handler")
		}
	}))

	before := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))

	select {
	case d := <-dl:
		// The deadline should be set within `budget` of when the middleware
		// ran. Mathematically: `before + budget <= d <= now() + budget`.
		// Allow generous slack on the upper bound for slow CI schedulers —
		// the goal is to assert the middleware applied a deadline of the
		// configured magnitude, not to time it precisely.
		if d.Before(before) {
			t.Errorf("deadline %s is in the past relative to call start %s", d, before)
		}
		if delta := d.Sub(before); delta > budget+50*time.Millisecond {
			t.Errorf("deadline = call-start + %s; want ~%s", delta, budget)
		}
	default:
		t.Errorf("deadline channel empty — inner handler never ran")
	}
}

func TestTimeout_CancelsInnerHandlerOnExpiry(t *testing.T) {
	t.Parallel()

	gotErr := make(chan error, 1)
	h := Timeout(40 * time.Millisecond)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		gotErr <- r.Context().Err()
	}))

	start := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	elapsed := time.Since(start)

	select {
	case err := <-gotErr:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("ctx.Err() = %v, want context.DeadlineExceeded", err)
		}
	default:
		t.Errorf("inner handler never observed cancellation")
	}
	// Generous upper bound: middleware should return ~40ms after the
	// deadline; allow significant slack for slow CI schedulers under -race.
	if elapsed > time.Second {
		t.Errorf("middleware blocked for %s, want ~40ms", elapsed)
	}
}

func TestTimeout_ZeroDurationIsNoOp(t *testing.T) {
	t.Parallel()

	h := Timeout(0)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.Context().Deadline(); ok {
			t.Errorf("zero-duration timeout added a deadline; want no-op")
		}
		w.WriteHeader(http.StatusTeapot)
	}))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))

	if w.Code != http.StatusTeapot {
		t.Errorf("status = %d, want 418", w.Code)
	}
}

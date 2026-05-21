package middleware

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestPanic_RecoversBeforeHeaderAndReturns500(t *testing.T) {
	t.Parallel()

	terminal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})
	handler := PanicRecover(PanicConfig{})(terminal)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

func TestPanic_PostHeaderClosesQuietly(t *testing.T) {
	t.Parallel()

	terminal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("partial..."))
		panic("after-header boom")
	})
	handler := PanicRecover(PanicConfig{})(terminal)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r) // must not re-panic

	if w.Code != 200 {
		t.Errorf("status = %d; expected 200 (the already-written status); recovery cannot rewrite it", w.Code)
	}
}

func TestPanic_OnRecoverCallbackFires(t *testing.T) {
	t.Parallel()

	var called atomic.Int32
	var loc string

	cfg := PanicConfig{OnRecover: func(location string) {
		called.Add(1)
		loc = location
	}}
	handler := PanicRecover(cfg)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		panic("boom")
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r)

	if called.Load() != 1 {
		t.Errorf("OnRecover called %d times, want 1", called.Load())
	}
	if loc != "middleware-chain" {
		t.Errorf("location = %q, want %q", loc, "middleware-chain")
	}
}

func TestPanic_OnRecoverDistinguishesPostHeader(t *testing.T) {
	t.Parallel()

	var loc string
	cfg := PanicConfig{OnRecover: func(location string) { loc = location }}
	handler := PanicRecover(cfg)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		panic("boom")
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))

	if loc != "middleware-chain-post-header" {
		t.Errorf("location = %q, want middleware-chain-post-header", loc)
	}
}

func TestPanic_NormalFlowUnaffected(t *testing.T) {
	t.Parallel()

	handler := PanicRecover(PanicConfig{})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != 204 {
		t.Errorf("status = %d, want 204", w.Code)
	}
}

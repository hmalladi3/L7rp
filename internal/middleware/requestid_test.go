package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestID_GeneratesWhenAbsent(t *testing.T) {
	t.Parallel()

	var got string
	terminal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = RequestIDFromContext(r.Context())
		w.WriteHeader(200)
	})
	handler := RequestID()(terminal)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r)

	if got == "" {
		t.Fatal("expected a generated request ID on context")
	}
	if w.Header().Get("X-Request-ID") != got {
		t.Errorf("response X-Request-ID=%q, want %q", w.Header().Get("X-Request-ID"), got)
	}
}

func TestRequestID_HonorsInboundHeader(t *testing.T) {
	t.Parallel()

	const incoming = "abc-123-xyz"
	var got string
	terminal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = RequestIDFromContext(r.Context())
	})
	handler := RequestID()(terminal)

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Request-ID", incoming)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if got != incoming {
		t.Errorf("got %q, want inbound value %q", got, incoming)
	}
	if w.Header().Get("X-Request-ID") != incoming {
		t.Errorf("response header = %q, want %q", w.Header().Get("X-Request-ID"), incoming)
	}
}

func TestRequestID_DistinctAcrossRequests(t *testing.T) {
	t.Parallel()

	seen := make(map[string]bool)
	terminal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := RequestIDFromContext(r.Context())
		if seen[id] {
			t.Errorf("duplicate ID generated: %q", id)
		}
		seen[id] = true
	})
	handler := RequestID()(terminal)

	for i := 0; i < 500; i++ {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/", nil)
		handler.ServeHTTP(w, r)
	}
	if len(seen) != 500 {
		t.Errorf("expected 500 distinct IDs, got %d", len(seen))
	}
}

func TestRequestIDFromContext_EmptyWhenAbsent(t *testing.T) {
	t.Parallel()
	if got := RequestIDFromContext(httptest.NewRequest("GET", "/", nil).Context()); got != "" {
		t.Errorf("got %q from bare context, want empty", got)
	}
}

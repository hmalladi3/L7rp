package upstream

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/harimalladi/l7rp/internal/lb"
	"github.com/harimalladi/l7rp/internal/observability"
)

// TestProxy_ForwardsTrailers proves the proxy preserves HTTP trailers across
// the round-trip. Without trailer forwarding every gRPC call would lose its
// grpc-status frame and downstream clients would see "RPC failed: incomplete
// response".
func TestProxy_ForwardsTrailers(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Trailer", "Grpc-Status, Grpc-Message")
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("rpc-body"))
		// Trailer values set via http.TrailerPrefix are flushed as actual
		// HTTP trailers when the body finishes.
		w.Header().Set(http.TrailerPrefix+"Grpc-Status", "0")
		w.Header().Set(http.TrailerPrefix+"Grpc-Message", "OK")
	}))
	defer backend.Close()

	proxyURL := newProxyServer(t, backend.URL)

	resp, err := http.Get(proxyURL)
	if err != nil {
		t.Fatalf("GET proxy: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "rpc-body" {
		t.Errorf("body = %q, want %q", body, "rpc-body")
	}
	if got := resp.Trailer.Get("Grpc-Status"); got != "0" {
		t.Errorf("trailer Grpc-Status = %q, want %q", got, "0")
	}
	if got := resp.Trailer.Get("Grpc-Message"); got != "OK" {
		t.Errorf("trailer Grpc-Message = %q, want %q", got, "OK")
	}
}

// TestProxy_StreamingFlush asserts that chunks emitted by the upstream reach
// the client promptly rather than being buffered until the upstream closes
// the body. We send three chunks with delays and require each to arrive in
// its own window.
func TestProxy_StreamingFlush(t *testing.T) {
	t.Parallel()

	const chunkDelay = 80 * time.Millisecond
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		flusher := w.(http.Flusher)
		for i := 0; i < 3; i++ {
			_, _ = w.Write([]byte("chunk\n"))
			flusher.Flush()
			time.Sleep(chunkDelay)
		}
	}))
	defer backend.Close()

	proxyURL := newProxyServer(t, backend.URL)

	resp, err := http.Get(proxyURL)
	if err != nil {
		t.Fatalf("GET proxy: %v", err)
	}
	defer resp.Body.Close()

	// Read one chunk at a time and confirm we observe at least two distinct
	// arrival times separated by approximately the chunk delay. If the proxy
	// buffered, all three chunks would arrive together at the end.
	var arrivals []time.Time
	buf := make([]byte, 64)
	for len(arrivals) < 3 {
		n, _ := resp.Body.Read(buf)
		if n > 0 {
			arrivals = append(arrivals, time.Now())
		} else {
			break
		}
	}
	if len(arrivals) < 2 {
		t.Fatalf("only %d chunks observed, expected 3", len(arrivals))
	}
	gap := arrivals[1].Sub(arrivals[0])
	if gap < chunkDelay/2 {
		t.Errorf("chunks arrived too close together (%s); proxy is buffering", gap)
	}
}

// newProxyServer stands up a fresh Proxy in front of the given upstream URL
// and returns the proxy's base URL. Caller closes the proxy via t.Cleanup.
func newProxyServer(t *testing.T, upstreamURL string) string {
	t.Helper()

	u, err := url.Parse(upstreamURL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	ups := &lb.Upstream{URL: u, Breaker: lb.NewCircuitBreaker(lb.DefaultCircuitBreakerConfig())}
	ups.Eligible.Store(true)

	sel := lb.NewRoundRobin([]*lb.Upstream{ups})
	metrics := observability.NewMetrics()
	p := NewProxy("test", sel, http.DefaultTransport, metrics)

	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	return srv.URL
}

package upstream

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/harimalladi/l7rp/internal/lb"
)

func TestIsWebSocketUpgrade(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		method string
		hdrs   map[string]string
		want   bool
	}{
		{"happy GET upgrade", "GET", map[string]string{"Upgrade": "websocket", "Connection": "Upgrade"}, true},
		{"connection multi-value", "GET", map[string]string{"Upgrade": "websocket", "Connection": "keep-alive, Upgrade"}, true},
		{"case-insensitive", "GET", map[string]string{"Upgrade": "WebSocket", "Connection": "UPGRADE"}, true},
		{"POST is not ws upgrade", "POST", map[string]string{"Upgrade": "websocket", "Connection": "Upgrade"}, false},
		{"no upgrade header", "GET", map[string]string{"Connection": "Upgrade"}, false},
		{"upgrade not websocket", "GET", map[string]string{"Upgrade": "h2c", "Connection": "Upgrade"}, false},
		{"connection without upgrade token", "GET", map[string]string{"Upgrade": "websocket", "Connection": "keep-alive"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(c.method, "/", nil)
			for k, v := range c.hdrs {
				r.Header.Set(k, v)
			}
			if got := isWebSocketUpgrade(r); got != c.want {
				t.Errorf("got %v, want %v", got, c.want)
			}
		})
	}
}

// rawUpstream is a minimal TCP server that performs a fake WebSocket upgrade:
// reads the GET request, writes a 101 response, then echoes any bytes back.
// We don't need a real RFC 6455 framer for this test — the proxy only cares
// about the upgrade handshake and the byte streaming after.
func rawUpstream(t *testing.T) (addr string, stop func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				// Read request.
				_, err := http.ReadRequest(br)
				if err != nil {
					return
				}
				// Write 101.
				resp := "HTTP/1.1 101 Switching Protocols\r\n" +
					"Upgrade: websocket\r\n" +
					"Connection: Upgrade\r\n\r\n"
				if _, err := c.Write([]byte(resp)); err != nil {
					return
				}
				// Echo.
				_, _ = io.Copy(c, br)
			}(conn)
		}
	}()

	return ln.Addr().String(), func() { _ = ln.Close() }
}

func TestWebSocket_EndToEndEcho(t *testing.T) {
	t.Parallel()

	upstreamAddr, stop := rawUpstream(t)
	defer stop()

	u, _ := url.Parse("http://" + upstreamAddr)
	upstream := &lb.Upstream{
		URL:     u,
		Breaker: lb.NewCircuitBreaker(lb.DefaultCircuitBreakerConfig()),
	}
	upstream.Eligible.Store(true)

	pool := []*lb.Upstream{upstream}
	selector := lb.NewRoundRobin(pool)

	proxy := NewProxy("pool", selector, http.DefaultTransport, nil)

	// Spin up an HTTP server hosting the proxy so we can hit it with a real
	// hijacking client.
	srv := httptest.NewServer(proxy)
	defer srv.Close()

	// Dial the proxy.
	proxyURL, _ := url.Parse(srv.URL)
	conn, err := net.DialTimeout("tcp", proxyURL.Host, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	req := "GET /ws HTTP/1.1\r\n" +
		"Host: " + proxyURL.Host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("write request: %v", err)
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != 101 {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}

	// Now echo: write data, read it back via the proxy chain.
	if _, err := conn.Write([]byte("hello-ws")); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	buf := make([]byte, 8)
	if _, err := io.ReadFull(br, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(buf) != "hello-ws" {
		t.Errorf("got %q, want %q", buf, "hello-ws")
	}
}

func TestWebSocket_NonHijackerReturns500(t *testing.T) {
	t.Parallel()

	upstream := &lb.Upstream{
		URL:     &url.URL{Scheme: "http", Host: "127.0.0.1:1"}, // unreachable
		Breaker: lb.NewCircuitBreaker(lb.DefaultCircuitBreakerConfig()),
	}
	upstream.Eligible.Store(true)
	selector := lb.NewRoundRobin([]*lb.Upstream{upstream})
	proxy := NewProxy("pool", selector, http.DefaultTransport, nil)

	// httptest.ResponseRecorder doesn't implement http.Hijacker.
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Connection", "Upgrade")

	proxy.ServeHTTP(w, r)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 (non-hijacker)", w.Code)
	}
}

// TestWebSocket_UpstreamConnectFailureReturns502 simulates an upstream that
// can't be dialed; the proxy must hand the downstream a 502 response over the
// hijacked connection.
func TestWebSocket_UpstreamConnectFailureReturns502(t *testing.T) {
	t.Parallel()

	// Bind a port then close it, leaving the address unreachable.
	tmpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := tmpLn.Addr().String()
	tmpLn.Close()

	u, _ := url.Parse("http://" + deadAddr)
	upstream := &lb.Upstream{URL: u, Breaker: lb.NewCircuitBreaker(lb.DefaultCircuitBreakerConfig())}
	upstream.Eligible.Store(true)

	selector := lb.NewRoundRobin([]*lb.Upstream{upstream})
	proxy := NewProxy("pool", selector, http.DefaultTransport, nil)
	srv := httptest.NewServer(proxy)
	defer srv.Close()

	proxyURL, _ := url.Parse(srv.URL)
	conn, err := net.DialTimeout("tcp", proxyURL.Host, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	req := "GET /ws HTTP/1.1\r\nHost: " + proxyURL.Host + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", resp.StatusCode)
	}
}

func TestWebSocket_ConcurrentSessionsIndependent(t *testing.T) {
	t.Parallel()

	upstreamAddr, stop := rawUpstream(t)
	defer stop()
	u, _ := url.Parse("http://" + upstreamAddr)
	upstream := &lb.Upstream{URL: u, Breaker: lb.NewCircuitBreaker(lb.DefaultCircuitBreakerConfig())}
	upstream.Eligible.Store(true)

	proxy := NewProxy("pool", lb.NewRoundRobin([]*lb.Upstream{upstream}), http.DefaultTransport, nil)
	srv := httptest.NewServer(proxy)
	defer srv.Close()

	proxyURL, _ := url.Parse(srv.URL)

	const sessions = 4
	var wg sync.WaitGroup
	for i := 0; i < sessions; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", proxyURL.Host)
			if err != nil {
				t.Errorf("session %d dial: %v", id, err)
				return
			}
			defer conn.Close()
			req := "GET /ws HTTP/1.1\r\nHost: " + proxyURL.Host + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
			if _, err := conn.Write([]byte(req)); err != nil {
				t.Errorf("session %d write: %v", id, err)
				return
			}
			br := bufio.NewReader(conn)
			resp, err := http.ReadResponse(br, nil)
			if err != nil {
				t.Errorf("session %d read: %v", id, err)
				return
			}
			if resp.StatusCode != 101 {
				t.Errorf("session %d status %d", id, resp.StatusCode)
			}
			payload := strings.Repeat("x", 32)
			if _, err := conn.Write([]byte(payload)); err != nil {
				t.Errorf("session %d echo write: %v", id, err)
				return
			}
			buf := make([]byte, len(payload))
			if _, err := io.ReadFull(br, buf); err != nil {
				t.Errorf("session %d echo read: %v", id, err)
				return
			}
			if string(buf) != payload {
				t.Errorf("session %d echo mismatch", id)
			}
		}(i)
	}
	wg.Wait()

	// The proxy's session-cleanup goroutines run after the client's TCP close
	// propagates through both directions; poll briefly for the inflight count
	// to drop rather than asserting immediately.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if upstream.InFlight.Load() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := upstream.InFlight.Load(); got != 0 {
		t.Errorf("upstream inflight at end = %d, want 0 (all sessions closed)", got)
	}
}

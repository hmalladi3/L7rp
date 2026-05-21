package upstream

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/harimalladi/l7rp/internal/lb"
)

// isWebSocketUpgrade reports whether the request is a WebSocket upgrade
// handshake per RFC 6455 §4.1. The detection is deliberately conservative —
// `Connection: Upgrade` (case-insensitive, possibly multi-valued) plus
// `Upgrade: websocket` and `GET` are the load-bearing signals.
func isWebSocketUpgrade(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	for _, c := range strings.Split(r.Header.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(c), "upgrade") {
			return true
		}
	}
	return false
}

// serveWebSocket proxies a WebSocket upgrade between the already-connected
// downstream client and the chosen upstream.
//
// Lifecycle:
//
//  1. Hijack the downstream net.Conn from the underlying http.Server.
//  2. Dial a fresh TCP (or TLS) connection to the upstream — we bypass the
//     proxy's http.Transport pool entirely, since hijacked connections don't
//     interact well with HTTP/1.1 keep-alive accounting.
//  3. Replay the upgrade request to the upstream with the URL retargeted.
//  4. Read the upstream's response. If it's not 101, write it back and close.
//  5. Otherwise, write the 101 to the downstream and run two io.Copy
//     goroutines for bidirectional traffic until either side closes.
//
// The connection counts as one in-flight on the upstream for the duration of
// the session. We don't report success/failure outcomes to the breaker for
// the long-lived stream — outcome semantics are ambiguous for connections
// that may run for hours.
func (p *Proxy) serveWebSocket(w http.ResponseWriter, r *http.Request, u *lb.Upstream) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		writeError(w, http.StatusInternalServerError, "websocket: hijack unsupported")
		return
	}

	downConn, downBuf, err := hijacker.Hijack()
	if err != nil {
		// We can still respond via the (un-hijacked) ResponseWriter.
		writeError(w, http.StatusInternalServerError, "websocket: hijack failed")
		return
	}
	defer downConn.Close()

	upConn, err := dialUpstream(r.Context(), u)
	if err != nil {
		writeRawError(downBuf, http.StatusBadGateway)
		return
	}
	defer upConn.Close()

	// Replay the request to upstream with retargeted URL. Headers are passed
	// through verbatim (hop-by-hop stripping happens upstream of the proxy
	// in the header-transform middleware).
	upReq := r.Clone(r.Context())
	upReq.URL.Scheme = u.URL.Scheme
	upReq.URL.Host = u.URL.Host
	upReq.Host = u.URL.Host
	upReq.RequestURI = ""
	if err := upReq.Write(upConn); err != nil {
		writeRawError(downBuf, http.StatusBadGateway)
		return
	}

	upReader := bufio.NewReader(upConn)
	upResp, err := http.ReadResponse(upReader, upReq)
	if err != nil {
		writeRawError(downBuf, http.StatusBadGateway)
		return
	}

	// Whatever the upstream sent — 101 or otherwise — gets relayed downstream.
	if err := upResp.Write(downBuf); err != nil {
		_ = upResp.Body.Close()
		return
	}
	if err := downBuf.Flush(); err != nil {
		_ = upResp.Body.Close()
		return
	}

	if upResp.StatusCode != http.StatusSwitchingProtocols {
		// Upstream declined the upgrade. The response is already on the wire;
		// just close.
		_ = upResp.Body.Close()
		return
	}

	// 101 acknowledged. The breaker counts this as a success — the upstream
	// participated in the protocol.
	u.Breaker.Record(lb.OutcomeSuccess)

	u.InFlight.Add(1)
	if p.metrics != nil {
		p.metrics.UpstreamInflight.WithLabelValues(p.poolName, u.URL.String()).Inc()
	}
	defer func() {
		u.InFlight.Add(-1)
		if p.metrics != nil {
			p.metrics.UpstreamInflight.WithLabelValues(p.poolName, u.URL.String()).Dec()
		}
	}()

	// Bidirectional shuttle. Either side closing ends the session; we propagate
	// the half-close on the other direction when the underlying conn supports
	// CloseWrite (TCP does; raw TLS doesn't expose it on net.Conn, so we just
	// full-close in that case).
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(upConn, downBuf)
		closeWriteOrFull(upConn)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(downConn, upReader)
		closeWriteOrFull(downConn)
	}()
	wg.Wait()
}

type closeWriter interface {
	CloseWrite() error
}

func closeWriteOrFull(c net.Conn) {
	if cw, ok := c.(closeWriter); ok {
		_ = cw.CloseWrite()
		return
	}
	_ = c.Close()
}

// dialUpstream opens a TCP or TLS connection to u's host:port. Bypasses the
// proxy's http.Transport pool — hijacked connections need their own dialer
// because keep-alive accounting would otherwise consider the long-lived
// session as occupying a pool slot indefinitely.
func dialUpstream(ctx context.Context, u *lb.Upstream) (net.Conn, error) {
	addr := u.URL.Host
	if !strings.Contains(addr, ":") {
		switch u.URL.Scheme {
		case "https", "wss":
			addr += ":443"
		default:
			addr += ":80"
		}
	}

	dialer := &net.Dialer{Timeout: 5 * time.Second}
	rawConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	switch u.URL.Scheme {
	case "https", "wss":
		tlsConn := tls.Client(rawConn, &tls.Config{ServerName: u.URL.Hostname()})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = rawConn.Close()
			return nil, err
		}
		return tlsConn, nil
	default:
		return rawConn, nil
	}
}

// writeRawError emits a minimal HTTP response on a hijacked connection. After
// http.Hijacker has handed us the raw socket, the standard ResponseWriter is
// no longer usable, so we hand-format the wire bytes.
func writeRawError(w *bufio.ReadWriter, status int) {
	statusText := http.StatusText(status)
	body := statusText
	_, _ = w.WriteString("HTTP/1.1 " + strconv.Itoa(status) + " " + statusText + "\r\n")
	_, _ = w.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	_, _ = w.WriteString("Content-Length: " + strconv.Itoa(len(body)+1) + "\r\n")
	_, _ = w.WriteString("Connection: close\r\n\r\n")
	_, _ = w.WriteString(body + "\n")
	_ = w.Flush()
}

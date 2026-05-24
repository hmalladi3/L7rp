// Package l4 implements raw TCP load balancing — the proxy accepts a TCP
// connection, picks an upstream from the pool, dials it, and copies bytes
// bidirectionally until either side closes. No HTTP parsing happens at this
// layer; the proxy is protocol-agnostic.
//
// This complements the L7 package. The two run side by side: an operator
// can have HTTP routes on :443 and a Redis tunnel on :6379 in the same
// l7rp process.
package l4

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/harimalladi/l7rp/internal/lb"
)

// Proxy is the per-connection dispatcher. One Proxy instance is shared by
// all connections accepted on a given listener; it holds the selector and
// dial parameters but no per-connection state.
type Proxy struct {
	PoolName       string
	Selector       lb.Selector
	ConnectTimeout time.Duration

	// OnDispatched is called once per successful dispatch with the chosen
	// upstream and bytes copied. Used by metrics. May be nil.
	OnDispatched func(upstream *lb.Upstream, bytesUp, bytesDown int64, dur time.Duration)

	dialer    net.Dialer
	dialerSet sync.Once
}

// ServeConn handles one accepted client connection. Blocks until both sides
// finish; the caller usually invokes this in a goroutine per accept.
//
// The client connection is closed by this function before it returns.
func (p *Proxy) ServeConn(ctx context.Context, client net.Conn) {
	defer client.Close()
	p.ensureDialer()

	start := time.Now()

	upstream, err := p.pickUpstream(ctx, client)
	if err != nil {
		slog.Debug("l4: no eligible upstream", "pool", p.PoolName, "err", err)
		return
	}

	dialCtx := ctx
	if p.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, p.ConnectTimeout)
		defer cancel()
	}

	upConn, err := p.dialer.DialContext(dialCtx, "tcp", upstream.URL.Host)
	if err != nil {
		slog.Debug("l4: upstream dial failed", "pool", p.PoolName, "upstream", upstream.URL.Host, "err", err)
		upstream.Breaker.Record(lb.OutcomeFailure)
		return
	}
	defer upConn.Close()

	upstream.InFlight.Add(1)
	defer upstream.InFlight.Add(-1)

	bytesUp, bytesDown := copyBidirectional(client, upConn)
	dur := time.Since(start)

	if p.OnDispatched != nil {
		p.OnDispatched(upstream, bytesUp, bytesDown, dur)
	}
	upstream.Breaker.Record(lb.OutcomeSuccess)
	// Feed the EWMA selector with the end-to-end session duration so
	// long-lived TCP sessions count against this upstream the way many
	// short HTTP requests would. 30s half-life matches the L7 default.
	upstream.RecordLatency(time.Now(), dur, 30*time.Second)
}

// pickUpstream wraps the existing L7 selector interface by synthesizing a
// minimal http.Request that exposes the client's RemoteAddr. This lets
// consistent-hash-by-client-ip work for L4 too; selectors that ignore the
// request (RR, WRR, LC, P2C-EWMA) are unaffected.
func (p *Proxy) pickUpstream(ctx context.Context, client net.Conn) (*lb.Upstream, error) {
	req := &http.Request{RemoteAddr: client.RemoteAddr().String()}
	return p.Selector.Pick(ctx, req, lb.PickHint{})
}

func (p *Proxy) ensureDialer() {
	p.dialerSet.Do(func() {
		t := p.ConnectTimeout
		if t <= 0 {
			t = 5 * time.Second
		}
		p.dialer = net.Dialer{Timeout: t, KeepAlive: 30 * time.Second}
	})
}

// copyBidirectional runs two io.Copy loops in parallel, one for each
// direction. Returns (clientToUpstream, upstreamToClient) byte counts.
// Whichever side finishes first triggers a CloseWrite on its peer to
// propagate the half-close — important for protocols like FTP and SMTP
// that signal end-of-stream via half-close rather than connection close.
func copyBidirectional(client, upstream net.Conn) (int64, int64) {
	var wg sync.WaitGroup
	var bytesUp, bytesDown atomic.Int64

	wg.Add(2)
	go func() {
		defer wg.Done()
		n, _ := io.Copy(upstream, client)
		bytesUp.Store(n)
		closeWriteOrFull(upstream)
	}()
	go func() {
		defer wg.Done()
		n, _ := io.Copy(client, upstream)
		bytesDown.Store(n)
		closeWriteOrFull(client)
	}()

	wg.Wait()
	return bytesUp.Load(), bytesDown.Load()
}

// closeWriteOrFull issues a half-close (FIN) on the conn if its concrete
// type supports CloseWrite (TCPConn does); otherwise falls back to closing
// the whole connection.
func closeWriteOrFull(c net.Conn) {
	type closeWriter interface{ CloseWrite() error }
	if cw, ok := c.(closeWriter); ok {
		_ = cw.CloseWrite()
		return
	}
	_ = c.Close()
}

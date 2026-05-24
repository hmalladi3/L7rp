// Package listener owns the TCP listener and HTTP server for each configured
// bind address. TLS termination, SNI cert routing, and h2c are deferred to
// v1.x; v1 listens on cleartext HTTP with explicit per-listener timeouts.
//
// The handler chain is materialized outside this package and passed to New.
// Listener does not import the router or config-build logic; it just hosts
// an http.Server with appropriate defaults.
package listener

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/harimalladi/l7rp/internal/config"
)

// Listener owns a single TCP listener and the http.Server that wraps it.
//
// The underlying TCP socket is bound with SO_REUSEPORT, so a reload can bind
// a fresh socket on the same (interface, port) before the previous listener
// has finished draining. The kernel-level load balancing during the overlap
// is benign — new connections land on whichever socket the kernel chooses,
// and any in-flight connections on the old socket complete against it.
type Listener struct {
	Name string

	server   *http.Server
	listener net.Listener
	tls      bool
	http3    *http3Server // non-nil when enable_http3 is true on the config

	// done is closed when the Serve goroutine returns. Used by the reload
	// path to wait for a previous listener instance to fully drain before
	// declaring success.
	done chan struct{}
}

// New constructs a Listener bound to cfg.Bind. The returned Listener has its
// socket open and is ready to Serve.
//
// Per-listener timeouts default to conservative values when zero in the
// configuration:
//
//	ReadHeader  5s
//	Read       30s
//	Write      30s
//	Idle      120s
//
// MaxHeaderBytes defaults to net/http's default (1 MiB) when zero.
func New(cfg config.ListenerConfig, handler http.Handler) (*Listener, error) {
	lc := net.ListenConfig{Control: setSocketOpts}
	ln, err := lc.Listen(context.Background(), "tcp", cfg.Bind)
	if err != nil {
		return nil, fmt.Errorf("listener %q: bind %s: %w", cfg.Name, cfg.Bind, err)
	}

	tlsCfg, err := buildTLSConfig(cfg.TLS)
	if err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("listener %q: tls: %w", cfg.Name, err)
	}

	wrapped := smugglingHandler(handler)
	// When HTTP/3 is also being served, every h1/h2 response gets an Alt-Svc
	// header advertising the QUIC port so capable clients upgrade on the
	// next request.
	if cfg.EnableHTTP3 {
		wrapped = altSvcMiddleware(wrapped, cfg.Bind)
	}

	t := withTimeoutDefaults(cfg.Timeouts)
	srv := &http.Server{
		Handler:           wrapped,
		ReadHeaderTimeout: t.ReadHeader,
		ReadTimeout:       t.Read,
		WriteTimeout:      t.Write,
		IdleTimeout:       t.Idle,
		TLSConfig:         tlsCfg,
	}
	if t.MaxHeaderBytes > 0 {
		srv.MaxHeaderBytes = t.MaxHeaderBytes
	}

	l := &Listener{
		Name:     cfg.Name,
		server:   srv,
		listener: ln,
		tls:      tlsCfg != nil,
		done:     make(chan struct{}),
	}

	if cfg.EnableHTTP3 {
		h3, err := startHTTP3(cfg.Bind, tlsCfg, wrapped)
		if err != nil {
			_ = ln.Close()
			return nil, fmt.Errorf("listener %q: %w", cfg.Name, err)
		}
		l.http3 = h3
	}

	return l, nil
}

// setSocketOpts enables SO_REUSEADDR and SO_REUSEPORT on the underlying
// socket so that a fresh Listener can bind to the same (host, port) tuple
// while the previous one is still draining. The kernel splits inbound
// connections across the two sockets briefly; both serve real traffic
// correctly until the old one finishes its drain.
//
// Supported on Linux 3.9+, macOS, and the BSDs. Windows is not in v1's
// supported runtime set; setSocketOpts is a no-op there.
func setSocketOpts(network, address string, c syscall.RawConn) error {
	var sockErr error
	err := c.Control(func(fd uintptr) {
		if e := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); e != nil {
			sockErr = fmt.Errorf("SO_REUSEADDR: %w", e)
			return
		}
		if e := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); e != nil {
			sockErr = fmt.Errorf("SO_REUSEPORT: %w", e)
			return
		}
	})
	if err != nil {
		return err
	}
	return sockErr
}

// Done returns a channel that is closed when the listener has fully drained
// (Serve has returned). Reload uses this to wait for a previous instance to
// exit before declaring the swap complete.
func (l *Listener) Done() <-chan struct{} {
	return l.done
}

func withTimeoutDefaults(t config.ListenerTimeouts) config.ListenerTimeouts {
	if t.ReadHeader <= 0 {
		t.ReadHeader = 5 * time.Second
	}
	if t.Read <= 0 {
		t.Read = 30 * time.Second
	}
	if t.Write <= 0 {
		t.Write = 30 * time.Second
	}
	if t.Idle <= 0 {
		t.Idle = 120 * time.Second
	}
	return t
}

// Addr returns the listener's bound address. Useful for logging and for tests
// that need to know which port the kernel assigned to a :0 bind.
func (l *Listener) Addr() net.Addr {
	return l.listener.Addr()
}

// Serve blocks until the server stops. Returns nil when shutdown completes
// cleanly; returns the http.Server error otherwise. Routes to ServeTLS when
// the listener has a TLS configuration; otherwise serves cleartext.
//
// `l.done` is closed when Serve returns, regardless of outcome — so callers
// waiting on Done() always unblock on graceful Shutdown or on error.
func (l *Listener) Serve() error {
	defer close(l.done)

	scheme := "http"
	if l.tls {
		scheme = "https"
	}
	slog.Info("listener serving", "name", l.Name, "addr", l.listener.Addr(), "scheme", scheme)

	var err error
	if l.tls {
		// ServeTLS reads the cert from disk; we've already loaded certs into
		// TLSConfig.GetCertificate, so passing empty strings is correct.
		err = l.server.ServeTLS(l.listener, "", "")
	} else {
		err = l.server.Serve(l.listener)
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully stops the server, waiting for in-flight requests to
// complete or for ctx to expire. When HTTP/3 is enabled, the QUIC server is
// closed in parallel — quic-go does not yet support graceful per-stream
// drain so this is closer to an immediate shutdown for the h3 path.
func (l *Listener) Shutdown(ctx context.Context) error {
	slog.Info("listener draining", "name", l.Name)
	if l.http3 != nil {
		_ = l.http3.Shutdown(ctx)
	}
	return l.server.Shutdown(ctx)
}

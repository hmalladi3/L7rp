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
	"time"

	"github.com/harimalladi/l7rp/internal/config"
)

// Listener owns a single TCP listener and the http.Server that wraps it.
type Listener struct {
	Name string

	server   *http.Server
	listener net.Listener
	tls      bool
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
	ln, err := net.Listen("tcp", cfg.Bind)
	if err != nil {
		return nil, fmt.Errorf("listener %q: bind %s: %w", cfg.Name, cfg.Bind, err)
	}

	tlsCfg, err := buildTLSConfig(cfg.TLS)
	if err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("listener %q: tls: %w", cfg.Name, err)
	}

	t := withTimeoutDefaults(cfg.Timeouts)
	srv := &http.Server{
		Handler:           smugglingHandler(handler),
		ReadHeaderTimeout: t.ReadHeader,
		ReadTimeout:       t.Read,
		WriteTimeout:      t.Write,
		IdleTimeout:       t.Idle,
		TLSConfig:         tlsCfg,
	}
	if t.MaxHeaderBytes > 0 {
		srv.MaxHeaderBytes = t.MaxHeaderBytes
	}

	return &Listener{Name: cfg.Name, server: srv, listener: ln, tls: tlsCfg != nil}, nil
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
func (l *Listener) Serve() error {
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
// complete or for ctx to expire.
func (l *Listener) Shutdown(ctx context.Context) error {
	slog.Info("listener draining", "name", l.Name)
	return l.server.Shutdown(ctx)
}

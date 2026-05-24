package listener

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"

	"github.com/quic-go/quic-go/http3"
)

// http3Server bundles the UDP socket and quic-go http3.Server for a single
// listener. Lifecycle mirrors the TCP path so the parent Listener can drive
// both shutdowns from one place.
type http3Server struct {
	srv      *http3.Server
	conn     net.PacketConn
	done     chan struct{}
	bindAddr string
}

// startHTTP3 binds a UDP socket on the same (host, port) as the TCP listener,
// configures an http3.Server with the same TLS config (with "h3" added to
// NextProtos), and starts serving in a goroutine. The returned struct lets
// the parent Listener Wait/Shutdown both transports together.
func startHTTP3(bindAddr string, baseTLS *tls.Config, handler http.Handler) (*http3Server, error) {
	if baseTLS == nil {
		return nil, fmt.Errorf("http3: requires TLS config")
	}

	tlsCfg := baseTLS.Clone()
	tlsCfg.NextProtos = ensureALPN(tlsCfg.NextProtos, "h3")

	pc, err := net.ListenPacket("udp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("http3: bind udp %s: %w", bindAddr, err)
	}

	srv := &http3.Server{
		Addr:      bindAddr,
		TLSConfig: tlsCfg,
		Handler:   handler,
	}

	h := &http3Server{srv: srv, conn: pc, done: make(chan struct{}), bindAddr: bindAddr}
	go func() {
		defer close(h.done)
		if err := srv.Serve(pc); err != nil && err != http.ErrServerClosed {
			slog.Error("http3 listener stopped with error", "addr", bindAddr, "err", err)
		}
	}()
	slog.Info("http3 listener serving", "addr", bindAddr)
	return h, nil
}

// Shutdown closes the QUIC server, which causes in-flight streams to finish
// or error out (quic-go doesn't yet support graceful drain at the stream
// level, so this is effectively a hard close).
func (h *http3Server) Shutdown(_ context.Context) error {
	_ = h.srv.Close()
	_ = h.conn.Close()
	<-h.done
	return nil
}

// altSvcMiddleware wraps an HTTP handler and injects the Alt-Svc header on
// every response so clients know they can switch to HTTP/3 on the next
// request. The advertised port is the same UDP port we're listening on.
func altSvcMiddleware(handler http.Handler, bindAddr string) http.Handler {
	port := portFromBind(bindAddr)
	value := fmt.Sprintf(`h3=":%d"; ma=3600`, port)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Alt-Svc", value)
		handler.ServeHTTP(w, r)
	})
}

func portFromBind(bind string) int {
	_, p, err := net.SplitHostPort(bind)
	if err != nil {
		return 443
	}
	n, err := strconv.Atoi(p)
	if err != nil {
		return 443
	}
	return n
}

// ensureALPN appends proto to next-protos if not already present. The order
// in NextProtos signals preference during ALPN negotiation, but for HTTP/3
// negotiation happens at the QUIC layer rather than during TLS handshake,
// so ordering is mostly cosmetic here.
func ensureALPN(have []string, proto string) []string {
	for _, p := range have {
		if p == proto {
			return have
		}
	}
	return append(have, proto)
}

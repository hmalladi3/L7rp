package l4

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Listener owns a single TCP listener and dispatches each accepted
// connection to a Proxy. Mirrors the L7 listener's lifecycle (Serve,
// Shutdown, Done) so the parent runtime can drive both uniformly.
type Listener struct {
	Name string

	bind     string
	ln       net.Listener
	proxy    *Proxy
	conns    sync.WaitGroup
	stopping atomic.Bool
	done     chan struct{}
}

// New binds a TCP listener with SO_REUSEPORT (matching the L7 path so a
// reload can swap sockets without dropping connections) and returns it
// ready to Serve.
func New(name, bind string, proxy *Proxy) (*Listener, error) {
	lc := net.ListenConfig{Control: setSocketOpts}
	ln, err := lc.Listen(context.Background(), "tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("l4 listener %q: bind %s: %w", name, bind, err)
	}
	return &Listener{
		Name:  name,
		bind:  bind,
		ln:    ln,
		proxy: proxy,
		done:  make(chan struct{}),
	}, nil
}

// Addr returns the bound address. Useful for tests that bind to :0.
func (l *Listener) Addr() net.Addr { return l.ln.Addr() }

// Serve runs the accept loop until Shutdown is called or the listener
// errors out. Each accepted connection is dispatched to a goroutine.
func (l *Listener) Serve() error {
	defer close(l.done)
	slog.Info("l4 listener serving", "name", l.Name, "addr", l.ln.Addr())

	for {
		conn, err := l.ln.Accept()
		if err != nil {
			if l.stopping.Load() {
				break
			}
			// Transient accept errors (e.g., ECONNRESET on close) are
			// logged but don't stop the loop. A persistent error will
			// loop tight, which is fine — the supervisor restarts us.
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				break
			}
			slog.Warn("l4 accept error", "name", l.Name, "err", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}

		l.conns.Add(1)
		go func() {
			defer l.conns.Done()
			l.proxy.ServeConn(context.Background(), conn)
		}()
	}
	return nil
}

// Shutdown closes the listener (rejecting new connections) and waits for
// in-flight ones to finish, up to ctx's deadline. Returns ctx.Err() if the
// deadline fires before all connections drain.
func (l *Listener) Shutdown(ctx context.Context) error {
	l.stopping.Store(true)
	_ = l.ln.Close()
	slog.Info("l4 listener draining", "name", l.Name)

	done := make(chan struct{})
	go func() {
		l.conns.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Done returns a channel closed after Serve returns. Reload waits on this
// before declaring the previous instance fully drained.
func (l *Listener) Done() <-chan struct{} { return l.done }

// setSocketOpts mirrors the L7 listener's SO_REUSEADDR + SO_REUSEPORT
// configuration so reload semantics are uniform across L4 and L7.
func setSocketOpts(_, _ string, c syscall.RawConn) error {
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

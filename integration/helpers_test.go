//go:build integration || soak

package integration

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// binaryPath is the path to the built l7rp binary, set by TestMain.
var binaryPath string

func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "l7rp-integration-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "mkdtemp:", err)
		os.Exit(2)
	}
	defer os.RemoveAll(tmpDir)

	binaryPath = filepath.Join(tmpDir, "l7rp")

	projectRoot, err := filepath.Abs("..")
	if err != nil {
		fmt.Fprintln(os.Stderr, "abs:", err)
		os.Exit(2)
	}

	build := exec.Command("go", "build", "-o", binaryPath, "./cmd/l7rp")
	build.Dir = projectRoot
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build:", err)
		os.Exit(2)
	}

	os.Exit(m.Run())
}

// proxy wraps a running l7rp subprocess.
type proxy struct {
	cmd         *exec.Cmd
	cfgPath     string
	metricsAddr string
	dataAddr    string
	logBuf      *syncBuffer
	stopped     atomic.Bool
}

// startProxy renders the given config template (with {{DATA_PORT}} and any
// other placeholders already substituted by the caller) and launches the
// proxy as a subprocess. Returns a handle that the test should drive and
// that's automatically stopped via t.Cleanup.
func startProxy(t *testing.T, cfg string) *proxy {
	t.Helper()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	metricsPort := freePort(t)
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", metricsPort)

	cmd := exec.Command(binaryPath,
		"--config", cfgPath,
		"--metrics-bind", metricsAddr,
		"--log-level", "info",
	)
	logBuf := newSyncBuffer()
	cmd.Stdout = logBuf
	cmd.Stderr = logBuf

	if err := cmd.Start(); err != nil {
		t.Fatalf("start proxy: %v", err)
	}

	p := &proxy{cmd: cmd, cfgPath: cfgPath, metricsAddr: metricsAddr, logBuf: logBuf}

	stop := func() {
		if p.stopped.Swap(true) {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}

	// Wait for /-/ready before returning.
	if !waitFor(3*time.Second, func() bool {
		resp, err := http.Get("http://" + metricsAddr + "/-/ready")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == 200
	}) {
		stop()
		t.Fatalf("proxy never became ready:\n%s", logBuf.String())
	}

	t.Cleanup(func() {
		stop()
		if t.Failed() {
			t.Logf("proxy log:\n%s", logBuf.String())
		}
	})

	return p
}

// reload writes a new config and sends SIGHUP. Caller-supplied wait covers
// the reload latency budget.
func (p *proxy) reload(t *testing.T, newCfg string) {
	t.Helper()
	if err := os.WriteFile(p.cfgPath, []byte(newCfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatalf("SIGHUP: %v", err)
	}
}

// metricLine returns the first metrics-output line containing the given
// substring, or "" if no match.
func (p *proxy) metricLine(t *testing.T, substr string) string {
	t.Helper()
	resp, err := http.Get("http://" + p.metricsAddr + "/metrics")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.Contains(line, substr) {
			return line
		}
	}
	return ""
}

// backend is a controllable httptest server. Use SetStatus to change the
// status it returns for non-/healthz requests; the /healthz path always
// returns 200 unless DisableHealth is true.
type backend struct {
	*httptest.Server
	name          string
	statusCode    atomic.Int32
	calls         atomic.Int64
	disableHealth atomic.Bool
}

func startBackend(t *testing.T, name string) *backend {
	t.Helper()
	b := &backend{name: name}
	b.statusCode.Store(200)
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" && !b.disableHealth.Load() {
			w.WriteHeader(200)
			_, _ = w.Write([]byte("ok"))
			return
		}
		b.calls.Add(1)
		code := int(b.statusCode.Load())
		w.Header().Set("X-Backend", b.name)
		w.WriteHeader(code)
		_, _ = w.Write([]byte(name))
	}))
	t.Cleanup(b.Close)
	return b
}

// URL returns the backend's base URL.
func (b *backend) URL() string { return b.Server.URL }

// generateTestCert produces a fresh ECDSA P-256 self-signed certificate
// valid for localhost and 127.0.0.1, returning paths to the cert and key
// files in a t.TempDir-managed location. Used for TLS-bearing listener
// tests where httptest's built-in cert isn't accessible.
func generateTestCert(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa keygen: %v", err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames:              []string{"localhost"},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

// startTLSBackend stands up an https://... backend with a self-signed cert
// (the standard one httptest generates). Returns the backend plus a path to
// a PEM file containing the cert, suitable for use as `ca_file` in the
// proxy's upstream_tls block.
func startTLSBackend(t *testing.T, name string) (*backend, string) {
	t.Helper()
	b := &backend{name: name}
	b.statusCode.Store(200)
	b.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" && !b.disableHealth.Load() {
			w.WriteHeader(200)
			_, _ = w.Write([]byte("ok"))
			return
		}
		b.calls.Add(1)
		w.Header().Set("X-Backend", b.name)
		w.WriteHeader(int(b.statusCode.Load()))
		_, _ = w.Write([]byte(name))
	}))
	t.Cleanup(b.Close)

	caPath := filepath.Join(t.TempDir(), "backend-ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: b.Server.Certificate().Raw,
	})
	if err := os.WriteFile(caPath, pemBytes, 0o644); err != nil {
		t.Fatalf("write CA file: %v", err)
	}
	return b, caPath
}

// startSlowBackend returns a backend whose /healthz responds immediately but
// every other path sleeps for `delay` before responding 200. Used to drive
// timeout-enforcement scenarios.
func startSlowBackend(t *testing.T, delay time.Duration) *backend {
	t.Helper()
	b := &backend{name: "slow"}
	b.statusCode.Store(200)
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(200)
			_, _ = w.Write([]byte("ok"))
			return
		}
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
		b.calls.Add(1)
		w.Header().Set("X-Backend", b.name)
		w.WriteHeader(int(b.statusCode.Load()))
	}))
	t.Cleanup(b.Close)
	return b
}

// startBackendOnPort binds the backend's listener to a specific TCP port
// instead of letting httptest pick an ephemeral one. Used by restart-on-same-
// port scenarios so the upstream URL in the proxy config stays valid across
// kill-and-relaunch cycles.
func startBackendOnPort(t *testing.T, name string, port int) *backend {
	t.Helper()
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("bind backend on port %d: %v", port, err)
	}
	b := &backend{name: name}
	b.statusCode.Store(200)
	b.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" && !b.disableHealth.Load() {
			w.WriteHeader(200)
			_, _ = w.Write([]byte("ok"))
			return
		}
		b.calls.Add(1)
		w.Header().Set("X-Backend", b.name)
		w.WriteHeader(int(b.statusCode.Load()))
		_, _ = w.Write([]byte(name))
	}))
	b.Server.Listener.Close()
	b.Server.Listener = ln
	b.Server.Start()
	t.Cleanup(b.Close)
	return b
}

// port extracts the TCP port the backend is bound to.
func (b *backend) port() int {
	u := b.URL()
	if i := strings.LastIndex(u, ":"); i >= 0 {
		var port int
		fmt.Sscanf(u[i+1:], "%d", &port)
		return port
	}
	return 0
}

// SetStatus changes the status returned for non-/healthz requests.
func (b *backend) SetStatus(code int) { b.statusCode.Store(int32(code)) }

// DisableHealth makes /healthz also use the controllable status (so probes
// see 5xx). Calls to this method are sticky — flip via re-enabling.
func (b *backend) DisableHealth(disabled bool) { b.disableHealth.Store(disabled) }

// freePort binds 127.0.0.1:0 to claim a port, then closes the listener so the
// caller can re-bind. Vulnerable to TOCTOU under heavy local concurrency; the
// SO_REUSEPORT bind in the proxy mitigates collisions.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// waitFor polls cond every 25ms until it returns true or the deadline expires.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return cond()
}

// curl issues a single GET to the proxy at the data address with a Host
// header set. Returns status code, response headers, and body.
func curl(t *testing.T, dataAddr, host, path string) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequest("GET", "http://"+dataAddr+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, string(body)
}

// syncBuffer is a goroutine-safe bytes.Buffer for capturing subprocess output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func newSyncBuffer() *syncBuffer { return &syncBuffer{} }

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

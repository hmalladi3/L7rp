package listener

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/harimalladi/l7rp/internal/config"
)

// generateTestCert produces a self-signed ECDSA P-256 cert valid for the given
// SAN DNS names, writes cert + key to a tempdir, and returns the file paths.
func generateTestCert(t *testing.T, dnsNames ...string) (certPath, keyPath string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa: %v", err)
	}

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: dnsNames[0]},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              dnsNames,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, certPEM, 0o644); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

func TestTLS_BuildConfigDefaultsTLS12(t *testing.T) {
	t.Parallel()

	cp, kp := generateTestCert(t, "example.com")
	tlsCfg, err := buildTLSConfig(&config.TLSConfig{
		Certs: []config.TLSCert{{Cert: cp, Key: kp, Hosts: []string{"example.com"}}},
	})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if tlsCfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want %x", tlsCfg.MinVersion, tls.VersionTLS12)
	}
	if len(tlsCfg.CipherSuites) == 0 {
		t.Error("CipherSuites is empty; expected modern AEAD allowlist")
	}
	for _, suite := range tlsCfg.CipherSuites {
		// No CBC suites (the legacy ones contain "CBC" in the constant name).
		name := tls.CipherSuiteName(suite)
		if strings.Contains(strings.ToUpper(name), "CBC") {
			t.Errorf("modern cipher suite list includes CBC: %s", name)
		}
	}
}

func TestTLS_BuildConfigRejectsUnsupportedMinVersion(t *testing.T) {
	t.Parallel()

	cp, kp := generateTestCert(t, "example.com")
	_, err := buildTLSConfig(&config.TLSConfig{
		MinVersion: "1.1",
		Certs:      []config.TLSCert{{Cert: cp, Key: kp, Hosts: []string{"example.com"}}},
	})
	if err == nil {
		t.Error("expected error on unsupported min_version 1.1")
	}
}

// TestTLS_SNIExactMatch verifies the SNI callback returns the cert mapped to
// the exact hostname.
func TestTLS_SNIExactMatch(t *testing.T) {
	t.Parallel()

	cpA, kpA := generateTestCert(t, "api.example.com")
	cpB, kpB := generateTestCert(t, "www.example.com")
	tlsCfg, err := buildTLSConfig(&config.TLSConfig{
		Certs: []config.TLSCert{
			{Cert: cpA, Key: kpA, Hosts: []string{"api.example.com"}},
			{Cert: cpB, Key: kpB, Hosts: []string{"www.example.com"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	gotA, _ := tlsCfg.GetCertificate(&tls.ClientHelloInfo{ServerName: "api.example.com"})
	gotB, _ := tlsCfg.GetCertificate(&tls.ClientHelloInfo{ServerName: "www.example.com"})

	leafA, _ := x509.ParseCertificate(gotA.Certificate[0])
	leafB, _ := x509.ParseCertificate(gotB.Certificate[0])
	if leafA.Subject.CommonName != "api.example.com" {
		t.Errorf("SNI api.example.com → CN %q, want api.example.com", leafA.Subject.CommonName)
	}
	if leafB.Subject.CommonName != "www.example.com" {
		t.Errorf("SNI www.example.com → CN %q, want www.example.com", leafB.Subject.CommonName)
	}
}

// TestTLS_SNIWildcard verifies that a wildcard pattern matches a single label.
func TestTLS_SNIWildcard(t *testing.T) {
	t.Parallel()

	cp, kp := generateTestCert(t, "*.example.com")
	tlsCfg, err := buildTLSConfig(&config.TLSConfig{
		Certs: []config.TLSCert{{Cert: cp, Key: kp, Hosts: []string{"*.example.com"}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		sni  string
		want bool
	}{
		{"api.example.com", true},
		{"www.example.com", true},
		{"v2.api.example.com", false}, // wildcard matches one label only
		{"example.com", false},        // bare apex doesn't match wildcard
	}
	for _, c := range cases {
		got, err := tlsCfg.GetCertificate(&tls.ClientHelloInfo{ServerName: c.sni})
		if c.want {
			if err != nil || got == nil {
				t.Errorf("SNI %q: expected cert, got err=%v cert=%v", c.sni, err, got)
			}
		} else {
			if err == nil {
				t.Errorf("SNI %q: expected error (no cert), got cert", c.sni)
			}
		}
	}
}

// TestTLS_SNIExactBeatsWildcard verifies the match-order precedence.
func TestTLS_SNIExactBeatsWildcard(t *testing.T) {
	t.Parallel()

	cpWild, kpWild := generateTestCert(t, "*.example.com")
	cpSpec, kpSpec := generateTestCert(t, "api.example.com")
	tlsCfg, err := buildTLSConfig(&config.TLSConfig{
		Certs: []config.TLSCert{
			{Cert: cpWild, Key: kpWild, Hosts: []string{"*.example.com"}},
			{Cert: cpSpec, Key: kpSpec, Hosts: []string{"api.example.com"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := tlsCfg.GetCertificate(&tls.ClientHelloInfo{ServerName: "api.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(got.Certificate[0])
	if leaf.Subject.CommonName != "api.example.com" {
		t.Errorf("exact match should win; got CN %q", leaf.Subject.CommonName)
	}
}

// TestTLS_SNIUsesFallbackForUnknown verifies the no-hosts cert serves as a
// default for unknown SNI names.
func TestTLS_SNIUsesFallbackForUnknown(t *testing.T) {
	t.Parallel()

	cpFallback, kpFallback := generateTestCert(t, "default.example.com")
	cpSpecific, kpSpecific := generateTestCert(t, "api.example.com")

	tlsCfg, err := buildTLSConfig(&config.TLSConfig{
		Certs: []config.TLSCert{
			{Cert: cpFallback, Key: kpFallback}, // no Hosts → fallback
			{Cert: cpSpecific, Key: kpSpecific, Hosts: []string{"api.example.com"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := tlsCfg.GetCertificate(&tls.ClientHelloInfo{ServerName: "unknown.example.org"})
	if err != nil || got == nil {
		t.Fatalf("expected fallback cert, got err=%v", err)
	}
	leaf, _ := x509.ParseCertificate(got.Certificate[0])
	if leaf.Subject.CommonName != "default.example.com" {
		t.Errorf("expected fallback cert; got CN %q", leaf.Subject.CommonName)
	}
}

// TestTLS_SNINoMatchNoFallbackReturnsError verifies that without a fallback,
// unknown SNI yields the TLS handshake error path.
func TestTLS_SNINoMatchNoFallbackReturnsError(t *testing.T) {
	t.Parallel()

	cp, kp := generateTestCert(t, "api.example.com")
	tlsCfg, err := buildTLSConfig(&config.TLSConfig{
		Certs: []config.TLSCert{{Cert: cp, Key: kp, Hosts: []string{"api.example.com"}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = tlsCfg.GetCertificate(&tls.ClientHelloInfo{ServerName: "unknown.example.org"})
	if err == nil {
		t.Error("expected error for unknown SNI without fallback")
	}
}

// TestListener_TLSEndToEnd brings up an actual TLS listener with self-signed
// certs, dials it with a TLS client that pins the cert, and verifies the
// request flows through to the handler.
func TestListener_TLSEndToEnd(t *testing.T) {
	t.Parallel()

	cp, kp := generateTestCert(t, "localhost")
	cfg := config.ListenerConfig{
		Name: "tls-test",
		Bind: "127.0.0.1:0", // OS-assigned port
		TLS: &config.TLSConfig{
			Certs: []config.TLSCert{{Cert: cp, Key: kp, Hosts: []string{"localhost"}}},
		},
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("tls ok"))
	})

	ln, err := New(cfg, handler)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	addr := ln.Addr().String()

	done := make(chan struct{})
	go func() {
		_ = ln.Serve()
		close(done)
	}()

	// Build a client that trusts the self-signed cert by reading the cert PEM.
	pemBytes, _ := os.ReadFile(cp)
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("could not append test cert to pool")
	}
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}},
	}

	resp, err := client.Get("https://" + addr + "/")
	if err != nil {
		t.Fatalf("client.Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	_ = ln.server.Close()
	<-done
}

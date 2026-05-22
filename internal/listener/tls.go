package listener

import (
	"crypto/tls"
	"fmt"
	"sort"
	"strings"

	"golang.org/x/crypto/acme/autocert"

	"github.com/harimalladi/l7rp/internal/config"
)

// buildTLSConfig produces a *tls.Config for a listener. Returns nil when the
// listener has no TLS section — the listener will serve cleartext.
//
// SNI-based cert routing is performed via the GetCertificate callback, which
// closes over an immutable cert map built once here. The map honors three
// match tiers (exact match → longest-suffix wildcard → fallback), so
// operators can ship a single listener that serves many host certs.
func buildTLSConfig(cfg *config.TLSConfig) (*tls.Config, error) {
	if cfg == nil {
		return nil, nil
	}

	var minVersion uint16
	switch cfg.MinVersion {
	case "", "1.2":
		minVersion = tls.VersionTLS12
	case "1.3":
		minVersion = tls.VersionTLS13
	default:
		return nil, fmt.Errorf("tls: unsupported min_version %q", cfg.MinVersion)
	}

	// Autocert path: when configured, certificates are obtained on-demand
	// from Let's Encrypt for the configured AllowedHosts. The acme-tls/1 ALPN
	// is required for tls-alpn-01 challenges.
	if cfg.Autocert != nil && len(cfg.Autocert.AllowedHosts) > 0 {
		m := &autocert.Manager{
			Cache:      autocert.DirCache(cfg.Autocert.CacheDir),
			Prompt:     autocert.AcceptTOS,
			HostPolicy: autocert.HostWhitelist(cfg.Autocert.AllowedHosts...),
			Email:      cfg.Autocert.Email,
		}
		return &tls.Config{
			MinVersion:       minVersion,
			CipherSuites:     modernCipherSuites,
			CurvePreferences: []tls.CurveID{tls.X25519, tls.CurveP256},
			NextProtos:       []string{"h2", "http/1.1", "acme-tls/1"},
			GetCertificate:   m.GetCertificate,
		}, nil
	}

	cm, err := buildCertMap(cfg.Certs)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		MinVersion:       minVersion,
		CipherSuites:     modernCipherSuites,
		CurvePreferences: []tls.CurveID{tls.X25519, tls.CurveP256},
		NextProtos:       []string{"h2", "http/1.1"},
		GetCertificate:   cm.GetCertificate,
	}, nil
}

// modernCipherSuites is the TLS 1.2 cipher allowlist. TLS 1.3 picks its own
// suites internally; this list applies only to 1.2 handshakes. Aligned with
// Mozilla's "Modern" profile — modern AEAD constructions only, no CBC, no
// RC4, no 3DES.
var modernCipherSuites = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
}

// certMap is the immutable SNI-routing structure. Reads are lock-free; the
// map is built once at listener construction time.
type certMap struct {
	exact     map[string]*tls.Certificate
	wildcards []wildcardCert // sorted by suffix length descending
	fallback  *tls.Certificate
}

type wildcardCert struct {
	suffix string // includes leading dot, e.g. ".example.com"
	cert   *tls.Certificate
}

// buildCertMap loads each configured (cert, key) pair and indexes it by the
// hosts it serves. A cert with no Hosts becomes the fallback; subsequent
// no-hosts entries overwrite the previous fallback (operator's responsibility
// to avoid this in config).
func buildCertMap(entries []config.TLSCert) (*certMap, error) {
	cm := &certMap{exact: make(map[string]*tls.Certificate)}

	for i, entry := range entries {
		cert, err := tls.LoadX509KeyPair(entry.Cert, entry.Key)
		if err != nil {
			return nil, fmt.Errorf("certs[%d] %q + %q: %w", i, entry.Cert, entry.Key, err)
		}
		certPtr := cert // local copy so &certPtr survives the loop iteration
		if len(entry.Hosts) == 0 {
			cm.fallback = &certPtr
			continue
		}
		for _, h := range entry.Hosts {
			host := strings.ToLower(h)
			if strings.HasPrefix(host, "*.") {
				cm.wildcards = append(cm.wildcards, wildcardCert{
					suffix: host[1:], // ".example.com"
					cert:   &certPtr,
				})
				continue
			}
			cm.exact[host] = &certPtr
		}
	}

	// Sort wildcards longest-suffix-first so the most specific match wins.
	sort.SliceStable(cm.wildcards, func(i, j int) bool {
		return len(cm.wildcards[i].suffix) > len(cm.wildcards[j].suffix)
	})
	return cm, nil
}

// GetCertificate is the TLS callback. The match order is exact → longest
// wildcard suffix → fallback. RFC 6125 §6.4.3 wildcard semantics: the `*`
// matches exactly one label, never zero or multiple.
func (cm *certMap) GetCertificate(chi *tls.ClientHelloInfo) (*tls.Certificate, error) {
	name := strings.ToLower(chi.ServerName)

	if cert, ok := cm.exact[name]; ok {
		return cert, nil
	}

	for _, w := range cm.wildcards {
		if !strings.HasSuffix(name, w.suffix) {
			continue
		}
		// Whatever is to the left of the suffix must be exactly one label.
		prefix := name[:len(name)-len(w.suffix)]
		if prefix == "" || strings.Contains(prefix, ".") {
			continue
		}
		return w.cert, nil
	}

	if cm.fallback != nil {
		return cm.fallback, nil
	}
	return nil, fmt.Errorf("no certificate available for SNI %q", chi.ServerName)
}

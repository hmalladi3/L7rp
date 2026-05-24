package middleware

import (
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
)

// CompressConfig parameterizes the Compress middleware.
type CompressConfig struct {
	// MinBytes is the minimum response size in bytes before compression
	// kicks in. Below this the cost of the round trip dominates the gain
	// from smaller bytes on the wire. Zero falls back to 1024.
	MinBytes int
	// SkipContentTypes are MIME types we never compress (already-compressed
	// formats: images, video, archives). Defaults below.
	SkipContentTypes []string
}

// Compress returns middleware that transparently compresses response bodies
// when the client advertises support via Accept-Encoding. Encoding choice
// honors client q-values; ties resolve in encoder-quality order
// (br > zstd > gzip — best ratio first because all three are competitive
// in throughput on modern hardware).
//
// Responses already encoded by the upstream (Content-Encoding present) are
// passed through unchanged — we don't double-compress.
func Compress(cfg CompressConfig) Middleware {
	minBytes := cfg.MinBytes
	if minBytes <= 0 {
		minBytes = 1024
	}
	skip := make(map[string]bool, len(cfg.SkipContentTypes)+len(defaultSkipContentTypes))
	for _, ct := range defaultSkipContentTypes {
		skip[ct] = true
	}
	for _, ct := range cfg.SkipContentTypes {
		skip[strings.ToLower(strings.TrimSpace(ct))] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			enc := pickEncoding(r.Header.Get("Accept-Encoding"))
			if enc == encodingNone {
				next.ServeHTTP(w, r)
				return
			}
			cw := &compressingWriter{
				ResponseWriter: w,
				enc:            enc,
				minBytes:       minBytes,
				skipTypes:      skip,
			}
			defer cw.Close()
			next.ServeHTTP(cw, r)
		})
	}
}

type encoding int

const (
	encodingNone encoding = iota
	encodingGzip
	encodingZstd
	encodingBrotli
)

func (e encoding) name() string {
	switch e {
	case encodingGzip:
		return "gzip"
	case encodingZstd:
		return "zstd"
	case encodingBrotli:
		return "br"
	default:
		return ""
	}
}

// defaultSkipContentTypes covers formats that are already entropy-coded; a
// second compression pass usually grows the response.
var defaultSkipContentTypes = []string{
	"image/png", "image/jpeg", "image/gif", "image/webp", "image/avif",
	"video/mp4", "video/webm", "audio/mp4", "audio/aac", "audio/ogg",
	"application/zip", "application/gzip", "application/x-bzip2",
	"application/x-7z-compressed", "application/x-rar-compressed",
	"application/pdf",
	"application/grpc", // gRPC manages its own per-message compression.
}

// pickEncoding parses Accept-Encoding and returns the best encoding we
// support, honoring q-values. Returns encodingNone when no supported encoder
// is acceptable.
func pickEncoding(header string) encoding {
	if header == "" {
		return encodingNone
	}
	best := encodingNone
	var bestQ float64 = -1
	for _, part := range strings.Split(header, ",") {
		name, q := parseAcceptEncoding(strings.TrimSpace(part))
		if q == 0 {
			continue
		}
		var candidate encoding
		switch name {
		case "br":
			candidate = encodingBrotli
		case "zstd":
			candidate = encodingZstd
		case "gzip":
			candidate = encodingGzip
		default:
			continue
		}
		// Higher q wins; on tie, prefer the better encoder (Brotli > zstd > gzip).
		if q > bestQ || (q == bestQ && candidate > best) {
			best = candidate
			bestQ = q
		}
	}
	return best
}

// parseAcceptEncoding splits a single Accept-Encoding directive like
// "gzip;q=0.5" into name and quality. Missing q defaults to 1.0.
func parseAcceptEncoding(s string) (string, float64) {
	parts := strings.Split(s, ";")
	name := strings.ToLower(strings.TrimSpace(parts[0]))
	q := 1.0
	for _, p := range parts[1:] {
		p = strings.TrimSpace(p)
		if rest, ok := strings.CutPrefix(p, "q="); ok {
			if v, err := strconv.ParseFloat(rest, 64); err == nil {
				q = v
			}
		}
	}
	return name, q
}

// compressingWriter wraps http.ResponseWriter and rewrites response bytes
// through the chosen encoder. We buffer until either WriteHeader fires or
// the buffer exceeds the configured threshold, at which point we commit
// the encoding decision and stream through the encoder.
type compressingWriter struct {
	http.ResponseWriter
	enc       encoding
	minBytes  int
	skipTypes map[string]bool

	wroteHeader bool
	committed   bool // true once we've decided whether to compress
	compress    bool // final decision (set when committed = true)
	encoder     io.WriteCloser
	statusCode  int
	buf         []byte
}

func (c *compressingWriter) WriteHeader(code int) {
	if c.wroteHeader {
		return
	}
	c.wroteHeader = true
	c.statusCode = code
	// We can't write the header yet — we may need to add or change
	// Content-Encoding, and we may need to remove Content-Length.
}

func (c *compressingWriter) Write(p []byte) (int, error) {
	if !c.wroteHeader {
		c.WriteHeader(http.StatusOK)
	}
	if c.committed {
		if c.compress {
			return c.encoder.Write(p)
		}
		return c.ResponseWriter.Write(p)
	}
	// Buffer until we've seen enough bytes to decide whether compression
	// is worth it.
	c.buf = append(c.buf, p...)
	if len(c.buf) < c.minBytes {
		return len(p), nil
	}
	return len(p), c.commit()
}

// Flush forces an early commit so streaming responses don't wait for the
// buffer threshold. After commit, calls to the underlying writer's Flusher
// propagate through.
func (c *compressingWriter) Flush() {
	if !c.committed {
		_ = c.commit()
	}
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// commit decides whether to compress, writes headers, and replays the
// buffer through the chosen path. Safe to call once; subsequent writes
// go directly to the committed sink.
func (c *compressingWriter) commit() error {
	c.committed = true
	c.compress = c.shouldCompress()

	if c.compress {
		c.ResponseWriter.Header().Set("Content-Encoding", c.enc.name())
		c.ResponseWriter.Header().Del("Content-Length")
		c.ResponseWriter.Header().Add("Vary", "Accept-Encoding")
	}

	if c.statusCode == 0 {
		c.statusCode = http.StatusOK
	}
	c.ResponseWriter.WriteHeader(c.statusCode)

	if !c.compress {
		if len(c.buf) > 0 {
			_, err := c.ResponseWriter.Write(c.buf)
			c.buf = nil
			return err
		}
		return nil
	}

	c.encoder = newEncoder(c.enc, c.ResponseWriter)
	if len(c.buf) > 0 {
		_, err := c.encoder.Write(c.buf)
		c.buf = nil
		if err != nil {
			return err
		}
	}
	return nil
}

// shouldCompress applies the skip rules: upstream-supplied
// Content-Encoding, blacklisted Content-Type, and any response too small
// to be worth it.
func (c *compressingWriter) shouldCompress() bool {
	if c.ResponseWriter.Header().Get("Content-Encoding") != "" {
		return false
	}
	if c.statusCode == http.StatusNoContent || c.statusCode == http.StatusNotModified {
		return false
	}
	ct := strings.ToLower(c.ResponseWriter.Header().Get("Content-Type"))
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	if c.skipTypes[ct] {
		return false
	}
	if len(c.buf) > 0 && len(c.buf) < c.minBytes {
		return false
	}
	return true
}

// Close flushes the encoder (if active) and releases pooled resources.
// Idempotent; safe to call multiple times.
func (c *compressingWriter) Close() {
	if !c.committed {
		_ = c.commit()
	}
	if c.encoder != nil {
		_ = c.encoder.Close()
		releaseEncoder(c.enc, c.encoder)
		c.encoder = nil
	}
}

// Encoder pools — compression encoders allocate state that's expensive to
// build per request. Reusing them across requests cuts allocations in the
// hot path significantly.
var (
	gzipPool   = sync.Pool{New: func() any { w, _ := gzip.NewWriterLevel(io.Discard, gzip.DefaultCompression); return w }}
	brotliPool = sync.Pool{New: func() any { return brotli.NewWriterLevel(io.Discard, brotli.DefaultCompression) }}
	zstdPool   = sync.Pool{New: func() any { w, _ := zstd.NewWriter(io.Discard); return w }}
)

func newEncoder(e encoding, w io.Writer) io.WriteCloser {
	switch e {
	case encodingGzip:
		gz := gzipPool.Get().(*gzip.Writer)
		gz.Reset(w)
		return gz
	case encodingBrotli:
		br := brotliPool.Get().(*brotli.Writer)
		br.Reset(w)
		return br
	case encodingZstd:
		zw := zstdPool.Get().(*zstd.Encoder)
		zw.Reset(w)
		return zw
	default:
		return nopWriteCloser{w}
	}
}

func releaseEncoder(e encoding, wc io.WriteCloser) {
	switch e {
	case encodingGzip:
		if gz, ok := wc.(*gzip.Writer); ok {
			gzipPool.Put(gz)
		}
	case encodingBrotli:
		if br, ok := wc.(*brotli.Writer); ok {
			brotliPool.Put(br)
		}
	case encodingZstd:
		if zw, ok := wc.(*zstd.Encoder); ok {
			zstdPool.Put(zw)
		}
	}
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

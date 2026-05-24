# Examples

Self-contained configurations demonstrating one feature each. All examples are
validated by the config loader at startup; check syntax without serving with:

```sh
l7rp --check --config examples/<name>.yaml
```

| File | Demonstrates |
|---|---|
| [`two-backends.yaml`](two-backends.yaml) | Two-backend pool with P2C+EWMA selection, active health checks, retry with hedging |
| [`tls-autocert.yaml`](tls-autocert.yaml) | TLS termination with Let's Encrypt autocert + a manual fallback certificate |
| [`websocket.yaml`](websocket.yaml) | WebSocket upgrade pass-through with `least-conn` selection for long-lived sessions |
| [`per-route-rate-limit.yaml`](per-route-rate-limit.yaml) | Different rate limits and header transforms per route |
| [`consistent-hash.yaml`](consistent-hash.yaml) | Consistent-hash with bounded loads, keyed by a session cookie |
| [`upstream-mtls.yaml`](upstream-mtls.yaml) | TLS and mTLS to backends — CA pinning, client cert, SNI override |
| [`compression.yaml`](compression.yaml) | Response compression with gzip/brotli/zstd content negotiation |
| [`http3.yaml`](http3.yaml) | HTTP/3 (QUIC) over UDP, alongside h1/h2 on the same port |
| [`tcp-loadbalancer.yaml`](tcp-loadbalancer.yaml) | Layer-4 TCP load balancing — Redis, Postgres, anything protocol-agnostic |

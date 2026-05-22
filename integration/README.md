# integration

End-to-end tests that drive the real `l7rp` binary against real backends.
Two build tags are available:

| Tag | Purpose | Runtime |
|---|---|---|
| `integration` | Smoke + behavioral tests run on every CI build. | ~7 seconds |
| `soak` | Longer scenarios that drive sustained load, restart backends, or fire many SIGHUPs. | ~25 seconds |

```sh
# Fast suite (CI-eligible)
go test -tags=integration -count=1 -timeout=5m ./integration/...

# Soak / chaos suite (manual)
go test -tags=soak -count=1 -timeout=5m ./integration/...
```

Tests build the binary once in `TestMain`, then for each test:

1. Spin up one or more `httptest` backends with controllable status / availability.
2. Write a config to a temp dir referencing the backends' assigned ports.
3. Start the proxy as a subprocess; wait for `/-/ready`.
4. Send real HTTP requests; assert observed behavior or metrics.
5. `t.Cleanup` sends SIGTERM and waits for graceful shutdown.

Failures surface the proxy's stdout/stderr in the test output so the failure
mode is debuggable without re-running.

The soak suite is deliberately excluded from CI: it drives ~30k requests per
run, which is sensitive to local ephemeral-port pressure on shared CI runners.

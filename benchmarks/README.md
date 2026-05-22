# Benchmarks

A reproducible harness for measuring `l7rp` performance side-by-side with
nginx, plus a behavioral comparison that highlights where the two proxies
differ structurally.

## Why both raw throughput and behavior

Raw throughput numbers compress two different proxies into a single dimension
that often favors whichever is more aggressive about defaults. Numbers from a
laptop don't generalize. Both proxies will saturate at very different points
depending on tuning, kernel version, and TCP-stack settings.

The behavioral comparison is the more interesting half: it shows what each
proxy *does* in the presence of failures, which is what determines whether
either tool is appropriate for a given deployment.

## What's measured

| Scenario | Tool | What it tells you |
|---|---|---|
| Steady-state RPS, p50 / p99 latency on a hello-world backend | [`hey`](https://github.com/rakyll/hey) | How much overhead each proxy adds to a trivial request |
| Time to eject a misbehaving upstream | curl in a loop | How quickly each proxy stops sending traffic to a backend that started returning 5xx |
| Reload-under-load — is steady-state traffic disrupted? | `hey` + SIGHUP | Whether config reloads cause connection drops or 5xx |

Latency-tail at high concurrency is the metric portfolio reviewers most often
care about, and it's also where reverse-proxy implementations differ most.

## Equivalent configurations

Both proxies are set up with the same logical surface:

- One listener on `127.0.0.1:8080`
- One pool of 3 upstreams on `127.0.0.1:9001-9003`
- Round-robin selection
- Active health check on `/healthz` every 2s
- No TLS (so we're measuring proxy overhead, not handshake cost)

The configs are in [`l7rp.yaml`](l7rp.yaml) and [`nginx.conf`](nginx.conf).
Diffs between them are differences in expressiveness, not behavior.

## Running it

```sh
# One-time setup
brew install nginx hey
go install github.com/rakyll/hey@latest

# From the repo root
cd benchmarks
./run.sh throughput   # steady-state RPS comparison
./run.sh ejection     # how fast does each proxy notice a dead backend
./run.sh reload       # SIGHUP under load
```

Each scenario produces a short summary; no numbers are committed to this
repository because they're hardware-dependent and will be misleading if read
out of context.

## Bias notes

- nginx has been hand-tuned for two decades and is written in C; expect it
  to win raw throughput on a hello-world workload by a clear margin
- `l7rp`'s slgo allocator and standard `net/http` server are competitive
  but not optimal; the p99 tail under high concurrency reflects Go's GC
  pauses, which a real deployment would tune (`GOGC`, `GOMEMLIMIT`)
- The ejection test is the one place `l7rp` is structurally faster: active
  probes (default 2s interval) plus passive EWMA scoring catch failing
  upstreams within seconds, while open-source nginx without `nginx-plus`
  has no built-in passive ejection (you'd need `lua-resty-upstream-healthcheck`
  or similar)

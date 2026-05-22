#!/usr/bin/env bash
# Side-by-side benchmark harness for l7rp and nginx.
#
# Usage:
#   ./run.sh throughput   - steady-state RPS comparison
#   ./run.sh ejection     - how fast does each proxy notice a dead backend
#   ./run.sh reload       - SIGHUP under sustained load (l7rp only)
#
# Requires: nginx, hey, go (>=1.25), and the l7rp binary built at ../l7rp.

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$BENCH_DIR/.." && pwd)"

L7RP_BIN="$REPO_ROOT/l7rp"
BACKEND_BIN="$BENCH_DIR/backend"

# l7rp listens on :8080, nginx on :8081, backends on :9001-9003.
L7RP_ADDR="127.0.0.1:8080"
NGINX_ADDR="127.0.0.1:8081"

cleanup() {
    pkill -f "$BACKEND_BIN" 2>/dev/null || true
    pkill -f "$L7RP_BIN" 2>/dev/null || true
    if [[ -f /tmp/l7rp-bench-nginx.pid ]]; then
        nginx -p "$BENCH_DIR" -c "$BENCH_DIR/nginx.conf" -s quit 2>/dev/null || true
    fi
}
trap cleanup EXIT

require() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "missing required tool: $1" >&2
        echo "install with: $2" >&2
        exit 1
    }
}

require nginx "brew install nginx"
require hey   "go install github.com/rakyll/hey@latest"

ensure_built() {
    if [[ ! -x "$L7RP_BIN" ]]; then
        echo "building l7rp..."
        (cd "$REPO_ROOT" && go build -o l7rp ./cmd/l7rp)
    fi
    if [[ ! -x "$BACKEND_BIN" ]]; then
        echo "building benchmark backend..."
        (cd "$REPO_ROOT" && go build -o "$BACKEND_BIN" ./benchmarks/backend)
    fi
}

start_backends() {
    "$BACKEND_BIN" --addr 127.0.0.1:9001 >/tmp/l7rp-bench-be1.log 2>&1 &
    "$BACKEND_BIN" --addr 127.0.0.1:9002 >/tmp/l7rp-bench-be2.log 2>&1 &
    "$BACKEND_BIN" --addr 127.0.0.1:9003 >/tmp/l7rp-bench-be3.log 2>&1 &
    sleep 1
}

start_l7rp() {
    "$L7RP_BIN" --config "$BENCH_DIR/l7rp.yaml" --metrics-bind 127.0.0.1:9090 \
        >/tmp/l7rp-bench-l7rp.log 2>&1 &
    L7RP_PID=$!
    # Wait for /-/ready.
    for _ in {1..20}; do
        if curl -sf http://127.0.0.1:9090/-/ready >/dev/null; then return; fi
        sleep 0.2
    done
    echo "l7rp never became ready" >&2
    exit 1
}

start_nginx() {
    nginx -p "$BENCH_DIR" -c "$BENCH_DIR/nginx.conf" &
    NGINX_PID=$!
    sleep 0.5
}

throughput_scenario() {
    ensure_built
    start_backends
    start_l7rp
    start_nginx

    local DURATION=15s
    local CONNS=64

    echo
    echo "=== l7rp ($L7RP_ADDR) ==="
    hey -z "$DURATION" -c "$CONNS" -disable-keepalive=false \
        -h2=false "http://$L7RP_ADDR/" | grep -E "(Requests|Average|Latency|99%|50%)"

    echo
    echo "=== nginx ($NGINX_ADDR) ==="
    hey -z "$DURATION" -c "$CONNS" -disable-keepalive=false \
        -h2=false "http://$NGINX_ADDR/" | grep -E "(Requests|Average|Latency|99%|50%)"
}

ejection_scenario() {
    ensure_built
    start_backends
    start_l7rp
    start_nginx

    echo "warmup: 2s of clean traffic to populate health state..."
    hey -z 2s -c 16 "http://$L7RP_ADDR/" >/dev/null 2>&1 &
    hey -z 2s -c 16 "http://$NGINX_ADDR/" >/dev/null 2>&1 &
    wait

    echo
    echo "killing backend on :9001 (curl driving against both proxies)..."
    pkill -f "$BACKEND_BIN --addr 127.0.0.1:9001"

    echo
    echo "=== l7rp — time-to-recover after backend death ==="
    measure_recovery "$L7RP_ADDR"

    echo
    echo "=== nginx — time-to-recover after backend death ==="
    measure_recovery "$NGINX_ADDR"
}

measure_recovery() {
    local addr="$1"
    local start_time
    start_time=$(date +%s.%N)
    local fail=0
    for i in $(seq 1 100); do
        local code
        code=$(curl -s -o /dev/null -w "%{http_code}" -m 2 "http://$addr/" || echo "0")
        if [[ "$code" != "200" ]]; then
            fail=$((fail+1))
        elif [[ $fail -gt 2 ]]; then
            local now
            now=$(date +%s.%N)
            printf "  recovered after %s seconds and %d failed requests\n" \
                "$(echo "$now - $start_time" | bc)" "$fail"
            return
        fi
        sleep 0.1
    done
    echo "  did not stabilize within 10s; fails=$fail"
}

reload_scenario() {
    ensure_built
    start_backends
    start_l7rp

    echo "driving 10s of load while SIGHUP'ing every 1s..."
    hey -z 10s -c 32 "http://$L7RP_ADDR/" > /tmp/l7rp-bench-hey.txt &
    HEY_PID=$!

    for _ in {1..10}; do
        sleep 1
        kill -HUP "$L7RP_PID"
    done

    wait "$HEY_PID" || true
    grep -E "(Requests|Latency|99%|Status|Error|distribution)" /tmp/l7rp-bench-hey.txt | head -20
}

case "${1:-help}" in
    throughput) throughput_scenario ;;
    ejection)   ejection_scenario   ;;
    reload)     reload_scenario     ;;
    *)
        echo "usage: $0 {throughput|ejection|reload}" >&2
        exit 2
        ;;
esac

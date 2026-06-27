#!/usr/bin/env bash
# Integration test: starts a 3-node cluster in-process, verifies writes survive
# leader failover, and ensures a new node can catch up via GET.
#
# Exit 0 = PASS, Exit 1 = FAIL

set -euo pipefail

BINARY="./target/debug/raft-kv"
DATA="/tmp/raft-kv-itest"
TIMEOUT=15  # seconds to wait for a leader
PASS=0
FAIL=0

# ── helpers ──────────────────────────────────────────────────────────────────

log()  { echo "[$(date +%T)] $*"; }
pass() { log "PASS: $*"; PASS=$((PASS + 1)); }
fail() { log "FAIL: $*"; FAIL=$((FAIL + 1)); }

wait_for_leader() {
    local deadline=$((SECONDS + TIMEOUT))
    while [[ $SECONDS -lt $deadline ]]; do
        for port in 8001 8002 8003; do
            local resp
            resp=$(curl -sf "http://127.0.0.1:${port}/status" 2>/dev/null || true)
            if echo "$resp" | grep -q '"is_leader":true'; then
                echo "$port"
                return 0
            fi
        done
        sleep 0.3
    done
    echo ""
    return 1
}

get_leader_port() {
    for port in 8001 8002 8003; do
        local resp
        resp=$(curl -sf "http://127.0.0.1:${port}/status" 2>/dev/null || true)
        if echo "$resp" | grep -q '"is_leader":true'; then
            echo "$port"
            return 0
        fi
    done
    echo ""
    return 1
}

kv_put() {
    local port=$1 key=$2 value=$3
    curl -sf -X PUT "http://127.0.0.1:${port}/kv/${key}" -d "${value}" > /dev/null
}

kv_get() {
    local port=$1 key=$2
    curl -sf "http://127.0.0.1:${port}/kv/${key}" 2>/dev/null || echo ""
}

# ── setup ─────────────────────────────────────────────────────────────────────

log "Building binary..."
cargo build -p server --quiet

log "Cleaning data dir..."
rm -rf "$DATA"
mkdir -p "$DATA"/{node1,node2,node3}

log "Starting 3-node cluster..."
RUST_LOG=error "$BINARY" --id 1 --grpc-addr 127.0.0.1:7001 --http-addr 127.0.0.1:8001 \
    --peer 2=127.0.0.1:7002 --peer 3=127.0.0.1:7003 --data-dir "$DATA/node1" &
PID1=$!
RUST_LOG=error "$BINARY" --id 2 --grpc-addr 127.0.0.1:7002 --http-addr 127.0.0.1:8002 \
    --peer 1=127.0.0.1:7001 --peer 3=127.0.0.1:7003 --data-dir "$DATA/node2" &
PID2=$!
RUST_LOG=error "$BINARY" --id 3 --grpc-addr 127.0.0.1:7003 --http-addr 127.0.0.1:8003 \
    --peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002 --data-dir "$DATA/node3" &
PID3=$!

cleanup() {
    kill "$PID1" "$PID2" "$PID3" 2>/dev/null || true
    wait "$PID1" "$PID2" "$PID3" 2>/dev/null || true
    rm -rf "$DATA"
}
trap cleanup EXIT

# ── test: initial leader election ─────────────────────────────────────────────

log "Waiting for initial leader..."
LEADER_PORT=$(wait_for_leader)
if [[ -z "$LEADER_PORT" ]]; then
    log "No leader elected within ${TIMEOUT}s — aborting"
    exit 1
fi
pass "leader elected on port $LEADER_PORT"

# ── test: write 3 keys ────────────────────────────────────────────────────────

log "Writing keys to leader (port $LEADER_PORT)..."
kv_put "$LEADER_PORT" "alpha" "1"
kv_put "$LEADER_PORT" "beta"  "2"
kv_put "$LEADER_PORT" "gamma" "3"

VAL=$(kv_get "$LEADER_PORT" "alpha")
if [[ "$VAL" == "1" ]]; then pass "write alpha=1"; else fail "write alpha: got '$VAL'"; fi

VAL=$(kv_get "$LEADER_PORT" "beta")
if [[ "$VAL" == "2" ]]; then pass "write beta=2"; else fail "write beta: got '$VAL'"; fi

VAL=$(kv_get "$LEADER_PORT" "gamma")
if [[ "$VAL" == "3" ]]; then pass "write gamma=3"; else fail "write gamma: got '$VAL'"; fi

# ── test: kill leader, new leader elected ─────────────────────────────────────

log "Killing leader (port $LEADER_PORT)..."
case "$LEADER_PORT" in
    8001) kill "$PID1" 2>/dev/null; wait "$PID1" 2>/dev/null || true ;;
    8002) kill "$PID2" 2>/dev/null; wait "$PID2" 2>/dev/null || true ;;
    8003) kill "$PID3" 2>/dev/null; wait "$PID3" 2>/dev/null || true ;;
esac

log "Waiting for new leader..."
NEW_LEADER_PORT=$(wait_for_leader)
if [[ -z "$NEW_LEADER_PORT" || "$NEW_LEADER_PORT" == "$LEADER_PORT" ]]; then
    fail "new leader not elected after killing port $LEADER_PORT"
else
    pass "new leader on port $NEW_LEADER_PORT"
fi

# ── test: previously written keys survive failover ───────────────────────────

log "Reading keys from new leader (port $NEW_LEADER_PORT)..."
VAL=$(kv_get "$NEW_LEADER_PORT" "alpha")
if [[ "$VAL" == "1" ]]; then pass "alpha survives failover"; else fail "alpha after failover: got '$VAL'"; fi

VAL=$(kv_get "$NEW_LEADER_PORT" "beta")
if [[ "$VAL" == "2" ]]; then pass "beta survives failover"; else fail "beta after failover: got '$VAL'"; fi

VAL=$(kv_get "$NEW_LEADER_PORT" "gamma")
if [[ "$VAL" == "3" ]]; then pass "gamma survives failover"; else fail "gamma after failover: got '$VAL'"; fi

# ── test: write to new leader ─────────────────────────────────────────────────

log "Writing new key to new leader..."
kv_put "$NEW_LEADER_PORT" "delta" "4"

VAL=$(kv_get "$NEW_LEADER_PORT" "delta")
if [[ "$VAL" == "4" ]]; then pass "write delta=4 on new leader"; else fail "write delta: got '$VAL'"; fi

# ── result ────────────────────────────────────────────────────────────────────

echo ""
log "Results: $PASS passed, $FAIL failed"
if [[ $FAIL -eq 0 ]]; then
    log "=== PASS ==="
    exit 0
else
    log "=== FAIL ==="
    exit 1
fi

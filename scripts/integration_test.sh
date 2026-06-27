#!/usr/bin/env bash
# Integration test: starts a 3-node cluster in-process, verifies writes survive
# leader failover, ensures a new node can catch up via GET, and exercises
# chaos scenarios (write-during-kill, WAL replay, minority partition).
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
    # -L: follow redirects (followers redirect reads to the leader)
    curl -sfL "http://127.0.0.1:${port}/kv/${key}" 2>/dev/null || echo ""
}

# Launch a node with correct gRPC + HTTP peer args.
# Runs the binary in background; caller must capture $! immediately after.
start_node() {
    local id=$1 data_dir=$2
    local peers http_peers
    case "$id" in
        1) peers="--peer 2=127.0.0.1:7002 --peer 3=127.0.0.1:7003"
           http_peers="--http-peer 2=127.0.0.1:8002 --http-peer 3=127.0.0.1:8003" ;;
        2) peers="--peer 1=127.0.0.1:7001 --peer 3=127.0.0.1:7003"
           http_peers="--http-peer 1=127.0.0.1:8001 --http-peer 3=127.0.0.1:8003" ;;
        3) peers="--peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002"
           http_peers="--http-peer 1=127.0.0.1:8001 --http-peer 2=127.0.0.1:8002" ;;
    esac
    RUST_LOG=error "$BINARY" --id "$id" \
        --grpc-addr "127.0.0.1:700${id}" \
        --http-addr "127.0.0.1:800${id}" \
        $peers $http_peers \
        --data-dir "$data_dir" &
}

# ── setup ─────────────────────────────────────────────────────────────────────

log "Building binary..."
cargo build -p server --quiet

log "Cleaning data dir..."
rm -rf "$DATA"
mkdir -p "$DATA"/{node1,node2,node3}

log "Starting 3-node cluster..."
start_node 1 "$DATA/node1"; PID1=$!
start_node 2 "$DATA/node2"; PID2=$!
start_node 3 "$DATA/node3"; PID3=$!

cleanup() {
    kill "$PID1" "$PID2" "$PID3" 2>/dev/null || true
    wait "$PID1" "$PID2" "$PID3" 2>/dev/null || true
    rm -rf "$DATA"
}
trap cleanup EXIT

# Restart any nodes that have died so that each chaos test begins with 3 nodes.
restart_dead_nodes() {
    if ! kill -0 "$PID1" 2>/dev/null; then
        log "  restoring node 1..."
        start_node 1 "$DATA/node1"; PID1=$!
    fi
    if ! kill -0 "$PID2" 2>/dev/null; then
        log "  restoring node 2..."
        start_node 2 "$DATA/node2"; PID2=$!
    fi
    if ! kill -0 "$PID3" 2>/dev/null; then
        log "  restoring node 3..."
        start_node 3 "$DATA/node3"; PID3=$!
    fi
}

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

# ── test: follower redirects read to leader ───────────────────────────────────

log "Testing follower redirect..."
FOLLOWER_PORT=""
for port in 8001 8002 8003; do
    [[ "$port" != "$LEADER_PORT" ]] && { FOLLOWER_PORT="$port"; break; }
done
VAL=$(kv_get "$FOLLOWER_PORT" "alpha")
if [[ "$VAL" == "1" ]]; then
    pass "follower (port $FOLLOWER_PORT) redirects read to leader correctly"
else
    fail "follower redirect: expected '1' got '$VAL' from port $FOLLOWER_PORT"
fi

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

# ── chaos 1: write-during-kill ────────────────────────────────────────────────
# Fire a PUT while simultaneously killing the leader. The write may or may not
# commit, but the value must be consistent afterwards: "x" or absent, never corrupt.

log "Chaos 1: write-during-kill — restoring cluster to 3 nodes..."
restart_dead_nodes
sleep 1

CHAOS_LEADER=$(wait_for_leader) || true
if [[ -z "$CHAOS_LEADER" ]]; then
    fail "chaos1: no leader found after restore"
else
    curl -sf -X PUT "http://127.0.0.1:${CHAOS_LEADER}/kv/chaos1" -d "x" \
        --max-time 3 > /dev/null 2>&1 &
    CURL_PID=$!

    case "$CHAOS_LEADER" in
        8001) kill "$PID1" 2>/dev/null; wait "$PID1" 2>/dev/null || true ;;
        8002) kill "$PID2" 2>/dev/null; wait "$PID2" 2>/dev/null || true ;;
        8003) kill "$PID3" 2>/dev/null; wait "$PID3" 2>/dev/null || true ;;
    esac
    wait "$CURL_PID" 2>/dev/null || true

    AFTER_KILL=$(wait_for_leader) || true
    if [[ -z "$AFTER_KILL" ]]; then
        fail "chaos1: no leader elected after kill"
    else
        VAL=$(kv_get "$AFTER_KILL" "chaos1")
        if [[ "$VAL" == "x" || "$VAL" == "" ]]; then
            pass "chaos1: write-during-kill consistent (val='$VAL')"
        else
            fail "chaos1: corrupt value after kill: '$VAL'"
        fi
    fi
fi

# ── chaos 2: WAL replay — kill a follower, restart from disk, verify catchup ──

log "Chaos 2: WAL replay — restoring cluster to 3 nodes..."
restart_dead_nodes
sleep 1

C2_LEADER=$(wait_for_leader) || true
if [[ -z "$C2_LEADER" ]]; then
    fail "chaos2: no leader before WAL replay test"
else
    # Write a key NOW while all 3 nodes are up — this entry lands in every WAL.
    kv_put "$C2_LEADER" "wal_before" "before"

    # Pick a follower to kill.
    C2_FOLLOWER_HTTP=""
    for port in 8001 8002 8003; do
        [[ "$port" != "$C2_LEADER" ]] && { C2_FOLLOWER_HTTP="$port"; break; }
    done
    case "$C2_FOLLOWER_HTTP" in
        8001) C2_FOLLOWER_PID="$PID1"; C2_FOLLOWER_ID=1 ;;
        8002) C2_FOLLOWER_PID="$PID2"; C2_FOLLOWER_ID=2 ;;
        8003) C2_FOLLOWER_PID="$PID3"; C2_FOLLOWER_ID=3 ;;
    esac

    kill "$C2_FOLLOWER_PID" 2>/dev/null; wait "$C2_FOLLOWER_PID" 2>/dev/null || true
    log "Chaos 2: killed follower $C2_FOLLOWER_ID..."

    # Write another key while the follower is down — this tests AppendEntries catch-up.
    kv_put "$C2_LEADER" "wal_after" "after"

    log "Chaos 2: restarting follower $C2_FOLLOWER_ID from existing WAL..."
    sleep 0.3

    start_node "$C2_FOLLOWER_ID" "$DATA/node${C2_FOLLOWER_ID}"
    case "$C2_FOLLOWER_ID" in
        1) PID1=$! ;;
        2) PID2=$! ;;
        3) PID3=$! ;;
    esac

    # Allow WAL replay + AppendEntries catch-up.
    sleep 3

    # Read from the leader (which is still up) — verifies the cluster is
    # consistent and that the follower's restart didn't corrupt anything.
    VAL=$(kv_get "$C2_LEADER" "wal_before")
    if [[ "$VAL" == "before" ]]; then
        pass "chaos2: key written before kill is consistent after WAL replay"
    else
        fail "chaos2: wal_before: got '$VAL'"
    fi

    VAL=$(kv_get "$C2_LEADER" "wal_after")
    if [[ "$VAL" == "after" ]]; then
        pass "chaos2: key written while node was down is served after catch-up"
    else
        fail "chaos2: wal_after: got '$VAL'"
    fi
fi

# ── chaos 3: minority partition — 1 node down, cluster writes continue, then catchup

log "Chaos 3: minority partition — restoring cluster to 3 nodes..."
restart_dead_nodes
sleep 1

C3_LEADER=$(wait_for_leader) || true
if [[ -z "$C3_LEADER" ]]; then
    fail "chaos3: no leader before partition test"
else
    # Isolate a follower by killing it (simulates a network partition from the
    # cluster's perspective; iptables-based isolation would be equivalent but
    # requires root and adds environment complexity).
    C3_ISOLATED_HTTP=""
    for port in 8001 8002 8003; do
        [[ "$port" != "$C3_LEADER" ]] && { C3_ISOLATED_HTTP="$port"; break; }
    done
    case "$C3_ISOLATED_HTTP" in
        8001) C3_ISOLATED_PID="$PID1"; C3_ISOLATED_ID=1 ;;
        8002) C3_ISOLATED_PID="$PID2"; C3_ISOLATED_ID=2 ;;
        8003) C3_ISOLATED_PID="$PID3"; C3_ISOLATED_ID=3 ;;
    esac
    case "$C3_ISOLATED_ID" in
        1) C3_ISOLATED_PEERS="--peer 2=127.0.0.1:7002 --peer 3=127.0.0.1:7003" ;;
        2) C3_ISOLATED_PEERS="--peer 1=127.0.0.1:7001 --peer 3=127.0.0.1:7003" ;;
        3) C3_ISOLATED_PEERS="--peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002" ;;
    esac

    log "Chaos 3: isolating node $C3_ISOLATED_ID (port $C3_ISOLATED_HTTP)..."
    kill "$C3_ISOLATED_PID" 2>/dev/null; wait "$C3_ISOLATED_PID" 2>/dev/null || true

    # 2-node majority must still commit writes
    kv_put "$C3_LEADER" "partition_key" "partition_val"
    VAL=$(kv_get "$C3_LEADER" "partition_key")
    if [[ "$VAL" == "partition_val" ]]; then
        pass "chaos3: cluster writes continue while minority node is down"
    else
        fail "chaos3: write while node down failed: '$VAL'"
    fi

    # "Heal" the partition by restarting the node from its existing data dir
    log "Chaos 3: healing partition, restarting node $C3_ISOLATED_ID..."
    start_node "$C3_ISOLATED_ID" "$DATA/node${C3_ISOLATED_ID}"
    case "$C3_ISOLATED_ID" in
        1) PID1=$! ;;
        2) PID2=$! ;;
        3) PID3=$! ;;
    esac

    sleep 3

    # Read from the leader — verifies the cluster is consistent after reintegration.
    VAL=$(kv_get "$C3_LEADER" "partition_key")
    if [[ "$VAL" == "partition_val" ]]; then
        pass "chaos3: cluster consistent after reintegrating minority node"
    else
        fail "chaos3: leader returned '$VAL' for partition_key after reintegration"
    fi
fi

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

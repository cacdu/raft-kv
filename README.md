# raft-kv

Distributed key-value store built on the Raft consensus algorithm, implemented from scratch in Rust.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Client                               │
│              curl / raft-kv-cli                             │
└─────────────────────┬───────────────────────────────────────┘
                      │ HTTP (GET/PUT/DELETE /kv/{key})
          ┌───────────▼───────────┐
          │       Node 1          │  ◄──gRPC──►  Node 2
          │   Axum HTTP server    │  ◄──gRPC──►  Node 3
          │   Raft state machine  │
          │   WAL + KV store      │
          └───────────────────────┘
```

Three nodes form a cluster. One is elected Leader via Raft. All writes go through the leader, which replicates entries to followers before committing. Reads use **linearizable ReadIndex**: the leader captures its commit index at request time and waits until that index is applied before serving the value.

Non-leader nodes return `307 Redirect` to the leader's HTTP address (path-preserving), so any node can be addressed by a client.

## Crates

| Crate | Role |
|---|---|
| `raft` | Pure Raft state machine — no I/O, no async |
| `storage` | Write-Ahead Log (WAL) + KV state machine |
| `server` | Axum HTTP API + tonic gRPC peer RPCs + Raft driver |
| `client` | CLI tool (`raft-kv-cli`) |

### `crates/raft` — The algorithm

The Raft state machine is intentionally pure: it takes a `Message` in and returns a `Ready` struct out. No network calls, no disk I/O, no async. This makes it trivially unit-testable.

```
node.step(Message::Tick)           → Ready { messages, entries_to_persist, entries_to_apply }
node.step(Message::Propose { .. }) → Ready { .. }
node.step(Message::AppendEntries)  → Ready { .. }
```

The caller (`NodeHandle`) is responsible for acting on `Ready`:
1. Persist `entries_to_persist` to WAL before sending any messages
2. Apply `entries_to_apply` to the KV state machine
3. Fan out `messages` to peer nodes via gRPC

### `crates/storage`

**WAL record format** (binary, little-endian):
```
[4 bytes] payload length (u32)
[4 bytes] CRC32 checksum
[N bytes] JSON-encoded WalRecord (HardState | Entry | Snapshot)
```

Records with invalid checksums are discarded during recovery (truncated tail). The WAL is append-only and fsynced on every write.

**KvStore** is an in-memory `BTreeMap<String, String>`. Commands (`Set` / `Delete`) are JSON-encoded and stored as the `command` field of each `LogEntry`.

### `crates/server`

The server binary drives the Raft state machine from a tokio event loop:

- **Tick loop**: calls `node.step(Tick)` every 10ms to advance election and heartbeat timers.
- **gRPC server** (`tonic`): receives `RequestVote` and `AppendEntries` RPCs from peers.
- **gRPC client** (`peer.rs`): sends Raft messages to each peer node.
- **HTTP server** (`axum`): exposes the KV API to clients.

### `crates/client`

```bash
raft-kv-cli --addr http://127.0.0.1:8001 set foo bar
raft-kv-cli --addr http://127.0.0.1:8001 get foo
raft-kv-cli --addr http://127.0.0.1:8001 delete foo
raft-kv-cli --addr http://127.0.0.1:8001 status
```

## Running a cluster

```bash
# Build
cargo build

# Start 3 nodes in separate terminals
make node1
make node2
make node3
```

Or manually:
```bash
# --peer        id=grpc_addr   — used for Raft peer RPCs
# --http-peer   id=http_addr   — used to redirect clients to the leader
RUST_LOG=info cargo run -p server -- \
  --id 1 \
  --grpc-addr 127.0.0.1:7001 --http-addr 127.0.0.1:8001 \
  --peer 2=127.0.0.1:7002 --peer 3=127.0.0.1:7003 \
  --http-peer 2=127.0.0.1:8002 --http-peer 3=127.0.0.1:8003

# In another terminal
raft-kv-cli set hello world
raft-kv-cli get hello   # → world

# Prometheus metrics
curl http://127.0.0.1:8001/metrics
```

## Prerequisites

```bash
# Rust stable
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# protoc (for tonic gRPC codegen)
# Arch:   sudo pacman -S protobuf
# Debian: sudo apt install protobuf-compiler
# macOS:  brew install protobuf
```

## Stack

| Layer | Technology |
|---|---|
| Consensus | Raft (from scratch — no raft-rs) |
| gRPC | tonic 0.12 + prost 0.13 |
| HTTP | axum 0.8 |
| Async | tokio |
| Persistence | Custom WAL (CRC32 + JSON) |
| Serialization | serde_json (WAL), prost (gRPC) |
| Observability | prometheus 0.13 |

---

## What's implemented

- [x] Leader election with randomized timeouts
- [x] Log replication (AppendEntries)
- [x] Commit index advancement (quorum-based)
- [x] No-op entry on leader election (fixes stale commit index)
- [x] Fast log rollback on conflict
- [x] Log compaction with WAL snapshot record
- [x] Write-Ahead Log with CRC32 integrity check
- [x] WAL replay on startup (term, voted_for, log entries, snapshot)
- [x] KV state machine (`set` / `delete`)
- [x] HTTP API with linearizable ReadIndex reads
- [x] Path-preserving `307 Redirect` on non-leader nodes (`--http-peer` flag)
- [x] gRPC peer communication (tonic) with full response routing
- [x] Snapshot transfer via `InstallSnapshot` RPC
- [x] CLI client
- [x] Unit tests — 15 for `raft` SM, 13 for `server` (WAL replay, proposals, ReadIndex)
- [x] Integration + chaos tests (15/15): election, writes, failover, write-during-kill, WAL replay, minority partition
- [x] Prometheus metrics at `GET /metrics` — counters, gauges, histograms

## Next steps

- [ ] **Membership changes** — add/remove nodes from a running cluster (joint consensus)

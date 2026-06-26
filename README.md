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

Three nodes form a cluster. One is elected Leader via Raft. All writes go through the leader, which replicates entries to followers before committing. Reads are served locally (eventual) or through the leader (linearizable — TODO).

Non-leader nodes return `307 Redirect` pointing the client to the current leader.

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
RUST_LOG=info cargo run -p server -- \
  --id 1 --grpc-addr 127.0.0.1:7001 --http-addr 127.0.0.1:8001 \
  --peer 2=127.0.0.1:7002 --peer 3=127.0.0.1:7003

# In another terminal
raft-kv-cli set hello world
raft-kv-cli get hello   # → world
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

---

## What's implemented

- [x] Leader election with randomized timeouts
- [x] Log replication (AppendEntries)
- [x] Commit index advancement (quorum-based)
- [x] No-op entry on leader election (fixes stale commit index)
- [x] Fast log rollback on conflict
- [x] Log compaction skeleton (`RaftLog::compact`)
- [x] Write-Ahead Log with CRC32 integrity check
- [x] KV state machine (`set` / `delete`)
- [x] HTTP API with leader redirect
- [x] gRPC peer communication (tonic)
- [x] CLI client

## Next steps

### Required to be production-correct

- [ ] **WAL replay on startup** — `NodeHandle::new` receives WAL records but doesn't replay them yet. Restore `current_term`, `voted_for`, and log entries before starting the tick loop.
- [ ] **gRPC response routing** — currently gRPC handlers return empty acks. Responses (vote granted / append success) need to be sent back through the Raft state machine as `Message::RequestVoteResponse` / `Message::AppendEntriesResponse`.
- [ ] **Snapshot transfer** — when a follower is too far behind, the leader must send a full KV snapshot instead of individual log entries. Implement `InstallSnapshot` RPC.
- [ ] **Linearizable reads** — reads currently serve stale data. To guarantee linearizability, reads must go through the leader and verify it's still leader (ReadIndex or lease-read).
- [ ] **Persist hard state** — `current_term` and `voted_for` must be written to WAL before responding to any RPC, not just log entries.

### Quality of life

- [ ] **Unit tests for `raft` crate** — the pure state machine makes this straightforward. Test election, replication, and split-vote scenarios without any network.
- [ ] **Membership changes** — add/remove nodes from a running cluster (joint consensus).
- [ ] **Metrics** — expose Prometheus metrics (term, commit index, role, replication lag).
- [ ] **Chaos tests** — kill nodes randomly, verify the cluster recovers and data is consistent.

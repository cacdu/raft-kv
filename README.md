# raft-kv

[![CI](https://github.com/cacdu/raft-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/cacdu/raft-kv/actions/workflows/ci.yml)
![Rust](https://img.shields.io/badge/rust-stable-orange)
![License](https://img.shields.io/badge/license-MIT-blue)

A distributed key-value store built on a from-scratch Raft implementation in Rust.
Designed as a learning-grade TiKV — minimal, readable, and production-pattern-complete.

Runs standalone as a server, or **embeds as a library** (the rqlite/dqlite model): link `raft-kv` into each replica of your app and get a shared, linearizable KV store with a watch API — no external database. See [gambas](https://github.com/cacdu/gambas), a distributed pixel canvas built exactly that way.

```
$ curl -X PUT http://127.0.0.1:8001/kv/hello -d "world"
$ curl http://127.0.0.1:8002/kv/hello   # any node, redirects to leader
world
```

---

## What it does

- **Consensus** — full Raft: leader election, log replication, log compaction, snapshot install, membership changes (add/remove nodes at runtime)
- **Linearizable reads** — ReadIndex protocol: reads always reflect the latest committed write
- **Fault tolerance** — cluster stays available as long as a majority is up; minority partitions heal automatically
- **HTTP API** — `GET`, `PUT`, `DELETE /kv/{key}`, prefix scan `GET /kv?prefix=`, `307 Redirect` from followers to leader
- **Watch API (embedded)** — `subscribe()` streams every applied write in log order, the same sequence on every node (like etcd watches)
- **gRPC peer RPCs** — `RequestVote`, `AppendEntries`, `InstallSnapshot` via tonic
- **Durable WAL** — append-only write-ahead log with CRC32 integrity check; survives crashes
- **Observability** — Prometheus metrics at `GET /metrics`

---

## Architecture

```
  Client (curl / raft-kv-cli)
       │
       │  HTTP  (GET/PUT/DELETE /kv/{key})
       ▼
  ┌────────────┐     gRPC (RequestVote      ┌────────────┐
  │   Node 1   │◄────AppendEntries    ──────│   Node 2   │
  │  (Leader)  │     InstallSnapshot)       │ (Follower) │
  │            │────────────────────────────│            │
  │ ┌────────┐ │                            └────────────┘
  │ │  Raft  │ │
  │ │   SM   │ │     gRPC                   ┌────────────┐
  │ └────────┘ │────────────────────────────│   Node 3   │
  │ ┌────────┐ │                            │ (Follower) │
  │ │  WAL   │ │                            └────────────┘
  │ │  + KV  │ │
  │ └────────┘ │
  └────────────┘
```

**Write path:** client → leader HTTP → `propose()` → WAL → `AppendEntries` to peers → quorum ack → apply KV → HTTP 200

**Read path:** client → leader HTTP → capture `commit_index` → wait until applied → read KV → HTTP 200

**Non-leader:** any node receiving a client request returns `307 Redirect` to the leader's HTTP address.

---

## Crate layout

```
raft-kv/
├── crates/
│   ├── raft/      Pure state machine — no I/O, no async. step(Message) → Ready.
│   ├── storage/   WAL (CRC32 + JSON records) + in-memory KV (BTreeMap).
│   ├── raft-kv/   The embeddable node: RaftKv facade, NodeHandle (I/O driver),
│   │              axum HTTP + tonic gRPC. Also builds the standalone binary.
│   └── client/    CLI: raft-kv-cli set/get/delete/status/scan.
└── scripts/
    └── integration_test.sh   End-to-end cluster tests + chaos scenarios.
```

### `crates/raft` — The algorithm

The state machine is a pure function: no network, no disk, no async.

```rust
let ready = node.step(Message::AppendEntries { from, msg });
// ready.messages          → RPCs to send
// ready.entries_to_persist → write to WAL before replying
// ready.entries_to_apply   → apply to KV state machine
// ready.membership_change  → add/remove peer connection
```

`NodeHandle` (in `raft-kv`) is the only component that does I/O — it owns the WAL, the KV store, the gRPC clients, and the tick loop.

### WAL record format

```
┌──────────┬──────────┬──────────────────────────────┐
│  length  │  CRC32   │  JSON payload (WalRecord)    │
│  4 bytes │  4 bytes │  N bytes                     │
└──────────┴──────────┴──────────────────────────────┘
```

Records with invalid checksums are silently dropped on replay (truncated-tail recovery). The WAL is fsynced on every append.

---

## Embed it

Add the crate and every replica of your app becomes a Raft node:

```toml
[dependencies]
raft-kv = { git = "https://github.com/cacdu/raft-kv" }
```

```rust
use raft_kv::{RaftKv, RaftKvOptions, Event};

let node = RaftKv::start(RaftKvOptions {
    id: 1,
    raft_addr: "0.0.0.0:7001".into(),                          // gRPC between nodes
    peers: [(2, "node2:7001".into()), (3, "node3:7001".into())].into(),
    app_addrs: [(2, "node2:8080".into()), (3, "node3:8080".into())].into(),
    data_dir: "data".into(),
    learner: false,
}).await?;

node.put("hello", "world").await?;     // quorum-committed write (leader only)
let v = node.get("hello").await?;      // linearizable read (leader only)
let v = node.get_local("hello").await; // local read (any node)

// Watch every committed write, in log order — same sequence on every node:
let mut events = node.subscribe();
while let Ok(event) = events.recv().await {
    match event {
        Event::Set { key, value } => { /* push to clients, update caches… */ }
        Event::Delete { key } => { /* … */ }
        Event::SnapshotApplied => { /* state replaced wholesale: resync */ }
    }
}
```

On a follower, writes fail with `Error::NotLeader { leader_addr, .. }` carrying the leader's app-level address (from `app_addrs`) so your app can forward the request. [gambas](https://github.com/cacdu/gambas) — a distributed pixel canvas where each web replica embeds a node and streams pixel deltas to browsers over WebSockets — is the reference consumer.

## Quick start

**Prerequisites**

```bash
rustup update stable
# protoc (for tonic gRPC codegen)
# Arch:   sudo pacman -S protobuf
# Debian: sudo apt install protobuf-compiler
# macOS:  brew install protobuf
```

**Run a 3-node cluster** (three terminals)

```bash
cargo build
make node1   # id=1, gRPC :7001, HTTP :8001
make node2   # id=2, gRPC :7002, HTTP :8002
make node3   # id=3, gRPC :7003, HTTP :8003
```

**Use the CLI**

```bash
cargo run -p client -- set hello world
cargo run -p client -- get hello          # → world
cargo run -p client -- scan --prefix ""   # → all keys
cargo run -p client -- delete hello
cargo run -p client -- status
```

**Or raw HTTP**

```bash
curl -X PUT  http://127.0.0.1:8001/kv/city -d "monterrey"
curl          http://127.0.0.1:8001/kv/city          # → monterrey
curl          http://127.0.0.1:8001/kv?prefix=c       # → {"city":"monterrey"}
curl -X DELETE http://127.0.0.1:8001/kv/city
curl          http://127.0.0.1:8001/status
curl          http://127.0.0.1:8001/metrics
```

**Membership changes**

```bash
# Add a fourth node (started with --learner)
curl -X POST http://127.0.0.1:8001/cluster/add \
  -H "Content-Type: application/json" \
  -d '{"id":4,"raft_addr":"127.0.0.1:7004","http_addr":"127.0.0.1:8004"}'

# Remove it
curl -X POST http://127.0.0.1:8001/cluster/remove \
  -H "Content-Type: application/json" \
  -d '{"id":4}'
```

**Run all tests**

```bash
make test                 # 28 unit tests
make integration-test     # full cluster + chaos + membership
```

---

## HTTP API

| Method | Path | Body | Description |
|--------|------|------|-------------|
| `GET` | `/kv/{key}` | — | Get a value (linearizable) |
| `PUT` | `/kv/{key}` | plain text | Set a value |
| `DELETE` | `/kv/{key}` | — | Delete a key |
| `GET` | `/kv?prefix={p}` | — | Scan all keys with prefix (JSON map) |
| `GET` | `/status` | — | Node status (is_leader, leader_id) |
| `GET` | `/metrics` | — | Prometheus metrics |
| `POST` | `/cluster/add` | JSON | Add a node to the cluster |
| `POST` | `/cluster/remove` | JSON | Remove a node from the cluster |

Non-leader nodes return `307 Temporary Redirect` to the leader for all write endpoints and `/kv` reads.

---

## What's implemented

**Raft core**
- [x] Leader election with randomized timeouts
- [x] Log replication (`AppendEntries`) with fast conflict rollback
- [x] Quorum-based commit index advancement
- [x] No-op entry on leader win (fixes stale commit index from previous terms)
- [x] Log compaction + WAL snapshot record
- [x] `InstallSnapshot` RPC for lagging peers
- [x] Membership changes — add/remove nodes at runtime (single-server changes, pending-conf-change guard)
- [x] Learner mode (`--learner`) — node joins without voting until `ConfChange(Add)` commits

**Storage**
- [x] Append-only WAL with CRC32 per record
- [x] WAL replay on startup (HardState, log entries, snapshot)
- [x] In-memory KV (`BTreeMap`) with `set` / `delete` / `scan_prefix`
- [x] Snapshot serialization (serde_json of the BTreeMap)

**Server**
- [x] Linearizable reads via ReadIndex protocol
- [x] `307 Redirect` on non-leader nodes (path-preserving)
- [x] Prefix scan endpoint (`GET /kv?prefix=`)
- [x] gRPC peer server + client (tonic 0.12)
- [x] `entry_type` propagated over the wire (Normal vs ConfChange)
- [x] Prometheus metrics — term, commit index, applied index, read/write counters, latency histograms
- [x] Embeddable library API (`RaftKv`) — start/put/get/scan + `subscribe()` watch stream
- [x] Non-blocking fan-out — peer RPCs on background tasks with connect/request timeouts; responses stepped as they arrive, so dead peers never delay elections or commits

**Tests**
- [x] 15 unit tests — pure Raft SM (election, replication, conflict, snapshot, term monotonicity)
- [x] 13 unit tests — server layer (WAL replay, proposals, ReadIndex, drain on leadership loss)
- [x] Integration + chaos tests: initial election, writes, follower redirect, leader failover, write-during-kill, WAL replay & catchup, minority partition & heal, membership add/remove

---

## Stack

| Layer | Crate |
|-------|-------|
| Consensus | hand-rolled (no `raft-rs`) |
| HTTP | `axum` 0.8 |
| gRPC | `tonic` 0.12 + `prost` 0.13 |
| Async runtime | `tokio` |
| Serialization | `serde_json` (WAL + commands), `prost` (wire) |
| Observability | `prometheus` 0.13 |
| CLI | `clap` 4 |

---

## Design notes

**Why a pure state machine?**
Separating the Raft algorithm from I/O makes it trivially unit-testable: every test drives the SM with hand-crafted `Message`s and asserts on the returned `Ready` struct — no network mocking required.

**Why a custom WAL instead of RocksDB?**
Keeps the dependency tree minimal and the code readable. The WAL is ~60 lines and the recovery logic is ~30 lines. The tradeoff is no compaction of the WAL file itself (only Raft log compaction via snapshots).

**Why peer responses are stepped as they arrive**
Network fan-out runs on background tasks, decoupled from the tick loop, and each peer's response is stepped through the SM the moment it lands. Both halves matter and both were found by killing nodes under load: if the tick loop awaits peer I/O, a dead peer throttles the very election timers that should replace it; and if responses are collected before stepping, the vote of a live peer waits on the connect timeout of a dead one. Raft tolerates the resulting out-of-order delivery by design — every message carries the term/index checks needed to reject stale state.

**Membership changes strategy**
Single-server changes (one node at a time) with a `pending_conf_change` guard to prevent concurrent changes. New nodes start as learners: the leader replicates to them immediately but they don't count toward quorum until their `ConfChange(Add)` commits.

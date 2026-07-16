# server

The binary that turns the pure Raft state machine into a running cluster node.

It owns all I/O: the WAL, the KV store, the gRPC peer connections, the HTTP
server, and the tick loop. The Raft algorithm itself lives in `crates/raft`
and never touches the network or disk directly.

---

## Binary

```
raft-kv --id <N>
        --grpc-addr <host:port>   # Raft peer RPCs
        --http-addr <host:port>   # Client-facing KV API
        --peer <id=host:port>     # (repeatable) gRPC addresses of peers
        --http-peer <id=host:port># (repeatable) HTTP addresses of peers
        --data-dir <path>         # WAL location (default: data/)
        [--learner]               # join without voting rights
```

---

## Module map

### `node_handle.rs` — the glue

`NodeHandle` is the central coordinator. It wraps `RaftNode` behind a
`Mutex` and drives the full Ready pipeline on every step:

```
node.step(msg) → Ready
                    │
     ┌──────────────┼──────────────────────────────┐
     ▼              ▼                               ▼
  WAL append    KV apply              fan-out gRPC messages to peers
  (hard_state,  (entries_to_apply)    collect responses → step again
   entries)
     │              │                               │
     └──────────────┴────── notify pending HTTP handlers ─┘
```

Key methods:

| Method | Description |
|--------|-------------|
| `tick()` | Called every 10 ms; steps `Message::Tick` |
| `propose(cmd)` | Propose a KV command; returns a receiver that fires on commit |
| `propose_conf_change(op, id, ..)` | Propose membership change |
| `step_rpc(msg)` | Called by gRPC handlers; returns the response message |
| `try_compact(index)` | Snapshot KV + compact Raft log every 50 entries |
| `apply_snapshot(snap)` | Replace KV store from snapshot (follower catch-up) |

**Lock order:** `node` → `pending_proposals`. Never reversed.

### `http.rs` — client API

| Route | Description |
|-------|-------------|
| `GET /kv/{key}` | Linearizable read via ReadIndex |
| `PUT /kv/{key}` | Replicate a Set command through Raft |
| `DELETE /kv/{key}` | Replicate a Delete command |
| `GET /kv?prefix={p}` | Scan all keys with prefix (linearizable) |
| `POST /cluster/add` | Add a node: `{"id", "raft_addr", "http_addr"}` |
| `POST /cluster/remove` | Remove a node: `{"id"}` |
| `GET /status` | JSON: `{is_leader, leader_id}` |
| `GET /metrics` | Prometheus text format |

Non-leader nodes return `307 Temporary Redirect` to the leader's HTTP
address for all write endpoints and reads. The redirect preserves the
full path and query string.

**Linearizable reads** use the ReadIndex protocol: the leader captures
`commit_index` at request time, then waits on a `watch::Receiver<LogIndex>`
until `applied_index >= commit_index` before reading from the KV store.

### `grpc.rs` — peer RPC server

Implements `RaftService` (tonic):

- `request_vote` — decodes proto, calls `NodeHandle::step_rpc`, encodes response
- `append_entries` — same pattern; maps `entry_type` field to `EntryType` enum
- `install_snapshot` — handled inline by `NodeHandle`

### `peer.rs` — peer RPC client

`PeerClient` wraps a tonic `RaftServiceClient`. Used by `NodeHandle` to
fan out outbound RPCs. One client per peer, lazily connected on each call.

```rust
let client = PeerClient::new(node_id, "127.0.0.1:7002".to_string());
client.send(Message::RequestVote { .. }).await;
```

`send()` returns `Option<Message>` — `None` on network error (Raft handles
unreachable peers by retrying on the next heartbeat tick).

### `config.rs` — CLI parsing

`NodeConfig` uses `clap::Parser`. Peer addresses are `id=host:port` pairs
parsed by a custom `value_parser`. Produces two maps:

- `peers_map()` → `HashMap<NodeId, String>` for gRPC
- `http_peers_map()` → `HashMap<NodeId, String>` for HTTP redirects

### `metrics.rs` — Prometheus gauges and counters

| Metric | Type | Description |
|--------|------|-------------|
| `raft_kv_writes_total` | Counter | Write operations committed |
| `raft_kv_reads_total` | Counter | Read operations served by leader |
| `raft_kv_applied_index` | Gauge | Last log index applied to KV |
| `raft_kv_commit_index` | Gauge | Current Raft commit index |
| `raft_kv_current_term` | Gauge | Current Raft term |
| `raft_kv_is_leader` | Gauge | 1 if leader, 0 otherwise |
| `raft_kv_request_duration_seconds` | Histogram | Latency by operation type |

---

## Request flow: write

```
PUT /kv/city  body="monterrey"
   │
   ▼ http.rs: kv_put()
NodeHandle::propose(Command::Set{..})
   │  ← lock: node
   ▼
RaftNode::step(Propose{command})
   └─ returns Ready { entries_to_persist, messages }
        │
        ├─ WAL::append(Entry)
        └─ fan-out AppendEntries to peers
              └─ quorum acks → advance commit_index → apply KV
                    └─ oneshot::send(()) → HTTP 200
```

## Request flow: read

```
GET /kv/city
   │
   ▼ http.rs: kv_get()
NodeHandle::read_index_if_leader() → commit_index
   │
   ▼ subscribe_applied().changed() until applied >= commit_index
KvStore::get("city") → "monterrey"
   └─ HTTP 200 "monterrey"
```

---

## Testing

```bash
cargo test -p server
```

13 unit tests in `node_handle.rs` covering WAL replay, proposal lifecycle,
ReadIndex, and leadership-loss draining. No network required.

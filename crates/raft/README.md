# raft

Pure Raft consensus state machine — no I/O, no async, no network.

This crate implements the Raft algorithm as a deterministic function:
given a `Message` in, it returns a `Ready` struct out. All side effects
(disk writes, network calls, timers) are the caller's responsibility.
This makes the algorithm trivially unit-testable without mocking.

## Core types

### `Message` — inputs

```rust
enum Message {
    Tick,                    // advance election / heartbeat timers
    Propose { command },     // replicate a command through the cluster
    ProposeConfChange { .. },// add or remove a voting member
    RequestVote { .. },      // incoming RPC from a candidate
    RequestVoteResponse { .. },
    AppendEntries { .. },    // incoming RPC from the leader
    AppendEntriesResponse { .. },
    InstallSnapshot { .. },  // incoming RPC (lagging peer catch-up)
    InstallSnapshotResponse { .. },
}
```

### `Ready` — outputs

```rust
struct Ready {
    hard_state: Option<HardState>,       // persist before replying
    entries_to_persist: Vec<LogEntry>,   // write to WAL
    messages: Vec<(NodeId, Message)>,    // send to peers
    entries_to_apply: Vec<LogEntry>,     // apply to KV state machine
    snapshot_to_send: Vec<NodeId>,       // lagging peers that need a full snapshot
    snapshot_to_apply: Option<Snapshot>, // replace KV store with this
    membership_change: Option<ConfChangeCmd>, // update peer connections
}
```

**Ordering contract:** the caller must process fields in this order on every step:

1. Persist `hard_state` and `entries_to_persist` to durable storage
2. Apply `entries_to_apply` to the state machine
3. Send `messages` to peers
4. Handle `snapshot_to_send` / `snapshot_to_apply`
5. React to `membership_change`

### `RaftNode` — the state machine

```rust
let mut node = RaftNode::new(config);

// restore durable state from WAL on startup
node.restore(term, voted_for, snapshot_index, snapshot_term, entries);

// drive the SM — call this for every incoming message and every timer tick
let ready = node.step(msg);
```

## Internals

| File | Responsibility |
|------|----------------|
| `node/mod.rs` | `RaftNode` struct, constructors, `step()` dispatch, role transitions |
| `node/election.rs` | `tick()`, `start_election()`, `handle_request_vote*()` |
| `node/replication.rs` | `propose()`, `broadcast_append_entries()`, `handle_append_entries*()`, snapshot handling, `advance_commit_index()` |
| `node/membership.rs` | `propose_conf_change_inner()`, `apply_conf_change_entry()` |
| `node/node_tests.rs` | 15 unit tests covering election, replication, conflicts, snapshots |
| `log.rs` | In-memory Raft log with snapshot compaction |
| `message.rs` | All message and data types |
| `config.rs` | Timeouts and tuning knobs |

## Membership changes

Single-server changes only (one node at a time). A `pending_conf_change`
guard prevents concurrent changes. New nodes join as **learners** via
`RaftNode::new_learner()` — they receive log entries but cannot vote until
a `ConfChange(Add, id)` entry commits, at which point `is_voter` flips to
`true` and election timeouts begin firing.

## Tuning

```rust
Config {
    election_timeout: 10,       // ticks before election (randomized ×2)
    heartbeat_timeout: 3,       // ticks between leader heartbeats
    max_entries_per_append: 100,// entries per AppendEntries RPC
}
```

Tick interval is set by the caller (10 ms in the server crate).
The rule `heartbeat_timeout << election_timeout` must hold.

## Testing

```bash
cargo test -p raft
```

15 tests, all pure in-process — no ports, no files.

# storage

Durable storage layer: Write-Ahead Log (WAL) and in-memory KV state machine.

This crate has no knowledge of Raft. It provides two independent components
that `server::NodeHandle` combines to give the cluster its persistence and
its application state.

---

## `Wal` — Write-Ahead Log

Append-only log file that records every state change before it takes effect.
Used to reconstruct `RaftNode` state after a crash.

### Record format

```
┌───────────────┬───────────────┬──────────────────────────────┐
│  length: u32  │  CRC32: u32   │  JSON payload (WalRecord)    │
│  little-endian│  little-endian│  variable length             │
└───────────────┴───────────────┴──────────────────────────────┘
```

Each record is framed with its byte length and a CRC32 checksum.
On recovery, any record with a mismatched checksum is silently dropped —
this handles truncated writes from a crash mid-append.

### Record types

```rust
enum WalRecord {
    HardState { term, voted_for },  // Raft durable state
    Entry(LogEntry),                 // a replicated log entry
    Snapshot { last_index, last_term }, // log compaction marker
}
```

### Usage

```rust
// Open (or create) the WAL and replay existing records
let (wal, records) = Wal::open("node-1.wal")?;

// Append a record — flushed to disk before returning
wal.append(&WalRecord::HardState { term: 3, voted_for: Some(1) })?;
```

### Recovery

`NodeHandle` makes three passes over the replayed records at startup:

1. **`replay_hard_state`** — take the last `HardState` record
2. **`replay_snapshot`** — take the last `Snapshot` record  
3. **`replay_entries`** — rebuild the log, letting later records at the
   same index overwrite earlier ones (matches `RaftLog::truncate_and_append`)

Then calls `RaftNode::restore(term, voted_for, snapshot_index, snapshot_term, entries)`.

---

## `KvStore` — Key-Value State Machine

In-memory ordered map applied on top of committed Raft log entries.

```rust
let mut kv = KvStore::default();

kv.apply(b"{\"Set\":{\"key\":\"city\",\"value\":\"monterrey\"}}")?;
kv.get("city");          // → Some("monterrey")
kv.scan_prefix("ci");   // → [("city", "monterrey")]
```

### Commands

```rust
enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}
```

Commands are JSON-encoded and stored as the `command` field of each
`LogEntry`. Empty `command` bytes (no-op entries appended on leader win)
are silently skipped.

### Snapshot

```rust
let bytes = kv.snapshot();      // serialize → Vec<u8>
kv.restore(&bytes)?;            // deserialize and replace state
```

Used by `NodeHandle::try_compact()` (compaction trigger every 50 applied
entries) and `InstallSnapshot` RPC handling (full state transfer to lagging
peers).

### `scan_prefix`

```rust
kv.scan_prefix("user:");
// → [("user:alice", "..."), ("user:bob", "...")]
```

Exploits `BTreeMap`'s lexicographic ordering: ranges into the prefix
position and stops at the first key that no longer matches — O(k) where k
is the result count, regardless of total store size.

---

## Testing

```bash
cargo test -p storage
```

# client

Command-line client for a running raft-kv cluster.

Talks to any node via HTTP. Non-leader nodes redirect automatically —
the client follows redirects transparently via `reqwest`.

## Installation

```bash
cargo build -p client
# binary: target/debug/raft-kv-cli
```

## Usage

```
raft-kv-cli [--addr <url>] <command>

Options:
  --addr   Base URL of any cluster node (default: http://127.0.0.1:8001)

Commands:
  get <key>
  set <key> <value>
  delete <key>
  scan [--prefix <prefix>]   List keys (empty prefix = all keys)
  status                     Print node status JSON
```

## Examples

```bash
# Write and read
raft-kv-cli set hello world
raft-kv-cli get hello
# → world

# Scan with prefix
raft-kv-cli set user:alice '{"role":"admin"}'
raft-kv-cli set user:bob   '{"role":"viewer"}'
raft-kv-cli scan --prefix user:
# → user:alice = {"role":"admin"}
#   user:bob   = {"role":"viewer"}

# All keys
raft-kv-cli scan

# Check which node is leader
raft-kv-cli status
# → {
#     "is_leader": false,
#     "leader_id": 2
#   }

# Point at a different node
raft-kv-cli --addr http://127.0.0.1:8003 get hello
# → world  (follows 307 redirect to leader)

# Delete
raft-kv-cli delete hello
```

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | HTTP error, network error, or key not found |

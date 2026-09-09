use serde::{Deserialize, Serialize};

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
    /// Highest commit index this node has made durable.
    ///
    /// The Raft dissertation treats commitIndex as volatile, recoverable from
    /// the leader, so persisting it is not required for §5 safety. But this
    /// implementation builds guards *on top of* commit_index, and they go
    /// silently vacuous when it resets to the snapshot base on restart:
    ///
    /// - the anti-truncation floor in `conflict_hint` collapses to
    ///   `snapshot_index + 1`, so a restarted node offers to rewind over
    ///   entries it had already acknowledged as committed;
    /// - `read_index_if_leader` returns a read index near the snapshot base, so
    ///   a linearizable read's wait-for-apply is trivially satisfied and the
    ///   read can be served from a state machine that is behind.
    ///
    /// `default` keeps 0.1.x WALs readable.
    #[serde(default)]
    pub commit: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum EntryType {
    #[default]
    Normal,
    ConfChange,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConfChangeOp {
    Add,
    Remove,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfChangeCmd {
    pub op: ConfChangeOp,
    pub node_id: NodeId,
    /// gRPC address (host:port) — only present for Add operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raft_addr: Option<String>,
    /// HTTP address (host:port) — only present for Add operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_addr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    pub index: LogIndex,
    pub term: Term,
    #[serde(default)]
    pub entry_type: EntryType,
    /// Serialized KV command (set/delete). Empty = no-op. ConfChange = ConfChangeCmd.
    /// `serde_bytes` hands the payload to the serializer as one blob instead of
    /// element by element — free with JSON (which writes the same array of
    /// integers either way), a real saving with the WAL's binary codec.
    #[serde(with = "serde_bytes")]
    pub command: Vec<u8>,
}

// ── Raft RPCs ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntries {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    /// On success: the index of the last replicated entry.
    /// On failure: the first index of the conflicting term (for fast rollback).
    pub match_index: LogIndex,
}

// ── Snapshot ───────────────────────────────────────────────────────────────

/// Compact representation of the KV state machine at a given log position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub last_index: LogIndex,
    pub last_term: Term,
    /// Serialized KV store (serde_json of the BTreeMap). Stays JSON on purpose:
    /// unlike the WAL, this crosses the wire in InstallSnapshot, so changing it
    /// would break a cluster mid-upgrade.
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshot {
    pub term: Term,
    pub leader_id: NodeId,
    pub snapshot: Snapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub term: Term,
    pub success: bool,
}

// ── Internal messages ──────────────────────────────────────────────────────

/// All inputs to the Raft state machine.
/// The node is purely functional: it consumes Messages and produces Ready.
#[derive(Debug, Clone)]
pub enum Message {
    /// Periodic clock tick. Drives election and heartbeat timeouts.
    Tick,
    /// A client wants to append a command to the replicated log.
    Propose {
        command: Vec<u8>,
    },
    /// Propose a membership change (add or remove a voter).
    /// raft_addr and http_addr are embedded in the log entry so followers learn the addresses.
    ProposeConfChange {
        op: ConfChangeOp,
        node_id: NodeId,
        raft_addr: Option<String>,
        http_addr: Option<String>,
    },
    /// Incoming RPC from another node.
    RequestVote {
        from: NodeId,
        msg: RequestVote,
    },
    RequestVoteResponse {
        from: NodeId,
        msg: RequestVoteResponse,
    },
    AppendEntries {
        from: NodeId,
        msg: AppendEntries,
    },
    AppendEntriesResponse {
        from: NodeId,
        msg: AppendEntriesResponse,
    },
    InstallSnapshot {
        from: NodeId,
        msg: InstallSnapshot,
    },
    InstallSnapshotResponse {
        from: NodeId,
        msg: InstallSnapshotResponse,
    },
}

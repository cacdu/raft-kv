use serde::{Deserialize, Serialize};

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
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
    /// Serialized KV store (serde_json of the BTreeMap).
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

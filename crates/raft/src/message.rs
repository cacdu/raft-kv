use serde::{Deserialize, Serialize};

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    pub index: LogIndex,
    pub term: Term,
    /// Serialized KV command (set/delete). Empty = no-op (leader heartbeat).
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

// ── Internal messages ──────────────────────────────────────────────────────

/// All inputs to the Raft state machine.
/// The node is purely functional: it consumes Messages and produces Ready.
#[derive(Debug, Clone)]
pub enum Message {
    /// Periodic clock tick. Drives election and heartbeat timeouts.
    Tick,
    /// A client wants to append a command to the replicated log.
    Propose { command: Vec<u8> },
    /// Incoming RPC from another node.
    RequestVote { from: NodeId, msg: RequestVote },
    RequestVoteResponse { from: NodeId, msg: RequestVoteResponse },
    AppendEntries { from: NodeId, msg: AppendEntries },
    AppendEntriesResponse { from: NodeId, msg: AppendEntriesResponse },
}

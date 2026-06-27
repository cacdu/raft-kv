use std::collections::{HashMap, HashSet};

use rand::Rng;
use tracing::info;

use crate::{
    config::Config,
    log::RaftLog,
    message::{HardState, LogEntry, LogIndex, Message, NodeId, Snapshot, Term},
};

mod election;
mod membership;
#[cfg(test)]
mod node_tests;
mod replication;

// ── Role ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Role {
    Follower {
        leader_id: Option<NodeId>,
    },
    Candidate {
        votes_received: std::collections::HashSet<NodeId>,
    },
    Leader {
        next_index: HashMap<NodeId, LogIndex>,
        match_index: HashMap<NodeId, LogIndex>,
    },
}

// ── Ready ──────────────────────────────────────────────────────────────────

/// Output of one step of the Raft state machine.
/// The caller (server) must process every field before calling step() again.
#[derive(Debug, Default)]
pub struct Ready {
    /// Hard state that must reach durable storage before any RPC response is sent.
    /// Only Some when current_term or voted_for changed in this step.
    pub hard_state: Option<HardState>,
    /// Messages to send to other nodes: (destination, message).
    pub messages: Vec<(NodeId, Message)>,
    /// Entries that must be persisted to stable storage before sending messages.
    pub entries_to_persist: Vec<LogEntry>,
    /// Entries committed and ready to be applied to the KV state machine.
    pub entries_to_apply: Vec<LogEntry>,
    /// Leader: peers that need a full snapshot (their next_index is before our snapshot).
    /// NodeHandle serializes the KV store and sends InstallSnapshot RPC to each peer.
    pub snapshot_to_send: Vec<NodeId>,
    /// Follower: a snapshot received from the leader that must replace the KV store.
    pub snapshot_to_apply: Option<Snapshot>,
    /// Present when a ConfChange entry was applied this step.
    /// Carries the full command so NodeHandle can add/remove gRPC + HTTP peer connections.
    pub membership_change: Option<crate::message::ConfChangeCmd>,
}

// ── RaftNode ───────────────────────────────────────────────────────────────

pub struct RaftNode {
    pub id: NodeId,
    pub role: Role,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,

    pub log: RaftLog,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,

    /// Current set of voting members (includes self). Updated when ConfChange entries are applied.
    pub membership: HashSet<NodeId>,
    /// False for a learner node that has not yet been admitted to the cluster.
    /// Learners receive log entries but do not participate in elections.
    pub is_voter: bool,
    /// True while a ConfChange entry is in the log but not yet applied.
    /// Prevents concurrent membership changes.
    pub(crate) pending_conf_change: bool,

    config: Config,
    election_ticks: u32,
    election_timeout: u32,
    heartbeat_ticks: u32,

    pending_ready: Ready,
}

impl RaftNode {
    pub fn new(config: Config) -> Self {
        Self::new_inner(config, true)
    }

    /// Start as a non-voting learner. Elections are suppressed until a
    /// ConfChange(Add, self.id) entry is applied, at which point is_voter flips to true.
    pub fn new_learner(config: Config) -> Self {
        Self::new_inner(config, false)
    }

    fn new_inner(config: Config, is_voter: bool) -> Self {
        let election_timeout =
            rand::rng().random_range(config.election_timeout..config.election_timeout * 2);
        let mut membership: HashSet<NodeId> = config.peers.iter().copied().collect();
        membership.insert(config.id);
        Self {
            id: config.id,
            role: Role::Follower { leader_id: None },
            current_term: 0,
            voted_for: None,
            log: RaftLog::new(),
            commit_index: 0,
            last_applied: 0,
            membership,
            is_voter,
            pending_conf_change: false,
            config,
            election_ticks: 0,
            election_timeout,
            heartbeat_ticks: 0,
            pending_ready: Ready::default(),
        }
    }

    /// Restore durable state from WAL replay. Call once after new(), before any step().
    pub fn restore(
        &mut self,
        term: Term,
        voted_for: Option<NodeId>,
        snapshot_index: LogIndex,
        snapshot_term: Term,
        entries: Vec<LogEntry>,
    ) {
        self.current_term = term;
        self.voted_for = voted_for;
        self.log.restore(snapshot_index, snapshot_term, entries);
        // commit_index and last_applied start at snapshot_index:
        // the KV state machine was rebuilt from the snapshot, entries above it are uncommitted
        // until the new leader re-drives them through AppendEntries.
        self.commit_index = snapshot_index;
        self.last_applied = snapshot_index;
    }

    /// Advance the state machine by one message. Returns accumulated Ready.
    pub fn step(&mut self, msg: Message) -> Ready {
        match msg {
            Message::Tick => self.tick(),
            Message::Propose { command } => self.propose(command),
            Message::ProposeConfChange {
                op,
                node_id,
                raft_addr,
                http_addr,
            } => {
                self.propose_conf_change_inner(op, node_id, raft_addr, http_addr);
            }
            Message::RequestVote { from, msg } => self.handle_request_vote(from, msg),
            Message::RequestVoteResponse { from, msg } => {
                self.handle_request_vote_response(from, msg)
            }
            Message::AppendEntries { from, msg } => self.handle_append_entries(from, msg),
            Message::AppendEntriesResponse { from, msg } => {
                self.handle_append_entries_response(from, msg)
            }
            Message::InstallSnapshot { from, msg } => self.handle_install_snapshot(from, msg),
            Message::InstallSnapshotResponse { from, msg } => {
                self.handle_install_snapshot_response(from, msg)
            }
        }
        std::mem::take(&mut self.pending_ready)
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader { .. })
    }

    pub fn leader_id(&self) -> Option<NodeId> {
        match &self.role {
            Role::Follower { leader_id } => *leader_id,
            Role::Leader { .. } => Some(self.id),
            Role::Candidate { .. } => None,
        }
    }

    // ── Role transitions ──────────────────────────────────────────────────

    fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
            self.emit_hard_state();
        }
        self.role = Role::Follower { leader_id };
        self.reset_election_timeout();
    }

    fn become_leader(&mut self) {
        info!(id = self.id, term = self.current_term, "became leader");
        let last = self.log.last_index();
        let next_index = self
            .membership
            .iter()
            .filter(|&&p| p != self.id)
            .map(|&p| (p, last + 1))
            .collect();
        let match_index = self
            .membership
            .iter()
            .filter(|&&p| p != self.id)
            .map(|&p| (p, 0))
            .collect();
        self.role = Role::Leader {
            next_index,
            match_index,
        };
        self.heartbeat_ticks = 0;

        // Leader appends a no-op entry to commit entries from previous terms
        self.propose(vec![]);
    }

    fn reset_election_timeout(&mut self) {
        self.election_ticks = 0;
        // Upper bound is exclusive, so max timeout = 2*election_timeout - 1 ticks.
        self.election_timeout = rand::rng()
            .random_range(self.config.election_timeout..self.config.election_timeout * 2);
    }

    fn emit_hard_state(&mut self) {
        self.pending_ready.hard_state = Some(HardState {
            term: self.current_term,
            voted_for: self.voted_for,
        });
    }
}

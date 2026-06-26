use std::collections::{HashMap, HashSet};

use rand::Rng;
use tracing::{debug, info, warn};

use crate::{
    config::Config,
    log::RaftLog,
    message::{
        AppendEntries, AppendEntriesResponse, LogEntry, LogIndex, Message, NodeId, RequestVote,
        RequestVoteResponse, Term,
    },
};

// ── Role ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Role {
    Follower { leader_id: Option<NodeId> },
    Candidate { votes_received: HashSet<NodeId> },
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
    /// Messages to send to other nodes: (destination, message).
    pub messages: Vec<(NodeId, Message)>,
    /// Entries that must be persisted to stable storage before sending messages.
    pub entries_to_persist: Vec<LogEntry>,
    /// Entries committed and ready to be applied to the KV state machine.
    pub entries_to_apply: Vec<LogEntry>,
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

    config: Config,
    election_ticks: u32,
    election_timeout: u32,
    heartbeat_ticks: u32,

    pending_ready: Ready,
}

impl RaftNode {
    pub fn new(config: Config) -> Self {
        let election_timeout = rand::rng().random_range(
            config.election_timeout..config.election_timeout * 2,
        );
        Self {
            id: config.id,
            role: Role::Follower { leader_id: None },
            current_term: 0,
            voted_for: None,
            log: RaftLog::new(),
            commit_index: 0,
            last_applied: 0,
            config,
            election_ticks: 0,
            election_timeout,
            heartbeat_ticks: 0,
            pending_ready: Ready::default(),
        }
    }

    /// Advance the state machine by one message. Returns accumulated Ready.
    pub fn step(&mut self, msg: Message) -> Ready {
        match msg {
            Message::Tick => self.tick(),
            Message::Propose { command } => self.propose(command),
            Message::RequestVote { from, msg } => self.handle_request_vote(from, msg),
            Message::RequestVoteResponse { from, msg } => {
                self.handle_request_vote_response(from, msg)
            }
            Message::AppendEntries { from, msg } => self.handle_append_entries(from, msg),
            Message::AppendEntriesResponse { from, msg } => {
                self.handle_append_entries_response(from, msg)
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

    // ── Tick ─────────────────────────────────────────────────────────────

    fn tick(&mut self) {
        match &self.role.clone() {
            Role::Follower { .. } | Role::Candidate { .. } => {
                self.election_ticks += 1;
                if self.election_ticks >= self.election_timeout {
                    self.start_election();
                }
            }
            Role::Leader { .. } => {
                self.heartbeat_ticks += 1;
                if self.heartbeat_ticks >= self.config.heartbeat_timeout {
                    self.heartbeat_ticks = 0;
                    self.broadcast_append_entries();
                }
            }
        }
    }

    // ── Elections ─────────────────────────────────────────────────────────

    fn start_election(&mut self) {
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.reset_election_timeout();

        let mut votes = HashSet::new();
        votes.insert(self.id);
        self.role = Role::Candidate { votes_received: votes };

        info!(id = self.id, term = self.current_term, "starting election");

        let msg = RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };

        for &peer in &self.config.peers {
            self.pending_ready.messages.push((
                peer,
                Message::RequestVote { from: self.id, msg: msg.clone() },
            ));
        }
    }

    fn handle_request_vote(&mut self, from: NodeId, msg: RequestVote) {
        if msg.term > self.current_term {
            self.become_follower(msg.term, None);
        }

        let log_ok = msg.last_log_term > self.log.last_term()
            || (msg.last_log_term == self.log.last_term()
                && msg.last_log_index >= self.log.last_index());

        let can_vote = msg.term == self.current_term
            && log_ok
            && (self.voted_for.is_none() || self.voted_for == Some(from));

        if can_vote {
            self.voted_for = Some(from);
            debug!(id = self.id, from, "granting vote");
        }

        self.pending_ready.messages.push((
            from,
            Message::RequestVoteResponse {
                from: self.id,
                msg: RequestVoteResponse {
                    term: self.current_term,
                    vote_granted: can_vote,
                },
            },
        ));
    }

    fn handle_request_vote_response(&mut self, from: NodeId, msg: RequestVoteResponse) {
        if msg.term > self.current_term {
            self.become_follower(msg.term, None);
            return;
        }

        let Role::Candidate { ref mut votes_received } = self.role else {
            return;
        };

        if msg.vote_granted {
            votes_received.insert(from);
            let quorum = (self.config.peers.len() + 1) / 2 + 1;
            if votes_received.len() >= quorum {
                self.become_leader();
            }
        }
    }

    // ── Log replication ───────────────────────────────────────────────────

    fn propose(&mut self, command: Vec<u8>) {
        if !self.is_leader() {
            warn!(id = self.id, "not leader, dropping proposal");
            return;
        }
        let entry = LogEntry {
            index: self.log.last_index() + 1,
            term: self.current_term,
            command,
        };
        self.pending_ready.entries_to_persist.push(entry.clone());
        self.log.append(entry);
        self.broadcast_append_entries();
    }

    fn broadcast_append_entries(&mut self) {
        let Role::Leader { next_index, .. } = &self.role else {
            return;
        };
        let peers = self.config.peers.clone();
        let next_index = next_index.clone();

        for peer in peers {
            let ni = *next_index.get(&peer).unwrap_or(&1);
            let prev_log_index = ni - 1;
            let prev_log_term =
                self.log.term_at(prev_log_index).unwrap_or(0);
            let entries = self
                .log
                .entries_from(ni)
                .iter()
                .take(self.config.max_entries_per_append)
                .cloned()
                .collect();

            self.pending_ready.messages.push((
                peer,
                Message::AppendEntries {
                    from: self.id,
                    msg: AppendEntries {
                        term: self.current_term,
                        leader_id: self.id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit: self.commit_index,
                    },
                },
            ));
        }
    }

    fn handle_append_entries(&mut self, from: NodeId, msg: AppendEntries) {
        if msg.term < self.current_term {
            self.pending_ready.messages.push((
                from,
                Message::AppendEntriesResponse {
                    from: self.id,
                    msg: AppendEntriesResponse {
                        term: self.current_term,
                        success: false,
                        match_index: 0,
                    },
                },
            ));
            return;
        }

        self.become_follower(msg.term, Some(from));
        self.reset_election_timeout();

        // Consistency check
        let prev_term_ok = self
            .log
            .term_at(msg.prev_log_index)
            .map(|t| t == msg.prev_log_term)
            .unwrap_or(false);

        if !prev_term_ok {
            self.pending_ready.messages.push((
                from,
                Message::AppendEntriesResponse {
                    from: self.id,
                    msg: AppendEntriesResponse {
                        term: self.current_term,
                        success: false,
                        match_index: self.log.last_index(),
                    },
                },
            ));
            return;
        }

        // Append new entries (truncating conflicts)
        if !msg.entries.is_empty() {
            let first = msg.entries[0].index;
            self.log.truncate_and_append(first, msg.entries.clone());
            self.pending_ready.entries_to_persist.extend(msg.entries);
        }

        // Advance commit index
        if msg.leader_commit > self.commit_index {
            self.commit_index = msg.leader_commit.min(self.log.last_index());
            self.apply_committed();
        }

        self.pending_ready.messages.push((
            from,
            Message::AppendEntriesResponse {
                from: self.id,
                msg: AppendEntriesResponse {
                    term: self.current_term,
                    success: true,
                    match_index: self.log.last_index(),
                },
            },
        ));
    }

    fn handle_append_entries_response(&mut self, from: NodeId, msg: AppendEntriesResponse) {
        if msg.term > self.current_term {
            self.become_follower(msg.term, None);
            return;
        }

        let Role::Leader { next_index, match_index } = &mut self.role else {
            return;
        };

        if msg.success {
            *match_index.entry(from).or_insert(0) = msg.match_index;
            *next_index.entry(from).or_insert(1) = msg.match_index + 1;
            self.advance_commit_index();
        } else {
            // Back off next_index for this peer
            let ni = next_index.entry(from).or_insert(1);
            *ni = (*ni).saturating_sub(1).max(msg.match_index + 1).max(1);
        }
    }

    // ── Commit ────────────────────────────────────────────────────────────

    fn advance_commit_index(&mut self) {
        let Role::Leader { match_index, .. } = &self.role else {
            return;
        };

        // Find the highest N such that a quorum has match_index >= N and log[N].term == current_term
        let quorum = (self.config.peers.len() + 1) / 2 + 1;
        let mut indices: Vec<LogIndex> = match_index.values().copied().collect();
        indices.push(self.log.last_index()); // leader always matches itself
        indices.sort_unstable();

        if let Some(&n) = indices.get(indices.len() - quorum) {
            if n > self.commit_index
                && self.log.term_at(n) == Some(self.current_term)
            {
                self.commit_index = n;
                self.apply_committed();
            }
        }
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(term) = self.log.term_at(self.last_applied) {
                let entry = LogEntry {
                    index: self.last_applied,
                    term,
                    command: self
                        .log
                        .entries_from(self.last_applied)
                        .first()
                        .map(|e| e.command.clone())
                        .unwrap_or_default(),
                };
                if !entry.command.is_empty() {
                    self.pending_ready.entries_to_apply.push(entry);
                }
            }
        }
    }

    // ── Role transitions ──────────────────────────────────────────────────

    fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower { leader_id };
        self.reset_election_timeout();
    }

    fn become_leader(&mut self) {
        info!(id = self.id, term = self.current_term, "became leader");
        let last = self.log.last_index();
        let next_index = self.config.peers.iter().map(|&p| (p, last + 1)).collect();
        let match_index = self.config.peers.iter().map(|&p| (p, 0)).collect();
        self.role = Role::Leader { next_index, match_index };
        self.heartbeat_ticks = 0;

        // Leader appends a no-op entry to commit entries from previous terms
        self.propose(vec![]);
    }

    fn reset_election_timeout(&mut self) {
        self.election_ticks = 0;
        self.election_timeout = rand::rng().random_range(
            self.config.election_timeout..self.config.election_timeout * 2,
        );
    }
}

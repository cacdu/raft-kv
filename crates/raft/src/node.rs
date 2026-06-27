use std::collections::{HashMap, HashSet};

use rand::Rng;
use tracing::{debug, info, warn};

use crate::{
    config::Config,
    log::RaftLog,
    message::{
        AppendEntries, AppendEntriesResponse, HardState, InstallSnapshot,
        InstallSnapshotResponse, LogEntry, LogIndex, Message, NodeId,
        RequestVote, RequestVoteResponse, Snapshot, Term,
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
        self.emit_hard_state();
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
            self.emit_hard_state();
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

            // If the peer's next_index falls inside the compacted log, we can't
            // send entries — send a snapshot instead. NodeHandle fills the data.
            if ni <= self.log.snapshot_index() {
                self.pending_ready.snapshot_to_send.push(peer);
                continue;
            }

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

    fn handle_install_snapshot(&mut self, from: NodeId, msg: InstallSnapshot) {
        if msg.term < self.current_term {
            self.pending_ready.messages.push((
                from,
                Message::InstallSnapshotResponse {
                    from: self.id,
                    msg: InstallSnapshotResponse { term: self.current_term, success: false },
                },
            ));
            return;
        }

        self.become_follower(msg.term, Some(from));
        self.reset_election_timeout();

        let snap = &msg.snapshot;

        // Only apply if the snapshot is newer than what we have.
        if snap.last_index <= self.commit_index {
            self.pending_ready.messages.push((
                from,
                Message::InstallSnapshotResponse {
                    from: self.id,
                    msg: InstallSnapshotResponse { term: self.current_term, success: true },
                },
            ));
            return;
        }

        // Compact the log to the snapshot position.
        self.log.compact(snap.last_index, snap.last_term);
        self.commit_index = snap.last_index;
        self.last_applied = snap.last_index;

        // Signal NodeHandle to replace the KV store with the snapshot data.
        self.pending_ready.snapshot_to_apply = Some(msg.snapshot.clone());

        self.pending_ready.messages.push((
            from,
            Message::InstallSnapshotResponse {
                from: self.id,
                msg: InstallSnapshotResponse { term: self.current_term, success: true },
            },
        ));
    }

    fn handle_install_snapshot_response(&mut self, from: NodeId, msg: InstallSnapshotResponse) {
        if msg.term > self.current_term {
            self.become_follower(msg.term, None);
            return;
        }
        if !msg.success {
            return;
        }
        let Role::Leader { next_index, match_index } = &mut self.role else {
            return;
        };
        // The peer now has the full snapshot; advance its indices.
        let snap_index = self.log.snapshot_index();
        *match_index.entry(from).or_insert(0) = snap_index;
        *next_index.entry(from).or_insert(1) = snap_index + 1;
        self.advance_commit_index();
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
                self.pending_ready.entries_to_apply.push(entry);
            }
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
        let next_index = self.config.peers.iter().map(|&p| (p, last + 1)).collect();
        let match_index = self.config.peers.iter().map(|&p| (p, 0)).collect();
        self.role = Role::Leader { next_index, match_index };
        self.heartbeat_ticks = 0;

        // Leader appends a no-op entry to commit entries from previous terms
        self.propose(vec![]);
    }

    fn reset_election_timeout(&mut self) {
        self.election_ticks = 0;
        // Upper bound is exclusive, so max timeout = 2*election_timeout - 1 ticks.
        self.election_timeout = rand::rng().random_range(
            self.config.election_timeout..self.config.election_timeout * 2,
        );
    }

    fn emit_hard_state(&mut self) {
        self.pending_ready.hard_state = Some(HardState {
            term: self.current_term,
            voted_for: self.voted_for,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::message::{AppendEntries, RequestVote};

    fn node(id: u64, peers: Vec<u64>) -> RaftNode {
        let mut cfg = Config::new(id, peers);
        // Pin timeouts so tests are deterministic: election fires after exactly 10 ticks.
        cfg.election_timeout = 10;
        cfg.heartbeat_timeout = 3;
        RaftNode::new(cfg)
    }

    /// Drive the node with N ticks and collect all Ready outputs.
    fn tick_n(node: &mut RaftNode, n: u32) -> Vec<Ready> {
        (0..n).map(|_| node.step(Message::Tick)).collect()
    }

    // ── 1.1.d: start_election emits HardState ────────────────────────────────

    #[test]
    fn tick_triggers_election_and_emits_hard_state() {
        let mut n = node(1, vec![2, 3]);
        // election_timeout is randomized in [10, 19], so 20 ticks always fires.
        let readies = tick_n(&mut n, 20);

        // Exactly one Ready should carry a HardState (the one where election fired).
        let with_hs: Vec<_> = readies.iter().filter(|r| r.hard_state.is_some()).collect();
        assert_eq!(with_hs.len(), 1, "expected exactly one HardState across all ticks");

        let hs = with_hs[0].hard_state.as_ref().unwrap();
        assert_eq!(hs.term, 1, "term must advance to 1 on first election");
        assert_eq!(hs.voted_for, Some(1), "candidate votes for itself");

        // The same Ready must contain RequestVote messages for each peer.
        let rv_count = with_hs[0]
            .messages
            .iter()
            .filter(|(_, m)| matches!(m, Message::RequestVote { .. }))
            .count();
        assert_eq!(rv_count, 2, "must send RequestVote to both peers");
    }

    // ── 1.1.c: become_follower emits HardState on term bump ──────────────────

    #[test]
    fn higher_term_append_entries_emits_hard_state() {
        let mut n = node(1, vec![2, 3]);

        let ready = n.step(Message::AppendEntries {
            from: 2,
            msg: AppendEntries {
                term: 5,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
        });

        let hs = ready.hard_state.expect("HardState must be emitted when term increases");
        assert_eq!(hs.term, 5);
        assert_eq!(hs.voted_for, None, "voted_for resets when adopting a new term");
    }

    // ── 1.1.c: same term AppendEntries does NOT emit HardState ───────────────

    #[test]
    fn same_term_append_entries_does_not_emit_hard_state() {
        let mut n = node(1, vec![2, 3]);

        // Bring node to term 3 first (consuming the HardState from that).
        n.step(Message::AppendEntries {
            from: 2,
            msg: AppendEntries {
                term: 3,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
        });

        // Now send another AppendEntries at the same term: no state change.
        let ready = n.step(Message::AppendEntries {
            from: 2,
            msg: AppendEntries {
                term: 3,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
        });

        assert!(
            ready.hard_state.is_none(),
            "no WAL write expected when term does not change"
        );
    }

    // ── 1.1.e: handle_request_vote emits HardState when granting vote ─────────

    #[test]
    fn granting_vote_emits_hard_state_with_voted_for() {
        let mut n = node(1, vec![2, 3]);

        let ready = n.step(Message::RequestVote {
            from: 2,
            msg: RequestVote {
                term: 1,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            },
        });

        let hs = ready.hard_state.expect("HardState must be emitted when vote is granted");
        assert_eq!(hs.term, 1);
        assert_eq!(hs.voted_for, Some(2));

        // The response must also confirm the grant.
        let granted = ready.messages.iter().any(|(dest, m)| {
            *dest == 2
                && matches!(
                    m,
                    Message::RequestVoteResponse { msg, .. } if msg.vote_granted
                )
        });
        assert!(granted, "RequestVoteResponse must carry vote_granted=true");
    }

    // ── 1.1.e: refusing a vote does NOT emit HardState ────────────────────────

    #[test]
    fn refusing_vote_does_not_emit_hard_state() {
        let mut n = node(1, vec![2, 3]);

        // Grant vote to node 2 first.
        n.step(Message::RequestVote {
            from: 2,
            msg: RequestVote { term: 1, candidate_id: 2, last_log_index: 0, last_log_term: 0 },
        });

        // Node 3 asks for a vote in the same term — must be refused (already voted).
        let ready = n.step(Message::RequestVote {
            from: 3,
            msg: RequestVote { term: 1, candidate_id: 3, last_log_index: 0, last_log_term: 0 },
        });

        assert!(
            ready.hard_state.is_none(),
            "no WAL write expected when vote is refused"
        );

        let refused = ready.messages.iter().any(|(dest, m)| {
            *dest == 3
                && matches!(
                    m,
                    Message::RequestVoteResponse { msg, .. } if !msg.vote_granted
                )
        });
        assert!(refused, "RequestVoteResponse must carry vote_granted=false");
    }

    // ── 1.2: restore() ───────────────────────────────────────────────────────

    #[test]
    fn restore_sets_hard_state() {
        let mut n = node(1, vec![2, 3]);
        n.restore(7, Some(2), 0, 0, vec![]);
        assert_eq!(n.current_term, 7);
        assert_eq!(n.voted_for, Some(2));
    }

    #[test]
    fn restore_loads_log_entries() {
        let mut n = node(1, vec![2, 3]);
        let entries = vec![
            LogEntry { index: 1, term: 1, command: b"set a 1".to_vec() },
            LogEntry { index: 2, term: 1, command: b"set b 2".to_vec() },
        ];
        n.restore(1, None, 0, 0, entries);

        assert_eq!(n.log.last_index(), 2);
        assert_eq!(n.log.term_at(1), Some(1));
        assert_eq!(n.log.term_at(2), Some(1));
    }

    #[test]
    fn restore_with_snapshot_sets_commit_base() {
        let mut n = node(1, vec![2, 3]);
        // Snapshot at index 10, plus two entries beyond it.
        let entries = vec![
            LogEntry { index: 11, term: 3, command: b"set c 3".to_vec() },
            LogEntry { index: 12, term: 3, command: b"set d 4".to_vec() },
        ];
        n.restore(3, None, 10, 2, entries);

        assert_eq!(n.commit_index, 10, "commit_index must start at snapshot base");
        assert_eq!(n.last_applied, 10, "last_applied must start at snapshot base");
        assert_eq!(n.log.last_index(), 12);
        assert_eq!(n.log.term_at(11), Some(3));
    }

    #[test]
    fn restore_empty_wal_is_noop() {
        let mut n = node(1, vec![2, 3]);
        n.restore(0, None, 0, 0, vec![]);
        assert_eq!(n.current_term, 0);
        assert_eq!(n.log.last_index(), 0);
    }

    #[test]
    fn restore_does_not_emit_hard_state_into_ready() {
        // restore() is not a step() — it must not leave stale Ready output.
        let mut n = node(1, vec![2, 3]);
        n.restore(5, Some(2), 0, 0, vec![]);
        // The next step() should return an empty Ready (no leftover from restore).
        let ready = n.step(Message::Tick);
        assert!(ready.hard_state.is_none(), "restore must not pollute pending_ready");
    }

    // ── 4.2: election and replication ─────────────────────────────────────────

    /// Tick node 1 past its election timeout, then feed its RequestVote to node 2.
    /// Returns (n1, n2, n1_after_vote_grant) so tests can inspect state.
    fn run_election() -> (RaftNode, RaftNode) {
        let mut n1 = node(1, vec![2, 3]);
        let mut n2 = node(2, vec![1, 3]);

        // 20 ticks guarantees election fires (timeout randomized in [10, 19]).
        let readies = tick_n(&mut n1, 20);

        // Find the ready that carries RequestVote messages.
        let election_ready = readies
            .into_iter()
            .find(|r| r.messages.iter().any(|(_, m)| matches!(m, Message::RequestVote { .. })))
            .expect("election must fire within 11 ticks");

        // Deliver RequestVote to n2 and get grant.
        let (_, rv_for_n2) = election_ready
            .messages
            .iter()
            .find(|(dest, m)| *dest == 2 && matches!(m, Message::RequestVote { .. }))
            .unwrap();
        let n2_ready = n2.step(rv_for_n2.clone());

        // Feed n2's vote grant back to n1 → should reach quorum and become leader.
        let (_, rvr) = n2_ready.messages.iter().find(|(dest, _)| *dest == 1).unwrap();
        n1.step(rvr.clone());

        (n1, n2)
    }

    #[test]
    fn test_full_election_with_3_nodes() {
        let (n1, n2) = run_election();
        assert!(n1.is_leader(), "n1 must become leader after receiving quorum of votes");
        assert!(!n2.is_leader(), "n2 must remain follower");
        assert_eq!(n1.current_term, 1);
    }

    #[test]
    fn test_basic_log_replication() {
        let (mut n1, mut n2) = run_election();

        // Drain the no-op AppendEntries that become_leader sends.
        // n1 is leader; find the AppendEntries for n2 in its pending messages
        // by stepping n2 and then delivering its response back to n1.
        let become_leader_ready = n1.step(Message::Propose { command: vec![0xAB] });

        // Deliver AppendEntries to n2.
        let (_, ae_msg) = become_leader_ready
            .messages
            .iter()
            .find(|(dest, m)| *dest == 2 && matches!(m, Message::AppendEntries { .. }))
            .expect("leader must send AppendEntries after propose");
        let n2_ready = n2.step(ae_msg.clone());

        // n2 must respond with success.
        let (_, aer_msg) = n2_ready
            .messages
            .iter()
            .find(|(dest, _)| *dest == 1)
            .expect("follower must respond to AppendEntries");
        assert!(
            matches!(aer_msg, Message::AppendEntriesResponse { msg, .. } if msg.success),
            "follower response must be success"
        );

        // Feed the response back to n1 to advance commit_index.
        let apply_ready = n1.step(aer_msg.clone());
        assert!(
            n1.commit_index > 0 || !apply_ready.entries_to_apply.is_empty(),
            "leader must advance commit_index after quorum ack"
        );
    }

    #[test]
    fn test_log_rollback_on_conflict() {
        let mut n2 = node(2, vec![1, 3]);

        // Plant three stale entries from term 1 directly.
        n2.log.append(LogEntry { index: 1, term: 1, command: b"old-1".to_vec() });
        n2.log.append(LogEntry { index: 2, term: 1, command: b"old-2".to_vec() });
        n2.log.append(LogEntry { index: 3, term: 1, command: b"old-3".to_vec() });

        // Leader (term 2) sends conflicting entries from index 2 onward.
        let ready = n2.step(Message::AppendEntries {
            from: 1,
            msg: AppendEntries {
                term: 2,
                leader_id: 1,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![
                    LogEntry { index: 2, term: 2, command: b"new-2".to_vec() },
                    LogEntry { index: 3, term: 2, command: b"new-3".to_vec() },
                ],
                leader_commit: 0,
            },
        });

        assert_eq!(n2.log.last_index(), 3, "log length must be preserved");
        assert_eq!(n2.log.term_at(2), Some(2), "index 2 must be overwritten with term 2");
        assert_eq!(n2.log.term_at(3), Some(2), "index 3 must be overwritten with term 2");

        let success = ready.messages.iter().any(|(_, m)| {
            matches!(m, Message::AppendEntriesResponse { msg, .. } if msg.success)
        });
        assert!(success, "follower must accept after truncation");
    }

    #[test]
    fn test_term_monotonicity() {
        let mut n = node(1, vec![2, 3]);

        let terms = [1u64, 3, 2, 5, 4, 7];
        let mut last_seen = 0u64;

        for t in terms {
            n.step(Message::AppendEntries {
                from: 2,
                msg: AppendEntries {
                    term: t,
                    leader_id: 2,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit: 0,
                },
            });
            assert!(
                n.current_term >= last_seen,
                "term must never decrease: was {last_seen}, now {}",
                n.current_term
            );
            last_seen = n.current_term;
        }
        assert_eq!(n.current_term, 7, "term must have reached max seen value");
    }

    #[test]
    fn test_leader_appends_noop_on_becoming_leader() {
        let (n1, _) = run_election();

        // After becoming leader, the no-op entry must be in the log.
        let last = n1.log.last_index();
        assert!(last > 0, "leader must have at least one entry (the no-op)");
        // The no-op itself: empty command, current term.
        let noop_term = n1.log.term_at(last).expect("no-op entry must exist");
        assert_eq!(noop_term, n1.current_term, "no-op must carry the current term");
    }
}

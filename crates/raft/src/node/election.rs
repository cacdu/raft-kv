use std::collections::HashSet;

use tracing::{debug, info};

use crate::message::{Message, NodeId, RequestVote, RequestVoteResponse};

use super::{RaftNode, Role};

impl RaftNode {
    pub(super) fn tick(&mut self) {
        match &self.role.clone() {
            Role::Follower { .. } | Role::Candidate { .. } => {
                if !self.is_voter {
                    return; // learners never start elections
                }
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

    fn start_election(&mut self) {
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.emit_hard_state();
        self.reset_election_timeout();

        let mut votes = HashSet::new();
        votes.insert(self.id);
        self.role = Role::Candidate {
            votes_received: votes,
        };

        info!(id = self.id, term = self.current_term, "starting election");

        let msg = RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };

        for &peer in self.membership.iter().filter(|&&p| p != self.id) {
            self.pending_ready.messages.push((
                peer,
                Message::RequestVote {
                    from: self.id,
                    msg: msg.clone(),
                },
            ));
        }
    }

    pub(super) fn handle_request_vote(&mut self, from: NodeId, msg: RequestVote) {
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

    pub(super) fn handle_request_vote_response(&mut self, from: NodeId, msg: RequestVoteResponse) {
        if msg.term > self.current_term {
            self.become_follower(msg.term, None);
            return;
        }

        let Role::Candidate {
            ref mut votes_received,
        } = self.role
        else {
            return;
        };

        if msg.vote_granted {
            votes_received.insert(from);
            let quorum = self.membership.len() / 2 + 1;
            if votes_received.len() >= quorum {
                self.become_leader();
            }
        }
    }
}

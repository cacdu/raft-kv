use tracing::{info, warn};

use crate::message::{ConfChangeCmd, ConfChangeOp, EntryType, LogEntry, NodeId};

use super::{RaftNode, Role};

impl RaftNode {
    /// Propose adding or removing a single voter.
    /// Pre-registers the new node in next_index/match_index so the leader
    /// immediately begins replicating to it while the ConfChange is in flight.
    pub(super) fn propose_conf_change_inner(
        &mut self,
        op: ConfChangeOp,
        node_id: NodeId,
        raft_addr: Option<String>,
        http_addr: Option<String>,
    ) {
        if !self.is_leader() {
            warn!(id = self.id, "not leader, dropping conf change");
            return;
        }
        if self.pending_conf_change {
            warn!(id = self.id, "conf change already in flight, ignoring");
            return;
        }

        if matches!(op, ConfChangeOp::Add) {
            // Let the leader start replicating to the new node before the
            // ConfChange is committed — it will catch up in the background.
            if let Role::Leader {
                ref mut next_index,
                ref mut match_index,
            } = self.role
            {
                let last = self.log.last_index();
                next_index.entry(node_id).or_insert(last + 1);
                match_index.entry(node_id).or_insert(0);
            }
        }

        let cmd = ConfChangeCmd {
            op,
            node_id,
            raft_addr,
            http_addr,
        };
        let command = serde_json::to_vec(&cmd).expect("ConfChangeCmd serialization is infallible");
        let entry = LogEntry {
            index: self.log.last_index() + 1,
            term: self.current_term,
            entry_type: EntryType::ConfChange,
            command,
        };
        self.pending_conf_change = true;
        self.pending_ready.entries_to_persist.push(entry.clone());
        self.log.append(entry);
        self.broadcast_append_entries();
    }

    /// Apply a committed ConfChange entry. Called from apply_committed.
    pub(super) fn apply_conf_change_entry(&mut self, command: &[u8]) {
        let Ok(cmd) = serde_json::from_slice::<ConfChangeCmd>(command) else {
            warn!(id = self.id, "failed to decode ConfChangeCmd");
            return;
        };
        self.pending_conf_change = false;
        info!(id = self.id, op = ?cmd.op, node = cmd.node_id, "applying conf change");

        match cmd.op {
            ConfChangeOp::Add => {
                self.membership.insert(cmd.node_id);
                // If this node is the one being added (was a learner), promote it.
                if cmd.node_id == self.id {
                    self.is_voter = true;
                }
                // Ensure the leader tracks the new node for replication.
                if let Role::Leader {
                    ref mut next_index,
                    ref mut match_index,
                } = self.role
                {
                    let last = self.log.last_index();
                    next_index.entry(cmd.node_id).or_insert(last + 1);
                    match_index.entry(cmd.node_id).or_insert(0);
                }
            }
            ConfChangeOp::Remove => {
                self.membership.remove(&cmd.node_id);
                if let Role::Leader {
                    ref mut next_index,
                    ref mut match_index,
                } = self.role
                {
                    next_index.remove(&cmd.node_id);
                    match_index.remove(&cmd.node_id);
                }
                // If this node is being removed, step down immediately.
                if cmd.node_id == self.id {
                    self.is_voter = false;
                    self.become_follower(self.current_term, None);
                }
            }
        }

        self.pending_ready.membership_change = Some(cmd.clone());
    }
}

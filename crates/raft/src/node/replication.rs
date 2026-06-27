use tracing::warn;

use crate::message::{
    AppendEntries, AppendEntriesResponse, EntryType, InstallSnapshot, InstallSnapshotResponse,
    LogEntry, LogIndex, Message, NodeId,
};

use super::{RaftNode, Role};

impl RaftNode {
    pub(super) fn propose(&mut self, command: Vec<u8>) {
        if !self.is_leader() {
            warn!(id = self.id, "not leader, dropping proposal");
            return;
        }
        let entry = LogEntry {
            index: self.log.last_index() + 1,
            term: self.current_term,
            entry_type: EntryType::Normal,
            command,
        };
        self.pending_ready.entries_to_persist.push(entry.clone());
        self.log.append(entry);
        self.broadcast_append_entries();
    }

    pub(super) fn broadcast_append_entries(&mut self) {
        let Role::Leader { next_index, .. } = &self.role else {
            return;
        };
        let peers: Vec<NodeId> = next_index.keys().copied().collect();
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
            let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);
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

    pub(super) fn handle_install_snapshot(&mut self, from: NodeId, msg: InstallSnapshot) {
        if msg.term < self.current_term {
            self.pending_ready.messages.push((
                from,
                Message::InstallSnapshotResponse {
                    from: self.id,
                    msg: InstallSnapshotResponse {
                        term: self.current_term,
                        success: false,
                    },
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
                    msg: InstallSnapshotResponse {
                        term: self.current_term,
                        success: true,
                    },
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
                msg: InstallSnapshotResponse {
                    term: self.current_term,
                    success: true,
                },
            },
        ));
    }

    pub(super) fn handle_install_snapshot_response(
        &mut self,
        from: NodeId,
        msg: InstallSnapshotResponse,
    ) {
        if msg.term > self.current_term {
            self.become_follower(msg.term, None);
            return;
        }
        if !msg.success {
            return;
        }
        let Role::Leader {
            next_index,
            match_index,
        } = &mut self.role
        else {
            return;
        };
        // The peer now has the full snapshot; advance its indices.
        let snap_index = self.log.snapshot_index();
        *match_index.entry(from).or_insert(0) = snap_index;
        *next_index.entry(from).or_insert(1) = snap_index + 1;
        self.advance_commit_index();
    }

    pub(super) fn handle_append_entries(&mut self, from: NodeId, msg: AppendEntries) {
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

    pub(super) fn handle_append_entries_response(
        &mut self,
        from: NodeId,
        msg: AppendEntriesResponse,
    ) {
        if msg.term > self.current_term {
            self.become_follower(msg.term, None);
            return;
        }

        let Role::Leader {
            next_index,
            match_index,
        } = &mut self.role
        else {
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

    pub(super) fn advance_commit_index(&mut self) {
        let Role::Leader { match_index, .. } = &self.role else {
            return;
        };

        // Only count voters (not pre-members admitted via propose_conf_change_inner but
        // whose ConfChange hasn't been applied yet — they're in next_index but not in membership).
        let quorum = self.membership.len() / 2 + 1;
        let mut indices: Vec<LogIndex> = match_index
            .iter()
            .filter(|(id, _)| self.membership.contains(*id))
            .map(|(_, &idx)| idx)
            .collect();
        indices.push(self.log.last_index()); // leader (self) always counts
        indices.sort_unstable();

        if let Some(&n) = indices.get(indices.len() - quorum) {
            if n > self.commit_index && self.log.term_at(n) == Some(self.current_term) {
                self.commit_index = n;
                self.apply_committed();
            }
        }
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.entries_from(self.last_applied).first().cloned() {
                if entry.entry_type == EntryType::ConfChange && !entry.command.is_empty() {
                    self.apply_conf_change_entry(&entry.command.clone());
                }
                self.pending_ready.entries_to_apply.push(entry);
            }
        }
    }
}

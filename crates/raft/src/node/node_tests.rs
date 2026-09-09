use super::{RaftNode, Ready, Restore, Role};
use crate::config::Config;
use crate::message::{
    AppendEntries, AppendEntriesResponse, EntryType, InstallSnapshot, InstallSnapshotResponse,
    LogEntry, Message, NodeId, RequestVote, Snapshot,
};

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
    // Timeout is randomized in [10, 19]. 19 ticks fires exactly one election:
    // it always fires by tick 19, and a second would need >= 20 ticks (10 to the
    // first fire + 10 more), so the count here can't be flaky.
    let readies = tick_n(&mut n, 19);

    // Exactly one Ready should carry a HardState (the one where election fired).
    let with_hs: Vec<_> = readies.iter().filter(|r| r.hard_state.is_some()).collect();
    assert_eq!(
        with_hs.len(),
        1,
        "expected exactly one HardState across all ticks"
    );

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

    let hs = ready
        .hard_state
        .expect("HardState must be emitted when term increases");
    assert_eq!(hs.term, 5);
    assert_eq!(
        hs.voted_for, None,
        "voted_for resets when adopting a new term"
    );
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

    let hs = ready
        .hard_state
        .expect("HardState must be emitted when vote is granted");
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
        msg: RequestVote {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        },
    });

    // Node 3 asks for a vote in the same term — must be refused (already voted).
    let ready = n.step(Message::RequestVote {
        from: 3,
        msg: RequestVote {
            term: 1,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        },
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
    n.restore(Restore {
        term: 7,
        voted_for: Some(2),
        ..Default::default()
    });
    assert_eq!(n.current_term, 7);
    assert_eq!(n.voted_for, Some(2));
}

#[test]
fn restore_loads_log_entries() {
    let mut n = node(1, vec![2, 3]);
    let entries = vec![
        LogEntry {
            index: 1,
            term: 1,
            entry_type: EntryType::Normal,
            command: b"set a 1".to_vec(),
        },
        LogEntry {
            index: 2,
            term: 1,
            entry_type: EntryType::Normal,
            command: b"set b 2".to_vec(),
        },
    ];
    n.restore(Restore {
        term: 1,
        entries,
        ..Default::default()
    });

    assert_eq!(n.log.last_index(), 2);
    assert_eq!(n.log.term_at(1), Some(1));
    assert_eq!(n.log.term_at(2), Some(1));
}

#[test]
fn restore_with_snapshot_sets_commit_base() {
    let mut n = node(1, vec![2, 3]);
    // Snapshot at index 10, plus two entries beyond it.
    let entries = vec![
        LogEntry {
            index: 11,
            term: 3,
            entry_type: EntryType::Normal,
            command: b"set c 3".to_vec(),
        },
        LogEntry {
            index: 12,
            term: 3,
            entry_type: EntryType::Normal,
            command: b"set d 4".to_vec(),
        },
    ];
    n.restore(Restore {
        term: 3,
        snapshot_index: 10,
        snapshot_term: 2,
        entries,
        ..Default::default()
    });

    assert_eq!(
        n.commit_index, 10,
        "commit_index must start at snapshot base"
    );
    assert_eq!(
        n.last_applied, 10,
        "last_applied must start at snapshot base"
    );
    assert_eq!(n.log.last_index(), 12);
    assert_eq!(n.log.term_at(11), Some(3));
}

#[test]
fn restore_empty_wal_is_noop() {
    let mut n = node(1, vec![2, 3]);
    n.restore(Restore::default());
    assert_eq!(n.current_term, 0);
    assert_eq!(n.log.last_index(), 0);
}

#[test]
fn restore_does_not_emit_hard_state_into_ready() {
    // restore() is not a step() — it must not leave stale Ready output.
    let mut n = node(1, vec![2, 3]);
    n.restore(Restore {
        term: 5,
        voted_for: Some(2),
        ..Default::default()
    });
    // The next step() should return an empty Ready (no leftover from restore).
    let ready = n.step(Message::Tick);
    assert!(
        ready.hard_state.is_none(),
        "restore must not pollute pending_ready"
    );
}

// ── 4.2: election and replication ─────────────────────────────────────────

/// Tick node 1 past its election timeout, then feed its RequestVote to node 2.
/// Returns (n1, n2) so tests can inspect state.
fn run_election() -> (RaftNode, RaftNode) {
    let mut n1 = node(1, vec![2, 3]);
    let mut n2 = node(2, vec![1, 3]);

    // 19 ticks fires exactly one election (timeout in [10, 19]): it always fires
    // by tick 19, and a second needs >= 20 ticks — so n1 can't slip to term 2.
    let readies = tick_n(&mut n1, 19);

    // Find the ready that carries RequestVote messages.
    let election_ready = readies
        .into_iter()
        .find(|r| {
            r.messages
                .iter()
                .any(|(_, m)| matches!(m, Message::RequestVote { .. }))
        })
        .expect("election must fire within 19 ticks");

    // Deliver RequestVote to n2 and get grant.
    let (_, rv_for_n2) = election_ready
        .messages
        .iter()
        .find(|(dest, m)| *dest == 2 && matches!(m, Message::RequestVote { .. }))
        .unwrap();
    let n2_ready = n2.step(rv_for_n2.clone());

    // Feed n2's vote grant back to n1 → should reach quorum and become leader.
    let (_, rvr) = n2_ready
        .messages
        .iter()
        .find(|(dest, _)| *dest == 1)
        .unwrap();
    n1.step(rvr.clone());

    (n1, n2)
}

#[test]
fn test_full_election_with_3_nodes() {
    let (n1, n2) = run_election();
    assert!(
        n1.is_leader(),
        "n1 must become leader after receiving quorum of votes"
    );
    assert!(!n2.is_leader(), "n2 must remain follower");
    assert_eq!(n1.current_term, 1);
}

#[test]
fn test_basic_log_replication() {
    let (mut n1, mut n2) = run_election();

    // Drain the no-op AppendEntries that become_leader sends.
    // n1 is leader; find the AppendEntries for n2 in its pending messages
    // by stepping n2 and then delivering its response back to n1.
    let become_leader_ready = n1.step(Message::Propose {
        command: vec![0xAB],
    });

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
    n2.log.append(LogEntry {
        index: 1,
        term: 1,
        entry_type: EntryType::Normal,
        command: b"old-1".to_vec(),
    });
    n2.log.append(LogEntry {
        index: 2,
        term: 1,
        entry_type: EntryType::Normal,
        command: b"old-2".to_vec(),
    });
    n2.log.append(LogEntry {
        index: 3,
        term: 1,
        entry_type: EntryType::Normal,
        command: b"old-3".to_vec(),
    });

    // Leader (term 2) sends conflicting entries from index 2 onward.
    let ready = n2.step(Message::AppendEntries {
        from: 1,
        msg: AppendEntries {
            term: 2,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![
                LogEntry {
                    index: 2,
                    term: 2,
                    entry_type: EntryType::Normal,
                    command: b"new-2".to_vec(),
                },
                LogEntry {
                    index: 3,
                    term: 2,
                    entry_type: EntryType::Normal,
                    command: b"new-3".to_vec(),
                },
            ],
            leader_commit: 0,
        },
    });

    assert_eq!(n2.log.last_index(), 3, "log length must be preserved");
    assert_eq!(
        n2.log.term_at(2),
        Some(2),
        "index 2 must be overwritten with term 2"
    );
    assert_eq!(
        n2.log.term_at(3),
        Some(2),
        "index 3 must be overwritten with term 2"
    );

    let success = ready
        .messages
        .iter()
        .any(|(_, m)| matches!(m, Message::AppendEntriesResponse { msg, .. } if msg.success));
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
    assert_eq!(
        noop_term, n1.current_term,
        "no-op must carry the current term"
    );
}

// ── BUG 1: a stale/duplicate AppendEntries must not truncate committed entries ─
//
// Complements `test_log_rollback_on_conflict` (which checks the *conflict* case).
// Here the incoming entries already match what the follower holds — a delayed
// duplicate that only carries the prefix [1,2]. Raft §5.3: entries that already
// match must NOT be deleted; only the first *conflicting* entry (and everything
// after it) is truncated. The current code truncates unconditionally from
// entries[0].index, silently discarding already-committed entries 3, 4 and 5.
#[test]
fn stale_append_entries_must_not_truncate_committed_prefix() {
    let mut n = node(2, vec![1, 3]);

    // Leader (term 1) replicates and commits five entries.
    let entries: Vec<LogEntry> = (1..=5)
        .map(|i| LogEntry {
            index: i,
            term: 1,
            entry_type: EntryType::Normal,
            command: format!("set k{i} {i}").into_bytes(),
        })
        .collect();
    n.step(Message::AppendEntries {
        from: 1,
        msg: AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: 5,
        },
    });
    assert_eq!(
        n.log.last_index(),
        5,
        "precondition: all five entries present"
    );
    assert_eq!(n.commit_index, 5, "precondition: entries are committed");

    // A delayed duplicate arrives carrying only the already-present prefix [1,2]
    // (e.g. an old in-flight AppendEntries clamped by max_entries_per_append).
    n.step(Message::AppendEntries {
        from: 1,
        msg: AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![
                LogEntry {
                    index: 1,
                    term: 1,
                    entry_type: EntryType::Normal,
                    command: b"set k1 1".to_vec(),
                },
                LogEntry {
                    index: 2,
                    term: 1,
                    entry_type: EntryType::Normal,
                    command: b"set k2 2".to_vec(),
                },
            ],
            leader_commit: 5,
        },
    });

    assert_eq!(
        n.log.last_index(),
        5,
        "committed entries 3,4,5 must survive a duplicate that carries only the prefix"
    );
    assert_eq!(
        n.log.term_at(5),
        Some(1),
        "entry 5 must remain addressable after the duplicate"
    );
}

// ── Fast-rollback hint & backoff (regression: replication deadlock) ───────
//
// A follower that fell behind — or that carries a divergent, uncommitted tail
// from a stale term — used to report `last_index()` on a failed AppendEntries.
// The leader treated that as a *floor* for next_index, so it could never back
// down past the tail and the follower never reconciled. The follower now
// returns the first index of the conflicting term (floored at its committed
// prefix), and the leader jumps next_index straight there.

fn entry(index: u64, term: u64) -> LogEntry {
    LogEntry {
        index,
        term,
        entry_type: EntryType::Normal,
        command: format!("e{index}").into_bytes(),
    }
}

fn failure_hint(ready: &Ready) -> u64 {
    ready
        .messages
        .iter()
        .find_map(|(_, m)| match m {
            Message::AppendEntriesResponse { msg, .. } if !msg.success => Some(msg.match_index),
            _ => None,
        })
        .expect("expected a failed AppendEntriesResponse")
}

#[test]
fn reject_hint_is_conflict_index_not_last_index() {
    let mut n = node(2, vec![1, 3]);
    // Committed prefix 1..=3 @ term 1.
    for i in 1..=3 {
        n.log.append(entry(i, 1));
    }
    // Divergent, uncommitted tail 4..=8 @ term 5 (left by a stale candidacy).
    for i in 4..=8 {
        n.log.append(entry(i, 5));
    }
    n.commit_index = 3;

    // Leader (term 6) probes inside the divergent region; prev term won't match.
    let ready = n.step(Message::AppendEntries {
        from: 1,
        msg: AppendEntries {
            term: 6,
            leader_id: 1,
            prev_log_index: 7,
            prev_log_term: 6,
            entries: vec![],
            leader_commit: 3,
        },
    });

    let hint = failure_hint(&ready);
    assert_eq!(
        hint, 4,
        "hint must be the first index of the conflicting term (4), floored at commit_index+1"
    );
    assert!(
        hint < n.log.last_index(),
        "hint ({hint}) must be below last_index ({}) — returning last_index is the deadlock bug",
        n.log.last_index()
    );
    assert!(
        hint > n.commit_index,
        "hint must never rewind into the committed prefix"
    );
}

#[test]
fn reject_hint_when_log_too_short() {
    let mut n = node(2, vec![1, 3]);
    for i in 1..=3 {
        n.log.append(entry(i, 1));
    }
    n.commit_index = 2;

    // Leader probes far ahead of what the follower has.
    let ready = n.step(Message::AppendEntries {
        from: 1,
        msg: AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 50,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 2,
        },
    });

    let hint = failure_hint(&ready);
    assert_eq!(
        hint,
        n.log.last_index() + 1,
        "a too-short follower must ask to resume just past its own tail"
    );
}

#[test]
fn leader_jumps_next_index_to_hint_monotonically() {
    let (mut n1, _) = run_election(); // n1 leader @ term 1
    for i in n1.log.last_index() + 1..=20 {
        n1.log.append(entry(i, 1));
    }
    // Simulate having probed peer 2 near the tip.
    if let Role::Leader { next_index, .. } = &mut n1.role {
        next_index.insert(2, 21);
    }

    // A failure carrying hint=4 must jump next_index straight to 4.
    n1.step(Message::AppendEntriesResponse {
        from: 2,
        msg: AppendEntriesResponse {
            term: 1,
            success: false,
            match_index: 4,
        },
    });
    let ni = match &n1.role {
        Role::Leader { next_index, .. } => next_index[&2],
        _ => panic!("n1 must still be leader"),
    };
    assert_eq!(
        ni, 4,
        "next_index must jump to the hint, not decrement by one"
    );

    // A stale, reordered failure with a higher hint must NOT push next_index back up.
    n1.step(Message::AppendEntriesResponse {
        from: 2,
        msg: AppendEntriesResponse {
            term: 1,
            success: false,
            match_index: 9,
        },
    });
    let ni = match &n1.role {
        Role::Leader { next_index, .. } => next_index[&2],
        _ => panic!("n1 must still be leader"),
    };
    assert_eq!(
        ni, 4,
        "a stale higher hint must not raise next_index (monotonic backoff)"
    );
}

// ── InstallSnapshot records the real leader (regression: follower-of-self) ─
//
// `handle_install_snapshot(from, _)` adopts `from` as the leader. The raft-kv
// wire layer used to pass the *destination* peer's id as the snapshot's
// leader_id, so a follower that installed a snapshot recorded itself as leader.
// This guards the core invariant: `from` is the leader, and leader_id follows it.

#[test]
fn install_snapshot_sets_leader_to_sender() {
    let self_id: NodeId = 3;
    let leader: NodeId = 2;
    let mut n = node(self_id, vec![1, leader]);
    // A short log so the snapshot's compaction path has something to compact.
    for i in 1..=5 {
        n.log.append(entry(i, 1));
    }

    n.step(Message::InstallSnapshot {
        from: leader,
        msg: InstallSnapshot {
            term: 7,
            leader_id: leader,
            snapshot: Snapshot {
                last_index: 3,
                last_term: 1,
                data: b"{}".to_vec(),
            },
        },
    });

    assert_eq!(
        n.leader_id(),
        Some(leader),
        "after installing a snapshot the follower must point at the sending leader, never itself"
    );
    assert_ne!(
        n.leader_id(),
        Some(self_id),
        "must not record itself as leader"
    );
    assert!(!n.is_leader());
}

// ── The commit index has to survive a restart ─────────────────────────────
//
// 0.1.3 persisted only term and voted_for, and `restore` reset both
// commit_index and last_applied to the snapshot base. The Raft dissertation
// does treat commitIndex as volatile — but this implementation builds guards on
// top of it, and a restart made them vacuous. The anti-truncation floor added
// in v0.1.3 is the sharpest example: its comment says it "can never rewind
// below a committed entry", and after a restart it could.

#[test]
fn a_restored_node_does_not_offer_to_rewind_below_its_committed_prefix() {
    let mut n = node(1, vec![2, 3]);
    n.restore(Restore {
        term: 3,
        commit: 40,
        snapshot_index: 10,
        snapshot_term: 3,
        entries: (11..=50).map(|i| entry(i, 3)).collect(),
        ..Default::default()
    });
    assert_eq!(n.commit_index, 40, "the durable commit index is recovered");

    // A leader probes at an index inside the committed prefix and the terms
    // disagree, so the follower answers with a rollback hint.
    let ready = n.step(Message::AppendEntries {
        from: 2,
        msg: AppendEntries {
            term: 5,
            leader_id: 2,
            prev_log_index: 20,
            prev_log_term: 99,
            entries: vec![],
            leader_commit: 40,
        },
    });

    // Floored at commit_index + 1. With commit_index reset to the snapshot base
    // the walk-back would have run all the way down to 11 — offering to erase
    // entries 11..40 that this node had already acknowledged as committed.
    assert_eq!(
        failure_hint(&ready),
        41,
        "the hint must never rewind below a committed entry"
    );
}

#[test]
fn restore_re_applies_the_committed_entries_above_the_snapshot() {
    // The state machine starts at the snapshot — entries above it were never
    // written to the store — but the commit index says those entries are
    // committed, so they are staged for re-application instead of waiting for a
    // leader to re-drive them. 0.1.3 set both indices to the snapshot base and
    // staged nothing.
    let mut n = node(1, vec![2, 3]);
    n.restore(Restore {
        term: 3,
        commit: 15,
        snapshot_index: 10,
        snapshot_term: 3,
        entries: (11..=20).map(|i| entry(i, 3)).collect(),
        ..Default::default()
    });

    assert_eq!(n.commit_index, 15, "the commit index is durable");
    let staged = n.take_ready();
    assert_eq!(
        staged
            .entries_to_apply
            .iter()
            .map(|e| e.index)
            .collect::<Vec<_>>(),
        (11..=15).collect::<Vec<_>>(),
        "exactly the committed entries the snapshot does not cover"
    );
    assert_eq!(
        n.last_applied, 15,
        "and nothing above the commit index is replayed"
    );
}

#[test]
fn a_persisted_commit_above_the_recovered_log_is_clamped() {
    // The WAL's tail can be torn away after the commit index reached disk. The
    // entries the commit names are then simply not there to apply.
    let mut n = node(1, vec![2, 3]);
    n.restore(Restore {
        term: 3,
        commit: 100,
        snapshot_index: 10,
        snapshot_term: 3,
        entries: (11..=20).map(|i| entry(i, 3)).collect(),
        ..Default::default()
    });
    assert_eq!(n.commit_index, 20, "clamped to what the log actually holds");
}

#[test]
fn advancing_the_commit_index_emits_hard_state() {
    // Persisting the commit index is only worth anything if it is written when
    // it moves, not just on a term or vote change.
    let (mut leader, _follower) = run_election();
    let ready = leader.step(Message::AppendEntriesResponse {
        from: 2,
        msg: AppendEntriesResponse {
            term: leader.current_term,
            success: true,
            match_index: leader.log.last_index(),
        },
    });
    let hard_state = ready
        .hard_state
        .expect("a commit advance must reach durable storage");
    assert_eq!(hard_state.commit, leader.commit_index);
    assert!(hard_state.commit > 0);
}

// ── A lagging peer must be credited with the snapshot it actually got ─────
//
// `handle_install_snapshot_response` advanced the peer to the *leader's*
// current `log.snapshot_index()`. If the leader compacted again between
// sending the RPC and reading the reply, the peer was credited with entries it
// had never seen, and those entries could then be counted towards a quorum.

#[test]
fn an_install_snapshot_response_advances_the_peer_to_what_it_installed() {
    let (mut leader, _follower) = run_election();
    let term = leader.current_term;
    for index in leader.log.last_index() + 1..=20 {
        leader.log.append(entry(index, term));
    }
    leader.commit_index = 20;
    leader.last_applied = 20;
    // The leader has moved on since the snapshot went out.
    leader.log.compact(15, term);
    if let Role::Leader {
        next_index,
        match_index,
    } = &mut leader.role
    {
        next_index.insert(2, 1);
        match_index.insert(2, 0);
    }

    leader.step(Message::InstallSnapshotResponse {
        from: 2,
        msg: InstallSnapshotResponse {
            term,
            success: true,
            last_index: 8,
        },
    });

    let Role::Leader {
        next_index,
        match_index,
    } = &leader.role
    else {
        panic!("the leader must still be the leader");
    };
    assert_eq!(
        match_index[&2], 8,
        "the peer installed snapshot 8, not the leader's current 15"
    );
    assert_eq!(next_index[&2], 9);
}

#[test]
fn a_peer_that_reports_no_index_keeps_the_old_behaviour() {
    // Pre-0.1.4 followers do not fill the field in; 0 must not be read as
    // "the peer installed nothing".
    let (mut leader, _follower) = run_election();
    let term = leader.current_term;
    for index in leader.log.last_index() + 1..=20 {
        leader.log.append(entry(index, term));
    }
    leader.commit_index = 20;
    leader.last_applied = 20;
    leader.log.compact(15, term);
    if let Role::Leader {
        next_index,
        match_index,
    } = &mut leader.role
    {
        next_index.insert(2, 1);
        match_index.insert(2, 0);
    }

    leader.step(Message::InstallSnapshotResponse {
        from: 2,
        msg: InstallSnapshotResponse {
            term,
            success: true,
            last_index: 0,
        },
    });

    let Role::Leader { match_index, .. } = &leader.role else {
        panic!("the leader must still be the leader");
    };
    assert_eq!(match_index[&2], 15);
}

#[test]
fn a_node_with_an_empty_log_can_install_a_snapshot_from_far_ahead() {
    // The whole point of InstallSnapshot: the follower is so far behind that
    // the leader has no entries left to send it. Compacting to an index beyond
    // the end of the log used to panic.
    let mut n = node(3, vec![1, 2]);
    let ready = n.step(Message::InstallSnapshot {
        from: 1,
        msg: InstallSnapshot {
            term: 7,
            leader_id: 1,
            snapshot: Snapshot {
                last_index: 49_400,
                last_term: 7,
                data: b"{}".to_vec(),
            },
        },
    });

    assert_eq!(n.log.snapshot_index(), 49_400);
    assert_eq!(
        n.log.last_index(),
        49_400,
        "nothing survives below the base"
    );
    assert_eq!(n.commit_index, 49_400);
    assert!(
        ready.snapshot_to_apply.is_some(),
        "the state machine must be handed the snapshot to apply"
    );
    let hint = ready
        .messages
        .iter()
        .find_map(|(_, m)| match m {
            Message::InstallSnapshotResponse { msg, .. } => Some(msg),
            _ => None,
        })
        .expect("the follower must answer");
    assert!(hint.success);
    assert_eq!(hint.last_index, 49_400);
}

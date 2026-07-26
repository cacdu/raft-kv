use super::{RaftNode, Ready};
use crate::config::Config;
use crate::message::{AppendEntries, EntryType, LogEntry, Message, RequestVote};

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
    n.restore(7, Some(2), 0, 0, vec![]);
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
    n.restore(3, None, 10, 2, entries);

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

    // 20 ticks guarantees election fires (timeout randomized in [10, 19]).
    let readies = tick_n(&mut n1, 20);

    // Find the ready that carries RequestVote messages.
    let election_ready = readies
        .into_iter()
        .find(|r| {
            r.messages
                .iter()
                .any(|(_, m)| matches!(m, Message::RequestVote { .. }))
        })
        .expect("election must fire within 20 ticks");

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

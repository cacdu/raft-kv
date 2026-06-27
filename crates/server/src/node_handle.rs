/// NodeHandle wraps the Raft state machine behind a Mutex and drives it:
/// - applies Ready output (persists WAL, applies KV commands, fans out messages)
/// - provides async methods for the gRPC and HTTP layers
use std::{collections::HashMap, sync::Arc};

use tokio::sync::{oneshot, watch, Mutex};
use tracing::{debug, warn};

use raft::{Config, HardState, LogEntry, LogIndex, Message, NodeId, RaftNode, Ready, Snapshot, Term};
use storage::{KvStore, Wal};
use storage::wal::WalRecord;

use crate::peer::PeerClient;

pub struct NodeHandle {
    node: Mutex<RaftNode>,
    kv: Arc<Mutex<KvStore>>,
    wal: Arc<Mutex<Wal>>,
    peers: Mutex<HashMap<NodeId, PeerClient>>,
    /// Waiting HTTP handlers: log index → oneshot sender notified on commit.
    pending_proposals: Mutex<HashMap<LogIndex, oneshot::Sender<()>>>,
    /// Broadcast channel: carries the highest log index applied to the KV store.
    /// Read handlers subscribe to this to implement wait-for-apply.
    applied_tx: watch::Sender<LogIndex>,
    applied_rx: watch::Receiver<LogIndex>,
    /// Last snapshot taken for this node (used to send to lagging peers).
    last_snapshot: Mutex<Option<Snapshot>>,
}

impl NodeHandle {
    pub fn new(
        config: Config,
        records: Vec<WalRecord>,
        kv: Arc<Mutex<KvStore>>,
        wal: Arc<Mutex<Wal>>,
    ) -> Self {
        let mut node = RaftNode::new(config);
        if !records.is_empty() {
            let (term, voted_for) = replay_hard_state(&records);
            let (snapshot_index, snapshot_term) = replay_snapshot(&records);
            let entries = replay_entries(&records);
            node.restore(term, voted_for, snapshot_index, snapshot_term, entries);
        }
        let (applied_tx, applied_rx) = watch::channel(0u64);
        Self {
            node: Mutex::new(node),
            kv,
            wal,
            peers: Mutex::new(HashMap::new()),
            pending_proposals: Mutex::new(HashMap::new()),
            applied_tx,
            applied_rx,
            last_snapshot: Mutex::new(None),
        }
    }

    pub async fn register_peers(&self, peers: HashMap<NodeId, String>) {
        let mut map = self.peers.lock().await;
        for (id, addr) in peers {
            map.insert(id, PeerClient::new(id, addr));
        }
    }

    pub async fn tick(&self) {
        let ready = {
            let mut node = self.node.lock().await;
            node.step(Message::Tick)
        };
        self.process_ready(ready).await;
        self.update_state_metrics().await;
    }

    async fn update_state_metrics(&self) {
        let (term, is_leader, commit) = {
            let node = self.node.lock().await;
            (node.current_term, node.is_leader(), node.commit_index)
        };
        crate::metrics::CURRENT_TERM.set(term as f64);
        crate::metrics::IS_LEADER.set(if is_leader { 1.0 } else { 0.0 });
        crate::metrics::COMMIT_INDEX.set(commit as f64);
    }

    /// Submit a command to the replicated log.
    /// Returns a receiver that resolves when the entry is committed and applied,
    /// or None if this node is not the leader.
    /// Lock order: node → pending_proposals (never reversed elsewhere).
    pub async fn propose(&self, command: Vec<u8>) -> Option<oneshot::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        let ready = {
            let mut node = self.node.lock().await;
            if !node.is_leader() {
                return None;
            }
            let index = node.log.last_index() + 1;
            // Register before stepping: a concurrent tick must not apply the entry
            // before we have a receiver waiting for it.
            self.pending_proposals.lock().await.insert(index, tx);
            node.step(Message::Propose { command })
        };
        self.process_ready(ready).await;
        Some(rx)
    }

    pub async fn leader_id(&self) -> Option<NodeId> {
        self.node.lock().await.leader_id()
    }

    pub async fn is_leader(&self) -> bool {
        self.node.lock().await.is_leader()
    }

    /// If this node is the current leader, return its commit_index as the read_index.
    /// Returns None for followers — callers should redirect to the leader.
    pub async fn read_index_if_leader(&self) -> Option<LogIndex> {
        let node = self.node.lock().await;
        if node.is_leader() { Some(node.commit_index) } else { None }
    }

    /// Subscribe to applied-index updates. The returned receiver resolves each time
    /// the applied index advances. Clone it per-request — cloning is cheap.
    pub fn subscribe_applied(&self) -> watch::Receiver<LogIndex> {
        self.applied_rx.clone()
    }

    /// Serialize the KV store and compact the Raft log up to `index`.
    /// Writes a WalRecord::Snapshot so the snapshot survives restarts.
    /// No-op if `index` is not yet applied.
    pub async fn try_compact(&self, index: LogIndex) {
        let data = {
            let kv = self.kv.lock().await;
            kv.snapshot()
        };
        let (last_index, last_term) = {
            let mut node = self.node.lock().await;
            if index > node.commit_index { return; }
            let term = node.log.term_at(index).unwrap_or(0);
            node.log.compact(index, term);
            (index, term)
        };
        {
            let mut wal = self.wal.lock().await;
            if let Err(e) = wal.append(&WalRecord::Snapshot { last_index, last_term }) {
                warn!("WAL snapshot write failed: {e}");
            }
        }
        let snap = Snapshot { last_index, last_term, data };
        *self.last_snapshot.lock().await = Some(snap);
        debug!(index = last_index, "snapshot taken");
    }

    /// Replace the KV store with snapshot data received from the leader.
    /// Rebuilds the Raft log and WAL to reflect the new snapshot base.
    pub async fn apply_snapshot(&self, snapshot: Snapshot) {
        {
            let mut kv = self.kv.lock().await;
            if let Err(e) = kv.restore(&snapshot.data) {
                warn!("snapshot deserialize failed: {e}");
                return;
            }
        }
        {
            let mut node = self.node.lock().await;
            let (term, voted_for) = (node.current_term, node.voted_for);
            node.restore(term, voted_for, snapshot.last_index, snapshot.last_term, vec![]);
        }
        {
            let mut wal = self.wal.lock().await;
            if let Err(e) = wal.append(&WalRecord::Snapshot {
                last_index: snapshot.last_index,
                last_term: snapshot.last_term,
            }) {
                warn!("WAL snapshot write failed: {e}");
            }
        }
        let _ = self.applied_tx.send(snapshot.last_index);
        crate::metrics::APPLIED_INDEX.set(snapshot.last_index as f64);
        *self.last_snapshot.lock().await = Some(snapshot);
    }

    /// Drop all pending proposal senders, causing their receivers to resolve with Err.
    /// Called when this node loses leadership so HTTP handlers fail fast instead of timing out.
    pub async fn drain_pending_proposals(&self) {
        self.pending_proposals.lock().await.clear();
    }

    /// If a hard state change just happened and we are no longer leader, drain proposals.
    /// `hard_state.is_some()` is a cheap filter: term/voted_for only change on role transitions.
    async fn drain_if_lost_leadership(&self, ready: &Ready) {
        if ready.hard_state.is_some() && !self.node.lock().await.is_leader() {
            self.drain_pending_proposals().await;
        }
    }

    /// Called by gRPC server handlers: runs a single SM step, persists durable state,
    /// and returns the response message that must be sent back as the RPC reply.
    /// Does NOT fan out to peers — the response goes directly via the gRPC return value.
    pub async fn step_rpc(&self, msg: Message) -> Option<Message> {
        let ready = {
            let mut node = self.node.lock().await;
            node.step(msg)
        };
        let response = ready.messages.iter()
            .find(|(_, m)| matches!(
                m,
                Message::RequestVoteResponse { .. } | Message::AppendEntriesResponse { .. }
            ))
            .map(|(_, m)| m.clone());
        self.persist(&ready).await;
        self.drain_if_lost_leadership(&ready).await;
        response
    }

    async fn persist(&self, ready: &Ready) {
        let needs_wal = ready.hard_state.is_some() || !ready.entries_to_persist.is_empty();
        if needs_wal {
            let mut wal = self.wal.lock().await;
            if let Some(HardState { term, voted_for }) = ready.hard_state {
                if let Err(e) = wal.append(&WalRecord::HardState { term, voted_for }) {
                    warn!("WAL HardState write failed: {e}");
                }
            }
            for entry in &ready.entries_to_persist {
                if let Err(e) = wal.append(&WalRecord::Entry(entry.clone())) {
                    warn!("WAL entry write failed: {e}");
                }
            }
        }
        if !ready.entries_to_apply.is_empty() {
            {
                let mut kv = self.kv.lock().await;
                for entry in &ready.entries_to_apply {
                    if let Err(e) = kv.apply(&entry.command) {
                        warn!("KV apply failed: {e}");
                    }
                }
            } // kv lock released before notifying — client can read immediately
            let mut pending = self.pending_proposals.lock().await;
            for entry in &ready.entries_to_apply {
                if let Some(tx) = pending.remove(&entry.index) {
                    let _ = tx.send(());
                }
            }
            // Advance the applied watch so read handlers waiting on wait-for-apply unblock.
            // This goes after write notifications: a reader unblocking after a writer's 200 OK
            // is guaranteed to find the value in the KV store.
            if let Some(max_idx) = ready.entries_to_apply.iter().map(|e| e.index).max() {
                let _ = self.applied_tx.send(max_idx);
                crate::metrics::APPLIED_INDEX.set(max_idx as f64);
                // Compact the log every 50 applied entries to bound log size.
                const COMPACTION_THRESHOLD: u64 = 50;
                if max_idx % COMPACTION_THRESHOLD == 0 {
                    self.try_compact(max_idx).await;
                }
            }
        }

        // Follower received a snapshot from the leader — replace KV store.
        if let Some(snap) = ready.snapshot_to_apply.clone() {
            self.apply_snapshot(snap).await;
        }
    }

    async fn process_ready(&self, ready: Ready) {
        self.persist(&ready).await;

        // Leader: send snapshots to lagging peers that can't be caught up via AppendEntries.
        if !ready.snapshot_to_send.is_empty() {
            let snap_opt = self.last_snapshot.lock().await.clone();
            if let Some(snap) = snap_opt {
                let leader_term = self.node.lock().await.current_term;
                let clients: Vec<PeerClient> = {
                    let peers = self.peers.lock().await;
                    ready.snapshot_to_send.iter()
                        .filter_map(|id| peers.get(id).cloned())
                        .collect()
                };
                for client in clients {
                    debug!(peer = client.id, index = snap.last_index, "sending snapshot");
                    if let Some(resp) = client.send_install_snapshot(leader_term, snap.clone()).await {
                        let inner = { let mut node = self.node.lock().await; node.step(resp) };
                        self.persist(&inner).await;
                    }
                }
            }
        }

        if ready.messages.is_empty() {
            return;
        }

        // Clone peer clients before releasing the lock so we can await I/O without holding it.
        let sends: Vec<(PeerClient, Message)> = {
            let peers = self.peers.lock().await;
            ready.messages
                .into_iter()
                .filter_map(|(dest, msg)| peers.get(&dest).cloned().map(|c| (c, msg)))
                .collect()
        };

        // Fan out RPCs and collect responses (no lock held during network I/O).
        let mut responses = Vec::new();
        for (client, msg) in sends {
            debug!(peer = client.id, "sending message");
            if let Some(resp) = client.send(msg).await {
                responses.push(resp);
            }
        }

        if responses.is_empty() {
            return;
        }

        // Step the SM with all collected responses and persist the resulting state.
        // Responses can advance commit_index (entries_to_apply) or trigger role changes
        // (e.g., becoming leader after quorum of votes, which emits AppendEntries).
        let inner_ready = {
            let mut node = self.node.lock().await;
            let mut combined = Ready::default();
            for resp in responses {
                let r = node.step(resp);
                if r.hard_state.is_some() {
                    combined.hard_state = r.hard_state;
                }
                combined.entries_to_persist.extend(r.entries_to_persist);
                combined.entries_to_apply.extend(r.entries_to_apply);
                combined.messages.extend(r.messages);
            }
            combined
        };

        self.persist(&inner_ready).await;
        self.drain_if_lost_leadership(&inner_ready).await;

        // Fan out any messages generated by response handling (e.g., new leader's first
        // AppendEntries broadcast). We don't collect responses from this second level —
        // AppendEntries responses only advance commit_index, never generate new RPCs.
        if !inner_ready.messages.is_empty() {
            let sends2: Vec<(PeerClient, Message)> = {
                let peers = self.peers.lock().await;
                inner_ready.messages
                    .into_iter()
                    .filter_map(|(dest, msg)| peers.get(&dest).cloned().map(|c| (c, msg)))
                    .collect()
            };
            for (client, msg) in sends2 {
                debug!(peer = client.id, "sending message (post-response)");
                client.send(msg).await;
            }
        }
    }
}

// ── WAL replay helpers ────────────────────────────────────────────────────────
// Each function makes a single pass over the records; three passes is fine
// because WAL replay is a startup-only path.

fn replay_hard_state(records: &[WalRecord]) -> (Term, Option<NodeId>) {
    let mut term: Term = 0;
    let mut voted_for: Option<NodeId> = None;
    for r in records {
        if let WalRecord::HardState { term: t, voted_for: v } = r {
            term = *t;
            voted_for = *v;
        }
    }
    (term, voted_for)
}

fn replay_snapshot(records: &[WalRecord]) -> (LogIndex, Term) {
    let mut snapshot_index: LogIndex = 0;
    let mut snapshot_term: Term = 0;
    for r in records {
        if let WalRecord::Snapshot { last_index, last_term } = r {
            snapshot_index = *last_index;
            snapshot_term = *last_term;
        }
    }
    (snapshot_index, snapshot_term)
}

/// Rebuild the log entry list from WAL records, handling conflicts:
/// when a later record appears at an already-seen index, it truncates
/// forward — matching what `RaftLog::truncate_and_append` does at runtime.
fn replay_entries(records: &[WalRecord]) -> Vec<LogEntry> {
    let mut entries: Vec<LogEntry> = Vec::new();
    for r in records {
        if let WalRecord::Entry(e) = r {
            // Truncate anything at this index or beyond, then push.
            // This replays the same conflict-resolution logic as the live path.
            entries.retain(|x| x.index < e.index);
            entries.push(e.clone());
        }
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::LogEntry;
    use storage::wal::WalRecord;

    fn entry(index: u64, term: u64) -> WalRecord {
        WalRecord::Entry(LogEntry { index, term, command: vec![] })
    }

    fn hard_state(term: u64, voted_for: Option<u64>) -> WalRecord {
        WalRecord::HardState { term, voted_for }
    }

    // ── replay_hard_state ─────────────────────────────────────────────────────

    #[test]
    fn replay_hard_state_returns_last_record() {
        let records = vec![
            hard_state(1, Some(2)),
            hard_state(2, None),
            hard_state(3, Some(1)),
        ];
        let (term, voted_for) = replay_hard_state(&records);
        assert_eq!(term, 3);
        assert_eq!(voted_for, Some(1));
    }

    #[test]
    fn replay_hard_state_empty_returns_zero() {
        let (term, voted_for) = replay_hard_state(&[]);
        assert_eq!(term, 0);
        assert_eq!(voted_for, None);
    }

    // ── replay_entries ────────────────────────────────────────────────────────

    #[test]
    fn replay_entries_basic_sequence() {
        let records = vec![entry(1, 1), entry(2, 1), entry(3, 1)];
        let entries = replay_entries(&records);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[2].index, 3);
    }

    #[test]
    fn replay_entries_conflict_truncates_forward() {
        // Simulates a term change: index 2 and 3 were overwritten by a new leader.
        let records = vec![
            entry(1, 1),
            entry(2, 1),
            entry(3, 1),
            entry(2, 2), // new leader overwrites from index 2
            entry(3, 2),
        ];
        let entries = replay_entries(&records);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[1].index, 2);
        assert_eq!(entries[1].term, 2, "conflict at index 2: term 2 must win over term 1");
        assert_eq!(entries[2].term, 2, "entry 3 from term 2 must be kept");
    }

    #[test]
    fn replay_entries_ignores_non_entry_records() {
        let records = vec![
            hard_state(1, Some(2)),
            entry(1, 1),
            WalRecord::Snapshot { last_index: 5, last_term: 1 },
            entry(6, 2),
        ];
        let entries = replay_entries(&records);
        // Snapshot records don't filter entries here; that filtering is done in NodeHandle::new
        // by passing snapshot_index to RaftNode::restore which sets the log base.
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[1].index, 6);
    }

    // ── replay_snapshot ───────────────────────────────────────────────────────

    #[test]
    fn replay_snapshot_returns_last() {
        let records = vec![
            WalRecord::Snapshot { last_index: 10, last_term: 2 },
            WalRecord::Snapshot { last_index: 20, last_term: 3 },
        ];
        let (idx, term) = replay_snapshot(&records);
        assert_eq!(idx, 20);
        assert_eq!(term, 3);
    }

    #[test]
    fn replay_snapshot_none_returns_zero() {
        let (idx, term) = replay_snapshot(&[hard_state(1, None)]);
        assert_eq!(idx, 0);
        assert_eq!(term, 0);
    }

    // ── 2.1: pending_proposals ────────────────────────────────────────────────

    use tokio::sync::oneshot;

    #[test]
    fn propose_returns_none_if_not_leader() {
        // A freshly created node is a follower — propose must return None.
        let handle = make_handle(1, vec![2, 3]);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let rx = rt.block_on(handle.propose(b"set foo bar".to_vec()));
        assert!(rx.is_none(), "follower must not accept proposals");
    }

    #[test]
    fn drain_clears_all_pending_proposals() {
        // Insert two senders into pending_proposals and drain them.
        // Each receiver should resolve with Err (sender dropped).
        let handle = make_handle(1, vec![2, 3]);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let (tx1, mut rx1) = oneshot::channel::<()>();
        let (tx2, mut rx2) = oneshot::channel::<()>();
        rt.block_on(async {
            handle.pending_proposals.lock().await.insert(1, tx1);
            handle.pending_proposals.lock().await.insert(2, tx2);
            handle.drain_pending_proposals().await;
        });

        // Both receivers should immediately resolve with Err (senders were dropped).
        assert!(rx1.try_recv().is_err(), "rx1 should be resolved after drain");
        assert!(rx2.try_recv().is_err(), "rx2 should be resolved after drain");
        rt.block_on(async {
            assert!(
                handle.pending_proposals.lock().await.is_empty(),
                "pending_proposals must be empty after drain"
            );
        });
    }

    // ── 2.2: drain on leadership loss ─────────────────────────────────────────

    #[test]
    fn drain_if_lost_leadership_drains_when_not_leader() {
        // Simulate: node has a pending proposal, then receives a Ready that
        // carries a new HardState (term bump) and the node is a follower.
        let handle = make_handle(1, vec![2, 3]);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let (tx, mut rx) = oneshot::channel::<()>();

        rt.block_on(async {
            // Plant a pending proposal at index 1
            handle.pending_proposals.lock().await.insert(1, tx);

            // Build a Ready that looks like a term bump (hard_state present)
            // The node is still a follower (never became leader), so is_leader() == false
            let ready = raft::Ready {
                hard_state: Some(raft::HardState { term: 3, voted_for: Some(2) }),
                ..Default::default()
            };

            handle.drain_if_lost_leadership(&ready).await;
        });

        // The sender was dropped → receiver resolves with Err immediately
        assert!(rx.try_recv().is_err(), "proposal must be drained on leadership loss");
        rt.block_on(async {
            assert!(handle.pending_proposals.lock().await.is_empty());
        });
    }

    #[test]
    fn drain_if_lost_leadership_noop_when_hard_state_absent() {
        // If hard_state is None, no role change happened — proposals must survive.
        let handle = make_handle(1, vec![2, 3]);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let (tx, _rx) = oneshot::channel::<()>();
        rt.block_on(async {
            handle.pending_proposals.lock().await.insert(1, tx);

            let ready = raft::Ready { hard_state: None, ..Default::default() };
            handle.drain_if_lost_leadership(&ready).await;

            assert_eq!(
                handle.pending_proposals.lock().await.len(), 1,
                "proposals must survive when no role change occurred"
            );
        });
    }

    // ── 3.1: ReadIndex — applied watch ───────────────────────────────────────

    #[test]
    fn read_index_if_leader_returns_none_for_follower() {
        let handle = make_handle(1, vec![2, 3]);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(handle.read_index_if_leader());
        assert!(result.is_none(), "follower must return None for read_index");
    }

    #[test]
    fn applied_watch_advances_on_entry_apply() {
        let handle = make_handle(1, vec![2, 3]);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let rx = handle.subscribe_applied();
        assert_eq!(*rx.borrow(), 0, "initial applied index must be 0");

        let ready = Ready {
            entries_to_apply: vec![
                LogEntry { index: 1, term: 1, command: vec![] },
                LogEntry { index: 2, term: 1, command: vec![] },
            ],
            ..Default::default()
        };
        rt.block_on(handle.persist(&ready));

        // The watch must have advanced to 2 (the max applied index).
        assert_eq!(*rx.borrow(), 2, "applied watch must advance to max applied index");
    }

    fn make_handle(id: u64, peers: Vec<u64>) -> Arc<NodeHandle> {
        use raft::Config;
        use storage::{KvStore, Wal};
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let cfg = Config::new(id, peers);
        // Use a temp file WAL for tests
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let (wal, records) = Wal::open(tmp.path()).unwrap();
        let kv = Arc::new(Mutex::new(KvStore::default()));
        let wal = Arc::new(Mutex::new(wal));
        Arc::new(NodeHandle::new(cfg, records, kv, wal))
    }
}

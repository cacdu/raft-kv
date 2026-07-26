/// NodeHandle wraps the Raft state machine behind a Mutex and drives it:
/// - applies Ready output (persists WAL, applies KV commands, fans out messages)
/// - provides async methods for the gRPC and HTTP layers
use std::{collections::HashMap, sync::Arc};

use tokio::sync::{broadcast, oneshot, watch, Mutex};
use tracing::{debug, warn};

use raft::{
    message::EntryType, ConfChangeCmd, ConfChangeOp, Config, HardState, LogEntry, LogIndex,
    Message, NodeId, RaftNode, Ready, Snapshot, Term,
};
use storage::kv::Command;
use storage::wal::WalRecord;
use storage::{KvStore, Wal};

use crate::events::Event;
use crate::peer::PeerClient;

/// Buffered events per subscriber before a slow receiver starts lagging.
/// A lagged receiver gets `RecvError::Lagged` and must resync from the store.
const EVENT_CHANNEL_CAPACITY: usize = 4096;

pub struct NodeHandle {
    node: Mutex<RaftNode>,
    kv: Arc<Mutex<KvStore>>,
    wal: Arc<Mutex<Wal>>,
    peers: Mutex<HashMap<NodeId, PeerClient>>,
    /// HTTP addresses for peer nodes — used to forward client requests to the leader.
    pub http_peers: Mutex<HashMap<NodeId, String>>,
    /// Waiting HTTP handlers: log index → oneshot sender notified on commit.
    pending_proposals: Mutex<HashMap<LogIndex, oneshot::Sender<()>>>,
    /// Broadcast channel: carries the highest log index applied to the KV store.
    /// Read handlers subscribe to this to implement wait-for-apply.
    applied_tx: watch::Sender<LogIndex>,
    applied_rx: watch::Receiver<LogIndex>,
    /// Last snapshot taken for this node (used to send to lagging peers).
    last_snapshot: Mutex<Option<Snapshot>>,
    /// Broadcast channel: every command applied to the KV store, in log order.
    /// Backs `RaftKv::subscribe` — the embedded-mode watch API.
    events_tx: broadcast::Sender<Event>,
}

impl NodeHandle {
    pub fn new(
        config: Config,
        records: Vec<WalRecord>,
        kv: Arc<Mutex<KvStore>>,
        wal: Arc<Mutex<Wal>>,
    ) -> Self {
        Self::new_inner(RaftNode::new(config), records, kv, wal)
    }

    pub fn new_learner(
        config: Config,
        records: Vec<WalRecord>,
        kv: Arc<Mutex<KvStore>>,
        wal: Arc<Mutex<Wal>>,
    ) -> Self {
        Self::new_inner(RaftNode::new_learner(config), records, kv, wal)
    }

    fn new_inner(
        mut node: RaftNode,
        records: Vec<WalRecord>,
        kv: Arc<Mutex<KvStore>>,
        wal: Arc<Mutex<Wal>>,
    ) -> Self {
        let mut last_snapshot = None;
        let mut applied: LogIndex = 0;
        if !records.is_empty() {
            let (term, voted_for) = replay_hard_state(&records);
            let (snapshot_index, snapshot_term, snapshot_data) = replay_snapshot(&records);
            let entries = replay_entries(&records, snapshot_index);
            if snapshot_index > 0 {
                // The entries covered by the snapshot were compacted away and
                // will never be re-applied: the state machine must be rebuilt
                // from the snapshot data or those writes are silently lost.
                // Nothing else can hold the kv lock during construction.
                let mut kv_guard = kv.try_lock().expect("kv is uncontended during startup");
                if let Err(e) = kv_guard.restore(&snapshot_data) {
                    warn!("KV restore from WAL snapshot failed: {e}");
                }
                drop(kv_guard);
                applied = snapshot_index;
                // Rebuild the leader-side snapshot too, so a restarted leader
                // can still serve InstallSnapshot to lagging peers.
                last_snapshot = Some(Snapshot {
                    last_index: snapshot_index,
                    last_term: snapshot_term,
                    data: snapshot_data,
                });
            }
            node.restore(term, voted_for, snapshot_index, snapshot_term, entries);
        }
        let (applied_tx, applied_rx) = watch::channel(applied);
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            node: Mutex::new(node),
            kv,
            wal,
            peers: Mutex::new(HashMap::new()),
            http_peers: Mutex::new(HashMap::new()),
            pending_proposals: Mutex::new(HashMap::new()),
            applied_tx,
            applied_rx,
            last_snapshot: Mutex::new(last_snapshot),
            events_tx,
        }
    }

    pub async fn register_peers(
        &self,
        raft_peers: HashMap<NodeId, String>,
        http_peers: HashMap<NodeId, String>,
    ) {
        let mut map = self.peers.lock().await;
        for (id, addr) in raft_peers {
            map.insert(id, PeerClient::new(id, addr));
        }
        let mut http = self.http_peers.lock().await;
        for (id, addr) in http_peers {
            http.insert(id, addr);
        }
    }

    /// Propose a membership change to the cluster.
    /// Returns a receiver that resolves when the entry is committed, or None if not leader.
    pub async fn propose_conf_change(
        self: &Arc<Self>,
        op: ConfChangeOp,
        node_id: NodeId,
        raft_addr: Option<String>,
        http_addr: Option<String>,
    ) -> Option<oneshot::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        let ready = {
            let mut node = self.node.lock().await;
            if !node.is_leader() {
                return None;
            }
            let index = node.log.last_index() + 1;
            self.pending_proposals.lock().await.insert(index, tx);
            node.step(Message::ProposeConfChange {
                op,
                node_id,
                raft_addr,
                http_addr,
            })
        };
        self.process_ready(ready).await;
        Some(rx)
    }

    pub async fn tick(self: &Arc<Self>) {
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
    pub async fn propose(self: &Arc<Self>, command: Vec<u8>) -> Option<oneshot::Receiver<()>> {
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
        if node.is_leader() {
            Some(node.commit_index)
        } else {
            None
        }
    }

    /// Subscribe to applied-index updates. The returned receiver resolves each time
    /// the applied index advances. Clone it per-request — cloning is cheap.
    pub fn subscribe_applied(&self) -> watch::Receiver<LogIndex> {
        self.applied_rx.clone()
    }

    /// Subscribe to applied KV commands (the embedded watch API).
    /// Events arrive in log order; all nodes deliver the same sequence.
    pub fn subscribe_events(&self) -> broadcast::Receiver<Event> {
        self.events_tx.subscribe()
    }

    /// Decode an applied entry into an Event and broadcast it.
    /// No-op entries (empty command) and ConfChanges are not KV writes.
    fn broadcast_event(&self, entry: &LogEntry) {
        if entry.entry_type != EntryType::Normal || entry.command.is_empty() {
            return;
        }
        let event = match serde_json::from_slice::<Command>(&entry.command) {
            Ok(Command::Set { key, value }) => Event::Set { key, value },
            Ok(Command::Delete { key }) => Event::Delete { key },
            Err(e) => {
                warn!(
                    index = entry.index,
                    "unparseable command in event stream: {e}"
                );
                return;
            }
        };
        // Err means no subscribers — normal when running standalone.
        let _ = self.events_tx.send(event);
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
            if index > node.commit_index {
                return;
            }
            let term = node.log.term_at(index).unwrap_or(0);
            node.log.compact(index, term);
            (index, term)
        };
        {
            let mut wal = self.wal.lock().await;
            if let Err(e) = wal.append(&WalRecord::Snapshot {
                last_index,
                last_term,
                data: data.clone(),
            }) {
                warn!("WAL snapshot write failed: {e}");
            }
        }
        let snap = Snapshot {
            last_index,
            last_term,
            data,
        };
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
            node.restore(
                term,
                voted_for,
                snapshot.last_index,
                snapshot.last_term,
                vec![],
            );
        }
        {
            let mut wal = self.wal.lock().await;
            if let Err(e) = wal.append(&WalRecord::Snapshot {
                last_index: snapshot.last_index,
                last_term: snapshot.last_term,
                data: snapshot.data.clone(),
            }) {
                warn!("WAL snapshot write failed: {e}");
            }
        }
        let _ = self.applied_tx.send(snapshot.last_index);
        let _ = self.events_tx.send(Event::SnapshotApplied);
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
        let response = ready
            .messages
            .iter()
            .find(|(_, m)| {
                matches!(
                    m,
                    Message::RequestVoteResponse { .. } | Message::AppendEntriesResponse { .. }
                )
            })
            .map(|(_, m)| m.clone());
        self.persist(&ready).await;
        self.drain_if_lost_leadership(&ready).await;
        response
    }

    async fn persist(&self, ready: &Ready) {
        // Batch the whole Ready into one WAL write so a single fsync covers
        // the HardState and every entry.
        let mut batch = Vec::new();
        if let Some(HardState { term, voted_for }) = ready.hard_state {
            batch.push(WalRecord::HardState { term, voted_for });
        }
        for entry in &ready.entries_to_persist {
            batch.push(WalRecord::Entry(entry.clone()));
        }
        if !batch.is_empty() {
            let mut wal = self.wal.lock().await;
            if let Err(e) = wal.append_batch(&batch) {
                warn!("WAL write failed: {e}");
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
            for entry in &ready.entries_to_apply {
                self.broadcast_event(entry);
            }
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

        // A ConfChange entry was applied — update peer connection maps.
        if let Some(cmd) = ready.membership_change.clone() {
            self.apply_membership_cmd(cmd).await;
        }
    }

    async fn apply_membership_cmd(&self, cmd: ConfChangeCmd) {
        match cmd.op {
            ConfChangeOp::Add => {
                if let Some(raft_addr) = cmd.raft_addr {
                    self.peers
                        .lock()
                        .await
                        .insert(cmd.node_id, PeerClient::new(cmd.node_id, raft_addr));
                }
                if let Some(http_addr) = cmd.http_addr {
                    self.http_peers.lock().await.insert(cmd.node_id, http_addr);
                }
            }
            ConfChangeOp::Remove => {
                self.peers.lock().await.remove(&cmd.node_id);
                self.http_peers.lock().await.remove(&cmd.node_id);
            }
        }
    }

    /// Persist durable state synchronously, then hand the network fan-out to a
    /// background task. The caller (tick loop, propose) must never wait on
    /// peer I/O: a dead peer would throttle the tick-driven election and
    /// heartbeat timers of the whole node. Raft tolerates the resulting
    /// out-of-order/duplicate delivery by design (term and index checks).
    async fn process_ready(self: &Arc<Self>, ready: Ready) {
        self.persist(&ready).await;
        if ready.messages.is_empty() && ready.snapshot_to_send.is_empty() {
            return;
        }
        let this = Arc::clone(self);
        tokio::spawn(async move { this.fan_out(ready).await });
    }

    /// Send every outgoing message on its own task and step each response the
    /// moment it arrives. Responses must never wait on each other: an election
    /// needs only a quorum of votes, so the vote of a live peer must not sit
    /// behind the connect timeout of a dead one.
    async fn fan_out(self: Arc<Self>, ready: Ready) {
        // Leader: send snapshots to lagging peers that can't be caught up via AppendEntries.
        if !ready.snapshot_to_send.is_empty() {
            let snap_opt = self.last_snapshot.lock().await.clone();
            if let Some(snap) = snap_opt {
                let leader_term = self.node.lock().await.current_term;
                let clients: Vec<PeerClient> = {
                    let peers = self.peers.lock().await;
                    ready
                        .snapshot_to_send
                        .iter()
                        .filter_map(|id| peers.get(id).cloned())
                        .collect()
                };
                for client in clients {
                    debug!(
                        peer = client.id,
                        index = snap.last_index,
                        "sending snapshot"
                    );
                    let this = Arc::clone(&self);
                    let snap = snap.clone();
                    tokio::spawn(async move {
                        if let Some(resp) = client.send_install_snapshot(leader_term, snap).await {
                            this.step_and_persist(resp).await;
                        }
                    });
                }
            }
        }

        for (client, msg) in self.clients_for(ready.messages).await {
            debug!(peer = client.id, "sending message");
            let this = Arc::clone(&self);
            tokio::spawn(async move {
                let Some(resp) = client.send(msg).await else {
                    return;
                };
                // Stepping a response can emit follow-up messages — e.g. the
                // vote that wins an election emits the new leader's first
                // AppendEntries broadcast. Send those too; *their* responses
                // only advance commit_index and never generate further RPCs,
                // so two levels are all it takes.
                let followups = this.step_and_persist(resp).await;
                for (client2, msg2) in this.clients_for(followups).await {
                    debug!(peer = client2.id, "sending message (post-response)");
                    let this2 = Arc::clone(&this);
                    tokio::spawn(async move {
                        if let Some(resp2) = client2.send(msg2).await {
                            this2.step_and_persist(resp2).await;
                        }
                    });
                }
            });
        }
    }

    /// Step one peer response through the SM, persist and apply its effects,
    /// and return any messages it produced.
    async fn step_and_persist(&self, resp: Message) -> Vec<(NodeId, Message)> {
        let ready = {
            let mut node = self.node.lock().await;
            node.step(resp)
        };
        self.persist(&ready).await;
        self.drain_if_lost_leadership(&ready).await;
        ready.messages
    }

    /// Resolve destination node ids to peer clients, dropping unknown peers.
    async fn clients_for(&self, messages: Vec<(NodeId, Message)>) -> Vec<(PeerClient, Message)> {
        let peers = self.peers.lock().await;
        messages
            .into_iter()
            .filter_map(|(dest, msg)| peers.get(&dest).cloned().map(|c| (c, msg)))
            .collect()
    }
}

// ── WAL replay helpers ────────────────────────────────────────────────────────
// Each function makes a single pass over the records; three passes is fine
// because WAL replay is a startup-only path.

fn replay_hard_state(records: &[WalRecord]) -> (Term, Option<NodeId>) {
    let mut term: Term = 0;
    let mut voted_for: Option<NodeId> = None;
    for r in records {
        if let WalRecord::HardState {
            term: t,
            voted_for: v,
        } = r
        {
            term = *t;
            voted_for = *v;
        }
    }
    (term, voted_for)
}

fn replay_snapshot(records: &[WalRecord]) -> (LogIndex, Term, Vec<u8>) {
    let mut snapshot_index: LogIndex = 0;
    let mut snapshot_term: Term = 0;
    let mut snapshot_data: Vec<u8> = Vec::new();
    for r in records {
        if let WalRecord::Snapshot {
            last_index,
            last_term,
            data,
        } = r
        {
            snapshot_index = *last_index;
            snapshot_term = *last_term;
            snapshot_data = data.clone();
        }
    }
    (snapshot_index, snapshot_term, snapshot_data)
}

/// Rebuild the log entry list from WAL records, handling conflicts:
/// when a later record appears at an already-seen index, it truncates
/// forward — matching what `RaftLog::truncate_and_append` does at runtime.
/// Entries at or below `snapshot_index` are dropped: they live inside the
/// snapshot now, and keeping them would desync the log's offset arithmetic
/// (`RaftLog::restore` seats entries right after the snapshot sentinel).
fn replay_entries(records: &[WalRecord], snapshot_index: LogIndex) -> Vec<LogEntry> {
    let mut entries: Vec<LogEntry> = Vec::new();
    for r in records {
        if let WalRecord::Entry(e) = r {
            // Truncate anything at this index or beyond, then push.
            // This replays the same conflict-resolution logic as the live path.
            entries.retain(|x| x.index < e.index);
            entries.push(e.clone());
        }
    }
    entries.retain(|e| e.index > snapshot_index);
    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::{message::EntryType, LogEntry};
    use storage::wal::WalRecord;

    fn entry(index: u64, term: u64) -> WalRecord {
        WalRecord::Entry(LogEntry {
            index,
            term,
            entry_type: EntryType::Normal,
            command: vec![],
        })
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
        let entries = replay_entries(&records, 0);
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
        let entries = replay_entries(&records, 0);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[1].index, 2);
        assert_eq!(
            entries[1].term, 2,
            "conflict at index 2: term 2 must win over term 1"
        );
        assert_eq!(entries[2].term, 2, "entry 3 from term 2 must be kept");
    }

    #[test]
    fn replay_entries_drops_entries_covered_by_snapshot() {
        let records = vec![
            hard_state(1, Some(2)),
            entry(1, 1),
            WalRecord::Snapshot {
                last_index: 5,
                last_term: 1,
                data: vec![],
            },
            entry(6, 2),
        ];
        let entries = replay_entries(&records, 5);
        // Entry 1 lives inside the snapshot (index ≤ 5) and must not resurface —
        // it would desync the log's offset arithmetic. Entry 6 sits above the
        // snapshot and stays. Non-entry records are skipped either way.
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index, 6);
    }

    // ── replay_snapshot ───────────────────────────────────────────────────────

    #[test]
    fn replay_snapshot_returns_last() {
        let records = vec![
            WalRecord::Snapshot {
                last_index: 10,
                last_term: 2,
                data: b"old".to_vec(),
            },
            WalRecord::Snapshot {
                last_index: 20,
                last_term: 3,
                data: b"new".to_vec(),
            },
        ];
        let (idx, term, data) = replay_snapshot(&records);
        assert_eq!(idx, 20);
        assert_eq!(term, 3);
        assert_eq!(data, b"new", "the newest snapshot's data must win");
    }

    #[test]
    fn replay_snapshot_none_returns_zero() {
        let (idx, term, data) = replay_snapshot(&[hard_state(1, None)]);
        assert_eq!(idx, 0);
        assert_eq!(term, 0);
        assert!(data.is_empty());
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
        assert!(
            rx1.try_recv().is_err(),
            "rx1 should be resolved after drain"
        );
        assert!(
            rx2.try_recv().is_err(),
            "rx2 should be resolved after drain"
        );
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
                hard_state: Some(raft::HardState {
                    term: 3,
                    voted_for: Some(2),
                }),
                ..Default::default()
            };

            handle.drain_if_lost_leadership(&ready).await;
        });

        // The sender was dropped → receiver resolves with Err immediately
        assert!(
            rx.try_recv().is_err(),
            "proposal must be drained on leadership loss"
        );
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

            let ready = raft::Ready {
                hard_state: None,
                ..Default::default()
            };
            handle.drain_if_lost_leadership(&ready).await;

            assert_eq!(
                handle.pending_proposals.lock().await.len(),
                1,
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
                LogEntry {
                    index: 1,
                    term: 1,
                    entry_type: EntryType::Normal,
                    command: vec![],
                },
                LogEntry {
                    index: 2,
                    term: 1,
                    entry_type: EntryType::Normal,
                    command: vec![],
                },
            ],
            ..Default::default()
        };
        rt.block_on(handle.persist(&ready));

        // The watch must have advanced to 2 (the max applied index).
        assert_eq!(
            *rx.borrow(),
            2,
            "applied watch must advance to max applied index"
        );
    }

    // ── BUG 3: replay after compaction must reconcile entries with the snapshot ─
    //
    // The WAL is append-only, so after compacting at index 50 it still holds every
    // entry 1..=60 plus a Snapshot marker. Two defects on restart:
    //   1. `replay_entries` returns all 60 entries; `RaftLog::restore` re-inserts
    //      the ones <= the snapshot base above the sentinel, corrupting the index
    //      math (last_index / term_at go wrong).
    //   2. The Snapshot WAL record carried no data, so the KV store was never
    //      rebuilt — everything committed at or below the snapshot was lost.
    //
    // The fix persists the KV bytes in `WalRecord::Snapshot { data }` and filters
    // replayed entries below the snapshot base. This test pins both: the log must
    // be correctly indexed, and the KV store must be rehydrated from the snapshot.
    //
    // NOTE: written against the post-fix API — `WalRecord::Snapshot` gains `data`.
    #[test]
    fn replay_after_compaction_reindexes_log_and_restores_kv() {
        use std::collections::BTreeMap;
        use std::sync::Arc;
        use storage::{KvStore, Wal};
        use tokio::sync::Mutex;

        // The snapshot at index 50 carries the KV state as serialized bytes,
        // exactly as `KvStore::snapshot()` produces it (serde_json of the map).
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        map.insert("px:001:001".to_string(), "5".to_string());
        map.insert("px:002:002".to_string(), "9".to_string());
        let snap_data = serde_json::to_vec(&map).unwrap();

        let mut records: Vec<WalRecord> = (1..=60u64)
            .map(|i| {
                WalRecord::Entry(LogEntry {
                    index: i,
                    term: 1,
                    entry_type: EntryType::Normal,
                    command: vec![],
                })
            })
            .collect();
        records.push(WalRecord::Snapshot {
            last_index: 50,
            last_term: 1,
            data: snap_data,
        });

        let cfg = raft::Config::new(1, vec![2, 3]);
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let (wal, _) = Wal::open(tmp.path()).unwrap();
        let kv = Arc::new(Mutex::new(KvStore::default()));
        let wal = Arc::new(Mutex::new(wal));
        let handle = NodeHandle::new(cfg, records, kv, wal);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let node = handle.node.lock().await;
            assert_eq!(node.log.snapshot_index(), 50, "snapshot base must be 50");
            assert_eq!(
                node.log.last_index(),
                60,
                "log must end at 60, not snapshot_base + every replayed entry"
            );
            assert_eq!(
                node.log.term_at(60),
                Some(1),
                "entry 60 must remain addressable"
            );
            drop(node);

            let kv = handle.kv.lock().await;
            assert_eq!(
                kv.get("px:001:001"),
                Some("5"),
                "KV must be rehydrated from the snapshot data on restart"
            );
            assert_eq!(kv.get("px:002:002"), Some("9"));
        });
    }

    fn make_handle(id: u64, peers: Vec<u64>) -> Arc<NodeHandle> {
        use raft::Config;
        use std::sync::Arc;
        use storage::{KvStore, Wal};
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

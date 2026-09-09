/// NodeHandle wraps the Raft state machine behind a Mutex and drives it:
/// - applies Ready output (persists WAL, applies KV commands, fans out messages)
/// - provides async methods for the gRPC and HTTP layers
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use tokio::sync::{broadcast, oneshot, watch, Mutex};
use tracing::{debug, warn};

use raft::{
    message::EntryType, ConfChangeCmd, ConfChangeOp, Config, HardState, LogEntry, LogIndex,
    Message, NodeId, RaftNode, Ready, Restore, Snapshot, Term,
};
use storage::kv::Command;
use storage::wal::WalRecord;
use storage::{KvStore, SnapshotStore, Wal};

use crate::events::Event;
use crate::peer::PeerClient;

/// Buffered events per subscriber before a slow receiver starts lagging.
/// A lagged receiver gets `RecvError::Lagged` and must resync from the store.
const EVENT_CHANNEL_CAPACITY: usize = 4096;

/// How many log indices the log may grow past the last snapshot before the
/// next compaction. Every compaction serializes the whole state machine, so
/// this is a write-amplification knob as much as a log-length one: at the old
/// value of 50, gambas re-serialized a ~780 KB board ~988 times across 49,400
/// writes. Embedders that keep a small state machine can lower it.
pub const DEFAULT_COMPACTION_THRESHOLD: u64 = 5_000;

/// Snapshot files kept on disk. One is enough to restore; the previous one is
/// insurance against the newest being unreadable.
const SNAPSHOTS_KEPT: usize = 2;

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
    /// Snapshot files under this node's data directory.
    snapshots: SnapshotStore,
    /// See [`DEFAULT_COMPACTION_THRESHOLD`].
    compaction_threshold: u64,
    /// Index of the last snapshot taken. The compaction trigger measures the
    /// distance from here rather than testing `index % threshold == 0`.
    compaction_floor: AtomicU64,
    /// Broadcast channel: every command applied to the KV store, in log order.
    /// Backs `RaftKv::subscribe` — the embedded-mode watch API.
    events_tx: broadcast::Sender<Event>,
}

/// Everything a [`NodeHandle`] needs to come up. A struct rather than an
/// argument list: it grew a snapshot store and a compaction knob when
/// snapshots moved out of the WAL.
pub struct NodeHandleConfig {
    pub raft: Config,
    pub replay: WalReplay,
    pub kv: Arc<Mutex<KvStore>>,
    pub wal: Arc<Mutex<Wal>>,
    pub snapshots: SnapshotStore,
    /// `0` keeps [`DEFAULT_COMPACTION_THRESHOLD`].
    pub compaction_threshold: u64,
}

impl NodeHandle {
    pub fn new(config: NodeHandleConfig) -> Self {
        Self::new_inner(false, config)
    }

    pub fn new_learner(config: NodeHandleConfig) -> Self {
        Self::new_inner(true, config)
    }

    fn new_inner(learner: bool, config: NodeHandleConfig) -> Self {
        let NodeHandleConfig {
            raft,
            replay,
            kv,
            wal,
            snapshots,
            compaction_threshold,
        } = config;
        let mut node = if learner {
            RaftNode::new_learner(raft)
        } else {
            RaftNode::new(raft)
        };

        let (snapshot_index, snapshot_term) = replay.snapshot_position();
        let mut last_snapshot = None;
        let mut applied: LogIndex = 0;
        if let Some(snap) = replay.snapshot {
            // The entries covered by the snapshot were compacted away and will
            // never be re-applied: the state machine must be rebuilt from the
            // snapshot data or those writes are silently lost. Nothing else can
            // hold the kv lock during construction.
            let mut kv_guard = kv.try_lock().expect("kv is uncontended during startup");
            if let Err(e) = kv_guard.restore(&snap.data) {
                warn!("KV restore from snapshot failed: {e}");
            }
            drop(kv_guard);
            applied = snapshot_index;
            // Keep it as the leader-side snapshot too, so a restarted leader can
            // still serve InstallSnapshot to a lagging peer.
            last_snapshot = Some(snap);
        }
        if replay.has_state {
            node.restore(Restore {
                term: replay.term,
                voted_for: replay.voted_for,
                commit: replay.commit,
                snapshot_index,
                snapshot_term,
                entries: replay.entries,
            });
            // Entries between the snapshot and the persisted commit are
            // committed but absent from the store: the snapshot only carries
            // state up to its own index. Re-applying them here, before the
            // embedder has had a chance to subscribe, rebuilds the state
            // machine without replaying those writes as fresh events on
            // anyone's watch stream.
            let staged = node.take_ready();
            if !staged.entries_to_apply.is_empty() {
                let mut kv_guard = kv.try_lock().expect("kv is uncontended during startup");
                for entry in &staged.entries_to_apply {
                    if let Err(e) = kv_guard.apply(&entry.command) {
                        warn!(index = entry.index, "KV re-apply on restart failed: {e}");
                    }
                }
                drop(kv_guard);
                applied = staged
                    .entries_to_apply
                    .iter()
                    .map(|e| e.index)
                    .max()
                    .unwrap_or(applied);
                debug!(
                    from = snapshot_index,
                    to = applied,
                    "re-applied committed entries above the snapshot"
                );
            }
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
            snapshots,
            compaction_threshold: if compaction_threshold == 0 {
                DEFAULT_COMPACTION_THRESHOLD
            } else {
                compaction_threshold
            },
            compaction_floor: AtomicU64::new(snapshot_index),
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

    /// `(is_leader, leader_id)` read under a single lock, so the two can never
    /// disagree. Reading them through separate `is_leader()`/`leader_id()`
    /// calls is a TOCTOU: the role can change between the two locks, which
    /// could surface impossible states like a follower that reports itself as
    /// the leader.
    pub async fn role_status(&self) -> (bool, Option<NodeId>) {
        let node = self.node.lock().await;
        (node.is_leader(), node.leader_id())
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

    /// Snapshot the state machine into its own file, compact the Raft log up
    /// to that point, and rewrite the WAL with only what the snapshot does not
    /// cover. This is what bounds the WAL: 0.1.x appended the snapshot into the
    /// same append-only file and never dropped anything, so the file only grew.
    ///
    /// The WAL lock is held across the whole compaction. That is what makes the
    /// rewrite safe: a concurrent `persist` waits on it, so it can never append
    /// an entry that the rewrite then drops.
    pub async fn try_compact(&self, index: LogIndex) {
        // A snapshot is labelled with the index the *state machine* is at,
        // which is what the applied watch tracks: `node.last_applied` advances
        // inside `step()`, before `persist` has written the store. Labelling a
        // snapshot with an index whose entries are not in the store yet would
        // restore a node into a state it never had.
        let target = index.min(*self.applied_rx.borrow());
        if target == 0 {
            return;
        }

        // Lock order: the WAL is always taken first.
        let mut wal = self.wal.lock().await;

        let (last_term, hard_state) = {
            let node = self.node.lock().await;
            // `<` rather than `<=`: taking a snapshot *at* the current base is
            // a no-op for the log but is how a leader with no usable snapshot
            // in memory produces one for a lagging peer.
            if target < node.log.snapshot_index() || target > node.last_applied {
                return;
            }
            (
                node.log.term_at(target).unwrap_or(0),
                WalRecord::HardState {
                    term: node.current_term,
                    voted_for: node.voted_for,
                    commit: node.commit_index,
                },
            )
        };
        let data = {
            let kv = self.kv.lock().await;
            kv.snapshot()
        };
        let snap = Snapshot {
            last_index: target,
            last_term,
            data,
        };

        // Nothing is dropped until the snapshot that covers it is durable.
        if let Err(e) = self.snapshots.save(&snap) {
            warn!(
                index = target,
                "snapshot write failed, WAL left intact: {e}"
            );
            return;
        }

        let tail = {
            let mut node = self.node.lock().await;
            node.log.compact(target, last_term);
            node.log.entries_from(target + 1).to_vec()
        };

        // The rotated file must still carry the hard state: a node that comes
        // back without its term and vote can vote twice in one term.
        let mut records = Vec::with_capacity(tail.len() + 1);
        records.push(hard_state);
        let kept = tail.len();
        records.extend(tail.into_iter().map(WalRecord::Entry));
        if let Err(e) = wal.rewrite(&records) {
            warn!(index = target, "WAL rotation failed: {e}");
            return;
        }
        drop(wal);

        if let Err(e) = self.snapshots.prune(SNAPSHOTS_KEPT) {
            warn!("could not prune old snapshots: {e}");
        }
        self.compaction_floor.store(target, Ordering::Relaxed);
        *self.last_snapshot.lock().await = Some(snap);
        debug!(
            index = target,
            entries_kept = kept,
            "snapshot taken, WAL rotated"
        );
    }

    /// Replace the KV store with snapshot data received from the leader.
    /// Rebuilds the Raft log and rotates the WAL to the new snapshot base.
    pub async fn apply_snapshot(&self, snapshot: Snapshot) {
        // Lock order: the WAL is always taken first (see `try_compact`).
        let mut wal = self.wal.lock().await;
        {
            let mut kv = self.kv.lock().await;
            if let Err(e) = kv.restore(&snapshot.data) {
                warn!("snapshot deserialize failed: {e}");
                return;
            }
        }
        let hard_state = {
            let mut node = self.node.lock().await;
            let (term, voted_for) = (node.current_term, node.voted_for);
            node.restore(Restore {
                term,
                voted_for,
                commit: snapshot.last_index,
                snapshot_index: snapshot.last_index,
                snapshot_term: snapshot.last_term,
                entries: vec![],
            });
            WalRecord::HardState {
                term,
                voted_for,
                commit: node.commit_index,
            }
        };
        if let Err(e) = self.snapshots.save(&snapshot) {
            warn!("snapshot write failed: {e}");
        } else {
            // Every entry the WAL held is covered by the installed snapshot;
            // only the hard state has to survive the rotation.
            if let Err(e) = wal.rewrite(&[hard_state]) {
                warn!("WAL rotation after InstallSnapshot failed: {e}");
            }
        }
        drop(wal);

        self.compaction_floor
            .store(snapshot.last_index, Ordering::Relaxed);
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

    /// If durable state just changed and we are no longer leader, drain
    /// proposals. `hard_state.is_some()` is a cheap pre-filter — it now also
    /// fires when the commit index advances, which is harmless: the drain only
    /// happens when this node is not the leader, and then dropping the
    /// proposals is right regardless of what moved.
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
        // Every RPC the gRPC server exposes has to find its reply here.
        // InstallSnapshotResponse was missing from this filter, so *every*
        // InstallSnapshot RPC answered `Status::internal` even though the
        // follower had installed the snapshot: the leader saw a failed RPC,
        // never advanced the peer, and re-sent the whole snapshot on the next
        // tick, forever. That is the other half of "a node with an empty WAL
        // never catches up".
        let response = ready
            .messages
            .iter()
            .find(|(_, m)| {
                matches!(
                    m,
                    Message::RequestVoteResponse { .. }
                        | Message::AppendEntriesResponse { .. }
                        | Message::InstallSnapshotResponse { .. }
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
        if let Some(HardState {
            term,
            voted_for,
            commit,
        }) = ready.hard_state
        {
            batch.push(WalRecord::HardState {
                term,
                voted_for,
                commit,
            });
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
                // Compact once the log has grown a threshold past the last
                // snapshot. The old trigger was `max_idx % THRESHOLD == 0`,
                // which a batch straddling the boundary skips outright: a Ready
                // applying 48..52 never lands on a multiple of 50. Under write
                // load batches get bigger and the misses compound — gambas took
                // 696 of the 988 snapshots its 49,400 applies called for, so the
                // log grew past what the design assumes and compaction became
                // non-deterministic.
                let floor = self.compaction_floor.load(Ordering::Relaxed);
                if should_compact(max_idx, floor, self.compaction_threshold) {
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
            self.send_snapshots(&ready.snapshot_to_send).await;
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

    /// Send the current snapshot to every peer whose next_index has fallen
    /// inside the compacted log.
    ///
    /// 0.1.3 read `last_snapshot` and, if it was `None`, skipped the whole
    /// block — no log line, no fallback. The peer was simply not contacted for
    /// that tick, and the next tick reached the same dead end: a node brought
    /// up with an empty WAL never caught up. If there is no usable snapshot we
    /// take one now, and if we still cannot, we say so.
    async fn send_snapshots(self: &Arc<Self>, peers: &[NodeId]) {
        let snap = match self.snapshot_for_peers().await {
            Some(snap) => snap,
            None => {
                warn!(
                    ?peers,
                    "peers need a snapshot but none could be produced; they cannot catch up"
                );
                return;
            }
        };
        let (leader_id, leader_term) = {
            let node = self.node.lock().await;
            (node.id, node.current_term)
        };
        let clients: Vec<PeerClient> = {
            let peers_map = self.peers.lock().await;
            peers
                .iter()
                .filter_map(|id| peers_map.get(id).cloned())
                .collect()
        };
        for client in clients {
            debug!(
                peer = client.id,
                index = snap.last_index,
                "sending snapshot"
            );
            let this = Arc::clone(self);
            let snap = snap.clone();
            tokio::spawn(async move {
                if let Some(resp) = client
                    .send_install_snapshot(leader_id, leader_term, snap)
                    .await
                {
                    this.step_and_persist(resp).await;
                }
            });
        }
    }

    /// A snapshot that covers this node's compacted prefix, taking one now if
    /// the one in memory is missing or older than the log's base.
    async fn snapshot_for_peers(self: &Arc<Self>) -> Option<Snapshot> {
        let (base, applied) = {
            let node = self.node.lock().await;
            (node.log.snapshot_index(), node.last_applied)
        };
        if let Some(snap) = self.last_snapshot.lock().await.clone() {
            if snap.last_index >= base {
                return Some(snap);
            }
        }
        // Snapshot the point the state machine has actually reached, which is
        // always at or above the compacted prefix — exactly what the peer needs.
        self.try_compact(applied.max(base)).await;
        self.last_snapshot.lock().await.clone()
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

/// Whether a batch of applied entries ending at `max_index` should trigger a
/// compaction: has the log grown `threshold` indices past the last snapshot?
///
/// 0.1.3 asked `max_index % threshold == 0` instead, a divisibility test that a
/// batch straddling the boundary skips outright.
fn should_compact(max_index: LogIndex, floor: LogIndex, threshold: u64) -> bool {
    max_index.saturating_sub(floor) >= threshold
}

// ── WAL replay ────────────────────────────────────────────────────────────────

/// Folds WAL records into the state a node restarts from, one record at a time.
///
/// Streaming is the point. 0.1.x read the whole file into a `Vec<WalRecord>`
/// and walked it three times; a WAL that has never been rotated holds every
/// snapshot ever appended — ~988 of them at ~2.4 MB each in gambas — so the
/// replay itself was an OOM path on a 256 MB machine. This keeps one snapshot
/// blob and the entries above it, dropping the rest as it goes.
#[derive(Debug, Default)]
pub struct WalReplay {
    pub term: Term,
    pub voted_for: Option<NodeId>,
    /// Last commit index that reached disk. See `raft::HardState::commit`.
    pub commit: LogIndex,
    /// The newest snapshot seen, from the WAL (0.1.x) or adopted from the
    /// snapshot store.
    pub snapshot: Option<Snapshot>,
    /// Entries above the snapshot base, conflicts already resolved.
    pub entries: Vec<LogEntry>,
    /// False for a fresh node: there is nothing to restore.
    pub has_state: bool,
    /// True when a snapshot arrived inside the WAL itself — the 0.1.x layout,
    /// which the node migrates out of on startup.
    pub snapshot_in_wal: bool,
}

impl WalReplay {
    pub fn push(&mut self, record: WalRecord) {
        self.has_state = true;
        match record {
            WalRecord::HardState {
                term,
                voted_for,
                commit,
            } => {
                self.term = term;
                self.voted_for = voted_for;
                self.commit = self.commit.max(commit);
            }
            WalRecord::Entry(entry) => {
                // Truncate anything at this index or beyond, then push: the same
                // conflict resolution `RaftLog::truncate_and_append` does at
                // runtime. Entries the snapshot already covers are dropped —
                // keeping them would desync the log's offset arithmetic, which
                // seats entries right after the snapshot sentinel.
                self.entries.retain(|x| x.index < entry.index);
                if entry.index > self.snapshot_index() {
                    self.entries.push(entry);
                }
            }
            WalRecord::Snapshot {
                last_index,
                last_term,
                data,
            } => {
                self.snapshot_in_wal = true;
                self.adopt(Snapshot {
                    last_index,
                    last_term,
                    data,
                });
            }
        }
    }

    /// Take `snapshot` as the base if it is newer than anything seen so far,
    /// releasing the previous blob and the entries it covers.
    pub fn adopt(&mut self, snapshot: Snapshot) {
        if snapshot.last_index < self.snapshot_index() {
            return;
        }
        self.has_state = true;
        self.entries.retain(|e| e.index > snapshot.last_index);
        self.snapshot = Some(snapshot);
    }

    /// The records a rotated WAL must carry to reproduce this state.
    pub fn to_records(&self) -> Vec<WalRecord> {
        let mut records = Vec::with_capacity(self.entries.len() + 1);
        records.push(WalRecord::HardState {
            term: self.term,
            voted_for: self.voted_for,
            commit: self.commit,
        });
        records.extend(self.entries.iter().cloned().map(WalRecord::Entry));
        records
    }

    pub fn snapshot_index(&self) -> LogIndex {
        self.snapshot.as_ref().map_or(0, |s| s.last_index)
    }

    fn snapshot_position(&self) -> (LogIndex, Term) {
        self.snapshot
            .as_ref()
            .map_or((0, 0), |s| (s.last_index, s.last_term))
    }

    /// Fold a slice of records in one go. The streaming path is what runs in
    /// production; this keeps tests and small call sites readable.
    #[cfg(test)]
    pub fn from_records(records: Vec<WalRecord>) -> Self {
        let mut replay = Self::default();
        for record in records {
            replay.push(record);
        }
        replay
    }
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
        WalRecord::HardState {
            term,
            voted_for,
            commit: 0,
        }
    }

    // ── WalReplay ─────────────────────────────────────────────────────────────

    #[test]
    fn replay_takes_the_last_hard_state() {
        let replay = WalReplay::from_records(vec![
            hard_state(1, Some(2)),
            hard_state(2, None),
            hard_state(3, Some(1)),
        ]);
        assert_eq!(replay.term, 3);
        assert_eq!(replay.voted_for, Some(1));
    }

    #[test]
    fn replay_of_an_empty_wal_has_no_state() {
        let replay = WalReplay::from_records(vec![]);
        assert_eq!(replay.term, 0);
        assert_eq!(replay.voted_for, None);
        assert!(!replay.has_state, "a fresh node has nothing to restore");
    }

    #[test]
    fn replay_entries_basic_sequence() {
        let replay = WalReplay::from_records(vec![entry(1, 1), entry(2, 1), entry(3, 1)]);
        assert_eq!(replay.entries.len(), 3);
        assert_eq!(replay.entries[0].index, 1);
        assert_eq!(replay.entries[2].index, 3);
    }

    #[test]
    fn replay_entries_conflict_truncates_forward() {
        // Simulates a term change: index 2 and 3 were overwritten by a new leader.
        let replay = WalReplay::from_records(vec![
            entry(1, 1),
            entry(2, 1),
            entry(3, 1),
            entry(2, 2), // new leader overwrites from index 2
            entry(3, 2),
        ]);
        assert_eq!(replay.entries.len(), 3);
        assert_eq!(replay.entries[1].index, 2);
        assert_eq!(
            replay.entries[1].term, 2,
            "conflict at index 2: term 2 must win over term 1"
        );
        assert_eq!(
            replay.entries[2].term, 2,
            "entry 3 from term 2 must be kept"
        );
    }

    #[test]
    fn replay_drops_entries_covered_by_a_snapshot_as_it_goes() {
        // Entry 1 lives inside the snapshot and must not resurface — it would
        // desync the log's offset arithmetic. Dropping it the moment the
        // snapshot record arrives, rather than filtering at the end, is what
        // keeps replay memory proportional to the tail instead of the file.
        let replay = WalReplay::from_records(vec![
            hard_state(1, Some(2)),
            entry(1, 1),
            WalRecord::Snapshot {
                last_index: 5,
                last_term: 1,
                data: vec![],
            },
            entry(6, 2),
        ]);
        assert_eq!(replay.entries.len(), 1);
        assert_eq!(replay.entries[0].index, 6);
        assert!(replay.snapshot_in_wal, "the 0.1.x layout must be flagged");
    }

    // Since 0.1.1 (BUG 3 fix) a snapshot record also carries the serialized
    // state machine, and that data rebuilds the KV store on restart — the
    // entries it compacted away are gone, so losing it would silently drop
    // writes. Only the newest blob is ever held: a WAL that has never been
    // rotated carries ~988 of them in gambas, and keeping them all was an OOM
    // path on a 256 MB machine.

    #[test]
    fn replay_keeps_only_the_newest_snapshot() {
        let replay = WalReplay::from_records(vec![
            WalRecord::Snapshot {
                last_index: 10,
                last_term: 2,
                data: b"old".to_vec(),
            },
            entry(15, 3),
            WalRecord::Snapshot {
                last_index: 20,
                last_term: 3,
                data: b"new".to_vec(),
            },
            entry(21, 3),
        ]);
        let snapshot = replay.snapshot.as_ref().expect("a snapshot must survive");
        assert_eq!(snapshot.last_index, 20);
        assert_eq!(snapshot.last_term, 3);
        assert_eq!(snapshot.data, b"new", "the newest snapshot's data must win");
        assert_eq!(
            replay.entries.iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![21],
            "entry 15 is covered by the newer snapshot"
        );
    }

    #[test]
    fn replay_without_a_snapshot_has_no_base() {
        let replay = WalReplay::from_records(vec![hard_state(1, None)]);
        assert!(replay.snapshot.is_none());
        assert_eq!(replay.snapshot_index(), 0);
    }

    #[test]
    fn an_adopted_snapshot_wins_only_when_it_is_newer() {
        // Snapshots live in their own files now; the WAL of a migrated node may
        // still carry an older one.
        let mut replay = WalReplay::from_records(vec![
            WalRecord::Snapshot {
                last_index: 20,
                last_term: 3,
                data: b"wal".to_vec(),
            },
            entry(21, 3),
            entry(31, 3),
        ]);
        replay.adopt(Snapshot {
            last_index: 10,
            last_term: 2,
            data: b"older file".to_vec(),
        });
        assert_eq!(replay.snapshot.as_ref().unwrap().data, b"wal");

        replay.adopt(Snapshot {
            last_index: 30,
            last_term: 4,
            data: b"newer file".to_vec(),
        });
        assert_eq!(replay.snapshot.as_ref().unwrap().data, b"newer file");
        assert_eq!(
            replay.entries.iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![31],
            "adopting a newer snapshot drops the entries it covers"
        );
    }

    // ── 2.1: pending_proposals ────────────────────────────────────────────────

    use tokio::sync::oneshot;

    #[test]
    fn propose_returns_none_if_not_leader() {
        // A freshly created node is a follower — propose must return None.
        let (handle, _dir) = make_handle(1, vec![2, 3]);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let rx = rt.block_on(handle.propose(b"set foo bar".to_vec()));
        assert!(rx.is_none(), "follower must not accept proposals");
    }

    #[test]
    fn drain_clears_all_pending_proposals() {
        // Insert two senders into pending_proposals and drain them.
        // Each receiver should resolve with Err (sender dropped).
        let (handle, _dir) = make_handle(1, vec![2, 3]);
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
        let (handle, _dir) = make_handle(1, vec![2, 3]);
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
                    commit: 0,
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
        let (handle, _dir) = make_handle(1, vec![2, 3]);
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
        let (handle, _dir) = make_handle(1, vec![2, 3]);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(handle.read_index_if_leader());
        assert!(result.is_none(), "follower must return None for read_index");
    }

    #[test]
    fn applied_watch_advances_on_entry_apply() {
        let (handle, _dir) = make_handle(1, vec![2, 3]);
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
    // The WAL is append-only, so after compacting at index 50 it still holds
    // every entry 1..=60 plus a snapshot marker. Two defects on restart:
    //   1. replay returned all 60 entries; `RaftLog::restore` re-inserted the
    //      ones <= the snapshot base above the sentinel, corrupting the index
    //      math (last_index / term_at go wrong).
    //   2. The snapshot record carried no data, so the KV store was never
    //      rebuilt — everything committed at or below the snapshot was lost.
    //
    // Still pinned here against the 0.1.x WAL layout, which a migrating node
    // has to keep replaying correctly.
    #[test]
    fn replay_after_compaction_reindexes_log_and_restores_kv() {
        use std::collections::BTreeMap;

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

        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("node-1.wal");
        let (wal, _) = storage::Wal::open(&wal_path).unwrap();
        let handle = NodeHandle::new(NodeHandleConfig {
            raft: raft::Config::new(1, vec![2, 3]),
            replay: WalReplay::from_records(records),
            kv: Arc::new(Mutex::new(KvStore::default())),
            wal: Arc::new(Mutex::new(wal)),
            snapshots: storage::SnapshotStore::new(dir.path()),
            compaction_threshold: 0,
        });

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

    // ── The compaction trigger: ~30% of snapshots were skipped ────────────────
    //
    // 0.1.3 asked `max_idx % COMPACTION_THRESHOLD == 0`. `max_idx` is the
    // highest index in *this Ready's* batch, so a batch that straddles the
    // boundary steps over the multiple and never compacts. Under write load
    // batches get bigger and the misses compound: gambas took 696 snapshots
    // where 49,400 applies at a threshold of 50 called for 988. The log then
    // grows past what the design assumes — the last thing you want in 256 MB —
    // and compaction becomes non-deterministic across nodes.

    #[test]
    fn a_batch_straddling_the_threshold_still_compacts() {
        // A Ready applying 48..52: max index 52, which the divisibility test
        // misses because 52 % 50 != 0.
        assert!(
            should_compact(52, 0, 50),
            "the boundary must be crossed, not landed on"
        );
        // And the batch after it must not compact again: the distance is
        // measured from the new snapshot, not from zero.
        assert!(!should_compact(60, 52, 50));
        assert!(should_compact(102, 52, 50));
    }

    #[test]
    fn compaction_does_not_fire_below_the_threshold() {
        assert!(!should_compact(49, 0, 50));
        assert!(should_compact(50, 0, 50), "exactly at the threshold counts");
        assert!(
            !should_compact(10, 50, 50),
            "a stale batch cannot underflow"
        );
    }

    // ── The WAL must stop growing: snapshots move to their own files ─────────
    //
    // `try_compact` appended the whole serialized store into the same
    // append-only WAL and dropped nothing. 65,536 pixel keys are ~780 KB, so
    // ~988 snapshots wrote on the order of a gigabyte into a 1 GB volume — the
    // disk-full that truncated node2's last record.

    #[test]
    fn compacting_writes_a_snapshot_file_and_shrinks_the_wal() {
        let dir = tempfile::tempdir().unwrap();
        let handle = make_handle_in(dir.path(), 1, vec![2, 3], 50);
        let wal_path = dir.path().join("node-1.wal");
        let rt = tokio::runtime::Runtime::new().unwrap();

        let entries: Vec<LogEntry> = (1..=60u64)
            .map(|i| LogEntry {
                index: i,
                term: 1,
                entry_type: EntryType::Normal,
                command: serde_json::to_vec(&Command::Set {
                    key: format!("px:{i:03}:001"),
                    value: "7".to_string(),
                })
                .unwrap(),
            })
            .collect();

        rt.block_on(async {
            {
                let mut node = handle.node.lock().await;
                for entry in &entries {
                    node.log.append(entry.clone());
                }
                node.commit_index = 60;
                node.last_applied = 60;
            }
            {
                let mut kv = handle.kv.lock().await;
                for entry in &entries {
                    kv.apply(&entry.command).unwrap();
                }
            }
            {
                let mut wal = handle.wal.lock().await;
                let records: Vec<WalRecord> =
                    entries.iter().cloned().map(WalRecord::Entry).collect();
                wal.append_batch(&records).unwrap();
            }
            let before = wal_path.metadata().unwrap().len();
            let _ = handle.applied_tx.send(60);

            handle.try_compact(60).await;

            assert!(
                dir.path().join("snapshot-60.bin").exists(),
                "the snapshot must land in its own file"
            );
            assert!(
                wal_path.metadata().unwrap().len() < before,
                "the WAL must be rotated down, not appended to"
            );
            assert_eq!(
                handle.node.lock().await.log.snapshot_index(),
                60,
                "the in-memory log is compacted to the snapshot base"
            );
        });

        // Restart from what is on disk: the snapshot file plus a WAL that no
        // longer carries the entries it covers.
        let mut replay = WalReplay::default();
        storage::Wal::open_with(&wal_path, |r| replay.push(r)).unwrap();
        assert!(
            replay.entries.is_empty(),
            "the rotated WAL keeps no entry the snapshot covers"
        );
        assert!(
            !replay.snapshot_in_wal,
            "and no snapshot blob inside the WAL either"
        );
        let store = storage::SnapshotStore::new(dir.path());
        replay.adopt(store.load_latest().unwrap().expect("a snapshot file"));

        let restarted = make_restarted_handle(dir.path(), replay);
        rt.block_on(async {
            assert_eq!(restarted.node.lock().await.log.snapshot_index(), 60);
            assert_eq!(
                restarted.kv.lock().await.get("px:060:001"),
                Some("7"),
                "the state machine is rebuilt from the snapshot file"
            );
        });
    }

    // ── The commit index must survive a restart, end to end ──────────────────
    //
    // 0.1.3 persisted only term and voted_for, so a restarted node reported a
    // commit index of `snapshot_index` — which is what made node1 look wedged
    // during the incident, and what let a restarted leader satisfy a
    // linearizable read from a state machine that was behind.
    #[test]
    fn a_restart_recovers_the_commit_index_and_re_applies_above_the_snapshot() {
        use std::collections::BTreeMap;

        // The snapshot covers writes up to index 50. Entries 51..=55 committed
        // after it and live only in the WAL.
        let board = BTreeMap::from([("px:001:001".to_string(), "1".to_string())]);
        let mut records = vec![WalRecord::Snapshot {
            last_index: 50,
            last_term: 3,
            data: serde_json::to_vec(&board).unwrap(),
        }];
        records.extend((51..=55u64).map(|i| {
            WalRecord::Entry(LogEntry {
                index: i,
                term: 3,
                entry_type: EntryType::Normal,
                command: serde_json::to_vec(&Command::Set {
                    key: format!("px:{i:03}:002"),
                    value: "9".to_string(),
                })
                .unwrap(),
            })
        }));
        records.push(WalRecord::HardState {
            term: 3,
            voted_for: Some(1),
            commit: 55,
        });

        let dir = tempfile::tempdir().unwrap();
        let handle = make_restarted_handle(dir.path(), WalReplay::from_records(records));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            assert_eq!(
                handle.node.lock().await.commit_index,
                55,
                "the durable commit index is recovered, not reset to the snapshot base"
            );
            let kv = handle.kv.lock().await;
            assert_eq!(
                kv.get("px:001:001"),
                Some("1"),
                "state up to the snapshot comes from the snapshot"
            );
            assert_eq!(
                kv.get("px:055:002"),
                Some("9"),
                "and the committed entries above it are re-applied from the WAL"
            );
            drop(kv);
            assert_eq!(
                *handle.subscribe_applied().borrow(),
                55,
                "a linearizable read must not be satisfied by a stale wait"
            );
        });
    }

    // ── A lagging peer must never be silently abandoned ──────────────────────
    //
    // When a peer's next_index falls inside the compacted log the state machine
    // asks for a snapshot to be sent. `fan_out` read `last_snapshot` and, if it
    // was None, skipped the whole block — no log line, no fallback — so the peer
    // was not contacted that tick, and the next tick reached the same dead end.

    #[test]
    fn a_leader_without_a_snapshot_in_memory_takes_one_for_a_lagging_peer() {
        let dir = tempfile::tempdir().unwrap();
        let handle = make_handle_in(dir.path(), 1, vec![2, 3], 5_000);
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            {
                let mut node = handle.node.lock().await;
                for index in 1..=20u64 {
                    node.log.append(LogEntry {
                        index,
                        term: 1,
                        entry_type: EntryType::Normal,
                        command: serde_json::to_vec(&Command::Set {
                            key: format!("px:{index:03}:001"),
                            value: "7".to_string(),
                        })
                        .unwrap(),
                    });
                }
                node.commit_index = 20;
                node.last_applied = 20;
                node.log.compact(10, 1);
            }
            {
                let mut kv = handle.kv.lock().await;
                kv.apply(
                    &serde_json::to_vec(&Command::Set {
                        key: "px:020:001".to_string(),
                        value: "7".to_string(),
                    })
                    .unwrap(),
                )
                .unwrap();
            }
            let _ = handle.applied_tx.send(20);
            assert!(
                handle.last_snapshot.lock().await.is_none(),
                "precondition: this is exactly where 0.1.3 gave up"
            );

            let snap = handle
                .snapshot_for_peers()
                .await
                .expect("a snapshot must be produced on demand");
            assert!(
                snap.last_index >= 10,
                "the snapshot must cover the compacted prefix the peer is missing"
            );
            assert!(
                dir.path()
                    .join(format!("snapshot-{}.bin", snap.last_index))
                    .exists(),
                "and it must be durable before it is sent"
            );
        });
    }

    #[test]
    fn step_rpc_answers_an_install_snapshot() {
        // `step_rpc` matched only vote and append replies, so every
        // InstallSnapshot RPC returned Status::internal even though the
        // follower had installed the snapshot. The leader saw a failed RPC,
        // never advanced the peer, and re-sent the whole snapshot every tick.
        use std::collections::BTreeMap;

        let (handle, _dir) = make_handle(1, vec![2, 3]);
        let board = BTreeMap::from([("px:001:001".to_string(), "7".to_string())]);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let response = rt.block_on(handle.step_rpc(Message::InstallSnapshot {
            from: 2,
            msg: raft::InstallSnapshot {
                term: 5,
                leader_id: 2,
                snapshot: Snapshot {
                    last_index: 30,
                    last_term: 5,
                    data: serde_json::to_vec(&board).unwrap(),
                },
            },
        }));

        match response {
            Some(Message::InstallSnapshotResponse { msg, .. }) => {
                assert!(msg.success);
                assert_eq!(
                    msg.last_index, 30,
                    "the follower reports the snapshot it installed"
                );
            }
            other => panic!("InstallSnapshot must produce a reply, got {other:?}"),
        }
        rt.block_on(async {
            assert_eq!(
                handle.kv.lock().await.get("px:001:001"),
                Some("7"),
                "and the snapshot really is applied"
            );
        });
    }

    fn make_restarted_handle(dir: &std::path::Path, replay: WalReplay) -> Arc<NodeHandle> {
        let (wal, _) = storage::Wal::open(dir.join("node-1.wal")).unwrap();
        Arc::new(NodeHandle::new(NodeHandleConfig {
            raft: raft::Config::new(1, vec![2, 3]),
            replay,
            kv: Arc::new(Mutex::new(KvStore::default())),
            wal: Arc::new(Mutex::new(wal)),
            snapshots: storage::SnapshotStore::new(dir),
            compaction_threshold: 50,
        }))
    }

    /// A handle backed by a real directory. Rotation renames a temp file over
    /// the WAL and snapshots are files of their own, so the tests need a
    /// directory that outlives the node — not a bare NamedTempFile.
    fn make_handle_in(
        dir: &std::path::Path,
        id: u64,
        peers: Vec<u64>,
        compaction_threshold: u64,
    ) -> Arc<NodeHandle> {
        use raft::Config;
        use storage::{KvStore, SnapshotStore, Wal};

        let mut replay = WalReplay::default();
        let wal = Wal::open_with(dir.join(format!("node-{id}.wal")), |r| replay.push(r)).unwrap();
        Arc::new(NodeHandle::new(NodeHandleConfig {
            raft: Config::new(id, peers),
            replay,
            kv: Arc::new(Mutex::new(KvStore::default())),
            wal: Arc::new(Mutex::new(wal)),
            snapshots: SnapshotStore::new(dir),
            compaction_threshold,
        }))
    }

    fn make_handle(id: u64, peers: Vec<u64>) -> (Arc<NodeHandle>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let handle = make_handle_in(dir.path(), id, peers, 0);
        (handle, dir)
    }
}

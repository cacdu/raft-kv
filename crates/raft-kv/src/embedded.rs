/// Public facade for embedding a raft-kv node inside another application.
///
/// `RaftKv::start` owns everything the node needs to run: it opens the WAL,
/// replays it, spawns the gRPC peer server and the tick loop. The returned
/// handle is `Clone` and cheap to share across tasks.
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tokio::sync::{broadcast, Mutex};
use tracing::info;

use raft::NodeId;
use storage::{kv::Command, wal::WalError, KvStore, SnapshotStore, Wal};

use crate::{
    events::Event,
    node_handle::{NodeHandle, NodeHandleConfig, WalReplay},
};

/// How long a write waits for quorum commit, and a read waits for apply.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(5);

pub struct RaftKvOptions {
    /// Unique node id within the cluster.
    pub id: NodeId,
    /// Address to bind the Raft gRPC server on (e.g. "127.0.0.1:7001").
    pub raft_addr: String,
    /// Raft gRPC address of every peer, keyed by node id.
    pub peers: HashMap<NodeId, String>,
    /// Application-level address of every peer, keyed by node id.
    /// raft-kv never connects to these — it hands them back in
    /// [`Error::NotLeader`] so your app knows where to forward writes.
    pub app_addrs: HashMap<NodeId, String>,
    /// Directory for this node's WAL.
    pub data_dir: PathBuf,
    /// Start as a non-voting learner (join via a ConfChange on the leader).
    pub learner: bool,
    /// Ticks before a follower starts an election (the tick loop runs at 10ms).
    /// `0` keeps the built-in default (10 ticks). Raise it well above the
    /// inter-node round-trip time to avoid spurious elections on a WAN.
    pub election_timeout: u32,
    /// Ticks between leader heartbeats (10ms per tick). Must be `<<` the
    /// election timeout. `0` keeps the built-in default (3 ticks).
    pub heartbeat_timeout: u32,
    /// Applied entries between snapshots. Each one serializes the whole state
    /// machine and rotates the WAL, so this trades write amplification against
    /// how much log a restart (or a lagging peer) has to replay. `0` keeps the
    /// built-in default (5000).
    pub compaction_threshold: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// This node is not the leader. Forward the operation to `leader_addr`
    /// (the leader's app-level address from [`RaftKvOptions::app_addrs`]).
    #[error("not the leader (leader id: {leader_id:?})")]
    NotLeader {
        leader_id: Option<NodeId>,
        leader_addr: Option<String>,
    },
    /// Leadership was lost after the proposal was accepted but before it
    /// committed. The write may or may not survive — retry idempotently.
    #[error("proposal dropped: leadership lost before commit")]
    ProposalDropped,
    #[error("timed out waiting for quorum")]
    Timeout,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Status {
    pub id: NodeId,
    pub is_leader: bool,
    pub leader_id: Option<NodeId>,
    pub leader_addr: Option<String>,
}

/// A running raft-kv node, embedded in your process.
#[derive(Clone)]
pub struct RaftKv {
    id: NodeId,
    handle: Arc<NodeHandle>,
    kv: Arc<Mutex<KvStore>>,
}

impl RaftKv {
    /// Open (or create) the WAL under `data_dir`, replay it, and start the
    /// node: gRPC peer server + 10ms tick loop, both on background tasks.
    pub async fn start(opts: RaftKvOptions) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&opts.data_dir)?;
        let wal_path = opts.data_dir.join(format!("node-{}.wal", opts.id));
        let snapshots = SnapshotStore::new(&opts.data_dir);

        // Fold the WAL as it is read rather than materializing it: a file that
        // has never been rotated holds every snapshot ever appended.
        let mut replay = WalReplay::default();
        let mut wal = Wal::open_with(&wal_path, |record| replay.push(record))?;
        if let Some(snapshot) = snapshots.load_latest()? {
            replay.adopt(snapshot);
        }
        if wal.is_legacy() || replay.snapshot_in_wal {
            migrate_wal(&mut wal, &wal_path, &replay, &snapshots)?;
        }

        let wal = Arc::new(Mutex::new(wal));
        let kv = Arc::new(Mutex::new(KvStore::default()));

        let node_config = NodeHandleConfig {
            raft: build_raft_config(
                opts.id,
                opts.peers.keys().copied().collect(),
                opts.election_timeout,
                opts.heartbeat_timeout,
            ),
            replay,
            kv: Arc::clone(&kv),
            wal: Arc::clone(&wal),
            snapshots,
            compaction_threshold: opts.compaction_threshold,
        };
        let handle = Arc::new(if opts.learner {
            NodeHandle::new_learner(node_config)
        } else {
            NodeHandle::new(node_config)
        });
        handle.register_peers(opts.peers, opts.app_addrs).await;

        let grpc_addr: SocketAddr = opts.raft_addr.parse()?;
        let grpc_handle = Arc::clone(&handle);
        tokio::spawn(async move {
            if let Err(e) = crate::grpc::server(grpc_handle, grpc_addr).await {
                tracing::error!("raft gRPC server exited: {e}");
            }
        });

        let tick_handle = Arc::clone(&handle);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            loop {
                interval.tick().await;
                tick_handle.tick().await;
            }
        });

        Ok(Self {
            id: opts.id,
            handle,
            kv,
        })
    }

    /// Replicated write. Resolves once the entry is committed by a quorum
    /// and applied locally. Leader only — followers get [`Error::NotLeader`].
    pub async fn put(&self, key: impl Into<String>, value: impl Into<String>) -> Result<(), Error> {
        self.propose(Command::Set {
            key: key.into(),
            value: value.into(),
        })
        .await
    }

    /// Replicated delete. Same semantics as [`RaftKv::put`].
    pub async fn delete(&self, key: impl Into<String>) -> Result<(), Error> {
        self.propose(Command::Delete { key: key.into() }).await
    }

    /// Linearizable read via ReadIndex: captures the commit index, waits for
    /// it to apply, then reads. Leader only — followers get [`Error::NotLeader`].
    pub async fn get(&self, key: &str) -> Result<Option<String>, Error> {
        let read_index = self.read_index().await?;
        self.wait_applied(read_index).await?;
        Ok(self.kv.lock().await.get(key).map(str::to_string))
    }

    /// Linearizable prefix scan. Leader only.
    pub async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, String)>, Error> {
        let read_index = self.read_index().await?;
        self.wait_applied(read_index).await?;
        Ok(self.scan_prefix_local(prefix).await)
    }

    /// Local read from this node's applied state. Works on any node, no
    /// consensus round — may lag the leader by in-flight entries.
    pub async fn get_local(&self, key: &str) -> Option<String> {
        self.kv.lock().await.get(key).map(str::to_string)
    }

    /// Local prefix scan. Same freshness caveat as [`RaftKv::get_local`].
    pub async fn scan_prefix_local(&self, prefix: &str) -> Vec<(String, String)> {
        self.kv
            .lock()
            .await
            .scan_prefix(prefix)
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Subscribe to every write as it applies on this node, in log order.
    /// All nodes deliver the same sequence. On [`broadcast::error::RecvError::Lagged`]
    /// or [`Event::SnapshotApplied`], re-read the store to resync.
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.handle.subscribe_events()
    }

    pub async fn is_leader(&self) -> bool {
        self.handle.is_leader().await
    }

    pub async fn status(&self) -> Status {
        // Read is_leader and leader_id under one lock so they always describe
        // the same instant (see NodeHandle::role_status).
        let (is_leader, leader_id) = self.handle.role_status().await;
        let leader_addr = self.app_addr_of(leader_id).await;
        Status {
            id: self.id,
            is_leader,
            leader_id,
            leader_addr,
        }
    }

    /// The standalone HTTP KV API (`/kv`, `/status`, `/metrics`, `/cluster/*`)
    /// as an axum router — mount it in your app or serve it on its own port.
    pub fn http_router(&self) -> axum::Router {
        crate::http::router(Arc::clone(&self.handle), Arc::clone(&self.kv))
    }

    async fn propose(&self, cmd: Command) -> Result<(), Error> {
        let bytes = serde_json::to_vec(&cmd).expect("Command serialization is infallible");
        let rx = match self.handle.propose(bytes).await {
            None => return Err(self.not_leader().await),
            Some(rx) => rx,
        };
        match tokio::time::timeout(COMMIT_TIMEOUT, rx).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(Error::ProposalDropped),
            Err(_) => Err(Error::Timeout),
        }
    }

    async fn read_index(&self) -> Result<u64, Error> {
        match self.handle.read_index_if_leader().await {
            Some(idx) => Ok(idx),
            None => Err(self.not_leader().await),
        }
    }

    async fn wait_applied(&self, index: u64) -> Result<(), Error> {
        let mut rx = self.handle.subscribe_applied();
        tokio::time::timeout(COMMIT_TIMEOUT, async {
            loop {
                if *rx.borrow() >= index {
                    break;
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
        .map_err(|_| Error::Timeout)
    }

    async fn not_leader(&self) -> Error {
        let leader_id = self.handle.leader_id().await;
        let leader_addr = self.app_addr_of(leader_id).await;
        Error::NotLeader {
            leader_id,
            leader_addr,
        }
    }

    async fn app_addr_of(&self, id: Option<NodeId>) -> Option<String> {
        match id {
            Some(id) => self.handle.http_peers.lock().await.get(&id).cloned(),
            None => None,
        }
    }
}

/// Move a 0.1.x WAL to the 0.1.4 layout: its newest snapshot becomes a snapshot
/// file, and the WAL is rewritten down to the hard state plus the entries the
/// snapshot does not cover.
///
/// This is the one-time repair for a node that ran the old layout. node1 in
/// gambas holds ~988 appended snapshots in a single ~1 GB file on a 1 GB
/// volume; the migration turns that into a snapshot plus a WAL of the entries
/// since. The old file is kept under `.wal.legacy` — renaming costs no space —
/// and the operator can delete it once the node is verified.
fn migrate_wal(
    wal: &mut Wal,
    wal_path: &Path,
    replay: &WalReplay,
    snapshots: &SnapshotStore,
) -> anyhow::Result<()> {
    if let Some(snapshot) = &replay.snapshot {
        if snapshots.latest_index()? < Some(snapshot.last_index) {
            snapshots.save(snapshot)?;
        }
    }

    let legacy_path = wal_path.with_extension("wal.legacy");
    std::fs::rename(wal_path, &legacy_path)?;

    let records = replay.to_records();
    if let Err(e) = wal.rewrite(&records) {
        // A volume with no room left is exactly how this node got here, so the
        // one case worth handling is ENOSPC: drop the copy and try once more.
        if is_out_of_space(&e) {
            tracing::warn!(
                path = %legacy_path.display(),
                "no space to rotate the WAL; dropping the legacy copy and retrying"
            );
            std::fs::remove_file(&legacy_path)?;
            wal.rewrite(&records)?;
        } else {
            // Put the original back rather than leaving the node with no WAL
            // under its own name.
            std::fs::rename(&legacy_path, wal_path)?;
            return Err(e.into());
        }
    }

    info!(
        legacy = %legacy_path.display(),
        snapshot_index = replay.snapshot_index(),
        entries_kept = replay.entries.len(),
        "migrated a 0.1.x WAL: snapshots now live in their own files, \
         the legacy copy can be deleted once this node is verified"
    );
    Ok(())
}

/// ENOSPC by raw errno: `io::ErrorKind::StorageFull` is newer than this
/// crate's MSRV.
fn is_out_of_space(e: &WalError) -> bool {
    matches!(e, WalError::Io(io) if io.raw_os_error() == Some(28))
}

/// Build the Raft config, overriding the election/heartbeat timeouts only when
/// the caller supplies a non-zero value. `0` means "keep the built-in default",
/// which also avoids the empty `election_timeout..2*election_timeout` range
/// that a literal `0` would otherwise produce.
fn build_raft_config(
    id: NodeId,
    peers: Vec<NodeId>,
    election_timeout: u32,
    heartbeat_timeout: u32,
) -> raft::Config {
    let mut cfg = raft::Config::new(id, peers);
    if election_timeout > 0 {
        cfg.election_timeout = election_timeout;
    }
    if heartbeat_timeout > 0 {
        cfg.heartbeat_timeout = heartbeat_timeout;
    }
    cfg
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use raft::{message::EntryType, LogEntry};
    use storage::wal::WalRecord;

    use super::{build_raft_config, migrate_wal, Command, SnapshotStore, Wal, WalReplay};

    // ── A 0.1.x WAL has to migrate itself out of the old layout ──────────────
    //
    // 0.1.x appended the whole serialized store into the WAL on every
    // compaction and never dropped anything: node1's file is ~1 GB of entries
    // and ~988 snapshot blobs, on a 1 GB volume. Startup moves the newest
    // snapshot into a file of its own and rewrites the WAL down to the hard
    // state plus the entries the snapshot does not cover.

    fn entry(index: u64) -> WalRecord {
        WalRecord::Entry(LogEntry {
            index,
            term: 4,
            entry_type: EntryType::Normal,
            command: serde_json::to_vec(&Command::Set {
                key: format!("px:{index:03}:001"),
                value: "7".to_string(),
            })
            .unwrap(),
        })
    }

    #[test]
    fn migrating_moves_the_wal_snapshot_into_a_file_and_shrinks_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("node-1.wal");
        let snapshots = SnapshotStore::new(dir.path());

        let board = serde_json::to_vec(&BTreeMap::from([(
            "px:001:001".to_string(),
            "7".to_string(),
        )]))
        .unwrap();
        {
            let (mut wal, _) = Wal::open(&wal_path).unwrap();
            wal.append(&WalRecord::HardState {
                term: 4,
                voted_for: Some(1),
                commit: 51,
            })
            .unwrap();
            for index in 1..=50 {
                wal.append(&entry(index)).unwrap();
            }
            wal.append(&WalRecord::Snapshot {
                last_index: 50,
                last_term: 4,
                data: board.clone(),
            })
            .unwrap();
            wal.append(&entry(51)).unwrap();
        }
        let before = wal_path.metadata().unwrap().len();

        let mut replay = WalReplay::default();
        let mut wal = Wal::open_with(&wal_path, |r| replay.push(r)).unwrap();
        assert!(replay.snapshot_in_wal, "the old layout must be detected");
        migrate_wal(&mut wal, &wal_path, &replay, &snapshots).unwrap();

        assert_eq!(
            snapshots.load_latest().unwrap().unwrap().data,
            board,
            "the snapshot moves into a file of its own"
        );
        assert!(
            dir.path().join("node-1.wal.legacy").exists(),
            "the original is renamed aside, not deleted — a rename costs no space"
        );
        assert!(
            wal_path.metadata().unwrap().len() < before,
            "and the live WAL is rewritten down"
        );

        // Restart on the migrated layout: same durable state, and nothing left
        // for the migration to do a second time.
        let mut replay = WalReplay::default();
        Wal::open_with(&wal_path, |r| replay.push(r)).unwrap();
        assert!(!replay.snapshot_in_wal);
        assert_eq!(replay.term, 4);
        assert_eq!(
            replay.commit, 51,
            "the persisted commit index must survive the rotation too"
        );
        assert_eq!(
            replay.voted_for,
            Some(1),
            "a node that comes back without its vote can vote twice in one term"
        );
        replay.adopt(snapshots.load_latest().unwrap().unwrap());
        assert_eq!(replay.snapshot_index(), 50);
        assert_eq!(
            replay.entries.iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![51],
            "only the entries the snapshot does not cover survive"
        );
    }

    #[test]
    fn nonzero_timeouts_override_the_defaults() {
        let cfg = build_raft_config(1, vec![2, 3], 200, 25);
        assert_eq!(cfg.election_timeout, 200);
        assert_eq!(cfg.heartbeat_timeout, 25);
    }

    #[test]
    fn zero_timeouts_keep_the_builtin_defaults() {
        let default = raft::Config::new(1, vec![2, 3]);
        let cfg = build_raft_config(1, vec![2, 3], 0, 0);
        assert_eq!(cfg.election_timeout, default.election_timeout);
        assert_eq!(cfg.heartbeat_timeout, default.heartbeat_timeout);
    }

    #[test]
    fn each_timeout_overrides_independently() {
        let default = raft::Config::new(1, vec![2, 3]);
        let cfg = build_raft_config(1, vec![2, 3], 200, 0);
        assert_eq!(cfg.election_timeout, 200);
        assert_eq!(cfg.heartbeat_timeout, default.heartbeat_timeout);
    }
}

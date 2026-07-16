/// Public facade for embedding a raft-kv node inside another application.
///
/// `RaftKv::start` owns everything the node needs to run: it opens the WAL,
/// replays it, spawns the gRPC peer server and the tick loop. The returned
/// handle is `Clone` and cheap to share across tasks.
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use tokio::sync::{broadcast, Mutex};

use raft::NodeId;
use storage::{kv::Command, KvStore, Wal};

use crate::{events::Event, node_handle::NodeHandle};

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
        let (wal, records) = Wal::open(&wal_path)?;
        let wal = Arc::new(Mutex::new(wal));
        let kv = Arc::new(Mutex::new(KvStore::default()));

        let raft_config = raft::Config::new(opts.id, opts.peers.keys().copied().collect());
        let handle = if opts.learner {
            Arc::new(NodeHandle::new_learner(
                raft_config,
                records,
                Arc::clone(&kv),
                Arc::clone(&wal),
            ))
        } else {
            Arc::new(NodeHandle::new(
                raft_config,
                records,
                Arc::clone(&kv),
                Arc::clone(&wal),
            ))
        };
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
        let leader_id = self.handle.leader_id().await;
        let leader_addr = self.app_addr_of(leader_id).await;
        Status {
            id: self.id,
            is_leader: self.handle.is_leader().await,
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

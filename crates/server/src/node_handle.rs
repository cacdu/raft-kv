/// NodeHandle wraps the Raft state machine behind a Mutex and drives it:
/// - applies Ready output (persists WAL, applies KV commands, fans out messages)
/// - provides async methods for the gRPC and HTTP layers
use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;
use tracing::{debug, warn};

use raft::{Config, LogEntry, Message, NodeId, RaftNode, Ready};
use storage::{KvStore, Wal};
use storage::wal::WalRecord;

use crate::peer::PeerClient;

pub struct NodeHandle {
    node: Mutex<RaftNode>,
    kv: Arc<Mutex<KvStore>>,
    wal: Arc<Mutex<Wal>>,
    peers: Mutex<HashMap<NodeId, PeerClient>>,
}

impl NodeHandle {
    pub fn new(
        config: Config,
        _records: Vec<WalRecord>,
        kv: Arc<Mutex<KvStore>>,
        wal: Arc<Mutex<Wal>>,
    ) -> Self {
        // TODO: replay WAL records to restore hard state and log entries
        let node = RaftNode::new(config);
        Self {
            node: Mutex::new(node),
            kv,
            wal,
            peers: Mutex::new(HashMap::new()),
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
    }

    pub async fn step(&self, msg: Message) {
        let ready = {
            let mut node = self.node.lock().await;
            node.step(msg)
        };
        self.process_ready(ready).await;
    }

    pub async fn propose(&self, command: Vec<u8>) -> bool {
        let is_leader = self.node.lock().await.is_leader();
        if !is_leader {
            return false;
        }
        let ready = {
            let mut node = self.node.lock().await;
            node.step(Message::Propose { command })
        };
        self.process_ready(ready).await;
        true
    }

    pub async fn leader_id(&self) -> Option<NodeId> {
        self.node.lock().await.leader_id()
    }

    pub async fn is_leader(&self) -> bool {
        self.node.lock().await.is_leader()
    }

    async fn process_ready(&self, ready: Ready) {
        // 1. Persist entries to WAL before sending any messages
        if !ready.entries_to_persist.is_empty() {
            let mut wal = self.wal.lock().await;
            for entry in &ready.entries_to_persist {
                if let Err(e) = wal.append(&WalRecord::Entry(entry.clone())) {
                    warn!("WAL write failed: {e}");
                }
            }
        }

        // 2. Apply committed entries to KV state machine
        if !ready.entries_to_apply.is_empty() {
            let mut kv = self.kv.lock().await;
            for entry in &ready.entries_to_apply {
                if let Err(e) = kv.apply(&entry.command) {
                    warn!("KV apply failed: {e}");
                }
            }
        }

        // 3. Send messages to peers
        if !ready.messages.is_empty() {
            let peers = self.peers.lock().await;
            for (dest, msg) in ready.messages {
                if let Some(client) = peers.get(&dest) {
                    debug!(dest, "sending message");
                    client.send(msg).await;
                }
            }
        }
    }
}

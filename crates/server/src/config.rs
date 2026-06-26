use std::{collections::HashMap, path::PathBuf};

use clap::Parser;
use raft::message::NodeId;

#[derive(Debug, Parser)]
#[command(name = "raft-kv", about = "Distributed KV store with Raft consensus")]
pub struct NodeConfig {
    #[arg(long)]
    pub id: NodeId,

    /// gRPC address for node-to-node Raft RPCs (e.g. 127.0.0.1:7001)
    #[arg(long)]
    pub grpc_addr: String,

    /// HTTP address for client-facing KV API (e.g. 127.0.0.1:8001)
    #[arg(long)]
    pub http_addr: String,

    /// Peer nodes: repeated --peer id=addr (e.g. --peer 2=127.0.0.1:7002)
    #[arg(long = "peer", value_parser = parse_peer)]
    pub peers: Vec<(NodeId, String)>,

    #[arg(long, default_value = "data")]
    pub data_dir: PathBuf,
}

impl NodeConfig {
    /// Returns peers as a map for convenience.
    pub fn peers(&self) -> HashMap<NodeId, String> {
        self.peers.iter().cloned().collect()
    }
}

fn parse_peer(s: &str) -> Result<(NodeId, String), String> {
    let (id, addr) = s
        .split_once('=')
        .ok_or_else(|| format!("expected id=addr, got '{s}'"))?;
    let id: NodeId = id.parse().map_err(|_| format!("invalid node id: {id}"))?;
    Ok((id, addr.to_string()))
}

// Re-export peers as HashMap for use in main.rs
impl NodeConfig {
    pub fn peers_map(&self) -> HashMap<NodeId, String> {
        self.peers.iter().cloned().collect()
    }
}

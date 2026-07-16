/// Standalone raft-kv server: an embedded `RaftKv` node plus its HTTP KV API.
mod config;

use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use config::NodeConfig;
use raft_kv::{RaftKv, RaftKvOptions};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("raft_kv=info".parse()?),
        )
        .init();

    let cfg = NodeConfig::parse();
    info!(id = cfg.id, grpc_addr = %cfg.grpc_addr, http_addr = %cfg.http_addr, "starting node");

    let node = RaftKv::start(RaftKvOptions {
        id: cfg.id,
        raft_addr: cfg.grpc_addr.clone(),
        peers: cfg.peers_map(),
        app_addrs: cfg.http_peers_map(),
        data_dir: cfg.data_dir.clone(),
        learner: cfg.learner,
    })
    .await?;

    let http_addr: SocketAddr = cfg.http_addr.parse()?;
    info!("gRPC listening on {}", cfg.grpc_addr);
    info!("HTTP listening on {http_addr}");

    let listener = tokio::net::TcpListener::bind(http_addr).await?;
    axum::serve(listener, node.http_router()).await?;

    Ok(())
}

mod config;
mod grpc;
mod http;
mod node_handle;
mod peer;

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use anyhow::Result;
use clap::Parser;
use tokio::sync::{mpsc, Mutex};
use tracing::info;

use config::NodeConfig;
use node_handle::NodeHandle;
use storage::{KvStore, Wal};

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

    // ── Storage ──────────────────────────────────────────────────────────────
    let wal_path = cfg.data_dir.join(format!("node-{}.wal", cfg.id));
    std::fs::create_dir_all(&cfg.data_dir)?;
    let (wal, records) = Wal::open(&wal_path)?;
    let wal = Arc::new(Mutex::new(wal));
    let kv = Arc::new(Mutex::new(KvStore::default()));

    // ── Raft node ────────────────────────────────────────────────────────────
    let peers_map: std::collections::HashMap<_, _> = cfg.peers.iter().cloned().collect();
    let raft_config = raft::Config::new(cfg.id, peers_map.keys().copied().collect());
    let handle = Arc::new(NodeHandle::new(raft_config, records, Arc::clone(&kv), Arc::clone(&wal)));

    // ── gRPC peer server ─────────────────────────────────────────────────────
    let grpc_addr: SocketAddr = cfg.grpc_addr.parse()?;
    let grpc_server = grpc::server(Arc::clone(&handle));

    // ── HTTP client API ──────────────────────────────────────────────────────
    let http_addr: SocketAddr = cfg.http_addr.parse()?;
    let http_router = http::router(Arc::clone(&handle), Arc::clone(&kv), peers_map.clone());

    // ── Raft tick loop ───────────────────────────────────────────────────────
    let tick_handle = Arc::clone(&handle);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
        loop {
            interval.tick().await;
            tick_handle.tick().await;
        }
    });

    info!("gRPC listening on {grpc_addr}");
    info!("HTTP listening on {http_addr}");

    tokio::try_join!(
        grpc_server,
        async move {
            let listener = tokio::net::TcpListener::bind(http_addr).await?;
            axum::serve(listener, http_router).await?;
            Ok::<_, anyhow::Error>(())
        }
    )?;

    Ok(())
}

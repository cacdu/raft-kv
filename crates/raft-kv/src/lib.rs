//! # raft-kv
//!
//! An embeddable, Raft-replicated key-value store.
//!
//! Each [`RaftKv`] instance is a full Raft node: it persists a WAL, talks gRPC
//! to its peers, and applies committed commands to an in-memory KV store.
//! Embed one in each replica of your application and you get a shared,
//! linearizable KV store with no external database:
//!
//! ```no_run
//! use raft_kv::{RaftKv, RaftKvOptions, Event};
//!
//! # async fn example() -> anyhow::Result<()> {
//! let node = RaftKv::start(RaftKvOptions {
//!     id: 1,
//!     raft_addr: "127.0.0.1:7001".into(),
//!     peers: [(2, "127.0.0.1:7002".into()), (3, "127.0.0.1:7003".into())].into(),
//!     app_addrs: [(2, "127.0.0.1:8002".into()), (3, "127.0.0.1:8003".into())].into(),
//!     data_dir: "data/node1".into(),
//!     learner: false,
//! })
//! .await?;
//!
//! node.put("hello", "world").await?;            // replicated write (leader only)
//! let v = node.get("hello").await?;             // linearizable read (leader only)
//! let v = node.get_local("hello").await;        // local read (any node)
//!
//! // Watch every committed write as it applies on *this* node:
//! let mut events = node.subscribe();
//! while let Ok(event) = events.recv().await {
//!     match event {
//!         Event::Set { key, value } => println!("{key} = {value}"),
//!         Event::Delete { key } => println!("{key} deleted"),
//!         Event::SnapshotApplied => println!("state replaced, re-read caches"),
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The `raft-kv` binary in this crate is a thin wrapper: [`RaftKv::start`] plus
//! the HTTP KV API from [`RaftKv::http_router`] served standalone.

mod embedded;
mod events;
mod grpc;
mod http;
mod metrics;
mod node_handle;
mod peer;

pub use embedded::{Error, RaftKv, RaftKvOptions, Status};
pub use events::Event;
pub use raft::NodeId;

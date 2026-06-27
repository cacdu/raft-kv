/// HTTP API — client-facing KV operations.
///
/// GET    /kv/{key}         → 200 value | 404
/// PUT    /kv/{key}         → 200 (body = value)
/// DELETE /kv/{key}         → 200
/// GET    /status           → 200 JSON { id, role, leader_id, commit_index }
///
/// Non-leader nodes return 307 Redirect to the leader's HTTP address.
use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use tokio::sync::Mutex;

use raft::message::NodeId;
use storage::{KvStore, kv::Command};

use crate::node_handle::NodeHandle;

#[derive(Clone)]
struct AppState {
    handle: Arc<NodeHandle>,
    kv: Arc<Mutex<KvStore>>,
    /// peer_id → http address (for redirect)
    peers: HashMap<NodeId, String>,
}

pub fn router(
    handle: Arc<NodeHandle>,
    kv: Arc<Mutex<KvStore>>,
    peers: HashMap<NodeId, String>,
) -> Router {
    let state = AppState { handle, kv, peers };
    Router::new()
        .route("/kv/{key}", get(kv_get).put(kv_put).delete(kv_delete))
        .route("/status", get(status))
        .with_state(state)
}

async fn kv_get(Path(key): Path<String>, State(s): State<AppState>) -> Response {
    // ReadIndex: only the leader serves reads; followers redirect.
    let read_index = match s.handle.read_index_if_leader().await {
        Some(idx) => idx,
        None => return redirect_to_leader(&s).await,
    };

    // Wait until the leader has applied at least read_index to its KV store.
    // This ensures the read reflects every write that completed before this request.
    let mut applied_rx = s.handle.subscribe_applied();
    let wait = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if *applied_rx.borrow() >= read_index {
                break;
            }
            if applied_rx.changed().await.is_err() {
                break; // sender dropped (shutdown) — serve whatever we have
            }
        }
    });
    if wait.await.is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, "read timeout").into_response();
    }

    let kv = s.kv.lock().await;
    match kv.get(&key) {
        Some(v) => (StatusCode::OK, v.to_string()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn kv_put(
    Path(key): Path<String>,
    State(s): State<AppState>,
    body: Bytes,
) -> Response {
    let value = match String::from_utf8(body.to_vec()) {
        Ok(v) => v,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let cmd = Command::Set { key, value };
    let bytes = serde_json::to_vec(&cmd).unwrap();
    await_commit(&s, bytes).await
}

async fn kv_delete(Path(key): Path<String>, State(s): State<AppState>) -> Response {
    let cmd = Command::Delete { key };
    let bytes = serde_json::to_vec(&cmd).unwrap();
    await_commit(&s, bytes).await
}

/// Propose a command and wait for it to be committed, with a 5-second timeout.
async fn await_commit(s: &AppState, command: Vec<u8>) -> Response {
    let rx = match s.handle.propose(command).await {
        None => return redirect_to_leader(s).await,
        Some(rx) => rx,
    };
    match tokio::time::timeout(Duration::from_secs(5), rx).await {
        Ok(Ok(())) => StatusCode::OK.into_response(),
        Ok(Err(_)) => (StatusCode::SERVICE_UNAVAILABLE, "proposal dropped").into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "proposal timeout").into_response(),
    }
}

async fn status(State(s): State<AppState>) -> impl IntoResponse {
    let leader = s.handle.leader_id().await;
    let is_leader = s.handle.is_leader().await;
    axum::Json(serde_json::json!({
        "is_leader": is_leader,
        "leader_id": leader,
    }))
}

async fn redirect_to_leader(s: &AppState) -> Response {
    if let Some(leader_id) = s.handle.leader_id().await {
        if let Some(addr) = s.peers.get(&leader_id) {
            return Redirect::temporary(&format!("http://{addr}")).into_response();
        }
    }
    (StatusCode::SERVICE_UNAVAILABLE, "no leader").into_response()
}

/// HTTP API — client-facing KV operations.
///
/// GET    /kv/{key}         → 200 value | 404
/// PUT    /kv/{key}         → 200 (body = value)
/// DELETE /kv/{key}         → 200
/// GET    /status           → 200 JSON { id, role, leader_id, commit_index }
///
/// Non-leader nodes return 307 Redirect to the leader's HTTP address.
use std::{collections::HashMap, sync::Arc};

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    routing::{delete, get, put},
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

    if !s.handle.is_leader().await {
        return redirect_to_leader(&s).await;
    }

    let cmd = Command::Set { key, value };
    let bytes = serde_json::to_vec(&cmd).unwrap();
    s.handle.propose(bytes).await;
    StatusCode::OK.into_response()
}

async fn kv_delete(Path(key): Path<String>, State(s): State<AppState>) -> Response {
    if !s.handle.is_leader().await {
        return redirect_to_leader(&s).await;
    }
    let cmd = Command::Delete { key };
    let bytes = serde_json::to_vec(&cmd).unwrap();
    s.handle.propose(bytes).await;
    StatusCode::OK.into_response()
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

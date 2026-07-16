/// HTTP API — client-facing KV operations and cluster membership management.
///
/// GET    /kv/{key}         → 200 value | 404
/// PUT    /kv/{key}         → 200 (body = value)
/// DELETE /kv/{key}         → 200
/// GET    /status           → 200 JSON { id, role, leader_id, commit_index }
/// POST   /cluster/add      → 200 (body JSON: {id, raft_addr, http_addr})
/// POST   /cluster/remove   → 200 (body JSON: {id})
///
/// Non-leader nodes return 307 Redirect to the leader's HTTP address.
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use tokio::sync::Mutex;

use raft::message::{ConfChangeOp, NodeId};
use storage::{kv::Command, KvStore};

use crate::node_handle::NodeHandle;

#[derive(Clone)]
struct AppState {
    handle: Arc<NodeHandle>,
    kv: Arc<Mutex<KvStore>>,
}

pub fn router(handle: Arc<NodeHandle>, kv: Arc<Mutex<KvStore>>) -> Router {
    let state = AppState { handle, kv };
    Router::new()
        .route("/kv", get(kv_scan))
        .route("/kv/{key}", get(kv_get).put(kv_put).delete(kv_delete))
        .route("/status", get(status))
        .route("/metrics", get(metrics_handler))
        .route("/cluster/add", post(cluster_add))
        .route("/cluster/remove", post(cluster_remove))
        .with_state(state)
}

#[derive(Deserialize)]
struct ScanQuery {
    prefix: Option<String>,
}

async fn kv_scan(Query(q): Query<ScanQuery>, State(s): State<AppState>) -> Response {
    let read_index = match s.handle.read_index_if_leader().await {
        Some(idx) => idx,
        None => {
            let prefix_qs = q
                .prefix
                .as_deref()
                .map(|p| format!("?prefix={p}"))
                .unwrap_or_default();
            if let Some(leader_id) = s.handle.leader_id().await {
                let addr = s.handle.http_peers.lock().await.get(&leader_id).cloned();
                if let Some(addr) = addr {
                    return Redirect::temporary(&format!("http://{addr}/kv{prefix_qs}"))
                        .into_response();
                }
            }
            return (StatusCode::SERVICE_UNAVAILABLE, "no leader").into_response();
        }
    };

    let mut applied_rx = s.handle.subscribe_applied();
    let wait = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if *applied_rx.borrow() >= read_index {
                break;
            }
            if applied_rx.changed().await.is_err() {
                break;
            }
        }
    });
    if wait.await.is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, "read timeout").into_response();
    }

    let kv = s.kv.lock().await;
    let prefix = q.prefix.as_deref().unwrap_or("");
    let pairs: std::collections::HashMap<&str, &str> = kv.scan_prefix(prefix).into_iter().collect();
    Json(pairs).into_response()
}

async fn kv_get(Path(key): Path<String>, State(s): State<AppState>) -> Response {
    let read_index = match s.handle.read_index_if_leader().await {
        Some(idx) => idx,
        None => {
            if let Some(leader_id) = s.handle.leader_id().await {
                let addr = s.handle.http_peers.lock().await.get(&leader_id).cloned();
                if let Some(addr) = addr {
                    return Redirect::temporary(&format!("http://{addr}/kv/{key}")).into_response();
                }
            }
            return (StatusCode::SERVICE_UNAVAILABLE, "no leader").into_response();
        }
    };

    let start = Instant::now();
    let mut applied_rx = s.handle.subscribe_applied();
    let wait = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if *applied_rx.borrow() >= read_index {
                break;
            }
            if applied_rx.changed().await.is_err() {
                break;
            }
        }
    });
    if wait.await.is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, "read timeout").into_response();
    }

    let kv = s.kv.lock().await;
    let resp = match kv.get(&key) {
        Some(v) => (StatusCode::OK, v.to_string()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    };
    crate::metrics::READS_TOTAL.inc();
    crate::metrics::REQUEST_DURATION
        .with_label_values(&["read"])
        .observe(start.elapsed().as_secs_f64());
    resp
}

async fn kv_put(Path(key): Path<String>, State(s): State<AppState>, body: Bytes) -> Response {
    let value = match String::from_utf8(body.to_vec()) {
        Ok(v) => v,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let cmd = Command::Set {
        key: key.clone(),
        value,
    };
    let bytes = serde_json::to_vec(&cmd).unwrap();
    await_commit(&s, bytes, &format!("/kv/{key}")).await
}

async fn kv_delete(Path(key): Path<String>, State(s): State<AppState>) -> Response {
    let cmd = Command::Delete { key: key.clone() };
    let bytes = serde_json::to_vec(&cmd).unwrap();
    await_commit(&s, bytes, &format!("/kv/{key}")).await
}

#[derive(Deserialize)]
struct AddRequest {
    id: NodeId,
    raft_addr: String,
    http_addr: String,
}

async fn cluster_add(State(s): State<AppState>, Json(req): Json<AddRequest>) -> Response {
    let rx = s
        .handle
        .propose_conf_change(
            ConfChangeOp::Add,
            req.id,
            Some(req.raft_addr),
            Some(req.http_addr),
        )
        .await;
    match rx {
        None => redirect_to_leader(&s, "/cluster/add").await,
        Some(rx) => match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(())) => StatusCode::OK.into_response(),
            Ok(Err(_)) => (StatusCode::SERVICE_UNAVAILABLE, "conf change dropped").into_response(),
            Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "conf change timeout").into_response(),
        },
    }
}

#[derive(Deserialize)]
struct RemoveRequest {
    id: NodeId,
}

async fn cluster_remove(State(s): State<AppState>, Json(req): Json<RemoveRequest>) -> Response {
    let rx = s
        .handle
        .propose_conf_change(ConfChangeOp::Remove, req.id, None, None)
        .await;
    match rx {
        None => redirect_to_leader(&s, "/cluster/remove").await,
        Some(rx) => match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(())) => StatusCode::OK.into_response(),
            Ok(Err(_)) => (StatusCode::SERVICE_UNAVAILABLE, "conf change dropped").into_response(),
            Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "conf change timeout").into_response(),
        },
    }
}

async fn await_commit(s: &AppState, command: Vec<u8>, redirect_path: &str) -> Response {
    let start = Instant::now();
    let rx = match s.handle.propose(command).await {
        None => return redirect_to_leader(s, redirect_path).await,
        Some(rx) => rx,
    };
    let resp = match tokio::time::timeout(Duration::from_secs(5), rx).await {
        Ok(Ok(())) => StatusCode::OK.into_response(),
        Ok(Err(_)) => (StatusCode::SERVICE_UNAVAILABLE, "proposal dropped").into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "proposal timeout").into_response(),
    };
    if resp.status() == StatusCode::OK {
        crate::metrics::WRITES_TOTAL.inc();
        crate::metrics::REQUEST_DURATION
            .with_label_values(&["write"])
            .observe(start.elapsed().as_secs_f64());
    }
    resp
}

async fn status(State(s): State<AppState>) -> impl IntoResponse {
    let leader = s.handle.leader_id().await;
    let is_leader = s.handle.is_leader().await;
    axum::Json(serde_json::json!({
        "is_leader": is_leader,
        "leader_id": leader,
    }))
}

async fn metrics_handler() -> impl IntoResponse {
    use prometheus::TextEncoder;
    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    match encoder.encode_to_string(&families) {
        Ok(body) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn redirect_to_leader(s: &AppState, path: &str) -> Response {
    if let Some(leader_id) = s.handle.leader_id().await {
        let addr = s.handle.http_peers.lock().await.get(&leader_id).cloned();
        if let Some(addr) = addr {
            return Redirect::temporary(&format!("http://{addr}{path}")).into_response();
        }
    }
    (StatusCode::SERVICE_UNAVAILABLE, "no leader").into_response()
}

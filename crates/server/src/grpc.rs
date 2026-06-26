/// gRPC server — handles incoming Raft RPCs from peer nodes.
use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use tonic::{Request, Response, Status, transport::Server};

use raft::{
    message::{
        AppendEntries, AppendEntriesResponse, LogEntry, NodeId, RequestVote,
        RequestVoteResponse,
    },
    Message,
};

use crate::node_handle::NodeHandle;

pub mod proto {
    tonic::include_proto!("raft");
}

use proto::{
    raft_service_server::{RaftService, RaftServiceServer},
    AppendEntriesRequest, AppendEntriesResponse as ProtoAEResponse, RequestVoteRequest,
    RequestVoteResponse as ProtoRVResponse,
};

struct RaftGrpc {
    handle: Arc<NodeHandle>,
}

#[tonic::async_trait]
impl RaftService for RaftGrpc {
    async fn request_vote(
        &self,
        req: Request<RequestVoteRequest>,
    ) -> Result<Response<ProtoRVResponse>, Status> {
        let r = req.into_inner();
        let msg = Message::RequestVote {
            from: r.candidate_id,
            msg: RequestVote {
                term: r.term,
                candidate_id: r.candidate_id,
                last_log_index: r.last_log_index,
                last_log_term: r.last_log_term,
            },
        };
        self.handle.step(msg).await;
        // The response is sent asynchronously via the peer client.
        // For now, return an empty ack — the real response goes through Raft.
        Ok(Response::new(ProtoRVResponse { term: 0, vote_granted: false }))
    }

    async fn append_entries(
        &self,
        req: Request<AppendEntriesRequest>,
    ) -> Result<Response<ProtoAEResponse>, Status> {
        let r = req.into_inner();
        let entries = r
            .entries
            .into_iter()
            .map(|e| LogEntry { index: e.index, term: e.term, command: e.command.to_vec() })
            .collect();
        let msg = Message::AppendEntries {
            from: r.leader_id,
            msg: AppendEntries {
                term: r.term,
                leader_id: r.leader_id,
                prev_log_index: r.prev_log_index,
                prev_log_term: r.prev_log_term,
                entries,
                leader_commit: r.leader_commit,
            },
        };
        self.handle.step(msg).await;
        Ok(Response::new(ProtoAEResponse { term: 0, success: true, match_index: 0 }))
    }
}

pub async fn server(handle: Arc<NodeHandle>) -> Result<()> {
    // Address is bound by main.rs; we receive a pre-bound listener there.
    // This function is a placeholder — wiring happens in main.
    Ok(())
}

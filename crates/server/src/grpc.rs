/// gRPC server — handles incoming Raft RPCs from peer nodes.
use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use tonic::{Request, Response, Status, transport::Server};

use raft::{
    message::{AppendEntries, InstallSnapshot, LogEntry, RequestVote, Snapshot},
    Message,
};

use crate::node_handle::NodeHandle;

pub mod proto {
    tonic::include_proto!("raft");
}

use proto::{
    raft_service_server::{RaftService, RaftServiceServer},
    AppendEntriesRequest, AppendEntriesResponse as ProtoAEResponse,
    InstallSnapshotRequest, InstallSnapshotResponse as ProtoISResponse,
    RequestVoteRequest, RequestVoteResponse as ProtoRVResponse,
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

        let response = self.handle.step_rpc(msg).await;

        let rv = match response {
            Some(Message::RequestVoteResponse { msg, .. }) => msg,
            _ => return Err(Status::internal("SM produced no RequestVoteResponse")),
        };

        Ok(Response::new(ProtoRVResponse { term: rv.term, vote_granted: rv.vote_granted }))
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

        let response = self.handle.step_rpc(msg).await;

        let ae = match response {
            Some(Message::AppendEntriesResponse { msg, .. }) => msg,
            _ => return Err(Status::internal("SM produced no AppendEntriesResponse")),
        };

        Ok(Response::new(ProtoAEResponse {
            term: ae.term,
            success: ae.success,
            match_index: ae.match_index,
        }))
    }

    async fn install_snapshot(
        &self,
        req: Request<InstallSnapshotRequest>,
    ) -> Result<Response<ProtoISResponse>, Status> {
        let r = req.into_inner();
        let msg = Message::InstallSnapshot {
            from: r.leader_id,
            msg: InstallSnapshot {
                term: r.term,
                leader_id: r.leader_id,
                snapshot: Snapshot {
                    last_index: r.last_index,
                    last_term: r.last_term,
                    data: r.data.to_vec(),
                },
            },
        };

        let response = self.handle.step_rpc(msg).await;

        let isr = match response {
            Some(Message::InstallSnapshotResponse { msg, .. }) => msg,
            _ => return Err(Status::internal("SM produced no InstallSnapshotResponse")),
        };

        Ok(Response::new(ProtoISResponse { term: isr.term, success: isr.success }))
    }
}

pub async fn server(handle: Arc<NodeHandle>, addr: SocketAddr) -> Result<()> {
    Server::builder()
        .add_service(RaftServiceServer::new(RaftGrpc { handle }))
        .serve(addr)
        .await?;
    Ok(())
}

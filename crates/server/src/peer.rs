/// gRPC client for sending Raft RPCs to a peer node.
use raft::{
    message::{
        AppendEntries, AppendEntriesResponse, NodeId, RequestVote, RequestVoteResponse,
    },
    Message,
};
use tracing::warn;

// Generated gRPC code
pub mod proto {
    tonic::include_proto!("raft");
}

use proto::raft_service_client::RaftServiceClient;

pub struct PeerClient {
    id: NodeId,
    addr: String,
}

impl PeerClient {
    pub fn new(id: NodeId, addr: String) -> Self {
        Self { id, addr: format!("http://{addr}") }
    }

    pub async fn send(&self, msg: Message) {
        match msg {
            Message::RequestVote { from, msg } => {
                self.send_request_vote(from, msg).await;
            }
            Message::AppendEntries { from, msg } => {
                self.send_append_entries(from, msg).await;
            }
            _ => {}
        }
    }

    async fn send_request_vote(&self, from: NodeId, msg: RequestVote) {
        let Ok(mut client) = RaftServiceClient::connect(self.addr.clone()).await else {
            warn!(peer = self.id, "failed to connect for RequestVote");
            return;
        };
        let req = proto::RequestVoteRequest {
            term: msg.term,
            candidate_id: msg.candidate_id,
            last_log_index: msg.last_log_index,
            last_log_term: msg.last_log_term,
        };
        if let Err(e) = client.request_vote(req).await {
            warn!(peer = self.id, "RequestVote RPC failed: {e}");
        }
    }

    async fn send_append_entries(&self, from: NodeId, msg: AppendEntries) {
        let Ok(mut client) = RaftServiceClient::connect(self.addr.clone()).await else {
            warn!(peer = self.id, "failed to connect for AppendEntries");
            return;
        };
        let entries = msg
            .entries
            .into_iter()
            .map(|e| proto::LogEntry {
                index: e.index,
                term: e.term,
                command: e.command.into(),
            })
            .collect();
        let req = proto::AppendEntriesRequest {
            term: msg.term,
            leader_id: msg.leader_id,
            prev_log_index: msg.prev_log_index,
            prev_log_term: msg.prev_log_term,
            entries,
            leader_commit: msg.leader_commit,
        };
        if let Err(e) = client.append_entries(req).await {
            warn!(peer = self.id, "AppendEntries RPC failed: {e}");
        }
    }
}

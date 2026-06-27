/// gRPC client for sending Raft RPCs to a peer node.
use raft::{
    message::{
        AppendEntries, AppendEntriesResponse, InstallSnapshotResponse, NodeId,
        RequestVote, RequestVoteResponse, Snapshot,
    },
    Message,
};
use tracing::warn;

pub mod proto {
    tonic::include_proto!("raft");
}

use proto::raft_service_client::RaftServiceClient;

#[derive(Clone)]
pub struct PeerClient {
    pub id: NodeId,
    /// Full HTTP URL: "http://host:port"
    pub addr: String,
}

impl PeerClient {
    pub fn new(id: NodeId, addr: String) -> Self {
        Self { id, addr: format!("http://{addr}") }
    }

    /// Send a Raft RPC to this peer and return the response as a Message,
    /// or None if the peer is unreachable or the message type needs no response.
    pub async fn send(&self, msg: Message) -> Option<Message> {
        match msg {
            Message::RequestVote { from, msg } => self.send_request_vote(from, msg).await,
            Message::AppendEntries { from, msg } => self.send_append_entries(from, msg).await,
            _ => None,
        }
    }

    pub async fn send_install_snapshot(&self, leader_term: u64, snapshot: Snapshot) -> Option<Message> {
        let Ok(mut client) = RaftServiceClient::connect(self.addr.clone()).await else {
            warn!(peer = self.id, "failed to connect for InstallSnapshot");
            return None;
        };
        let req = proto::InstallSnapshotRequest {
            term: leader_term,
            leader_id: self.id,
            last_index: snapshot.last_index,
            last_term: snapshot.last_term,
            data: snapshot.data.into(),
        };
        match client.install_snapshot(req).await {
            Ok(resp) => {
                let r = resp.into_inner();
                Some(Message::InstallSnapshotResponse {
                    from: self.id,
                    msg: InstallSnapshotResponse { term: r.term, success: r.success },
                })
            }
            Err(e) => {
                warn!(peer = self.id, "InstallSnapshot RPC failed: {e}");
                None
            }
        }
    }

    async fn send_request_vote(&self, _from: NodeId, msg: RequestVote) -> Option<Message> {
        let Ok(mut client) = RaftServiceClient::connect(self.addr.clone()).await else {
            warn!(peer = self.id, "failed to connect for RequestVote");
            return None;
        };
        let req = proto::RequestVoteRequest {
            term: msg.term,
            candidate_id: msg.candidate_id,
            last_log_index: msg.last_log_index,
            last_log_term: msg.last_log_term,
        };
        match client.request_vote(req).await {
            Ok(resp) => {
                let r = resp.into_inner();
                Some(Message::RequestVoteResponse {
                    from: self.id,
                    msg: RequestVoteResponse { term: r.term, vote_granted: r.vote_granted },
                })
            }
            Err(e) => {
                warn!(peer = self.id, "RequestVote RPC failed: {e}");
                None
            }
        }
    }

    async fn send_append_entries(&self, _from: NodeId, msg: AppendEntries) -> Option<Message> {
        let Ok(mut client) = RaftServiceClient::connect(self.addr.clone()).await else {
            warn!(peer = self.id, "failed to connect for AppendEntries");
            return None;
        };
        let entries = msg
            .entries
            .into_iter()
            .map(|e| proto::LogEntry { index: e.index, term: e.term, command: e.command.into() })
            .collect();
        let req = proto::AppendEntriesRequest {
            term: msg.term,
            leader_id: msg.leader_id,
            prev_log_index: msg.prev_log_index,
            prev_log_term: msg.prev_log_term,
            entries,
            leader_commit: msg.leader_commit,
        };
        match client.append_entries(req).await {
            Ok(resp) => {
                let r = resp.into_inner();
                Some(Message::AppendEntriesResponse {
                    from: self.id,
                    msg: AppendEntriesResponse {
                        term: r.term,
                        success: r.success,
                        match_index: r.match_index,
                    },
                })
            }
            Err(e) => {
                warn!(peer = self.id, "AppendEntries RPC failed: {e}");
                None
            }
        }
    }
}

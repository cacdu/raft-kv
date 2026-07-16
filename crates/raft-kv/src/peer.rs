/// gRPC client for sending Raft RPCs to a peer node.
///
/// Each peer gets one lazily-connected channel, created at registration:
/// tonic reconnects it transparently, and the connect/request timeouts
/// bound every RPC so a dead peer can never stall the caller. Liveness of
/// the cluster must not depend on the liveness of any single peer.
use std::time::Duration;

use raft::{
    message::{
        AppendEntries, AppendEntriesResponse, EntryType, InstallSnapshotResponse, NodeId,
        RequestVote, RequestVoteResponse, Snapshot,
    },
    Message,
};
use tonic::transport::{Channel, Endpoint};
use tracing::warn;

pub mod proto {
    tonic::include_proto!("raft");
}

use proto::raft_service_client::RaftServiceClient;

const CONNECT_TIMEOUT: Duration = Duration::from_millis(300);
/// Generous enough for an InstallSnapshot payload on a LAN.
const RPC_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct PeerClient {
    pub id: NodeId,
    channel: Channel,
}

impl PeerClient {
    pub fn new(id: NodeId, addr: String) -> Self {
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .expect("peer address must form a valid URI")
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .connect_lazy();
        Self { id, channel }
    }

    fn client(&self) -> RaftServiceClient<Channel> {
        RaftServiceClient::new(self.channel.clone())
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

    pub async fn send_install_snapshot(
        &self,
        leader_term: u64,
        snapshot: Snapshot,
    ) -> Option<Message> {
        let req = proto::InstallSnapshotRequest {
            term: leader_term,
            leader_id: self.id,
            last_index: snapshot.last_index,
            last_term: snapshot.last_term,
            data: snapshot.data,
        };
        match self.client().install_snapshot(req).await {
            Ok(resp) => {
                let r = resp.into_inner();
                Some(Message::InstallSnapshotResponse {
                    from: self.id,
                    msg: InstallSnapshotResponse {
                        term: r.term,
                        success: r.success,
                    },
                })
            }
            Err(e) => {
                warn!(peer = self.id, "InstallSnapshot RPC failed: {e}");
                None
            }
        }
    }

    async fn send_request_vote(&self, _from: NodeId, msg: RequestVote) -> Option<Message> {
        let req = proto::RequestVoteRequest {
            term: msg.term,
            candidate_id: msg.candidate_id,
            last_log_index: msg.last_log_index,
            last_log_term: msg.last_log_term,
        };
        match self.client().request_vote(req).await {
            Ok(resp) => {
                let r = resp.into_inner();
                Some(Message::RequestVoteResponse {
                    from: self.id,
                    msg: RequestVoteResponse {
                        term: r.term,
                        vote_granted: r.vote_granted,
                    },
                })
            }
            Err(e) => {
                warn!(peer = self.id, "RequestVote RPC failed: {e}");
                None
            }
        }
    }

    async fn send_append_entries(&self, _from: NodeId, msg: AppendEntries) -> Option<Message> {
        let entries = msg
            .entries
            .into_iter()
            .map(|e| proto::LogEntry {
                index: e.index,
                term: e.term,
                command: e.command,
                entry_type: if e.entry_type == EntryType::ConfChange {
                    1
                } else {
                    0
                },
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
        match self.client().append_entries(req).await {
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

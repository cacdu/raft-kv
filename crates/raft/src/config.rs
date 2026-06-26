use crate::message::NodeId;

#[derive(Debug, Clone)]
pub struct Config {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    /// Ticks before a follower starts an election. Randomized between
    /// [election_timeout, 2 * election_timeout] to avoid split votes.
    pub election_timeout: u32,
    /// Ticks between leader heartbeats. Must be << election_timeout.
    pub heartbeat_timeout: u32,
    /// Maximum entries sent in a single AppendEntries RPC.
    pub max_entries_per_append: usize,
}

impl Config {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            id,
            peers,
            election_timeout: 10,
            heartbeat_timeout: 3,
            max_entries_per_append: 100,
        }
    }
}

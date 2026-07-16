/// A change applied to the replicated KV store on this node.
///
/// Delivered to [`crate::RaftKv::subscribe`] receivers in apply order —
/// the same total order on every node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Set {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    /// The entire store was replaced by a snapshot from the leader
    /// (this node was lagging too far to catch up entry-by-entry).
    /// Subscribers must re-read any state derived from the store.
    SnapshotApplied,
}

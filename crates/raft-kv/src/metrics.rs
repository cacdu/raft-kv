use std::sync::LazyLock;

use prometheus::{
    register_counter, register_gauge, register_histogram_vec, Counter, Gauge, HistogramVec,
};

pub static WRITES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    register_counter!(
        "raft_kv_writes_total",
        "Total committed write operations (PUT/DELETE)"
    )
    .unwrap()
});

pub static READS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    register_counter!(
        "raft_kv_reads_total",
        "Total read operations served by the leader"
    )
    .unwrap()
});

pub static APPLIED_INDEX: LazyLock<Gauge> = LazyLock::new(|| {
    register_gauge!(
        "raft_kv_applied_index",
        "Last log index applied to the KV store"
    )
    .unwrap()
});

pub static COMMIT_INDEX: LazyLock<Gauge> =
    LazyLock::new(|| register_gauge!("raft_kv_commit_index", "Current Raft commit index").unwrap());

pub static CURRENT_TERM: LazyLock<Gauge> =
    LazyLock::new(|| register_gauge!("raft_kv_current_term", "Current Raft term").unwrap());

pub static IS_LEADER: LazyLock<Gauge> = LazyLock::new(|| {
    register_gauge!("raft_kv_is_leader", "1 if this node is leader, 0 otherwise").unwrap()
});

pub static REQUEST_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "raft_kv_request_duration_seconds",
        "HTTP request latency in seconds",
        &["operation"],
        vec![0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0]
    )
    .unwrap()
});

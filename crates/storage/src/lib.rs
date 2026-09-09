pub mod kv;
pub mod snapshot;
pub mod wal;

pub use kv::KvStore;
pub use snapshot::SnapshotStore;
pub use wal::Wal;

/// fsync a directory so a `rename` inside it is itself durable. Renaming a
/// temp file over a live one is only atomic once the directory entry has
/// reached the platter; without this a crash can leave neither name.
pub(crate) fn sync_dir(dir: &std::path::Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

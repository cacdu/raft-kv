/// State-machine snapshots, one file each, outside the WAL.
///
/// 0.1.x appended the whole serialized store into the WAL as a
/// `WalRecord::Snapshot` every time it compacted. The WAL is append-only and
/// never rotated, so the file only ever grew: ~988 snapshots of a ~780 KB board
/// filled a 1 GB volume, the last append was cut short, and the node was
/// bricked. Snapshots belong in their own files so the WAL can be rewritten
/// down to the entries that are not covered by one.
///
/// File layout (little-endian):
///   [8 bytes]  `RKVSNAP\x01`
///   [8 bytes]  last_index
///   [8 bytes]  last_term
///   [4 bytes]  CRC32 of the data
///   [8 bytes]  data length
///   [N bytes]  data
use std::{
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use crc32fast::Hasher;
use raft::message::{LogIndex, Snapshot, Term};
use thiserror::Error;
use tracing::warn;

const SNAPSHOT_MAGIC: [u8; 8] = *b"RKVSNAP\x01";
const HEADER_LEN: u64 = 8 + 8 + 8 + 4 + 8;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("i/o: {0}")]
    Io(#[from] io::Error),
    #[error("corrupt snapshot {path}: {reason}")]
    Corrupt { path: String, reason: &'static str },
}

/// The `snapshot-{index}.bin` files under one data directory.
#[derive(Debug, Clone)]
pub struct SnapshotStore {
    dir: PathBuf,
}

impl SnapshotStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// Write `snapshot` durably: a temp file, an fsync, a rename, and an fsync
    /// of the directory. A reader either sees the previous snapshot or this
    /// one, never a half-written file — which is exactly what the old
    /// append-into-the-WAL scheme could not promise.
    pub fn save(&self, snapshot: &Snapshot) -> Result<(), SnapshotError> {
        fs::create_dir_all(&self.dir)?;
        let path = self.path_for(snapshot.last_index);
        let tmp = path.with_extension("bin.tmp");

        {
            let mut file = fs::File::create(&tmp)?;
            let mut hasher = Hasher::new();
            hasher.update(&snapshot.data);
            file.write_all(&SNAPSHOT_MAGIC)?;
            file.write_all(&snapshot.last_index.to_le_bytes())?;
            file.write_all(&snapshot.last_term.to_le_bytes())?;
            file.write_all(&hasher.finalize().to_le_bytes())?;
            file.write_all(&(snapshot.data.len() as u64).to_le_bytes())?;
            file.write_all(&snapshot.data)?;
            file.sync_all()?;
        }

        fs::rename(&tmp, &path)?;
        crate::sync_dir(&self.dir)?;
        Ok(())
    }

    /// The newest readable snapshot. A file that fails its checksum is skipped
    /// with a warning rather than failing the whole load: an older snapshot
    /// plus the WAL still restores the node, while refusing to start does not.
    pub fn load_latest(&self) -> Result<Option<Snapshot>, SnapshotError> {
        for (index, path) in self.list()? {
            match Self::read(&path) {
                Ok(snapshot) => return Ok(Some(snapshot)),
                Err(e) => warn!(index, path = %path.display(), "unreadable snapshot: {e}"),
            }
        }
        Ok(None)
    }

    /// Keep the `keep` newest snapshots and delete the rest, along with any
    /// temp file left behind by a crash mid-save.
    pub fn prune(&self, keep: usize) -> Result<(), SnapshotError> {
        for (_, path) in self.list()?.into_iter().skip(keep) {
            if let Err(e) = fs::remove_file(&path) {
                warn!(path = %path.display(), "could not remove old snapshot: {e}");
            }
        }
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return Ok(());
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "tmp") {
                let _ = fs::remove_file(&path);
            }
        }
        Ok(())
    }

    /// The index of the newest snapshot on disk, without reading its data.
    pub fn latest_index(&self) -> Result<Option<LogIndex>, SnapshotError> {
        Ok(self.list()?.first().map(|(index, _)| *index))
    }

    fn path_for(&self, index: LogIndex) -> PathBuf {
        self.dir.join(format!("snapshot-{index}.bin"))
    }

    /// Every `snapshot-{index}.bin` in the directory, newest first.
    fn list(&self) -> Result<Vec<(LogIndex, PathBuf)>, SnapshotError> {
        let entries = match fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let mut found: Vec<(LogIndex, PathBuf)> = entries
            .flatten()
            .filter_map(|entry| {
                let path = entry.path();
                let name = path.file_name()?.to_str()?;
                let index = name.strip_prefix("snapshot-")?.strip_suffix(".bin")?;
                Some((index.parse().ok()?, path))
            })
            .collect();
        found.sort_unstable_by_key(|(index, _)| std::cmp::Reverse(*index));
        Ok(found)
    }

    fn read(path: &Path) -> Result<Snapshot, SnapshotError> {
        let corrupt = |reason| SnapshotError::Corrupt {
            path: path.display().to_string(),
            reason,
        };

        let file_len = fs::metadata(path)?.len();
        if file_len < HEADER_LEN {
            return Err(corrupt("shorter than its header"));
        }
        let mut file = io::BufReader::new(fs::File::open(path)?);

        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if magic != SNAPSHOT_MAGIC {
            return Err(corrupt("bad magic"));
        }
        let last_index = read_u64(&mut file)?;
        let last_term = read_u64(&mut file)?;
        let mut crc_buf = [0u8; 4];
        file.read_exact(&mut crc_buf)?;
        let expected_crc = u32::from_le_bytes(crc_buf);
        let data_len = read_u64(&mut file)?;

        // Size the allocation from the file, never from the number in it: a
        // torn header would otherwise ask for up to 16 EB.
        if data_len != file_len - HEADER_LEN {
            return Err(corrupt("length does not match the file"));
        }
        let mut data = vec![0u8; data_len as usize];
        file.read_exact(&mut data)?;

        let mut hasher = Hasher::new();
        hasher.update(&data);
        if hasher.finalize() != expected_crc {
            return Err(corrupt("checksum mismatch"));
        }

        Ok(Snapshot {
            last_index: last_index as LogIndex,
            last_term: last_term as Term,
            data,
        })
    }
}

fn read_u64<R: Read>(reader: &mut R) -> Result<u64, SnapshotError> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(last_index: LogIndex, data: &[u8]) -> Snapshot {
        Snapshot {
            last_index,
            last_term: 3,
            data: data.to_vec(),
        }
    }

    #[test]
    fn a_saved_snapshot_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        assert!(
            store.load_latest().unwrap().is_none(),
            "empty dir, no snapshot"
        );

        store.save(&snapshot(50, b"{\"px:1:1\":\"7\"}")).unwrap();
        let loaded = store
            .load_latest()
            .unwrap()
            .expect("the snapshot must load");
        assert_eq!(loaded.last_index, 50);
        assert_eq!(loaded.last_term, 3);
        assert_eq!(loaded.data, b"{\"px:1:1\":\"7\"}");
        assert_eq!(store.latest_index().unwrap(), Some(50));
    }

    #[test]
    fn the_newest_snapshot_wins_and_a_damaged_one_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        store.save(&snapshot(50, b"older")).unwrap();
        store.save(&snapshot(100, b"newer")).unwrap();
        assert_eq!(store.load_latest().unwrap().unwrap().data, b"newer");

        // Corrupt the newest: an older snapshot plus the WAL still restores the
        // node, so this must degrade rather than refuse to start.
        let path = dir.path().join("snapshot-100.bin");
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();

        assert_eq!(store.load_latest().unwrap().unwrap().data, b"older");
    }

    #[test]
    fn prune_keeps_the_newest_and_sweeps_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        for index in [10u64, 20, 30, 40] {
            store.save(&snapshot(index, b"x")).unwrap();
        }
        std::fs::write(dir.path().join("snapshot-99.bin.tmp"), b"interrupted").unwrap();

        store.prune(2).unwrap();

        let mut left: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(left, vec!["snapshot-30.bin", "snapshot-40.bin"]);
    }
}

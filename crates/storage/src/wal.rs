/// Write-Ahead Log for Raft log entries and hard state.
///
/// Record format (binary, little-endian):
///   [4 bytes] payload length (u32)
///   [4 bytes] CRC32 checksum of payload
///   [N bytes] JSON-encoded WalRecord
///
/// On recovery, records with invalid checksums are discarded (truncated log).
use std::{
    io::{self, Read, Write},
    path::Path,
};

use crc32fast::Hasher;
use raft::message::{LogEntry, LogIndex, NodeId, Term};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("i/o: {0}")]
    Io(#[from] io::Error),
    #[error("corrupt record at offset {offset}: {reason}")]
    Corrupt { offset: u64, reason: &'static str },
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WalRecord {
    HardState {
        term: Term,
        voted_for: Option<NodeId>,
    },
    Entry(LogEntry),
    Snapshot {
        last_index: LogIndex,
        last_term: Term,
    },
}

pub struct Wal {
    file: std::fs::File,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, Vec<WalRecord>), WalError> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;

        let records = Self::read_all(&mut file)?;
        Ok((Self { file }, records))
    }

    /// Append a single record and fsync it to disk before returning.
    pub fn append(&mut self, record: &WalRecord) -> Result<(), WalError> {
        self.write_record(record)?;
        self.sync()
    }

    /// Append several records with a single fsync covering all of them.
    /// Raft only needs durability before the RPC response leaves the node,
    /// so one fsync can amortize a HardState plus a batch of entries.
    pub fn append_batch(&mut self, records: &[WalRecord]) -> Result<(), WalError> {
        for record in records {
            self.write_record(record)?;
        }
        self.sync()
    }

    fn write_record(&mut self, record: &WalRecord) -> Result<(), WalError> {
        let payload = serde_json::to_vec(record)?;
        let checksum = {
            let mut h = Hasher::new();
            h.update(&payload);
            h.finalize()
        };

        let len = payload.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&checksum.to_le_bytes())?;
        self.file.write_all(&payload)?;
        Ok(())
    }

    /// Force written records to durable storage. `File::flush` is a no-op for
    /// `std::fs::File` — the bytes sit in the kernel page cache. Raft safety
    /// depends on a granted vote or an acked entry surviving power loss, and
    /// only fdatasync provides that guarantee.
    fn sync(&mut self) -> Result<(), WalError> {
        self.file.sync_data()?;
        Ok(())
    }

    fn read_all(file: &mut std::fs::File) -> Result<Vec<WalRecord>, WalError> {
        let mut records = Vec::new();
        let mut offset: u64 = 0;

        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut crc_buf = [0u8; 4];
            file.read_exact(&mut crc_buf)?;
            let expected_crc = u32::from_le_bytes(crc_buf);

            let mut payload = vec![0u8; len];
            file.read_exact(&mut payload)?;

            let mut h = Hasher::new();
            h.update(&payload);
            if h.finalize() != expected_crc {
                return Err(WalError::Corrupt {
                    offset,
                    reason: "checksum mismatch",
                });
            }

            records.push(serde_json::from_slice(&payload)?);
            offset += 4 + 4 + len as u64;
        }

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::message::{EntryType, LogEntry};

    // ── BUG 2: WAL appends must be crash-durable, and batched ─────────────────
    //
    // The old `append` called `File::flush()` — a no-op for std::fs::File: the
    // record reaches the OS page cache but not the platter. Raft safety requires
    // HardState and log entries to be durable *before* an RPC is answered; a power
    // loss between the write and the (missing) fsync can make a node vote twice in
    // one term → split brain. The fix persists a whole Ready's records via
    // `append_batch`, fsyncing once before returning.
    //
    // NOTE: true crash-durability (surviving a power cut between write and fsync)
    // cannot be observed by an in-process test — it needs a fault-injection harness
    // that kills the process and reopens the file. This test pins the round-trip
    // contract of the batch API the fix introduces; the fsync itself is verified by
    // inspection of `append_batch`.
    #[test]
    fn append_batch_round_trips_all_records() {
        let tmp = tempfile::NamedTempFile::new().unwrap();

        let batch = vec![
            WalRecord::HardState {
                term: 7,
                voted_for: Some(2),
            },
            WalRecord::Entry(LogEntry {
                index: 1,
                term: 7,
                entry_type: EntryType::Normal,
                command: b"set a 1".to_vec(),
            }),
            WalRecord::Entry(LogEntry {
                index: 2,
                term: 7,
                entry_type: EntryType::Normal,
                command: b"set b 2".to_vec(),
            }),
        ];

        {
            let (mut wal, existing) = Wal::open(tmp.path()).unwrap();
            assert!(existing.is_empty(), "a fresh WAL starts empty");
            wal.append_batch(&batch).unwrap();
        } // drop the handle to be sure nothing lingers only in this process

        // Reopen from disk: every record must be recovered, in order.
        let (_wal, recovered) = Wal::open(tmp.path()).unwrap();
        assert_eq!(
            recovered.len(),
            3,
            "all batched records must survive a reopen"
        );
        assert!(
            matches!(
                recovered[0],
                WalRecord::HardState {
                    term: 7,
                    voted_for: Some(2)
                }
            ),
            "HardState must round-trip"
        );
        match &recovered[1] {
            WalRecord::Entry(e) => {
                assert_eq!(e.index, 1);
                assert_eq!(e.term, 7);
            }
            other => panic!("expected an Entry at position 1, got {other:?}"),
        }
    }
}

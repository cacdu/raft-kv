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

    pub fn append(&mut self, record: &WalRecord) -> Result<(), WalError> {
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
        self.file.flush()?;
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

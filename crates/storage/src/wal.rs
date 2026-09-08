/// Write-Ahead Log for Raft log entries and hard state.
///
/// Record format (binary, little-endian):
///   [4 bytes] payload length (u32)
///   [4 bytes] CRC32 checksum of payload
///   [N bytes] JSON-encoded WalRecord
///
/// On recovery, records with invalid checksums are discarded (truncated log).
use std::{
    io::{self, BufReader, Read, Write},
    path::Path,
};

use crc32fast::Hasher;
use raft::message::{LogEntry, LogIndex, NodeId, Term};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

/// Upper bound on a single record's payload. The length prefix comes off disk
/// unvalidated, so a torn header can name any u32: allocating from it would ask
/// for up to 4 GB on a 256 MB machine. Anything above this is not a record that
/// this code ever wrote — it is garbage in a torn tail.
const MAX_RECORD_LEN: usize = 64 * 1024 * 1024;

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
        /// Serialized state machine at `last_index`. Compaction discards the
        /// entries it covers, so without this the KV store cannot be rebuilt
        /// on restart. `default` keeps pre-0.1.1 WALs readable.
        #[serde(default)]
        data: Vec<u8>,
    },
}

pub struct Wal {
    file: std::fs::File,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, Vec<WalRecord>), WalError> {
        let path = path.as_ref();
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;

        let file_len = file.metadata()?.len();
        let (records, valid_len) = Self::read_all(&mut file, file_len)?;

        // A record that was never fsynced was never acknowledged to anyone, so
        // dropping the tail is safe — and it is the only way the node starts at
        // all after a crash mid-append or a volume that filled up. Truncate
        // before the first append, or the next record would be written behind
        // the garbage and the file would never recover.
        if valid_len < file_len {
            warn!(
                path = %path.display(),
                discarded_bytes = file_len - valid_len,
                valid_bytes = valid_len,
                "discarding a torn WAL tail"
            );
            file.set_len(valid_len)?;
            file.sync_all()?;
        }

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

    /// Replay every intact record and return them together with the offset just
    /// past the last one. Replay stops at the first record that is short,
    /// implausible or checksum-invalid: that is a torn tail, which the caller
    /// truncates away.
    ///
    /// A checksum failure on a record that is *not* last is a different animal —
    /// bit rot or a bug, not an interrupted append — and still surfaces as
    /// `Corrupt`, because silently dropping the rest of the log would hide it.
    fn read_all(
        file: &mut std::fs::File,
        file_len: u64,
    ) -> Result<(Vec<WalRecord>, u64), WalError> {
        let mut reader = BufReader::new(&*file);
        let mut records = Vec::new();
        let mut offset: u64 = 0;

        loop {
            let mut len_buf = [0u8; 4];
            if !read_or_eof(&mut reader, &mut len_buf)? {
                break;
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            if len > MAX_RECORD_LEN {
                warn!(
                    offset,
                    len, "implausible WAL record length; treating it as a torn tail"
                );
                break;
            }

            let mut crc_buf = [0u8; 4];
            if !read_or_eof(&mut reader, &mut crc_buf)? {
                break;
            }
            let expected_crc = u32::from_le_bytes(crc_buf);

            let mut payload = vec![0u8; len];
            if !read_or_eof(&mut reader, &mut payload)? {
                break;
            }

            let mut h = Hasher::new();
            h.update(&payload);
            if h.finalize() != expected_crc {
                // The frame is complete, so this was not an interrupted write.
                // If anything follows it, the damage is mid-file: report it.
                if offset + RECORD_HEADER_LEN + (len as u64) < file_len {
                    return Err(WalError::Corrupt {
                        offset,
                        reason: "checksum mismatch",
                    });
                }
                warn!(offset, "discarding the final WAL record: bad checksum");
                break;
            }

            records.push(serde_json::from_slice(&payload)?);
            offset += RECORD_HEADER_LEN + len as u64;
        }

        Ok((records, offset))
    }
}

/// Length prefix + checksum that precede every payload.
const RECORD_HEADER_LEN: u64 = 8;

/// `read_exact`, but a short read reports `false` instead of erroring: at the
/// end of a WAL that is exactly how a crash mid-append looks.
fn read_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<bool, WalError> {
    match reader.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e.into()),
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

    // ── BUG 4: a torn WAL tail must be discarded, not brick the node ──────────
    //
    // node2 crash-looped in production on `failed to fill whole buffer` — the
    // io::Error text for an UnexpectedEof inside `read_exact`. Its volume filled
    // up and the last append was cut short, but only an EOF on the 4-byte length
    // header counted as a clean end: a short read on the checksum or the payload
    // propagated as WalError::Io and `Wal::open` refused to start the node. A
    // record that was never fsynced was never acknowledged to anyone, so the
    // tail is safe to drop — which is exactly what the module doc always
    // promised and never did.

    use std::path::Path;

    /// A WAL holding HardState + Entry(1) + Entry(2). Returns the file, the
    /// length after the first two records, and the total length: the third
    /// record occupies `[two, total)`, so truncating to `two + k` cuts it at a
    /// chosen boundary.
    fn wal_with_three_records() -> (tempfile::NamedTempFile, u64, u64) {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut wal = Wal::open(tmp.path()).unwrap().0;
        wal.append(&WalRecord::HardState {
            term: 7,
            voted_for: Some(2),
        })
        .unwrap();
        wal.append(&WalRecord::Entry(LogEntry {
            index: 1,
            term: 7,
            entry_type: EntryType::Normal,
            command: b"set a 1".to_vec(),
        }))
        .unwrap();
        let two = tmp.path().metadata().unwrap().len();
        wal.append(&WalRecord::Entry(LogEntry {
            index: 2,
            term: 7,
            entry_type: EntryType::Normal,
            command: b"set b 2".to_vec(),
        }))
        .unwrap();
        let total = tmp.path().metadata().unwrap().len();
        (tmp, two, total)
    }

    fn truncate_to(path: &Path, len: u64) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(len)
            .unwrap();
    }

    fn flip_byte(path: &Path, at: usize) {
        let mut bytes = std::fs::read(path).unwrap();
        bytes[at] ^= 0xFF;
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn a_tail_cut_at_any_boundary_is_dropped_and_the_wal_keeps_working() {
        // 2 bytes into the length header, 2 into the checksum, 2 into the
        // payload: the three ways an append can be interrupted.
        for cut in [2u64, 6, 10] {
            let (tmp, two, total) = wal_with_three_records();
            assert!(two + cut < total, "the cut must land inside record 3");
            truncate_to(tmp.path(), two + cut);

            let (mut wal, records) = Wal::open(tmp.path())
                .unwrap_or_else(|e| panic!("a tail cut at +{cut} must not brick the node: {e}"));
            assert_eq!(records.len(), 2, "every record before the tear survives");
            assert_eq!(
                tmp.path().metadata().unwrap().len(),
                two,
                "the file is truncated back to the last intact record"
            );

            // And the recovered WAL must be writable again: had the torn bytes
            // stayed, this record would be appended behind garbage and the file
            // would never replay past it.
            wal.append(&WalRecord::Entry(LogEntry {
                index: 3,
                term: 7,
                entry_type: EntryType::Normal,
                command: b"set c 3".to_vec(),
            }))
            .unwrap();
            let (_wal, records) = Wal::open(tmp.path()).unwrap();
            assert_eq!(records.len(), 3, "the post-recovery append round-trips");
            match &records[2] {
                WalRecord::Entry(e) => assert_eq!(e.index, 3),
                other => panic!("expected Entry(3), got {other:?}"),
            }
        }
    }

    #[test]
    fn bad_checksum_in_the_final_record_is_discarded() {
        // The module doc has always promised this; it was never implemented.
        let (tmp, two, _total) = wal_with_three_records();
        flip_byte(tmp.path(), two as usize + RECORD_HEADER_LEN as usize);

        let (_wal, records) = Wal::open(tmp.path()).unwrap();
        assert_eq!(records.len(), 2, "the damaged final record is dropped");
        assert_eq!(tmp.path().metadata().unwrap().len(), two);
    }

    #[test]
    fn bad_checksum_in_the_middle_is_reported() {
        // Not an interrupted append: a complete frame with records after it.
        // Dropping the rest of the log here would hide bit rot (or a bug).
        let (tmp, _two, _total) = wal_with_three_records();
        flip_byte(tmp.path(), RECORD_HEADER_LEN as usize);

        match Wal::open(tmp.path()) {
            Err(WalError::Corrupt { offset, reason }) => {
                assert_eq!(offset, 0);
                assert_eq!(reason, "checksum mismatch");
            }
            Err(e) => panic!("expected Corrupt, got {e}"),
            Ok(_) => panic!("mid-file corruption must not be swallowed"),
        }
    }

    #[test]
    fn an_implausible_length_header_is_a_torn_tail_not_a_4gb_allocation() {
        // `vec![0u8; len]` used to size an allocation straight from a u32 read
        // off disk. On a 256 MB machine a torn header could ask for 4 GB.
        let (tmp, two, _total) = wal_with_three_records();
        truncate_to(tmp.path(), two);
        let mut bytes = std::fs::read(tmp.path()).unwrap();
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(tmp.path(), bytes).unwrap();

        let (_wal, records) = Wal::open(tmp.path()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(
            tmp.path().metadata().unwrap().len(),
            two,
            "the bogus header is truncated away"
        );
    }
}

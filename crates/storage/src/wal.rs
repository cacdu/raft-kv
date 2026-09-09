/// Write-Ahead Log for Raft log entries and hard state.
///
/// File layout:
///   [8 bytes] `RKVWAL\x02\n` — format marker, v2 files only
///   then a sequence of records:
///     [4 bytes] payload length (u32, little-endian)
///     [4 bytes] CRC32 checksum of payload
///     [N bytes] encoded WalRecord
///
/// A file with no marker is a 0.1.x WAL whose payloads are JSON; v2 payloads
/// are postcard. The codec is decided once at open time and every append uses
/// it, so a single file never mixes the two. Legacy files migrate on the first
/// rotation.
///
/// On recovery, a record that is short, implausibly sized or checksum-invalid
/// ends the replay: that is a torn tail, and it is truncated away.
use std::{
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
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

/// Marks a file whose records are postcard-encoded. Absent in 0.1.x WALs.
const WAL_MAGIC_V2: [u8; 8] = *b"RKVWAL\x02\n";

/// Length prefix + checksum that precede every payload.
const RECORD_HEADER_LEN: u64 = 8;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("i/o: {0}")]
    Io(#[from] io::Error),
    #[error("corrupt record at offset {offset}: {reason}")]
    Corrupt { offset: u64, reason: &'static str },
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("postcard: {0}")]
    Postcard(#[from] postcard::Error),
}

/// How the payloads in one file are encoded.
///
/// The WAL never leaves the node — unlike `Snapshot::data`, which travels over
/// InstallSnapshot and therefore stays JSON — so its encoding is free to change
/// as long as existing files keep replaying.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Codec {
    /// 0.1.x: `serde_json`. Every byte of a payload costs a decimal number and
    /// a comma inside another JSON document — a ~3.5x amplification on a store
    /// whose entries *are* bytes.
    LegacyJson,
    /// 0.1.4+: `postcard`. Varints shrink the log indices too.
    PostcardV2,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WalRecord {
    HardState {
        term: Term,
        voted_for: Option<NodeId>,
        /// See `raft::HardState::commit`. `default` keeps 0.1.x WALs readable.
        #[serde(default)]
        commit: LogIndex,
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
    codec: Codec,
    path: PathBuf,
}

impl Wal {
    /// Replay into a `Vec`. Convenient for tests and small logs; a node with a
    /// 0.1.x WAL should use [`Wal::open_with`] instead, which never holds the
    /// whole file at once.
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, Vec<WalRecord>), WalError> {
        let mut records = Vec::new();
        let wal = Self::open_with(path, |record| records.push(record))?;
        Ok((wal, records))
    }

    /// Replay the file record by record, handing each one to `visit`.
    ///
    /// Streaming is not a micro-optimization here: a 0.1.x WAL holds every
    /// snapshot ever appended — ~988 of them at ~2.4 MB each in gambas —
    /// and materializing them all was an OOM path of its own on a 256 MB
    /// machine. The caller folds as it goes and keeps only what it needs.
    pub fn open_with(
        path: impl AsRef<Path>,
        mut visit: impl FnMut(WalRecord),
    ) -> Result<Self, WalError> {
        let path = path.as_ref();
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;

        let file_len = file.metadata()?.len();
        let (mut codec, data_start) = Self::detect_codec(&mut file, file_len)?;
        let valid_len = Self::replay(&mut file, data_start, file_len, codec, &mut visit)?;

        // A record that was never fsynced was never acknowledged to anyone, so
        // dropping the tail is safe — and it is the only way the node starts at
        // all after a crash mid-append or a volume that filled up. Truncate
        // before the first append, or the next record would be written behind
        // the garbage and the file would never replay past it.
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

        // Nothing legible survived in a legacy file: there is no history left
        // to stay compatible with, so start it over as v2 rather than writing
        // JSON forever.
        if codec == Codec::LegacyJson && valid_len == 0 {
            file.write_all(&WAL_MAGIC_V2)?;
            file.sync_data()?;
            codec = Codec::PostcardV2;
        }

        Ok(Self {
            file,
            codec,
            path: path.to_path_buf(),
        })
    }

    /// True for a 0.1.x file: no format marker, JSON payloads. Such a file is
    /// still appended in its own codec — the golden copy must not find itself
    /// half rewritten — so the caller rotates it to migrate.
    pub fn is_legacy(&self) -> bool {
        self.codec == Codec::LegacyJson
    }

    /// Replace the file's contents with `records`, atomically and always as v2.
    ///
    /// This is what bounds the WAL. Compaction folds every entry up to the
    /// snapshot into a snapshot file, and those entries are then dead weight:
    /// rewriting drops them instead of letting the file grow forever. The live
    /// file is never truncated in place — a temp file, an fsync, a rename and
    /// an fsync of the directory mean a crash leaves either the old contents or
    /// the new ones.
    ///
    /// `records` must still carry the durable hard state: a node that comes
    /// back without its term and vote can vote twice in one term.
    pub fn rewrite(&mut self, records: &[WalRecord]) -> Result<(), WalError> {
        let tmp = self.path.with_extension("wal.tmp");
        {
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&WAL_MAGIC_V2)?;
            for record in records {
                write_frame(&mut file, Codec::PostcardV2, record)?;
            }
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        if let Some(dir) = self.path.parent() {
            crate::sync_dir(dir)?;
        }

        // The old handle still points at the replaced inode; every later append
        // has to land in the file that now carries the name.
        self.file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        self.codec = Codec::PostcardV2;
        Ok(())
    }

    /// Decide the on-disk codec once, and return the offset where records
    /// start. An empty file gets the v2 marker written now.
    fn detect_codec(file: &mut std::fs::File, file_len: u64) -> Result<(Codec, u64), WalError> {
        if file_len == 0 {
            file.write_all(&WAL_MAGIC_V2)?;
            file.sync_data()?;
            return Ok((Codec::PostcardV2, WAL_MAGIC_V2.len() as u64));
        }
        if file_len >= WAL_MAGIC_V2.len() as u64 {
            let mut magic = [0u8; WAL_MAGIC_V2.len()];
            file.read_exact(&mut magic)?;
            if magic == WAL_MAGIC_V2 {
                return Ok((Codec::PostcardV2, WAL_MAGIC_V2.len() as u64));
            }
        }
        Ok((Codec::LegacyJson, 0))
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
        write_frame(&mut self.file, self.codec, record)
    }

    /// Force written records to durable storage. `File::flush` is a no-op for
    /// `std::fs::File` — the bytes sit in the kernel page cache. Raft safety
    /// depends on a granted vote or an acked entry surviving power loss, and
    /// only fdatasync provides that guarantee.
    fn sync(&mut self) -> Result<(), WalError> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Hand every intact record to `visit` and return the offset just past the
    /// last one. Replay stops at the first record that is short, implausible or
    /// checksum-invalid: that is a torn tail, which the caller truncates away.
    ///
    /// A checksum failure on a record that is *not* last is a different animal —
    /// bit rot or a bug, not an interrupted append — and still surfaces as
    /// `Corrupt`, because silently dropping the rest of the log would hide it.
    fn replay(
        file: &mut std::fs::File,
        start: u64,
        file_len: u64,
        codec: Codec,
        visit: &mut impl FnMut(WalRecord),
    ) -> Result<u64, WalError> {
        file.seek(SeekFrom::Start(start))?;
        let mut reader = BufReader::new(&*file);
        let mut offset = start;

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

            visit(decode(codec, &payload)?);
            offset += RECORD_HEADER_LEN + len as u64;
        }

        Ok(offset)
    }
}

/// Frame one record: length, checksum, payload.
fn write_frame<W: Write>(writer: &mut W, codec: Codec, record: &WalRecord) -> Result<(), WalError> {
    let payload = encode(codec, record)?;
    let checksum = {
        let mut h = Hasher::new();
        h.update(&payload);
        h.finalize()
    };
    writer.write_all(&(payload.len() as u32).to_le_bytes())?;
    writer.write_all(&checksum.to_le_bytes())?;
    writer.write_all(&payload)?;
    Ok(())
}

fn encode(codec: Codec, record: &WalRecord) -> Result<Vec<u8>, WalError> {
    match codec {
        Codec::LegacyJson => Ok(serde_json::to_vec(record)?),
        Codec::PostcardV2 => Ok(postcard::to_stdvec(record)?),
    }
}

fn decode(codec: Codec, payload: &[u8]) -> Result<WalRecord, WalError> {
    match codec {
        Codec::LegacyJson => Ok(serde_json::from_slice(payload)?),
        Codec::PostcardV2 => Ok(postcard::from_bytes(payload)?),
    }
}

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
                commit: 0,
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
                    voted_for: Some(2),
                    commit: 0,
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
            commit: 0,
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
        let first_record = WAL_MAGIC_V2.len() as u64;
        flip_byte(tmp.path(), (first_record + RECORD_HEADER_LEN) as usize);

        match Wal::open(tmp.path()) {
            Err(WalError::Corrupt { offset, reason }) => {
                assert_eq!(offset, first_record);
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

    // ── The WAL codec: ~3.5x amplification from double JSON encoding ─────────
    //
    // Every record was `serde_json::to_vec`'d, so a payload that is *already*
    // bytes — LogEntry::command, Snapshot::data — was re-encoded one decimal
    // number and comma per byte, inside another JSON document. A ~60-byte pixel
    // command cost ~230 bytes on disk; the ~780 KB serialized board cost ~1.2 MB
    // per snapshot, times ~988 snapshots, on a 1 GB volume.
    //
    // `serde_bytes` alone does not fix this: serde_json's `serialize_bytes`
    // defaults to writing a JSON array of integers, byte for byte identical to
    // the seq encoding. The fix has to be the codec. Since the WAL never leaves
    // the node, changing it is safe — unlike `Snapshot::data`, which travels
    // over InstallSnapshot and stays JSON so mixed-version clusters keep working.

    /// A gambas pixel write, as the embedder actually produces it.
    fn pixel_entry(index: u64) -> WalRecord {
        let command = serde_json::to_vec(&crate::kv::Command::Set {
            key: "px:012:034".to_string(),
            value: "7".to_string(),
        })
        .unwrap();
        WalRecord::Entry(LogEntry {
            index,
            term: 4,
            entry_type: EntryType::Normal,
            command,
        })
    }

    /// Frame records the way 0.1.x did — no marker, JSON payloads — so the
    /// legacy read path is exercised against real bytes rather than a mock.
    fn write_legacy_wal(path: &Path, records: &[WalRecord]) {
        let mut bytes = Vec::new();
        for record in records {
            let payload = serde_json::to_vec(record).unwrap();
            let mut h = Hasher::new();
            h.update(&payload);
            bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&h.finalize().to_le_bytes());
            bytes.extend_from_slice(&payload);
        }
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn a_fresh_wal_is_marked_v2_and_round_trips() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        {
            let (mut wal, _) = Wal::open(tmp.path()).unwrap();
            wal.append(&pixel_entry(1)).unwrap();
        }
        let bytes = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&bytes[..8], &WAL_MAGIC_V2, "a new file carries the marker");

        let (_wal, records) = Wal::open(tmp.path()).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            WalRecord::Entry(e) => assert_eq!(e.index, 1),
            other => panic!("expected an Entry, got {other:?}"),
        }
    }

    #[test]
    fn a_legacy_json_wal_still_replays_and_stays_json() {
        // node1 is the golden copy and its WAL is 0.1.x JSON: it has to keep
        // opening, and it has to keep being appended in the codec it already
        // uses — a file must never hold both.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        write_legacy_wal(
            tmp.path(),
            &[
                WalRecord::HardState {
                    term: 3,
                    voted_for: Some(1),
                    commit: 0,
                },
                pixel_entry(1),
            ],
        );

        let (mut wal, records) = Wal::open(tmp.path()).unwrap();
        assert_eq!(records.len(), 2, "a 0.1.x WAL must still replay");
        wal.append(&pixel_entry(2)).unwrap();
        drop(wal);

        let bytes = std::fs::read(tmp.path()).unwrap();
        assert_ne!(
            &bytes[..8],
            &WAL_MAGIC_V2,
            "a legacy file is not relabelled"
        );
        let (_wal, records) = Wal::open(tmp.path()).unwrap();
        assert_eq!(records.len(), 3, "the appended record is readable too");
    }

    #[test]
    fn a_v2_record_is_a_fraction_of_its_json_size() {
        let v2 = tempfile::NamedTempFile::new().unwrap();
        {
            let (mut wal, _) = Wal::open(v2.path()).unwrap();
            wal.append(&pixel_entry(49_400)).unwrap();
        }
        // Minus the one-off file marker: compare record against record.
        let v2_len = v2.path().metadata().unwrap().len() - WAL_MAGIC_V2.len() as u64;

        let legacy = tempfile::NamedTempFile::new().unwrap();
        write_legacy_wal(legacy.path(), &[pixel_entry(49_400)]);
        let json_len = legacy.path().metadata().unwrap().len();

        assert!(
            v2_len * 2 < json_len,
            "a pixel record must cost less than half of its JSON form \
             (v2 {v2_len} B vs json {json_len} B)"
        );
    }

    #[test]
    fn rewrite_drops_the_covered_records_and_migrates_a_legacy_file() {
        // Rotation is what bounds the WAL: once compaction folds entries into a
        // snapshot file they are dead weight, and a legacy file migrates to v2
        // on the way.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        write_legacy_wal(
            tmp.path(),
            &[pixel_entry(1), pixel_entry(2), pixel_entry(3)],
        );
        let before = tmp.path().metadata().unwrap().len();

        let (mut wal, records) = Wal::open(tmp.path()).unwrap();
        assert_eq!(records.len(), 3);
        assert!(wal.is_legacy());

        wal.rewrite(&[
            WalRecord::HardState {
                term: 4,
                voted_for: Some(1),
                commit: 12,
            },
            pixel_entry(3),
        ])
        .unwrap();
        assert!(!wal.is_legacy(), "a rewritten file is always v2");

        // The reopened handle must write into the file that now carries the
        // name, not the replaced inode.
        wal.append(&pixel_entry(4)).unwrap();
        drop(wal);

        let (_wal, records) = Wal::open(tmp.path()).unwrap();
        assert_eq!(records.len(), 3, "hard state + entry 3 + the new entry 4");
        assert!(
            matches!(
                records[0],
                WalRecord::HardState {
                    term: 4,
                    commit: 12,
                    ..
                }
            ),
            "the rotated file must still carry the term, the vote and the commit"
        );
        match (&records[1], &records[2]) {
            (WalRecord::Entry(a), WalRecord::Entry(b)) => {
                assert_eq!((a.index, b.index), (3, 4));
            }
            other => panic!("expected two entries, got {other:?}"),
        }
        assert!(
            tmp.path().metadata().unwrap().len() < before,
            "rotation must shrink the file"
        );
    }
}

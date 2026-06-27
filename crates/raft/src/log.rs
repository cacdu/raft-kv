use crate::message::{EntryType, LogEntry, LogIndex, Term};

/// In-memory Raft log. Entries are 1-indexed; index 0 is a sentinel.
#[derive(Debug)]
pub struct RaftLog {
    entries: Vec<LogEntry>,
    /// Index of the last entry included in the latest snapshot (0 if none).
    snapshot_index: LogIndex,
    snapshot_term: Term,
}

impl RaftLog {
    pub fn new() -> Self {
        Self {
            // Sentinel at position 0: index=0, term=0
            entries: vec![LogEntry {
                index: 0,
                term: 0,
                entry_type: EntryType::Normal,
                command: vec![],
            }],
            snapshot_index: 0,
            snapshot_term: 0,
        }
    }

    pub fn snapshot_index(&self) -> LogIndex {
        self.snapshot_index
    }

    pub fn last_index(&self) -> LogIndex {
        self.snapshot_index + self.entries.len() as LogIndex - 1
    }

    pub fn last_term(&self) -> Term {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_term)
    }

    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        let offset = (index - self.snapshot_index) as usize;
        self.entries.get(offset).map(|e| e.term)
    }

    pub fn entries_from(&self, from: LogIndex) -> &[LogEntry] {
        let offset = (from - self.snapshot_index) as usize;
        &self.entries[offset..]
    }

    pub fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    /// Truncate all entries after `index` (inclusive) and append `new_entries`.
    pub fn truncate_and_append(&mut self, after: LogIndex, new_entries: Vec<LogEntry>) {
        let offset = (after - self.snapshot_index) as usize;
        self.entries.truncate(offset);
        self.entries.extend(new_entries);
    }

    /// Restore log state from WAL replay.
    /// `entries` must already be deduplicated (last writer wins per index).
    pub fn restore(
        &mut self,
        snapshot_index: LogIndex,
        snapshot_term: Term,
        entries: Vec<LogEntry>,
    ) {
        self.snapshot_index = snapshot_index;
        self.snapshot_term = snapshot_term;
        self.entries = vec![LogEntry {
            index: snapshot_index,
            term: snapshot_term,
            entry_type: EntryType::Normal,
            command: vec![],
        }];
        self.entries.extend(entries);
    }

    /// Discard entries up to and including `index` (snapshot compaction).
    pub fn compact(&mut self, index: LogIndex, term: Term) {
        let offset = (index - self.snapshot_index) as usize;
        self.entries.drain(0..=offset);
        self.snapshot_index = index;
        self.snapshot_term = term;
        // Re-insert sentinel
        self.entries.insert(
            0,
            LogEntry {
                index,
                term,
                entry_type: EntryType::Normal,
                command: vec![],
            },
        );
    }
}

impl Default for RaftLog {
    fn default() -> Self {
        Self::new()
    }
}

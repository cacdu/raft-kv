/// In-memory KV state machine applied on top of committed Raft log entries.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Default)]
pub struct KvStore {
    data: BTreeMap<String, String>,
}

impl KvStore {
    pub fn apply(&mut self, command_bytes: &[u8]) -> Result<(), serde_json::Error> {
        let cmd: Command = serde_json::from_slice(command_bytes)?;
        match cmd {
            Command::Set { key, value } => {
                self.data.insert(key, value);
            }
            Command::Delete { key } => {
                self.data.remove(&key);
            }
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.data.get(key).map(|s| s.as_str())
    }

    pub fn snapshot(&self) -> Vec<u8> {
        serde_json::to_vec(&self.data).unwrap_or_default()
    }

    pub fn restore(&mut self, snapshot: &[u8]) -> Result<(), serde_json::Error> {
        self.data = serde_json::from_slice(snapshot)?;
        Ok(())
    }
}

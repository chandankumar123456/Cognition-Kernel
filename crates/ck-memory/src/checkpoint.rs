use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointData {
    pub task_id: String,
    pub goal: String,
    pub status: String,
    pub plan_json: Option<String>,
    pub current_step: usize,
    pub retry_count: u32,
    pub replan_count: u32,
}

impl CheckpointData {
    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }
    pub fn deserialize(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }
}

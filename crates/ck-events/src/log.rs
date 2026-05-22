use crate::types::KernelEvent;

pub trait EventLog: Send + Sync {
    fn append(&self, event: &KernelEvent) -> Result<u64, EventLogError>;
    fn replay(&self, task_id: &str) -> Result<Vec<KernelEvent>, EventLogError>;
}

#[derive(Debug, thiserror::Error)]
pub enum EventLogError {
    #[error("storage error: {0}")]
    Storage(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

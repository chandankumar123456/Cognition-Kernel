use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryBudget {
    pub max_retries: u32,
    pub max_replans: u32,
}

impl RetryBudget {
    pub fn new(max_retries: u32, max_replans: u32) -> Self {
        Self { max_retries, max_replans }
    }
    pub fn default_budget() -> Self {
        Self { max_retries: 3, max_replans: 2 }
    }
}

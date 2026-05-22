use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelConfig {
    pub db_path: String,
    pub cognition_pipe: String,
    pub worker_pipe: String,
    pub max_concurrent_tasks: usize,
    pub max_retries: u32,
    pub max_replans: u32,
    pub default_timeout_ms: u64,
}

impl Default for KernelConfig {
    fn default() -> Self {
        Self {
            db_path: "cognition_kernel.db".into(),
            cognition_pipe: "ck-cognition".into(),
            worker_pipe: "ck-worker".into(),
            max_concurrent_tasks: 10,
            max_retries: 3,
            max_replans: 2,
            default_timeout_ms: 30_000,
        }
    }
}

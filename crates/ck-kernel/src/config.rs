use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelConfig {
    pub db_path: String,
    pub cognition_pipe: String,
    pub worker_pipe: String,
    pub max_concurrent_tasks: usize,
    pub max_retries: u32,
    pub max_replans: u32,
    pub default_timeout_ms: u64,
    /// Path to the Python cognition engine script (default: auto-detected)
    pub cognition_script: String,
    /// Python executable (default: "python")
    pub python_bin: String,
    /// Path to Go worker binary (default: auto-detected)
    pub worker_bin: String,
}

impl Default for KernelConfig {
    fn default() -> Self {
        // Detect project root relative to the executable or current dir
        let project_root = std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."));

        Self {
            db_path: "cognition_kernel.db".into(),
            cognition_pipe: "ck-cognition".into(),
            worker_pipe: "ck-worker".into(),
            max_concurrent_tasks: 10,
            max_retries: 3,
            max_replans: 2,
            default_timeout_ms: 30_000,
            cognition_script: project_root
                .join("cognition")
                .join("cognition_kernel")
                .join("engine.py")
                .to_string_lossy()
                .into(),
            python_bin: "python".into(),
            worker_bin: project_root
                .join("workers")
                .join("bin")
                .join(if cfg!(windows) { "ck-worker.exe" } else { "ck-worker" })
                .to_string_lossy()
                .into(),
        }
    }
}

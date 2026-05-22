use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VerificationStrategy {
    FileExists { path: PathBuf, content_contains: Option<String> },
    ExitCodeZero,
    OutputContains { expected: String },
    FileModified { path: PathBuf, after_ms: i64 },
    ProcessRunning { name: String },
    CognitionVerify { context: String },
}

#[derive(Debug, Clone)]
pub enum VerificationResult {
    Verified { evidence: String },
    Failed { reason: String, actual: String, expected: String },
}

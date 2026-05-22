use crate::strategies::{VerificationResult, VerificationStrategy};
use std::fs;

pub struct Verifier;

impl Verifier {
    pub fn verify_strategy(strategy: &VerificationStrategy) -> VerificationResult {
        match strategy {
            VerificationStrategy::FileExists { path, content_contains } => {
                if !path.exists() {
                    return VerificationResult::Failed {
                        reason: "file does not exist".into(),
                        actual: "missing".into(),
                        expected: path.display().to_string(),
                    };
                }
                if let Some(expected) = content_contains {
                    let content = fs::read_to_string(path).unwrap_or_default();
                    if !content.contains(expected.as_str()) {
                        return VerificationResult::Failed {
                            reason: "content mismatch".into(),
                            actual: content,
                            expected: expected.clone(),
                        };
                    }
                    VerificationResult::Verified { evidence: format!("file contains '{expected}'") }
                } else {
                    VerificationResult::Verified { evidence: format!("file exists: {}", path.display()) }
                }
            }
            _ => VerificationResult::Failed {
                reason: "strategy requires additional input".into(),
                actual: String::new(),
                expected: String::new(),
            },
        }
    }

    pub fn verify_with_exit_code(strategy: &VerificationStrategy, code: i32) -> VerificationResult {
        match strategy {
            VerificationStrategy::ExitCodeZero => {
                if code == 0 {
                    VerificationResult::Verified { evidence: "exit code 0".into() }
                } else {
                    VerificationResult::Failed {
                        reason: "non-zero exit code".into(),
                        actual: code.to_string(),
                        expected: "0".into(),
                    }
                }
            }
            _ => VerificationResult::Failed {
                reason: "strategy mismatch".into(),
                actual: String::new(),
                expected: "ExitCodeZero".into(),
            },
        }
    }

    pub fn verify_with_output(strategy: &VerificationStrategy, output: &str) -> VerificationResult {
        match strategy {
            VerificationStrategy::OutputContains { expected } => {
                if output.contains(expected.as_str()) {
                    VerificationResult::Verified { evidence: format!("output contains '{expected}'") }
                } else {
                    VerificationResult::Failed {
                        reason: "output missing expected string".into(),
                        actual: output.to_string(),
                        expected: expected.clone(),
                    }
                }
            }
            _ => VerificationResult::Failed {
                reason: "strategy mismatch".into(),
                actual: String::new(),
                expected: "OutputContains".into(),
            },
        }
    }
}

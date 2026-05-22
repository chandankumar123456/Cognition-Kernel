use ck_verify::engine::Verifier;
use ck_verify::strategies::{VerificationResult, VerificationStrategy};
use std::io::Write;
use std::path::PathBuf;
use tempfile::NamedTempFile;

#[test]
fn file_exists_pass() {
    let f = NamedTempFile::new().unwrap();
    let strategy = VerificationStrategy::FileExists { path: f.path().to_path_buf(), content_contains: None };
    assert!(matches!(Verifier::verify_strategy(&strategy), VerificationResult::Verified { .. }));
}

#[test]
fn file_exists_fail() {
    let strategy = VerificationStrategy::FileExists { path: PathBuf::from("/nonexistent_xyz"), content_contains: None };
    assert!(matches!(Verifier::verify_strategy(&strategy), VerificationResult::Failed { .. }));
}

#[test]
fn file_content_contains() {
    let mut f = NamedTempFile::new().unwrap();
    write!(f, "hello world").unwrap();
    let strategy = VerificationStrategy::FileExists { path: f.path().to_path_buf(), content_contains: Some("world".into()) };
    assert!(matches!(Verifier::verify_strategy(&strategy), VerificationResult::Verified { .. }));
}

#[test]
fn exit_code_zero() {
    let strategy = VerificationStrategy::ExitCodeZero;
    assert!(matches!(Verifier::verify_with_exit_code(&strategy, 0), VerificationResult::Verified { .. }));
}

#[test]
fn exit_code_nonzero() {
    let strategy = VerificationStrategy::ExitCodeZero;
    assert!(matches!(Verifier::verify_with_exit_code(&strategy, 1), VerificationResult::Failed { .. }));
}

#[test]
fn output_contains() {
    let strategy = VerificationStrategy::OutputContains { expected: "success".into() };
    assert!(matches!(Verifier::verify_with_output(&strategy, "operation success done"), VerificationResult::Verified { .. }));
}

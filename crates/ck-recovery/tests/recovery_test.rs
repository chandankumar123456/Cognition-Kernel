use ck_recovery::budget::RetryBudget;
use ck_recovery::engine::{FailureContext, RecoveryDecision, RecoveryEngine};

fn make_ctx(retry_count: u32, replan_count: u32) -> FailureContext {
    FailureContext {
        task_id: "t1".into(),
        action_id: "a1".into(),
        reason: "timeout".into(),
        retry_count,
        replan_count,
    }
}

#[test]
fn first_failure_retries() {
    let budget = RetryBudget::default_budget();
    let decision = RecoveryEngine::decide(&make_ctx(0, 0), &budget);
    assert!(matches!(decision, RecoveryDecision::Retry { .. }));
}

#[test]
fn exhausted_retries_replans() {
    let budget = RetryBudget::default_budget();
    let decision = RecoveryEngine::decide(&make_ctx(3, 0), &budget);
    assert!(matches!(decision, RecoveryDecision::Replan { .. }));
}

#[test]
fn exhausted_replans_escalates() {
    let budget = RetryBudget::default_budget();
    let decision = RecoveryEngine::decide(&make_ctx(3, 2), &budget);
    assert!(matches!(decision, RecoveryDecision::Escalate { .. }));
}

#[test]
fn retry_backoff_increases() {
    let budget = RetryBudget::default_budget();
    let d0 = RecoveryEngine::decide(&make_ctx(0, 0), &budget);
    let d1 = RecoveryEngine::decide(&make_ctx(1, 0), &budget);
    let b0 = match d0 { RecoveryDecision::Retry { backoff_ms } => backoff_ms, _ => panic!() };
    let b1 = match d1 { RecoveryDecision::Retry { backoff_ms } => backoff_ms, _ => panic!() };
    assert!(b1 > b0);
}

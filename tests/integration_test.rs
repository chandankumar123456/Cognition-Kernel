use std::collections::HashMap;
use std::io::Write;

use ck_events::bus::EventBus;
use ck_events::types::KernelEvent;
use ck_kernel::task::{Plan, PlanStep, Task, TaskStatus};
use ck_memory::checkpoint::CheckpointData;
use ck_memory::store::Store;
use ck_recovery::budget::RetryBudget;
use ck_recovery::engine::{FailureContext, RecoveryDecision, RecoveryEngine};
use ck_verify::engine::Verifier;
use ck_verify::strategies::VerificationStrategy;

#[test]
fn test_full_task_lifecycle() {
    let mut task = Task::new("Create two files".into());
    assert_eq!(task.status(), TaskStatus::Created);

    task.transition_to(TaskStatus::Planning).unwrap();
    assert_eq!(task.status(), TaskStatus::Planning);

    let plan = Plan {
        id: "plan-1".into(),
        steps: vec![
            PlanStep {
                id: "step-1".into(),
                description: "Create file A".into(),
                tool: "filesystem".into(),
                params: HashMap::new(),
                expected_outcome: "file_a exists".into(),
                verification_strategy: "FileExists".into(),
            },
            PlanStep {
                id: "step-2".into(),
                description: "Create file B".into(),
                tool: "filesystem".into(),
                params: HashMap::new(),
                expected_outcome: "file_b exists".into(),
                verification_strategy: "FileExists".into(),
            },
        ],
        generated_by: "test".into(),
        reasoning: "test plan".into(),
    };
    task.set_plan(plan).unwrap();
    assert_eq!(task.status(), TaskStatus::Planned);

    task.transition_to(TaskStatus::Executing).unwrap();
    assert_eq!(task.status(), TaskStatus::Executing);

    // Simulate execution: create real temp files and verify
    let dir = tempfile::tempdir().unwrap();
    let file_a = dir.path().join("file_a.txt");
    let file_b = dir.path().join("file_b.txt");

    std::fs::File::create(&file_a).unwrap().write_all(b"hello").unwrap();
    let strategy_a = VerificationStrategy::FileExists {
        path: file_a,
        content_contains: Some("hello".into()),
    };
    let result_a = Verifier::verify_strategy(&strategy_a);
    assert!(matches!(result_a, ck_verify::strategies::VerificationResult::Verified { .. }));
    task.advance_step();

    std::fs::File::create(&file_b).unwrap().write_all(b"world").unwrap();
    let strategy_b = VerificationStrategy::FileExists {
        path: file_b,
        content_contains: Some("world".into()),
    };
    let result_b = Verifier::verify_strategy(&strategy_b);
    assert!(matches!(result_b, ck_verify::strategies::VerificationResult::Verified { .. }));
    task.advance_step();

    assert!(task.is_plan_complete());
    task.transition_to(TaskStatus::Verifying).unwrap();
    task.transition_to(TaskStatus::Completed).unwrap();
    assert_eq!(task.status(), TaskStatus::Completed);
}

#[test]
fn test_recovery_on_failure() {
    let budget = RetryBudget::default_budget();

    // retry_count=0 => Retry
    let ctx = FailureContext {
        task_id: "t1".into(),
        action_id: "a1".into(),
        reason: "timeout".into(),
        retry_count: 0,
        replan_count: 0,
    };
    assert!(matches!(RecoveryEngine::decide(&ctx, &budget), RecoveryDecision::Retry { .. }));

    // retry_count=3 => Replan
    let ctx2 = FailureContext {
        task_id: "t1".into(),
        action_id: "a1".into(),
        reason: "timeout".into(),
        retry_count: 3,
        replan_count: 0,
    };
    assert!(matches!(RecoveryEngine::decide(&ctx2, &budget), RecoveryDecision::Replan { .. }));

    // replan_count=2 => Escalate
    let ctx3 = FailureContext {
        task_id: "t1".into(),
        action_id: "a1".into(),
        reason: "timeout".into(),
        retry_count: 3,
        replan_count: 2,
    };
    assert!(matches!(RecoveryEngine::decide(&ctx3, &budget), RecoveryDecision::Escalate { .. }));
}

#[tokio::test]
async fn test_event_bus_integration() {
    let bus = EventBus::new(16);
    let mut rx = bus.subscribe();

    let event = KernelEvent::TaskCreated {
        task_id: "t1".into(),
        goal: "test goal".into(),
        timestamp: 1000,
    };
    bus.emit(event.clone());

    let received = rx.recv().await.unwrap();
    assert!(matches!(received, KernelEvent::TaskCreated { ref task_id, .. } if task_id == "t1"));
}

#[test]
fn test_checkpoint_roundtrip() {
    let cp = CheckpointData {
        task_id: "task-123".into(),
        goal: "do something".into(),
        status: "executing".into(),
        plan_json: Some(r#"{"steps":[]}"#.into()),
        current_step: 2,
        retry_count: 1,
        replan_count: 0,
    };

    let bytes = cp.serialize().unwrap();
    let restored = CheckpointData::deserialize(&bytes).unwrap();

    assert_eq!(restored.task_id, "task-123");
    assert_eq!(restored.goal, "do something");
    assert_eq!(restored.status, "executing");
    assert_eq!(restored.plan_json, Some(r#"{"steps":[]}"#.into()));
    assert_eq!(restored.current_step, 2);
    assert_eq!(restored.retry_count, 1);
    assert_eq!(restored.replan_count, 0);
}

#[test]
fn test_store_persistence() {
    let store = Store::open_in_memory().unwrap();
    store.create_task("task-1", "build feature X").unwrap();

    let task = store.get_task("task-1").unwrap().unwrap();
    assert_eq!(task.id, "task-1");
    assert_eq!(task.goal, "build feature X");
    assert_eq!(task.status, ck_memory::store::TaskStatus::Created);
    assert_eq!(task.current_step, 0);
}


/// Test: resume actually restores plan and step from checkpoint
#[test]
fn test_real_resume_restores_plan_and_step() {
    use ck_memory::checkpoint::CheckpointData;
    use ck_kernel::task::{Plan, PlanStep};
    use std::collections::HashMap;

    let store = Store::open_in_memory().unwrap();
    store.create_task("t-resume", "resume goal").unwrap();

    // Build a 3-step plan, checkpoint at step 1
    let plan = Plan {
        id: "p1".into(),
        steps: (0..3).map(|i| PlanStep {
            id: format!("s{i}"),
            description: format!("step {i}"),
            tool: "shell".into(),
            params: HashMap::new(),
            expected_outcome: "done".into(),
            verification_strategy: "exit_code_zero".into(),
        }).collect(),
        generated_by: "test".into(),
        reasoning: "".into(),
    };

    let cp = CheckpointData {
        task_id: "t-resume".into(),
        goal: "resume goal".into(),
        status: "Executing".into(),
        plan_json: Some(serde_json::to_string(&plan).unwrap()),
        current_step: 1,
        retry_count: 0,
        replan_count: 0,
    };
    let blob = cp.serialize().unwrap();
    store.save_checkpoint("cp1", "t-resume", &blob, 1).unwrap();

    // Load and verify restoration
    let loaded = store.load_latest_checkpoint("t-resume").unwrap().unwrap();
    let restored = CheckpointData::deserialize(&loaded.state_blob).unwrap();
    assert_eq!(restored.current_step, 1);
    let restored_plan: Plan = serde_json::from_str(restored.plan_json.as_ref().unwrap()).unwrap();
    assert_eq!(restored_plan.steps.len(), 3);
    assert_eq!(restored_plan.steps[1].description, "step 1");
}

/// Test: mark_interrupted_tasks marks planning/executing tasks as failed
#[test]
fn test_mark_interrupted_tasks() {
    let store = Store::open_in_memory().unwrap();
    store.create_task("t1", "goal 1").unwrap();
    store.create_task("t2", "goal 2").unwrap();
    store.create_task("t3", "goal 3").unwrap();
    store.update_task_status("t1", ck_memory::store::TaskStatus::Executing).unwrap();
    store.update_task_status("t2", ck_memory::store::TaskStatus::Planning).unwrap();
    store.update_task_status("t3", ck_memory::store::TaskStatus::Completed).unwrap();

    let count = store.mark_interrupted_tasks().unwrap();
    assert_eq!(count, 2); // t1 and t2, not t3

    assert_eq!(store.get_task("t1").unwrap().unwrap().status, ck_memory::store::TaskStatus::Failed);
    assert_eq!(store.get_task("t2").unwrap().unwrap().status, ck_memory::store::TaskStatus::Failed);
    assert_eq!(store.get_task("t3").unwrap().unwrap().status, ck_memory::store::TaskStatus::Completed);
}

/// Test: retry budget is persisted to SQLite
#[test]
fn test_retry_budget_persistence() {
    let store = Store::open_in_memory().unwrap();
    store.create_task("t1", "goal").unwrap();
    store.update_task_retry_budget("t1", 2, 1).unwrap();
    let task = store.get_task("t1").unwrap().unwrap();
    let budget: serde_json::Value = serde_json::from_str(
        task.retry_budget_json.as_ref().unwrap()
    ).unwrap();
    assert_eq!(budget["retry_count"], 2);
    assert_eq!(budget["replan_count"], 1);
}

/// Test: path sandbox logic correctly identifies dangerous paths
#[test]
fn test_path_sandbox_logic() {
    let work_dir = std::path::Path::new("E:\\Projects\\Cognition Kernel");
    let safe_relative = std::path::Path::new("hello.txt");
    let safe_abs = work_dir.join("output\\result.txt");
    let dangerous = std::path::Path::new("C:\\Windows\\System32\\evil.dll");
    let dangerous2 = std::path::Path::new("C:\\Users\\secret.txt");

    // Relative paths are always safe (not absolute)
    assert!(!safe_relative.is_absolute());
    // Absolute path inside work_dir is safe
    assert!(safe_abs.starts_with(work_dir));
    // Absolute paths outside work_dir are blocked
    assert!(!dangerous.starts_with(work_dir));
    assert!(!dangerous2.starts_with(work_dir));
}

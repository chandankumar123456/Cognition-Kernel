use ck_memory::store::Store;
use ck_memory::checkpoint::CheckpointData;
use ck_kernel::task::{Task, TaskStatus, Plan, PlanStep};
use std::collections::HashMap;

#[test]
fn test_checkpoint_and_resume() {
    let store = Store::open_in_memory().unwrap();

    // Create a task with 4 steps
    let mut task = Task::new("four step task".into());
    task.transition_to(TaskStatus::Planning).unwrap();
    let plan = Plan {
        id: "p1".into(),
        steps: (0..4).map(|i| PlanStep {
            id: format!("s{}", i),
            description: format!("step {}", i),
            tool: "shell".into(),
            params: HashMap::new(),
            expected_outcome: "done".into(),
            verification_strategy: "exit_code_zero".into(),
        }).collect(),
        generated_by: "test".into(),
        reasoning: "test plan".into(),
    };
    task.set_plan(plan).unwrap();
    task.transition_to(TaskStatus::Executing).unwrap();

    // Execute steps 0 and 1
    task.advance_step();
    task.advance_step();

    // Checkpoint at step 2
    let checkpoint = CheckpointData {
        task_id: task.id().into(),
        goal: task.goal().into(),
        status: format!("{:?}", task.status()),
        plan_json: Some(serde_json::to_string(task.plan().unwrap()).unwrap()),
        current_step: task.current_step(),
        retry_count: 0,
        replan_count: 0,
    };
    let blob = checkpoint.serialize().unwrap();
    store.save_checkpoint("cp1", task.id(), &blob, task.current_step() as i64).unwrap();

    // "Crash" - drop the task
    let task_id = task.id().to_string();
    drop(task);

    // Resume from checkpoint
    let loaded = store.load_latest_checkpoint(&task_id).unwrap().unwrap();
    let restored = CheckpointData::deserialize(&loaded.state_blob).unwrap();
    assert_eq!(restored.task_id, task_id);
    assert_eq!(restored.current_step, 2);
    assert_eq!(restored.goal, "four step task");

    // Reconstruct task and continue from step 2
    let mut resumed_task = Task::new(restored.goal);
    resumed_task.transition_to(TaskStatus::Planning).unwrap();
    let plan: Plan = serde_json::from_str(restored.plan_json.as_ref().unwrap()).unwrap();
    resumed_task.set_plan(plan).unwrap();
    resumed_task.transition_to(TaskStatus::Executing).unwrap();

    // Fast-forward to step 2
    for _ in 0..restored.current_step {
        resumed_task.advance_step();
    }
    assert_eq!(resumed_task.current_step(), 2);

    // Continue: steps 2 and 3
    resumed_task.advance_step();
    resumed_task.advance_step();
    assert!(resumed_task.is_plan_complete());

    resumed_task.transition_to(TaskStatus::Verifying).unwrap();
    resumed_task.transition_to(TaskStatus::Completed).unwrap();
    assert_eq!(resumed_task.status(), TaskStatus::Completed);
}

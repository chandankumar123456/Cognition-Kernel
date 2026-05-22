use ck_kernel::task::*;
use std::collections::HashMap;

fn make_plan(steps: usize) -> Plan {
    Plan {
        id: "plan-1".into(),
        steps: (0..steps)
            .map(|i| PlanStep {
                id: format!("step-{i}"),
                description: format!("Step {i}"),
                tool: "test_tool".into(),
                params: HashMap::new(),
                expected_outcome: "ok".into(),
                verification_strategy: "check".into(),
            })
            .collect(),
        generated_by: "test".into(),
        reasoning: "test reasoning".into(),
    }
}

#[test]
fn test_task_creation() {
    let task = Task::new("do something".into());
    assert_eq!(task.status(), TaskStatus::Created);
    assert!(task.plan().is_none());
    assert_eq!(task.current_step(), 0);
    assert!(!task.goal().is_empty());
}

#[test]
fn test_valid_transition_created_to_planning() {
    let mut task = Task::new("goal".into());
    assert!(task.transition_to(TaskStatus::Planning).is_ok());
    assert_eq!(task.status(), TaskStatus::Planning);
}

#[test]
fn test_valid_transition_planning_to_planned() {
    let mut task = Task::new("goal".into());
    task.transition_to(TaskStatus::Planning).unwrap();
    let plan = make_plan(2);
    assert!(task.set_plan(plan).is_ok());
    assert_eq!(task.status(), TaskStatus::Planned);
    assert!(task.plan().is_some());
}

#[test]
fn test_invalid_transition_created_to_executing() {
    let mut task = Task::new("goal".into());
    let result = task.transition_to(TaskStatus::Executing);
    assert!(result.is_err());
}

#[test]
fn test_advance_step() {
    let mut task = Task::new("goal".into());
    task.transition_to(TaskStatus::Planning).unwrap();
    task.set_plan(make_plan(2)).unwrap();
    assert_eq!(task.current_step(), 0);
    assert!(task.current_plan_step().is_some());
    task.advance_step();
    assert_eq!(task.current_step(), 1);
    assert!(task.current_plan_step().is_some());
    task.advance_step();
    assert_eq!(task.current_step(), 2);
    assert!(task.is_plan_complete());
}

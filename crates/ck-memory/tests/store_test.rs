use ck_memory::store::{Store, TaskStatus};
use ck_memory::checkpoint::CheckpointData;

#[test]
fn create_and_get_task() {
    let store = Store::open_in_memory().unwrap();
    store.create_task("task-1", "do something").unwrap();
    let task = store.get_task("task-1").unwrap().unwrap();
    assert_eq!(task.id, "task-1");
    assert_eq!(task.goal, "do something");
    assert_eq!(task.status, TaskStatus::Created);
    assert_eq!(task.current_step, 0);
}

#[test]
fn update_task_status() {
    let store = Store::open_in_memory().unwrap();
    store.create_task("task-2", "goal").unwrap();
    store.update_task_status("task-2", TaskStatus::Executing).unwrap();
    let task = store.get_task("task-2").unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Executing);
}

#[test]
fn append_and_replay_events() {
    let store = Store::open_in_memory().unwrap();
    store.create_task("task-3", "goal").unwrap();
    store.append_event("task-3", "step_started", r#"{"step":0}"#).unwrap();
    store.append_event("task-3", "step_completed", r#"{"step":0}"#).unwrap();
    let events = store.replay_events("task-3").unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event_type, "step_started");
    assert_eq!(events[1].event_type, "step_completed");
}

#[test]
fn checkpoint_save_and_load() {
    let store = Store::open_in_memory().unwrap();
    store.create_task("task-4", "goal").unwrap();

    let data = CheckpointData {
        task_id: "task-4".into(),
        goal: "goal".into(),
        status: "executing".into(),
        plan_json: None,
        current_step: 2,
        retry_count: 1,
        replan_count: 0,
    };
    let blob = data.serialize().unwrap();
    store.save_checkpoint("cp-1", "task-4", &blob, 2).unwrap();

    let loaded = store.load_latest_checkpoint("task-4").unwrap().unwrap();
    assert_eq!(loaded.id, "cp-1");
    assert_eq!(loaded.step_index, 2);

    let restored = CheckpointData::deserialize(&loaded.state_blob).unwrap();
    assert_eq!(restored.task_id, "task-4");
    assert_eq!(restored.current_step, 2);
    assert_eq!(restored.retry_count, 1);
}

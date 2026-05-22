use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KernelEvent {
    TaskCreated { task_id: String, goal: String, timestamp: i64 },
    PlanGenerated { task_id: String, plan_id: String, step_count: usize },
    ActionDispatched { task_id: String, action_id: String, tool: String, timestamp: i64 },
    ActionCompleted { task_id: String, action_id: String, success: bool, duration_ms: u64 },
    VerificationPassed { task_id: String, action_id: String, evidence: String },
    VerificationFailed { task_id: String, action_id: String, reason: String, expected: String, actual: String },
    RecoveryTriggered { task_id: String, strategy: String, attempt: u32 },
    TaskCompleted { task_id: String, duration_ms: u64, steps_executed: usize },
    TaskFailed { task_id: String, reason: String },
    CheckpointSaved { task_id: String, checkpoint_id: String },
    WorkerSpawned { worker_type: String, pid: u32 },
    WorkerCrashed { worker_type: String, pid: u32, exit_code: i32 },
}

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Created,
    Planning,
    Planned,
    Executing,
    Verifying,
    Recovering,
    Completed,
    Failed,
    Escalated,
}

impl TaskStatus {
    fn valid_transitions(&self) -> &[TaskStatus] {
        match self {
            Self::Created => &[Self::Planning, Self::Failed],
            Self::Planning => &[Self::Planned, Self::Failed],
            Self::Planned => &[Self::Executing, Self::Failed],
            Self::Executing => &[Self::Verifying, Self::Failed],
            Self::Verifying => &[Self::Executing, Self::Recovering, Self::Completed, Self::Failed],
            Self::Recovering => &[Self::Executing, Self::Planned, Self::Escalated, Self::Failed],
            Self::Completed => &[],
            Self::Failed => &[],
            Self::Escalated => &[Self::Executing, Self::Failed],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub id: String,
    pub steps: Vec<PlanStep>,
    pub generated_by: String,
    pub reasoning: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    pub id: String,
    pub description: String,
    pub tool: String,
    pub params: HashMap<String, serde_json::Value>,
    pub expected_outcome: String,
    pub verification_strategy: String,
}

#[derive(Debug, Error)]
pub enum TaskError {
    #[error("invalid transition from {from:?} to {to:?}")]
    InvalidTransition { from: TaskStatus, to: TaskStatus },
    #[error("cannot set plan in state {0:?}")]
    InvalidPlanState(TaskStatus),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    id: String,
    goal: String,
    status: TaskStatus,
    plan: Option<Plan>,
    current_step: usize,
    retry_count: u32,
    replan_count: u32,
    created_at: i64,
    updated_at: i64,
    #[serde(default)]
    pub step_outputs: HashMap<String, String>,
    #[serde(default)]
    pub last_failure: Option<String>,
}

impl Task {
    pub fn new(goal: String) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            id: Ulid::new().to_string(),
            goal,
            status: TaskStatus::Created,
            plan: None,
            current_step: 0,
            retry_count: 0,
            replan_count: 0,
            created_at: now,
            updated_at: now,
            step_outputs: HashMap::new(),
            last_failure: None,
        }
    }

    /// Create a task with a specific ID (for resume from checkpoint)
    pub fn with_id(id: String, goal: String) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            id,
            goal,
            status: TaskStatus::Created,
            plan: None,
            current_step: 0,
            retry_count: 0,
            replan_count: 0,
            created_at: now,
            updated_at: now,
            step_outputs: HashMap::new(),
            last_failure: None,
        }
    }

    pub fn id(&self) -> &str { &self.id }
    pub fn goal(&self) -> &str { &self.goal }
    pub fn status(&self) -> TaskStatus { self.status }
    pub fn plan(&self) -> Option<&Plan> { self.plan.as_ref() }
    pub fn current_step(&self) -> usize { self.current_step }
    pub fn retry_count(&self) -> u32 { self.retry_count }
    pub fn replan_count(&self) -> u32 { self.replan_count }

    pub fn transition_to(&mut self, new_status: TaskStatus) -> Result<(), TaskError> {
        if self.status.valid_transitions().contains(&new_status) {
            self.status = new_status;
            self.updated_at = chrono::Utc::now().timestamp_millis();
            Ok(())
        } else {
            Err(TaskError::InvalidTransition { from: self.status, to: new_status })
        }
    }

    pub fn set_plan(&mut self, plan: Plan) -> Result<(), TaskError> {
        if self.status != TaskStatus::Planning {
            return Err(TaskError::InvalidPlanState(self.status));
        }
        self.plan = Some(plan);
        self.status = TaskStatus::Planned;
        self.current_step = 0;
        self.updated_at = chrono::Utc::now().timestamp_millis();
        Ok(())
    }

    pub fn advance_step(&mut self) {
        self.current_step += 1;
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    pub fn increment_retry(&mut self) { self.retry_count += 1; }

    pub fn increment_replan(&mut self) {
        self.replan_count += 1;
        self.retry_count = 0;
    }

    pub fn set_retry_state(&mut self, retry_count: u32, replan_count: u32) {
        self.retry_count = retry_count;
        self.replan_count = replan_count;
    }

    pub fn current_plan_step(&self) -> Option<&PlanStep> {
        self.plan.as_ref()?.steps.get(self.current_step)
    }

    pub fn is_plan_complete(&self) -> bool {
        match &self.plan {
            Some(plan) => self.current_step >= plan.steps.len(),
            None => false,
        }
    }

    /// Force-reset task to Planning state for replanning.
    /// This intentionally bypasses normal FSM transitions as part of recovery.
    /// Only call from RecoveryDecision::Replan handler.
    pub fn start_replan(&mut self) {
        self.plan = None;
        self.current_step = 0;
        self.status = TaskStatus::Planning;
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    pub fn record_output(&mut self, step_description: &str, output: &str) {
        self.step_outputs.insert(step_description.to_string(), output.to_string());
    }

    pub fn set_failure(&mut self, reason: &str) {
        self.last_failure = Some(reason.to_string());
    }
}

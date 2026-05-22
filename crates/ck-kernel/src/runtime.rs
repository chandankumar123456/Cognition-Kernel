use std::collections::HashMap;
use tokio::sync::mpsc;
use ulid::Ulid;

use ck_events::bus::EventBus;
use ck_events::types::KernelEvent;
use ck_ipc::server::{PipeConnection, PipeServer};
use ck_ipc::types::{CognitionRequest, CognitionResponse, ExecutionRequest, ExecutionResponse, PlanStep as IpcPlanStep};
use ck_memory::checkpoint::CheckpointData;
use ck_memory::store::Store;
use ck_recovery::budget::RetryBudget;
use ck_recovery::engine::{FailureContext, RecoveryDecision, RecoveryEngine};
use ck_verify::engine::Verifier;
use ck_verify::strategies::{VerificationResult, VerificationStrategy};

use crate::config::KernelConfig;
use crate::task::{Plan, PlanStep, Task, TaskStatus};

#[derive(Debug)]
pub enum RuntimeCommand {
    CreateTask { goal: String },
    PauseTask { task_id: String },
    ResumeTask { task_id: String },
    CancelTask { task_id: String },
    Shutdown,
}

pub struct Runtime {
    config: KernelConfig,
    store: Store,
    event_bus: EventBus,
    tasks: HashMap<String, Task>,
    cmd_rx: mpsc::Receiver<RuntimeCommand>,
    cognition_conn: Option<PipeConnection>,
    worker_conn: Option<PipeConnection>,
}

impl Runtime {
    pub fn new(config: KernelConfig, cmd_rx: mpsc::Receiver<RuntimeCommand>) -> Result<Self, Box<dyn std::error::Error>> {
        let store = Store::open(&config.db_path)?;
        let event_bus = EventBus::new(256);
        Ok(Self {
            config,
            store,
            event_bus,
            tasks: HashMap::new(),
            cmd_rx,
            cognition_conn: None,
            worker_conn: None,
        })
    }

    pub fn event_bus(&self) -> &EventBus {
        &self.event_bus
    }

    pub async fn connect_workers(&mut self, cognition_pipe: &str, worker_pipe: &str) {
        // Connect to cognition engine
        let cognition_server = PipeServer::new(cognition_pipe);
        match cognition_server.accept().await {
            Ok(conn) => {
                tracing::info!(pipe = %cognition_pipe, "cognition engine connected");
                self.cognition_conn = Some(conn);
            }
            Err(e) => {
                tracing::warn!(pipe = %cognition_pipe, error = %e, "failed to connect cognition engine, running without");
            }
        }

        // Connect to worker
        let worker_server = PipeServer::new(worker_pipe);
        match worker_server.accept().await {
            Ok(conn) => {
                tracing::info!(pipe = %worker_pipe, "tool worker connected");
                self.worker_conn = Some(conn);
            }
            Err(e) => {
                tracing::warn!(pipe = %worker_pipe, error = %e, "failed to connect tool worker, running without");
            }
        }
    }

    pub async fn run(&mut self) {
        tracing::info!("runtime loop started");
        loop {
            while let Ok(cmd) = self.cmd_rx.try_recv() {
                match cmd {
                    RuntimeCommand::CreateTask { goal } => self.handle_create_task(goal),
                    RuntimeCommand::PauseTask { task_id } => self.handle_pause_task(&task_id),
                    RuntimeCommand::ResumeTask { task_id } => self.handle_resume_task(&task_id),
                    RuntimeCommand::CancelTask { task_id } => self.handle_cancel_task(&task_id),
                    RuntimeCommand::Shutdown => {
                        tracing::info!("shutdown received");
                        return;
                    }
                }
            }

            let active_ids: Vec<String> = self.tasks.iter()
                .filter(|(_, t)| matches!(t.status(), TaskStatus::Created | TaskStatus::Planning | TaskStatus::Planned | TaskStatus::Executing))
                .map(|(id, _)| id.clone())
                .collect();

            for id in active_ids {
                self.step_task(&id).await;
            }

            // Remove terminal tasks
            self.tasks.retain(|_, t| !matches!(t.status(), TaskStatus::Completed | TaskStatus::Failed));

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    fn handle_create_task(&mut self, goal: String) {
        let task = Task::new(goal.clone());
        let task_id = task.id().to_string();
        let _ = self.store.create_task(&task_id, &goal);
        self.event_bus.emit(KernelEvent::TaskCreated {
            task_id: task_id.clone(),
            goal,
            timestamp: chrono::Utc::now().timestamp_millis(),
        });
        tracing::info!(task_id = %task_id, "task created");
        self.tasks.insert(task_id, task);
    }

    fn handle_pause_task(&mut self, task_id: &str) {
        if let Some(task) = self.tasks.get_mut(task_id) {
            if task.transition_to(TaskStatus::Escalated).is_ok() {
                let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Escalated);
                self.save_checkpoint(task_id);
                tracing::info!(task_id = %task_id, "task paused");
            }
        }
    }

    fn handle_resume_task(&mut self, task_id: &str) {
        if let Ok(Some(cp)) = self.store.load_latest_checkpoint(task_id) {
            tracing::info!(task_id = %task_id, step = cp.step_index, "checkpoint loaded for resume");
            if let Some(task) = self.tasks.get_mut(task_id) {
                let _ = task.transition_to(TaskStatus::Executing);
                let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Executing);
            }
        }
    }

    fn handle_cancel_task(&mut self, task_id: &str) {
        if let Some(task) = self.tasks.get_mut(task_id) {
            if task.transition_to(TaskStatus::Failed).is_ok() {
                let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Failed);
                tracing::info!(task_id = %task_id, "task cancelled");
            }
        }
    }

    async fn step_task(&mut self, task_id: &str) {
        let Some(task) = self.tasks.get(task_id) else { return };
        match task.status() {
            TaskStatus::Created => self.request_plan(task_id).await,
            TaskStatus::Planned | TaskStatus::Executing => self.execute_next_step(task_id).await,
            _ => {}
        }
    }

    async fn request_plan(&mut self, task_id: &str) {
        let Some(task) = self.tasks.get_mut(task_id) else { return };
        let objective = task.goal().to_string();
        let _ = task.transition_to(TaskStatus::Planning);
        let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Planning);

        let Some(conn) = self.cognition_conn.as_mut() else {
            tracing::warn!(task_id = %task_id, "no cognition connection, task stays in Planning");
            return;
        };

        let request = CognitionRequest {
            request_type: "plan".into(),
            task_id: task_id.to_string(),
            objective,
            current_state: HashMap::new(),
            memory_context: HashMap::new(),
            failure_context: None,
        };

        if let Err(e) = conn.write(&request).await {
            tracing::error!(task_id = %task_id, error = %e, "failed to send plan request");
            if let Some(task) = self.tasks.get_mut(task_id) {
                let _ = task.transition_to(TaskStatus::Failed);
            }
            return;
        }

        let response: CognitionResponse = match conn.read().await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(task_id = %task_id, error = %e, "failed to read plan response");
                if let Some(task) = self.tasks.get_mut(task_id) {
                    let _ = task.transition_to(TaskStatus::Failed);
                }
                return;
            }
        };

        if let Some(ipc_steps) = response.plan {
            let steps: Vec<PlanStep> = ipc_steps.iter().map(ipc_step_to_plan_step).collect();
            let plan = Plan {
                id: Ulid::new().to_string(),
                steps,
                generated_by: "cognition".into(),
                reasoning: response.reasoning,
            };
            if let Some(task) = self.tasks.get_mut(task_id) {
                if let Err(e) = task.set_plan(plan) {
                    tracing::error!(task_id = %task_id, error = %e, "failed to set plan");
                } else {
                    let plan_id = task.plan().map(|p| p.id.clone()).unwrap_or_default();
                    let step_count = task.plan().map(|p| p.steps.len()).unwrap_or(0);
                    self.save_checkpoint(task_id);
                    self.event_bus.emit(KernelEvent::PlanGenerated {
                        task_id: task_id.to_string(),
                        plan_id,
                        step_count,
                    });
                    tracing::info!(task_id = %task_id, "plan received and set");
                }
            }
        } else {
            tracing::error!(task_id = %task_id, "cognition returned no plan");
            if let Some(task) = self.tasks.get_mut(task_id) {
                let _ = task.transition_to(TaskStatus::Failed);
            }
        }
    }

    async fn execute_next_step(&mut self, task_id: &str) {
        let Some(task) = self.tasks.get_mut(task_id) else { return };

        if task.is_plan_complete() {
            let _ = task.transition_to(TaskStatus::Verifying);
            let _ = task.transition_to(TaskStatus::Completed);
            let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Completed);
            let steps_executed = self.tasks.get(task_id).map(|t| t.current_step()).unwrap_or(0);
            self.save_checkpoint(task_id);
            self.event_bus.emit(KernelEvent::TaskCompleted {
                task_id: task_id.to_string(),
                duration_ms: 0,
                steps_executed,
            });
            tracing::info!(task_id = %task_id, "task completed");
            return;
        }

        if task.status() == TaskStatus::Planned {
            let _ = task.transition_to(TaskStatus::Executing);
            let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Executing);
        }

        let action_id = Ulid::new().to_string();
        let step = task.current_plan_step().cloned();
        let Some(step) = step else { return };

        self.event_bus.emit(KernelEvent::ActionDispatched {
            task_id: task_id.to_string(),
            action_id: action_id.clone(),
            tool: step.tool.clone(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        });

        let Some(conn) = self.worker_conn.as_mut() else {
            tracing::warn!(task_id = %task_id, "no worker connection, advancing step");
            if let Some(task) = self.tasks.get_mut(task_id) {
                task.advance_step();
            }
            return;
        };

        let request = ExecutionRequest {
            task_id: task_id.to_string(),
            action_id: action_id.clone(),
            tool: step.tool.clone(),
            params: step.params.clone(),
            timeout_ms: self.config.default_timeout_ms,
        };

        if let Err(e) = conn.write(&request).await {
            tracing::error!(task_id = %task_id, error = %e, "failed to send execution request");
            if let Some(task) = self.tasks.get_mut(task_id) {
                let _ = task.transition_to(TaskStatus::Failed);
            }
            return;
        }

        let response: ExecutionResponse = match conn.read().await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(task_id = %task_id, error = %e, "failed to read execution response");
                if let Some(task) = self.tasks.get_mut(task_id) {
                    let _ = task.transition_to(TaskStatus::Failed);
                }
                return;
            }
        };

        // Determine exit code from output (0 if success, 1 otherwise)
        let exit_code = if response.success { 0 } else { 1 };
        let result = parse_verification(&step.verification_strategy, &response.output, exit_code, response.success);

        match result {
            VerificationResult::Verified { evidence } => {
                tracing::info!(task_id = %task_id, action_id = %action_id, evidence = %evidence, "verification passed");
                if let Some(task) = self.tasks.get_mut(task_id) {
                    task.advance_step();
                }
                self.save_checkpoint(task_id);
                self.event_bus.emit(KernelEvent::VerificationPassed {
                    task_id: task_id.to_string(),
                    action_id,
                    evidence,
                });
            }
            VerificationResult::Failed { reason, .. } => {
                tracing::warn!(task_id = %task_id, action_id = %action_id, reason = %reason, "verification failed");
                let task = self.tasks.get_mut(task_id).unwrap();
                let ctx = FailureContext {
                    task_id: task_id.to_string(),
                    action_id: action_id.clone(),
                    reason: reason.clone(),
                    retry_count: task.retry_count(),
                    replan_count: task.replan_count(),
                };
                let budget = RetryBudget::new(self.config.max_retries, self.config.max_replans);
                let decision = RecoveryEngine::decide(&ctx, &budget);

                match decision {
                    RecoveryDecision::Retry { backoff_ms } => {
                        task.increment_retry();
                        self.event_bus.emit(KernelEvent::RecoveryTriggered {
                            task_id: task_id.to_string(),
                            strategy: "retry".into(),
                            attempt: task.retry_count(),
                        });
                        tracing::info!(task_id = %task_id, backoff_ms, "retrying after backoff");
                        // Backoff handled by not advancing — next tick will re-dispatch
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    }
                    RecoveryDecision::Replan { failure_context } => {
                        task.increment_replan();
                        let _ = task.transition_to(TaskStatus::Verifying);
                        let _ = task.transition_to(TaskStatus::Recovering);
                        let _ = task.transition_to(TaskStatus::Planned);
                        let _ = task.transition_to(TaskStatus::Executing);
                        // Transition back to Planning for replan
                        // The FSM requires specific paths; go through valid transitions
                        tracing::info!(task_id = %task_id, "replanning due to: {}", failure_context);
                        self.event_bus.emit(KernelEvent::RecoveryTriggered {
                            task_id: task_id.to_string(),
                            strategy: "replan".into(),
                            attempt: task.replan_count(),
                        });
                    }
                    RecoveryDecision::Escalate { reason } => {
                        let _ = task.transition_to(TaskStatus::Verifying);
                        let _ = task.transition_to(TaskStatus::Recovering);
                        let _ = task.transition_to(TaskStatus::Escalated);
                        self.event_bus.emit(KernelEvent::TaskFailed {
                            task_id: task_id.to_string(),
                            reason: reason.clone(),
                        });
                        tracing::error!(task_id = %task_id, reason = %reason, "task escalated");
                    }
                    RecoveryDecision::Rollback { .. } => {
                        // Rollback not yet implemented, escalate instead
                        let _ = task.transition_to(TaskStatus::Verifying);
                        let _ = task.transition_to(TaskStatus::Recovering);
                        let _ = task.transition_to(TaskStatus::Escalated);
                        tracing::warn!(task_id = %task_id, "rollback not implemented, escalating");
                    }
                }
            }
        }
    }

    fn save_checkpoint(&self, task_id: &str) {
        let Some(task) = self.tasks.get(task_id) else { return };
        let data = CheckpointData {
            task_id: task_id.to_string(),
            goal: task.goal().to_string(),
            status: format!("{:?}", task.status()),
            plan_json: task.plan().map(|p| serde_json::to_string(p).unwrap_or_default()),
            current_step: task.current_step(),
            retry_count: task.retry_count(),
            replan_count: task.replan_count(),
        };
        let blob = data.serialize().unwrap_or_default();
        let cp_id = Ulid::new().to_string();
        let _ = self.store.save_checkpoint(&cp_id, task_id, &blob, task.current_step() as i64);
        self.event_bus.emit(KernelEvent::CheckpointSaved {
            task_id: task_id.to_string(),
            checkpoint_id: cp_id,
        });
    }
}

fn ipc_step_to_plan_step(s: &IpcPlanStep) -> PlanStep {
    PlanStep {
        id: Ulid::new().to_string(),
        description: s.description.clone(),
        tool: s.tool.clone(),
        params: s.params.clone(),
        expected_outcome: s.expected_outcome.clone(),
        verification_strategy: s.verification_strategy.clone(),
    }
}

fn parse_verification(strategy: &str, output: &str, exit_code: i32, success: bool) -> VerificationResult {
    if strategy == "exit_code_zero" {
        Verifier::verify_with_exit_code(&VerificationStrategy::ExitCodeZero, exit_code)
    } else if let Some(expected) = strategy.strip_prefix("output_contains:") {
        Verifier::verify_with_output(&VerificationStrategy::OutputContains { expected: expected.into() }, output)
    } else if let Some(path) = strategy.strip_prefix("file_exists:") {
        Verifier::verify_strategy(&VerificationStrategy::FileExists { path: std::path::PathBuf::from(path), content_contains: None })
    } else if success {
        VerificationResult::Verified { evidence: "worker reported success".into() }
    } else {
        VerificationResult::Failed { reason: "worker reported failure".into(), actual: "".into(), expected: "".into() }
    }
}

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
    supervisor: Option<crate::supervisor::Supervisor>,
}

impl Runtime {
    pub fn new(config: KernelConfig, cmd_rx: mpsc::Receiver<RuntimeCommand>) -> Result<Self, Box<dyn std::error::Error>> {
        let store = Store::open(&config.db_path)?;
        let interrupted = store.mark_interrupted_tasks().unwrap_or(0);
        if interrupted > 0 {
            println!("  [kernel] {interrupted} interrupted tasks marked as failed");
        }
        let event_bus = EventBus::new(256);
        Ok(Self {
            config,
            store,
            event_bus,
            tasks: HashMap::new(),
            cmd_rx,
            cognition_conn: None,
            worker_conn: None,
            supervisor: None,
        })
    }

    pub fn set_supervisor(&mut self, s: crate::supervisor::Supervisor) {
        self.supervisor = Some(s);
    }

    pub fn event_bus(&self) -> &EventBus {
        &self.event_bus
    }

    /// Create pipe endpoints (making them visible to clients) and return listeners.
    /// Call this BEFORE spawning workers, then call `await_workers` after spawning.
    pub fn listen(&self) -> Result<(ck_ipc::server::PipeListener, ck_ipc::server::PipeListener), ck_ipc::protocol::ProtocolError> {
        let cog = PipeServer::new(&self.config.cognition_pipe).listen()?;
        let wrk = PipeServer::new(&self.config.worker_pipe).listen()?;
        Ok((cog, wrk))
    }

    /// Wait for both workers to connect to already-listening pipe endpoints.
    pub async fn await_workers(
        &mut self,
        cog_listener: ck_ipc::server::PipeListener,
        wrk_listener: ck_ipc::server::PipeListener,
    ) {
        let timeout = tokio::time::Duration::from_secs(10);
        let (cog_result, wrk_result) = tokio::join!(
            tokio::time::timeout(timeout, cog_listener.accept()),
            tokio::time::timeout(timeout, wrk_listener.accept()),
        );
        match cog_result {
            Ok(Ok(conn)) => { println!("  [kernel] cognition engine connected"); self.cognition_conn = Some(conn); }
            Ok(Err(e)) => println!("  [kernel] cognition connect ERROR: {e}"),
            Err(_) => println!("  [kernel] cognition connect TIMED OUT"),
        }
        match wrk_result {
            Ok(Ok(conn)) => { println!("  [kernel] tool worker connected"); self.worker_conn = Some(conn); }
            Ok(Err(e)) => println!("  [kernel] worker connect ERROR: {e}"),
            Err(_) => println!("  [kernel] worker connect TIMED OUT"),
        }
    }

    pub async fn connect_workers(&mut self, cognition_pipe: &str, worker_pipe: &str) {
        let cog_server = PipeServer::new(cognition_pipe);
        let wrk_server = PipeServer::new(worker_pipe);
        let timeout = tokio::time::Duration::from_secs(10);

        let (cog_listener, wrk_listener) = match (cog_server.listen(), wrk_server.listen()) {
            (Ok(c), Ok(w)) => (c, w),
            (Err(e), _) | (_, Err(e)) => { tracing::warn!("failed to create pipe endpoints: {e}"); return; }
        };

        let (cog_result, wrk_result) = tokio::join!(
            tokio::time::timeout(timeout, cog_listener.accept()),
            tokio::time::timeout(timeout, wrk_listener.accept()),
        );

        match cog_result {
            Ok(Ok(conn)) => { tracing::info!("cognition connected"); self.cognition_conn = Some(conn); }
            Ok(Err(e)) => tracing::warn!("cognition connect failed: {e}"),
            Err(_) => tracing::warn!("cognition connect timed out"),
        }
        match wrk_result {
            Ok(Ok(conn)) => { tracing::info!("worker connected"); self.worker_conn = Some(conn); }
            Ok(Err(e)) => tracing::warn!("worker connect failed: {e}"),
            Err(_) => tracing::warn!("worker connect timed out"),
        }
    }

    pub async fn run(&mut self) {
        tracing::info!("runtime loop started");
        let mut last_health = tokio::time::Instant::now();
        loop {
            let mut channel_closed = false;
            loop {
                match self.cmd_rx.try_recv() {
                    Ok(cmd) => match cmd {
                        RuntimeCommand::CreateTask { goal } => self.handle_create_task(goal),
                        RuntimeCommand::PauseTask { task_id } => self.handle_pause_task(&task_id),
                        RuntimeCommand::ResumeTask { task_id } => self.handle_resume_task(&task_id),
                        RuntimeCommand::CancelTask { task_id } => self.handle_cancel_task(&task_id),
                        RuntimeCommand::Shutdown => {
                            tracing::info!("shutdown received");
                            return;
                        }
                    },
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => { channel_closed = true; break; }
                }
            }

            if last_health.elapsed() > tokio::time::Duration::from_secs(5) {
                if let Some(sup) = &mut self.supervisor {
                    let restarted = sup.check_and_restart();
                    for (wtype, _pid) in restarted {
                        match wtype {
                            crate::supervisor::WorkerType::Cognition => { self.cognition_conn = None; }
                            crate::supervisor::WorkerType::ToolWorker => { self.worker_conn = None; }
                        }
                    }
                }
                last_health = tokio::time::Instant::now();
            }

            let active_ids: Vec<String> = self.tasks.iter()
                .filter(|(_, t)| matches!(t.status(), TaskStatus::Created | TaskStatus::Planning | TaskStatus::Planned | TaskStatus::Executing))
                .map(|(id, _)| id.clone())
                .collect();

            for id in active_ids {
                self.step_task(&id).await;
            }

            // Remove terminal tasks
            self.tasks.retain(|_, t| !matches!(t.status(), TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Escalated));

            // Auto-shutdown: only when all tasks are done AND channel is closed
            if channel_closed && self.tasks.is_empty() {
                tracing::info!("all tasks complete and command channel closed, shutting down");
                return;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    fn handle_create_task(&mut self, goal: String) {
        let task = Task::new(goal.clone());
        let task_id = task.id().to_string();
        let _ = self.store.create_task(&task_id, &goal);
        self.emit(KernelEvent::TaskCreated {
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
        let cp = match self.store.load_latest_checkpoint(task_id) {
            Ok(Some(cp)) => cp,
            _ => { eprintln!("[resume] no checkpoint for {task_id}"); return; }
        };
        let data = match ck_memory::checkpoint::CheckpointData::deserialize(&cp.state_blob) {
            Ok(d) => d,
            Err(e) => { eprintln!("[resume] deserialize failed: {e}"); return; }
        };
        let Some(plan_json) = &data.plan_json else {
            eprintln!("[resume] no plan in checkpoint"); return;
        };
        let plan = match serde_json::from_str::<crate::task::Plan>(plan_json) {
            Ok(p) => p,
            Err(e) => { eprintln!("[resume] plan parse failed: {e}"); return; }
        };
        let mut task = crate::task::Task::new(data.goal.clone());
        let _ = task.transition_to(crate::task::TaskStatus::Planning);
        let _ = task.set_plan(plan); // -> Planned
        let _ = task.transition_to(crate::task::TaskStatus::Executing);
        for _ in 0..data.current_step {
            task.advance_step();
        }
        let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Executing);
        println!("[resume] task {task_id} resumed at step {}", data.current_step);
        self.tasks.insert(task_id.to_string(), task);
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

        let current_state: HashMap<String, serde_json::Value> = task
            .step_outputs
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect();

        let failure_context = if task.replan_count() > 0 {
            task.last_failure.as_ref().map(|f| {
                let mut m = HashMap::new();
                m.insert("reason".to_string(), serde_json::Value::String(f.clone()));
                m
            })
        } else {
            None
        };

        let Some(conn) = self.cognition_conn.as_mut() else {
            tracing::warn!(task_id = %task_id, "no cognition connection, task stays in Planning");
            return;
        };

        let request = CognitionRequest {
            request_type: if task.replan_count() > 0 { "replan".into() } else { "plan".into() },
            task_id: task_id.to_string(),
            objective,
            current_state,
            memory_context: HashMap::new(),
            failure_context,
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

        if response.response_type == "error" {
            eprintln!("  [cognition] planning error: {}", response.reasoning);
            if let Some(task) = self.tasks.get_mut(task_id) {
                let _ = task.transition_to(TaskStatus::Failed);
                let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Failed);
            }
            return;
        }

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
                    return;
                }
                let plan_id = task.plan().map(|p| p.id.clone()).unwrap_or_default();
                let step_count = task.plan().map(|p| p.steps.len()).unwrap_or(0);
                let plan_json = serde_json::to_string(task.plan().unwrap()).ok();
                // drop mutable borrow of task before calling self methods
                self.save_checkpoint(task_id);
                if let Some(pj) = plan_json {
                    let _ = self.store.update_task_plan(task_id, &pj);
                }
                self.emit(KernelEvent::PlanGenerated {
                    task_id: task_id.to_string(),
                    plan_id,
                    step_count,
                });
                tracing::info!(task_id = %task_id, "plan received and set");
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
            self.emit(KernelEvent::TaskCompleted {
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

        // Path sandbox: block absolute paths outside work_dir
        let work_dir = std::path::Path::new(&self.config.work_dir);
        for (key, val) in &step.params {
            if key == "path" || key == "work_dir" {
                if let Some(path_str) = val.as_str() {
                    let path = std::path::Path::new(path_str);
                    if path.is_absolute() && !path.starts_with(work_dir) {
                        eprintln!("  [sandbox] BLOCKED: '{}' is outside allowed dir '{}'",
                            path_str, self.config.work_dir);
                        if let Some(task) = self.tasks.get_mut(task_id) {
                            let _ = task.transition_to(TaskStatus::Failed);
                            let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Failed);
                        }
                        return;
                    }
                }
            }
        }

        self.emit(KernelEvent::ActionDispatched {
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
                    let _ = self.store.update_task_step(task_id, task.current_step() as i64);
                }
                if let Some(task) = self.tasks.get_mut(task_id) {
                    task.record_output(&step.description, &response.output);
                }
                let result_json = serde_json::json!({
                    "output": response.output,
                    "success": response.success,
                    "duration_ms": response.duration_ms
                }).to_string();
                let params_json = serde_json::to_string(&step.params).unwrap_or_default();
                let step_index = self.tasks.get(task_id).map(|t| t.current_step() as i64 - 1).unwrap_or(0);
                let _ = self.store.save_action(
                    &action_id,
                    task_id,
                    step_index,
                    &step.tool,
                    &params_json,
                    &result_json,
                    true,
                    response.duration_ms,
                );
                // Auto-check any side effects reported by the worker
                for side_effect in &response.side_effects {
                    if let Some(path) = side_effect.strip_prefix("write_file:").or_else(|| side_effect.strip_prefix("create_dir:")) {
                        let strat = ck_verify::strategies::VerificationStrategy::FileExists {
                            path: std::path::PathBuf::from(path),
                            content_contains: None,
                        };
                        if let ck_verify::strategies::VerificationResult::Failed { reason, .. } =
                            ck_verify::engine::Verifier::verify_strategy(&strat)
                        {
                            tracing::warn!(path = %path, reason = %reason, "side effect file not found after action");
                        }
                    }
                }
                self.save_checkpoint(task_id);
                self.emit(KernelEvent::VerificationPassed {
                    task_id: task_id.to_string(),
                    action_id,
                    evidence,
                });
            }
            VerificationResult::Failed { reason, .. } => {
                tracing::warn!(task_id = %task_id, action_id = %action_id, reason = %reason, "verification failed");
                if let Some(task) = self.tasks.get_mut(task_id) {
                    let failure_msg = format!("step '{}' [{}] failed: {}", step.description, step.tool, reason);
                    task.set_failure(&failure_msg);
                }
                let result_json = serde_json::json!({
                    "output": response.output,
                    "error": response.error,
                    "success": false,
                    "duration_ms": response.duration_ms
                }).to_string();
                let params_json = serde_json::to_string(&step.params).unwrap_or_default();
                let step_index = self.tasks.get(task_id).map(|t| t.current_step() as i64).unwrap_or(0);
                let _ = self.store.save_action(
                    &action_id,
                    task_id,
                    step_index,
                    &step.tool,
                    &params_json,
                    &result_json,
                    false,
                    response.duration_ms,
                );
                let (decision, _retry_count, _replan_count) = {
                    let task = self.tasks.get(task_id).unwrap();
                    let ctx = FailureContext {
                        task_id: task_id.to_string(),
                        action_id: action_id.clone(),
                        reason: reason.clone(),
                        retry_count: task.retry_count(),
                        replan_count: task.replan_count(),
                    };
                    let budget = RetryBudget::new(self.config.max_retries, self.config.max_replans);
                    (RecoveryEngine::decide(&ctx, &budget), task.retry_count(), task.replan_count())
                };

                match decision {
                    RecoveryDecision::Retry { backoff_ms } => {
                        let task = self.tasks.get_mut(task_id).unwrap();
                        task.increment_retry();
                        let _ = self.store.update_task_retry_budget(task_id, task.retry_count(), task.replan_count());
                        let attempt = task.retry_count();
                        self.emit(KernelEvent::RecoveryTriggered {
                            task_id: task_id.to_string(),
                            strategy: "retry".into(),
                            attempt,
                        });
                        tracing::info!(task_id = %task_id, backoff_ms, "retrying after backoff");
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    }
                    RecoveryDecision::Replan { failure_context } => {
                        let task = self.tasks.get_mut(task_id).unwrap();
                        task.increment_replan();
                        let attempt = task.replan_count();
                        task.start_replan();
                        let _ = self.store.update_task_retry_budget(task_id, task.retry_count(), task.replan_count());
                        let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Planning);
                        self.emit(KernelEvent::RecoveryTriggered {
                            task_id: task_id.to_string(),
                            strategy: "replan".into(),
                            attempt,
                        });
                        tracing::info!(task_id = %task_id, "replanning: {}", failure_context);
                    }
                    RecoveryDecision::Escalate { reason } => {
                        let task = self.tasks.get_mut(task_id).unwrap();
                        let _ = task.transition_to(TaskStatus::Verifying);
                        let _ = task.transition_to(TaskStatus::Recovering);
                        let _ = task.transition_to(TaskStatus::Escalated);
                        let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Escalated);
                        self.emit(KernelEvent::TaskFailed {
                            task_id: task_id.to_string(),
                            reason: reason.clone(),
                        });
                        tracing::error!(task_id = %task_id, reason = %reason, "task escalated");
                    }
                    RecoveryDecision::Rollback { .. } => {
                        if let Ok(Some(cp)) = self.store.load_latest_checkpoint(task_id) {
                            if let Ok(data) = ck_memory::checkpoint::CheckpointData::deserialize(&cp.state_blob) {
                                if let Some(pj) = &data.plan_json {
                                    if let Ok(plan) = serde_json::from_str::<crate::task::Plan>(pj) {
                                        if let Some(task) = self.tasks.get_mut(task_id) {
                                            task.start_replan();
                                            let _ = task.set_plan(plan);
                                            for _ in 0..data.current_step { task.advance_step(); }
                                            let _ = task.transition_to(TaskStatus::Executing);
                                            let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Executing);
                                            tracing::info!(task_id = %task_id, step = data.current_step, "rolled back");
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                        // fallback
                        if let Some(task) = self.tasks.get_mut(task_id) {
                            let _ = task.transition_to(TaskStatus::Verifying);
                            let _ = task.transition_to(TaskStatus::Recovering);
                            let _ = task.transition_to(TaskStatus::Escalated);
                            let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Escalated);
                        }
                        tracing::warn!(task_id = %task_id, "rollback fallback: escalating");
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
        self.emit(KernelEvent::CheckpointSaved {
            task_id: task_id.to_string(),
            checkpoint_id: cp_id,
        });
    }

    fn emit(&self, event: KernelEvent) {
        let task_id = event_task_id(&event);
        let event_type = event_type_name(&event);
        let payload = serde_json::to_string(&event).unwrap_or_default();
        let _ = self.store.append_event(task_id, event_type, &payload);
        self.event_bus.emit(event);
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

fn event_task_id(event: &KernelEvent) -> &str {
    match event {
        KernelEvent::TaskCreated { task_id, .. } => task_id,
        KernelEvent::PlanGenerated { task_id, .. } => task_id,
        KernelEvent::ActionDispatched { task_id, .. } => task_id,
        KernelEvent::ActionCompleted { task_id, .. } => task_id,
        KernelEvent::VerificationPassed { task_id, .. } => task_id,
        KernelEvent::VerificationFailed { task_id, .. } => task_id,
        KernelEvent::RecoveryTriggered { task_id, .. } => task_id,
        KernelEvent::TaskCompleted { task_id, .. } => task_id,
        KernelEvent::TaskFailed { task_id, .. } => task_id,
        KernelEvent::CheckpointSaved { task_id, .. } => task_id,
        KernelEvent::WorkerSpawned { .. } => "kernel",
        KernelEvent::WorkerCrashed { .. } => "kernel",
    }
}

fn event_type_name(event: &KernelEvent) -> &'static str {
    match event {
        KernelEvent::TaskCreated { .. } => "TaskCreated",
        KernelEvent::PlanGenerated { .. } => "PlanGenerated",
        KernelEvent::ActionDispatched { .. } => "ActionDispatched",
        KernelEvent::ActionCompleted { .. } => "ActionCompleted",
        KernelEvent::VerificationPassed { .. } => "VerificationPassed",
        KernelEvent::VerificationFailed { .. } => "VerificationFailed",
        KernelEvent::RecoveryTriggered { .. } => "RecoveryTriggered",
        KernelEvent::TaskCompleted { .. } => "TaskCompleted",
        KernelEvent::TaskFailed { .. } => "TaskFailed",
        KernelEvent::CheckpointSaved { .. } => "CheckpointSaved",
        KernelEvent::WorkerSpawned { .. } => "WorkerSpawned",
        KernelEvent::WorkerCrashed { .. } => "WorkerCrashed",
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

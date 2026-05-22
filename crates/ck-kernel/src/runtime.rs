use std::collections::HashMap;
use tokio::sync::mpsc;
use ulid::Ulid;

use ck_events::bus::EventBus;
use ck_events::types::KernelEvent;
use ck_ipc::types::CognitionRequest;
use ck_memory::checkpoint::CheckpointData;
use ck_memory::store::Store;

use crate::config::KernelConfig;
use crate::task::{Task, TaskStatus};

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
}

impl Runtime {
    pub fn new(config: KernelConfig, cmd_rx: mpsc::Receiver<RuntimeCommand>) -> Result<Self, Box<dyn std::error::Error>> {
        let store = Store::open(&config.db_path)?;
        let event_bus = EventBus::new(256);
        Ok(Self { config, store, event_bus, tasks: HashMap::new(), cmd_rx })
    }

    pub fn event_bus(&self) -> &EventBus {
        &self.event_bus
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
                self.step_task(&id);
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

    fn step_task(&mut self, task_id: &str) {
        let Some(task) = self.tasks.get_mut(task_id) else { return };
        match task.status() {
            TaskStatus::Created => self.request_plan(task_id),
            TaskStatus::Planned | TaskStatus::Executing => self.execute_next_step(task_id),
            _ => {}
        }
    }

    fn request_plan(&mut self, task_id: &str) {
        let Some(task) = self.tasks.get_mut(task_id) else { return };
        let request = CognitionRequest {
            request_type: "plan".into(),
            task_id: task_id.to_string(),
            objective: task.goal().to_string(),
            current_state: HashMap::new(),
            memory_context: HashMap::new(),
            failure_context: None,
        };
        tracing::info!(task_id = %task_id, "plan requested: {:?}", request.request_type);
        let _ = task.transition_to(TaskStatus::Planning);
        let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Planning);
    }

    fn execute_next_step(&mut self, task_id: &str) {
        let Some(task) = self.tasks.get_mut(task_id) else { return };
        if task.is_plan_complete() {
            let _ = task.transition_to(TaskStatus::Verifying);
            let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Verifying);
            tracing::info!(task_id = %task_id, "plan complete, verifying");
            return;
        }
        if task.status() == TaskStatus::Planned {
            let _ = task.transition_to(TaskStatus::Executing);
            let _ = self.store.update_task_status(task_id, ck_memory::store::TaskStatus::Executing);
        }
        let action_id = Ulid::new().to_string();
        let tool = task.current_plan_step().map(|s| s.tool.clone()).unwrap_or_default();
        self.event_bus.emit(KernelEvent::ActionDispatched {
            task_id: task_id.to_string(),
            action_id: action_id.clone(),
            tool: tool.clone(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        });
        tracing::info!(task_id = %task_id, action_id = %action_id, tool = %tool, "step dispatched");
        let task = self.tasks.get_mut(task_id).unwrap();
        task.advance_step();
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

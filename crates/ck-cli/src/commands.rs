use ck_kernel::config::KernelConfig;
use ck_kernel::runtime::{Runtime, RuntimeCommand};
use ck_kernel::supervisor::Supervisor;
use ck_memory::store::Store;
use tokio::sync::mpsc;

pub async fn cmd_start(goal: String) {
    let config = KernelConfig::default();

    // Build Go worker binary if it doesn't exist yet
    let worker_bin = std::path::Path::new(&config.worker_bin);
    if !worker_bin.exists() {
        println!("Building Go worker...");
        let status = std::process::Command::new("go")
            .args(["build", "-o", &config.worker_bin, "./cmd/ck-worker"])
            .current_dir("workers")
            .status();
        match status {
            Ok(s) if s.success() => println!("Worker built."),
            Ok(s) => { eprintln!("go build failed: exit {s}"); return; }
            Err(e) => { eprintln!("go build error: {e}"); return; }
        }
    }

    let (cmd_tx, cmd_rx) = mpsc::channel(16);
    let mut runtime = match Runtime::new(config.clone(), cmd_rx) {
        Ok(r) => r,
        Err(e) => { eprintln!("Failed to create runtime: {e}"); return; }
    };

    // Step 1: Create pipe endpoints FIRST so workers can find them on connect
    println!("Opening pipe endpoints...");
    let (cog_listener, wrk_listener) = match runtime.listen() {
        Ok(l) => l,
        Err(e) => { eprintln!("Failed to create pipes: {e}"); return; }
    };

    // Step 2: Spawn workers — pipes now exist for them to connect to
    let pipe_prefix = r"\\.\pipe\";
    let cognition_pipe_path = format!("{}{}", pipe_prefix, config.cognition_pipe);
    let worker_pipe_path = format!("{}{}", pipe_prefix, config.worker_pipe);

    let mut supervisor = Supervisor::new("ck");
    println!("Spawning Go worker...");
    match supervisor.spawn_tool_worker(&config.worker_bin, &worker_pipe_path) {
        Ok(pid) => println!("  Worker PID: {pid}"),
        Err(e) => { eprintln!("Failed to spawn worker: {e}"); return; }
    }
    println!("Spawning Python cognition...");
    // Run as module (-m) from the cognition directory to fix relative imports
    let cognition_dir = std::path::Path::new(&config.cognition_script)
        .parent().and_then(|p| p.parent())
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|| "cognition".into());
    match supervisor.spawn_cognition_module(&config.python_bin, &cognition_dir, &cognition_pipe_path) {
        Ok(pid) => println!("  Cognition PID: {pid}"),
        Err(e) => { eprintln!("Failed to spawn cognition: {e}"); return; }
    }

    // Step 3: Wait for both workers to connect
    println!("Waiting for workers to connect...");
    runtime.await_workers(cog_listener, wrk_listener).await;
    runtime.set_supervisor(supervisor);

    println!("Starting task: {goal}");
    cmd_tx.send(RuntimeCommand::CreateTask { goal }).await.ok();
    drop(cmd_tx);

    runtime.run().await;

    // Show what happened to the task
    if let Ok(store) = Store::open(&config.db_path) {
        if let Ok(tasks) = store.list_tasks() {
            // Find the most recently created task
            if let Some(task) = tasks.first() {
                let icon = match task.status {
                    ck_memory::store::TaskStatus::Completed => "✓",
                    ck_memory::store::TaskStatus::Failed => "✗",
                    ck_memory::store::TaskStatus::Escalated => "⚠",
                    _ => "?",
                };
                println!("{} Task {} — {} (step {}/{})",
                    icon,
                    &task.id[..task.id.len().min(12)],
                    task.status.as_str(),
                    task.current_step,
                    task.plan_json.as_ref()
                        .and_then(|p| serde_json::from_str::<serde_json::Value>(p).ok())
                        .and_then(|v| v["steps"].as_array().map(|s| s.len()))
                        .unwrap_or(0)
                );
                println!("  Run `ck trace {}` to see what happened.", &task.id[..task.id.len().min(12)]);
            }
        }
    }
}

pub fn cmd_status(task_id: Option<String>) {
    let config = KernelConfig::default();
    let store = match Store::open(&config.db_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to open store: {e}");
            return;
        }
    };

    match task_id {
        Some(id) => match store.get_task(&id) {
            Ok(Some(task)) => {
                println!("Task: {}", task.id);
                println!("  Goal:   {}", task.goal);
                println!("  Status: {}", task.status.as_str());
                println!("  Step:   {}", task.current_step);
            }
            Ok(None) => eprintln!("Task not found: {id}"),
            Err(e) => eprintln!("Error: {e}"),
        },
        None => {
            match store.list_tasks() {
                Ok(tasks) if tasks.is_empty() => println!("No tasks found."),
                Ok(tasks) => {
                    println!("{:<12} {:<12} {:<6} {}", "ID", "Status", "Step", "Goal");
                    println!("{}", "-".repeat(70));
                    for t in tasks {
                        let id_short = &t.id[..t.id.len().min(12)];
                        let goal_short: String = t.goal.chars().take(40).collect();
                        println!("{:<12} {:<12} {:<6} {}", id_short, t.status.as_str(), t.current_step, goal_short);
                    }
                }
                Err(e) => eprintln!("Error: {e}"),
            }
        }
    }
}

pub fn cmd_trace(task_id: String) {
    let config = KernelConfig::default();
    let store = match Store::open(&config.db_path) {
        Ok(s) => s,
        Err(e) => { eprintln!("Failed to open store: {e}"); return; }
    };

    // Show the plan if available
    if let Ok(Some(task)) = store.get_task(&task_id) {
        println!("Task: {} | Status: {} | Step: {}", &task_id[..task_id.len().min(12)], task.status.as_str(), task.current_step);
        println!("Goal: {}", task.goal);
        if let Some(plan_json) = &task.plan_json {
            if let Ok(plan) = serde_json::from_str::<serde_json::Value>(plan_json) {
                if let Some(steps) = plan["steps"].as_array() {
                    println!("\nPlan ({} steps):", steps.len());
                    for (i, step) in steps.iter().enumerate() {
                        let desc = step["description"].as_str().unwrap_or("?");
                        let tool = step["tool"].as_str().unwrap_or("?");
                        let marker = if (i as i64) < task.current_step { "\u{2713}" } else { "o" };
                        println!("  {} Step {}: [{}] {}", marker, i + 1, tool, desc);
                    }
                    println!();
                }
            }
        }
    }

    // Show events
    match store.replay_events(&task_id) {
        Ok(events) if events.is_empty() => println!("No events found."),
        Ok(events) => {
            println!("Events ({}):", events.len());
            for ev in &events {
                let ts = chrono::DateTime::from_timestamp(ev.timestamp, 0)
                    .map(|dt: chrono::DateTime<chrono::Utc>| dt.format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| ev.timestamp.to_string());
                let summary = summarize_event(&ev.event_type, &ev.payload_json);
                println!("  [{}] {} {}", ts, ev.event_type, summary);
            }
        }
        Err(e) => eprintln!("Error: {e}"),
    }
}

fn summarize_event(event_type: &str, payload_json: &str) -> String {
    let v = match serde_json::from_str::<serde_json::Value>(payload_json) {
        Ok(v) => v,
        Err(_) => return String::new(),
    };
    let inner = v.get(event_type).unwrap_or(&v);
    match event_type {
        "ActionDispatched" => format!("-> tool={}", inner["tool"].as_str().unwrap_or("?")),
        "VerificationPassed" => format!("ok {}", inner["evidence"].as_str().unwrap_or("")),
        "VerificationFailed" => format!("FAIL {}", inner["reason"].as_str().unwrap_or("")),
        "RecoveryTriggered" => format!("retry {} attempt={}", inner["strategy"].as_str().unwrap_or("?"), inner["attempt"]),
        "TaskCompleted" => format!("DONE steps={}", inner["steps_executed"]),
        "TaskFailed" => format!("FAIL {}", inner["reason"].as_str().unwrap_or("")),
        _ => String::new(),
    }
}

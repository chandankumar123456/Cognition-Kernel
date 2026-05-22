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

    let pipe_prefix = r"\\.\pipe\";
    let cognition_pipe_path = format!("{}{}", pipe_prefix, config.cognition_pipe);
    let worker_pipe_path = format!("{}{}", pipe_prefix, config.worker_pipe);

    // Spawn workers
    let mut supervisor = Supervisor::new("ck");
    println!("Spawning Go worker...");
    match supervisor.spawn_tool_worker(&config.worker_bin, &worker_pipe_path) {
        Ok(pid) => println!("  Worker PID: {pid}"),
        Err(e) => { eprintln!("Failed to spawn worker: {e}"); return; }
    }
    println!("Spawning Python cognition...");
    match supervisor.spawn_cognition(&config.python_bin, &config.cognition_script, &cognition_pipe_path) {
        Ok(pid) => println!("  Cognition PID: {pid}"),
        Err(e) => { eprintln!("Failed to spawn cognition: {e}"); return; }
    }

    // Give workers a moment to start before the kernel tries to connect
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let (cmd_tx, cmd_rx) = mpsc::channel(16);
    let mut runtime = match Runtime::new(config.clone(), cmd_rx) {
        Ok(r) => r,
        Err(e) => { eprintln!("Failed to create runtime: {e}"); return; }
    };

    println!("Connecting to workers...");
    runtime.connect_workers(&config.cognition_pipe, &config.worker_pipe).await;

    println!("Starting task: {goal}");
    cmd_tx.send(RuntimeCommand::CreateTask { goal }).await.ok();
    drop(cmd_tx);

    runtime.run().await;
    println!("Done.");
    // supervisor dropped here — kills spawned workers on exit
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
        Err(e) => {
            eprintln!("Failed to open store: {e}");
            return;
        }
    };

    match store.replay_events(&task_id) {
        Ok(events) if events.is_empty() => println!("No events found for task: {task_id}"),
        Ok(events) => {
            println!("Trace for task: {task_id}");
            for ev in events {
                let ts = chrono::DateTime::from_timestamp(ev.timestamp, 0)
                    .map(|dt: chrono::DateTime<chrono::Utc>| dt.format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| ev.timestamp.to_string());
                println!("  [{ts}] {}: {}", ev.event_type, ev.payload_json);
            }
        }
        Err(e) => eprintln!("Error replaying events: {e}"),
    }
}

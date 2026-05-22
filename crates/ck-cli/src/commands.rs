use ck_kernel::config::KernelConfig;
use ck_kernel::runtime::{Runtime, RuntimeCommand};
use ck_memory::store::Store;
use tokio::sync::mpsc;

pub async fn cmd_start(goal: String) {
    let config = KernelConfig::default();
    let (cmd_tx, cmd_rx) = mpsc::channel(16);
    let mut runtime = match Runtime::new(config, cmd_rx) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to create runtime: {e}");
            return;
        }
    };

    cmd_tx.send(RuntimeCommand::CreateTask { goal: goal.clone() }).await.ok();
    cmd_tx.send(RuntimeCommand::Shutdown).await.ok();

    println!("Starting task: {goal}");
    runtime.run().await;
    println!("Done.");
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
        None => println!("No task_id provided. Use `ck status <task_id>` to query a specific task."),
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
                    .map(|dt| dt.format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| ev.timestamp.to_string());
                println!("  [{ts}] {}: {}", ev.event_type, ev.payload_json);
            }
        }
        Err(e) => eprintln!("Error replaying events: {e}"),
    }
}

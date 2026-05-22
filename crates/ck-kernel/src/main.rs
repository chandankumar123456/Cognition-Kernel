use std::env;
use tokio::sync::mpsc;
use ck_kernel::config::KernelConfig;
use ck_kernel::runtime::{Runtime, RuntimeCommand};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().json().init();
    tracing::info!("Cognition Kernel starting");

    let args: Vec<String> = env::args().collect();
    let daemon_mode = args.contains(&"--daemon".to_string());

    let config = KernelConfig::default();
    let cognition_pipe = config.cognition_pipe.clone();
    let worker_pipe = config.worker_pipe.clone();
    let (cmd_tx, cmd_rx) = mpsc::channel::<RuntimeCommand>(64);

    let mut runtime = Runtime::new(config.clone(), cmd_rx).expect("failed to init runtime");

    // Shutdown on ctrl+c
    let tx_ctrlc = cmd_tx.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = tx_ctrlc.send(RuntimeCommand::Shutdown).await;
    });

    if daemon_mode {
        // Listen on control pipe for commands from CLI
        let tx_ctrl = cmd_tx.clone();
        tokio::spawn(async move {
            let control_server = ck_ipc::server::PipeServer::new("ck-control");
            loop {
                let listener = match control_server.listen() {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::warn!("control pipe listen failed: {e}");
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        continue;
                    }
                };
                let mut conn = match listener.accept().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!("control pipe accept failed: {e}");
                        continue;
                    }
                };
                // Handle commands from this client connection
                loop {
                    let msg: Result<serde_json::Value, _> = conn.read().await;
                    match msg {
                        Ok(v) => match v["cmd"].as_str() {
                            Some("create_task") => {
                                if let Some(goal) = v["goal"].as_str() {
                                    tracing::info!(goal = %goal, "daemon received task");
                                    let _ = tx_ctrl.send(RuntimeCommand::CreateTask {
                                        goal: goal.to_string(),
                                    }).await;
                                    let _ = conn.write(&serde_json::json!({"status": "queued"})).await;
                                }
                            }
                            Some("shutdown") => {
                                let _ = tx_ctrl.send(RuntimeCommand::Shutdown).await;
                                return;
                            }
                            Some("ping") => {
                                let _ = conn.write(&serde_json::json!({"status": "ok"})).await;
                            }
                            _ => {}
                        },
                        Err(_) => break, // client disconnected
                    }
                }
            }
        });
        println!("Daemon mode: listening on \\\\.\\pipe\\ck-control");
    }

    runtime.connect_workers(&cognition_pipe, &worker_pipe).await;
    runtime.run().await;
    tracing::info!("Cognition Kernel stopped");
}

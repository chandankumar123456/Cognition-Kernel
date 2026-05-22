use tokio::sync::mpsc;
use ck_kernel::config::KernelConfig;
use ck_kernel::runtime::Runtime;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().json().init();
    tracing::info!("Cognition Kernel starting");

    let config = KernelConfig::default();
    let (cmd_tx, cmd_rx) = mpsc::channel(64);

    let mut runtime = Runtime::new(config, cmd_rx).expect("failed to init runtime");

    // Shutdown on ctrl+c
    let tx = cmd_tx.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = tx.send(ck_kernel::runtime::RuntimeCommand::Shutdown).await;
    });

    runtime.run().await;
    tracing::info!("Cognition Kernel stopped");
}

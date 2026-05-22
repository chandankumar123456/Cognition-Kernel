use tracing_subscriber;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().json().init();
    tracing::info!("Cognition Kernel starting");
}

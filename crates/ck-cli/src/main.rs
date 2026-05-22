mod commands;

use clap::Parser;

#[derive(Parser)]
#[command(name = "ck", about = "Cognition Kernel CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Start a new task with the given goal
    Start { goal: String },
    /// Show status of a task (or list tasks)
    Status { task_id: Option<String> },
    /// Pause a running task
    Pause { task_id: String },
    /// Resume a paused task
    Resume { task_id: String },
    /// Cancel a task
    Cancel { task_id: String },
    /// Show event trace for a task
    Trace { task_id: String },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Start { goal } => commands::cmd_start(goal).await,
        Commands::Status { task_id } => commands::cmd_status(task_id),
        Commands::Pause { .. } => println!("Pause requires daemon mode (not yet implemented)."),
        Commands::Resume { .. } => println!("Resume requires daemon mode (not yet implemented)."),
        Commands::Cancel { .. } => println!("Cancel requires daemon mode (not yet implemented)."),
        Commands::Trace { task_id } => commands::cmd_trace(task_id),
    }
}

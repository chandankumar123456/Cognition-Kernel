use clap::Parser;

#[derive(Parser)]
#[command(name = "ck", about = "Cognition Kernel CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    Start { goal: String },
    Status { task_id: Option<String> },
}

fn main() {
    let _cli = Cli::parse();
    println!("Cognition Kernel CLI");
}

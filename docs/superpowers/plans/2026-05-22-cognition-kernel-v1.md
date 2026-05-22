# Cognition Kernel V1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a persistent local-first autonomous cognitive runtime with Rust kernel, Python cognition, and Go tool workers — full vertical slice from plan through execution to verified completion.

**Architecture:** Rust kernel (tokio) owns lifecycle/state/events/verification/recovery. Python process handles LLM cognition via LiteLLM. Go process executes tools (shell/filesystem) in isolation. IPC via Named Pipes + MessagePack.

**Tech Stack:** Rust (tokio, rusqlite, rmp-serde, clap, ratatui, bincode, ulid), Python (LiteLLM, msgpack, uv), Go (msgpack, os/exec)

---

## File Structure

### Rust Workspace (crates/)

| File | Responsibility |
|------|---------------|
| `Cargo.toml` | Workspace root, member definitions |
| `crates/ck-kernel/Cargo.toml` | Kernel crate dependencies |
| `crates/ck-kernel/src/main.rs` | Entry point, tokio runtime bootstrap |
| `crates/ck-kernel/src/runtime.rs` | Main execution loop |
| `crates/ck-kernel/src/task.rs` | Task model, type-safe FSM |
| `crates/ck-kernel/src/scheduler.rs` | Priority queue scheduling |
| `crates/ck-kernel/src/supervisor.rs` | Worker process spawn/monitor/restart |
| `crates/ck-kernel/src/config.rs` | Runtime configuration |
| `crates/ck-events/Cargo.toml` | Events crate dependencies |
| `crates/ck-events/src/lib.rs` | Re-exports |
| `crates/ck-events/src/bus.rs` | Broadcast channel event bus |
| `crates/ck-events/src/types.rs` | KernelEvent enum |
| `crates/ck-events/src/log.rs` | Append-only event persistence |
| `crates/ck-memory/Cargo.toml` | Memory crate dependencies |
| `crates/ck-memory/src/lib.rs` | Re-exports |
| `crates/ck-memory/src/store.rs` | SQLite CRUD operations |
| `crates/ck-memory/src/schema.rs` | Schema creation + migrations |
| `crates/ck-memory/src/checkpoint.rs` | Bincode checkpoint serialization |
| `crates/ck-ipc/Cargo.toml` | IPC crate dependencies |
| `crates/ck-ipc/src/lib.rs` | Re-exports |
| `crates/ck-ipc/src/protocol.rs` | MessagePack framing (length-prefix + serialize) |
| `crates/ck-ipc/src/server.rs` | Named pipe server (kernel side) |
| `crates/ck-ipc/src/types.rs` | CognitionRequest/Response, ExecutionRequest/Response |
| `crates/ck-verify/Cargo.toml` | Verification crate dependencies |
| `crates/ck-verify/src/lib.rs` | Re-exports |
| `crates/ck-verify/src/engine.rs` | Verification dispatch |
| `crates/ck-verify/src/strategies.rs` | FileExists, ExitCodeZero, OutputContains, etc. |
| `crates/ck-recovery/Cargo.toml` | Recovery crate dependencies |
| `crates/ck-recovery/src/lib.rs` | Re-exports |
| `crates/ck-recovery/src/engine.rs` | Recovery decision logic |
| `crates/ck-recovery/src/budget.rs` | Retry/replan budget tracking |
| `crates/ck-cli/Cargo.toml` | CLI crate dependencies |
| `crates/ck-cli/src/main.rs` | CLI entry point, clap commands |
| `crates/ck-cli/src/commands.rs` | Command handlers (start, status, pause, etc.) |
| `crates/ck-cli/src/tui.rs` | ratatui live view |

### Python Cognition (cognition/)

| File | Responsibility |
|------|---------------|
| `cognition/pyproject.toml` | Python project config, dependencies |
| `cognition/cognition_kernel/__init__.py` | Package init |
| `cognition/cognition_kernel/engine.py` | Main cognition loop (listen for requests, respond) |
| `cognition/cognition_kernel/planner.py` | LLM-based plan generation |
| `cognition/cognition_kernel/reasoner.py` | Reflection, evaluation |
| `cognition/cognition_kernel/context.py` | Context assembly for LLM calls |
| `cognition/cognition_kernel/models.py` | Data models (PlanStep, CognitionRequest, etc.) |
| `cognition/cognition_kernel/ipc.py` | MessagePack IPC client (Named Pipe) |
| `cognition/tests/test_planner.py` | Planner unit tests |
| `cognition/tests/test_ipc.py` | IPC protocol tests |

### Go Workers (workers/)

| File | Responsibility |
|------|---------------|
| `workers/go.mod` | Go module definition |
| `workers/cmd/ck-worker/main.go` | Worker process entry, router |
| `workers/internal/ipc/client.go` | MessagePack IPC client (Named Pipe) |
| `workers/internal/shell/executor.go` | Shell command execution |
| `workers/internal/filesystem/worker.go` | File operations |
| `workers/pkg/protocol/types.go` | Shared message types |
| `workers/internal/shell/executor_test.go` | Shell executor tests |
| `workers/internal/filesystem/worker_test.go` | Filesystem worker tests |

---

## Task Dependency Order

```
Task 1: Project scaffold + workspace setup
Task 2: Event types + event bus (ck-events)
Task 3: Memory system + SQLite schema (ck-memory)
Task 4: IPC protocol + Named Pipe server (ck-ipc)
Task 5: Verification engine (ck-verify)
Task 6: Recovery engine (ck-recovery)
Task 7: Task model + state machine (in ck-kernel)
Task 8: Runtime kernel main loop (ck-kernel)
Task 9: Worker supervisor (ck-kernel)
Task 10: Go tool workers (workers/)
Task 11: Python cognition engine (cognition/)
Task 12: CLI interface (ck-cli)
Task 13: Integration test — full end-to-end loop
Task 14: Checkpoint resume test
```

---


### Task 1: Project Scaffold + Workspace Setup

**Files:**
- Create: `Cargo.toml`
- Create: `crates/ck-kernel/Cargo.toml`
- Create: `crates/ck-kernel/src/main.rs`
- Create: `crates/ck-events/Cargo.toml`
- Create: `crates/ck-events/src/lib.rs`
- Create: `crates/ck-memory/Cargo.toml`
- Create: `crates/ck-memory/src/lib.rs`
- Create: `crates/ck-ipc/Cargo.toml`
- Create: `crates/ck-ipc/src/lib.rs`
- Create: `crates/ck-verify/Cargo.toml`
- Create: `crates/ck-verify/src/lib.rs`
- Create: `crates/ck-recovery/Cargo.toml`
- Create: `crates/ck-recovery/src/lib.rs`
- Create: `crates/ck-cli/Cargo.toml`
- Create: `crates/ck-cli/src/main.rs`
- Create: `cognition/pyproject.toml`
- Create: `cognition/cognition_kernel/__init__.py`
- Create: `workers/go.mod`
- Create: `workers/cmd/ck-worker/main.go`
- Create: `.gitignore`

- [ ] **Step 1: Initialize git repo**

```bash
cd "E:\Projects\Cognition Kernel"
git init
```

- [ ] **Step 2: Create .gitignore**

```gitignore
# Rust
target/
Cargo.lock

# Python
__pycache__/
*.pyc
.venv/
cognition/.venv/

# Go
workers/bin/

# Runtime
*.db
*.db-wal
*.db-shm

# IDE
.idea/
.vscode/
```

- [ ] **Step 3: Create Rust workspace Cargo.toml**

```toml
[workspace]
resolver = "2"
members = [
    "crates/ck-kernel",
    "crates/ck-events",
    "crates/ck-memory",
    "crates/ck-ipc",
    "crates/ck-verify",
    "crates/ck-recovery",
    "crates/ck-cli",
]

[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
rusqlite = { version = "0.31", features = ["bundled"] }
rmp-serde = "1"
bincode = "1"
ulid = "1"
thiserror = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json"] }
chrono = { version = "0.4", features = ["serde"] }
```

- [ ] **Step 4: Create ck-events crate**

`crates/ck-events/Cargo.toml`:
```toml
[package]
name = "ck-events"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
ulid = { workspace = true }
```

`crates/ck-events/src/lib.rs`:
```rust
pub mod bus;
pub mod types;
pub mod log;
```

- [ ] **Step 5: Create ck-memory crate**

`crates/ck-memory/Cargo.toml`:
```toml
[package]
name = "ck-memory"
version = "0.1.0"
edition = "2021"

[dependencies]
rusqlite = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
bincode = { workspace = true }
ulid = { workspace = true }
thiserror = { workspace = true }
chrono = { workspace = true }
```

`crates/ck-memory/src/lib.rs`:
```rust
pub mod store;
pub mod schema;
pub mod checkpoint;
```

- [ ] **Step 6: Create ck-ipc crate**

`crates/ck-ipc/Cargo.toml`:
```toml
[package]
name = "ck-ipc"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { workspace = true }
serde = { workspace = true }
rmp-serde = { workspace = true }
thiserror = { workspace = true }

[target.'cfg(windows)'.dependencies]
tokio = { workspace = true, features = ["net"] }
```

`crates/ck-ipc/src/lib.rs`:
```rust
pub mod protocol;
pub mod server;
pub mod types;
```

- [ ] **Step 7: Create ck-verify crate**

`crates/ck-verify/Cargo.toml`:
```toml
[package]
name = "ck-verify"
version = "0.1.0"
edition = "2021"

[dependencies]
serde = { workspace = true }
thiserror = { workspace = true }
chrono = { workspace = true }
```

`crates/ck-verify/src/lib.rs`:
```rust
pub mod engine;
pub mod strategies;
```

- [ ] **Step 8: Create ck-recovery crate**

`crates/ck-recovery/Cargo.toml`:
```toml
[package]
name = "ck-recovery"
version = "0.1.0"
edition = "2021"

[dependencies]
serde = { workspace = true }
thiserror = { workspace = true }
chrono = { workspace = true }
```

`crates/ck-recovery/src/lib.rs`:
```rust
pub mod engine;
pub mod budget;
```

- [ ] **Step 9: Create ck-kernel crate**

`crates/ck-kernel/Cargo.toml`:
```toml
[package]
name = "ck-kernel"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "ck-kernel"
path = "src/main.rs"

[dependencies]
ck-events = { path = "../ck-events" }
ck-memory = { path = "../ck-memory" }
ck-ipc = { path = "../ck-ipc" }
ck-verify = { path = "../ck-verify" }
ck-recovery = { path = "../ck-recovery" }
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
ulid = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
thiserror = { workspace = true }
chrono = { workspace = true }
```

`crates/ck-kernel/src/main.rs`:
```rust
use tracing_subscriber;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().json().init();
    tracing::info!("Cognition Kernel starting");
}
```

- [ ] **Step 10: Create ck-cli crate**

`crates/ck-cli/Cargo.toml`:
```toml
[package]
name = "ck-cli"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "ck"
path = "src/main.rs"

[dependencies]
clap = { version = "4", features = ["derive"] }
tokio = { workspace = true }
```

`crates/ck-cli/src/main.rs`:
```rust
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
```

- [ ] **Step 11: Create Python cognition scaffold**

`cognition/pyproject.toml`:
```toml
[project]
name = "cognition-kernel"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "litellm>=1.40.0",
    "msgpack>=1.0.0",
]

[project.optional-dependencies]
dev = ["pytest>=8.0.0"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

`cognition/cognition_kernel/__init__.py`:
```python
"""Cognition Kernel - LLM-powered cognition engine."""
```

- [ ] **Step 12: Create Go workers scaffold**

`workers/go.mod`:
```go
module github.com/cognition-kernel/workers

go 1.22

require github.com/vmihailenco/msgpack/v5 v5.4.1
```

`workers/cmd/ck-worker/main.go`:
```go
package main

import "fmt"

func main() {
	fmt.Println("Cognition Kernel Worker starting")
}
```

- [ ] **Step 13: Verify workspace compiles**

Run: `cargo build`
Expected: All crates compile (with empty module files as needed)

- [ ] **Step 14: Commit**

```bash
git add -A
git commit -m "feat: initialize workspace scaffold (Rust/Python/Go)"
```

---


### Task 2: Event Types + Event Bus (ck-events)

**Files:**
- Create: `crates/ck-events/src/types.rs`
- Create: `crates/ck-events/src/bus.rs`
- Create: `crates/ck-events/src/log.rs`
- Test: `crates/ck-events/tests/bus_test.rs`

- [ ] **Step 1: Write test for event bus**

`crates/ck-events/tests/bus_test.rs`:
```rust
use ck_events::bus::EventBus;
use ck_events::types::KernelEvent;
use ulid::Ulid;

#[tokio::test]
async fn test_emit_and_receive() {
    let bus = EventBus::new(128);
    let mut rx = bus.subscribe();

    let task_id = Ulid::new().to_string();
    let event = KernelEvent::TaskCreated {
        task_id: task_id.clone(),
        goal: "test goal".into(),
        timestamp: chrono::Utc::now().timestamp_millis(),
    };

    bus.emit(event.clone());

    let received = rx.recv().await.unwrap();
    match received {
        KernelEvent::TaskCreated { task_id: id, .. } => assert_eq!(id, task_id),
        _ => panic!("wrong event type"),
    }
}

#[tokio::test]
async fn test_multiple_subscribers() {
    let bus = EventBus::new(128);
    let mut rx1 = bus.subscribe();
    let mut rx2 = bus.subscribe();

    bus.emit(KernelEvent::TaskCompleted {
        task_id: "t1".into(),
        duration_ms: 100,
        steps_executed: 3,
    });

    assert!(rx1.recv().await.is_ok());
    assert!(rx2.recv().await.is_ok());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ck-events`
Expected: FAIL — modules don't exist yet

- [ ] **Step 3: Implement event types**

`crates/ck-events/src/types.rs`:
```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KernelEvent {
    TaskCreated {
        task_id: String,
        goal: String,
        timestamp: i64,
    },
    PlanGenerated {
        task_id: String,
        plan_id: String,
        step_count: usize,
    },
    ActionDispatched {
        task_id: String,
        action_id: String,
        tool: String,
        timestamp: i64,
    },
    ActionCompleted {
        task_id: String,
        action_id: String,
        success: bool,
        duration_ms: u64,
    },
    VerificationPassed {
        task_id: String,
        action_id: String,
        evidence: String,
    },
    VerificationFailed {
        task_id: String,
        action_id: String,
        reason: String,
        expected: String,
        actual: String,
    },
    RecoveryTriggered {
        task_id: String,
        strategy: String,
        attempt: u32,
    },
    TaskCompleted {
        task_id: String,
        duration_ms: u64,
        steps_executed: usize,
    },
    TaskFailed {
        task_id: String,
        reason: String,
    },
    CheckpointSaved {
        task_id: String,
        checkpoint_id: String,
    },
    WorkerSpawned {
        worker_type: String,
        pid: u32,
    },
    WorkerCrashed {
        worker_type: String,
        pid: u32,
        exit_code: i32,
    },
}
```

- [ ] **Step 4: Implement event bus**

`crates/ck-events/src/bus.rs`:
```rust
use tokio::sync::broadcast;
use crate::types::KernelEvent;

#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<KernelEvent>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn emit(&self, event: KernelEvent) {
        // Ignore error if no receivers (acceptable during startup)
        let _ = self.tx.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<KernelEvent> {
        self.tx.subscribe()
    }
}
```

- [ ] **Step 5: Implement event log (append-only persistence stub)**

`crates/ck-events/src/log.rs`:
```rust
use crate::types::KernelEvent;

/// Append-only event log. Actual persistence implemented in ck-memory.
/// This module defines the trait for event logging.
pub trait EventLog: Send + Sync {
    fn append(&self, event: &KernelEvent) -> Result<u64, EventLogError>;
    fn replay(&self, task_id: &str) -> Result<Vec<KernelEvent>, EventLogError>;
}

#[derive(Debug, thiserror::Error)]
pub enum EventLogError {
    #[error("storage error: {0}")]
    Storage(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}
```

Add `thiserror` to ck-events Cargo.toml dependencies:
```toml
thiserror = { workspace = true }
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p ck-events`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/ck-events/
git commit -m "feat(events): implement event bus with broadcast channels and event types"
```

---


### Task 3: Memory System + SQLite Schema (ck-memory)

**Files:**
- Create: `crates/ck-memory/src/schema.rs`
- Create: `crates/ck-memory/src/store.rs`
- Create: `crates/ck-memory/src/checkpoint.rs`
- Test: `crates/ck-memory/tests/store_test.rs`

- [ ] **Step 1: Write test for store operations**

`crates/ck-memory/tests/store_test.rs`:
```rust
use ck_memory::store::Store;
use ck_memory::store::{TaskRecord, TaskStatus};

#[test]
fn test_create_and_get_task() {
    let store = Store::open_in_memory().unwrap();
    let task = TaskRecord {
        id: "01HX1234".into(),
        goal: "test goal".into(),
        status: TaskStatus::Created,
        plan_json: None,
        current_step: 0,
        retry_budget_json: None,
        created_at: 1000,
        updated_at: 1000,
    };
    store.create_task(&task).unwrap();
    let fetched = store.get_task("01HX1234").unwrap().unwrap();
    assert_eq!(fetched.goal, "test goal");
    assert_eq!(fetched.status, TaskStatus::Created);
}

#[test]
fn test_update_task_status() {
    let store = Store::open_in_memory().unwrap();
    let task = TaskRecord {
        id: "01HX5678".into(),
        goal: "another goal".into(),
        status: TaskStatus::Created,
        plan_json: None,
        current_step: 0,
        retry_budget_json: None,
        created_at: 1000,
        updated_at: 1000,
    };
    store.create_task(&task).unwrap();
    store.update_task_status("01HX5678", TaskStatus::Planned, 2000).unwrap();
    let fetched = store.get_task("01HX5678").unwrap().unwrap();
    assert_eq!(fetched.status, TaskStatus::Planned);
}

#[test]
fn test_append_and_replay_events() {
    let store = Store::open_in_memory().unwrap();
    store.append_event("t1", "TaskCreated", r#"{"goal":"hi"}"#, 1000).unwrap();
    store.append_event("t1", "PlanGenerated", r#"{"steps":3}"#, 2000).unwrap();
    let events = store.replay_events("t1").unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event_type, "TaskCreated");
}

#[test]
fn test_checkpoint_save_and_load() {
    let store = Store::open_in_memory().unwrap();
    let data = vec![1u8, 2, 3, 4, 5];
    store.save_checkpoint("cp1", "t1", &data, 2).unwrap();
    let loaded = store.load_latest_checkpoint("t1").unwrap().unwrap();
    assert_eq!(loaded.state_blob, data);
    assert_eq!(loaded.step_index, 2);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ck-memory`
Expected: FAIL — modules don't exist

- [ ] **Step 3: Implement schema**

`crates/ck-memory/src/schema.rs`:
```rust
use rusqlite::Connection;

pub fn initialize(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;

        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            goal TEXT NOT NULL,
            status TEXT NOT NULL,
            plan_json TEXT,
            current_step INTEGER DEFAULT 0,
            retry_budget_json TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id, timestamp);

        CREATE TABLE IF NOT EXISTS checkpoints (
            id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            state_blob BLOB NOT NULL,
            step_index INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_checkpoints_task ON checkpoints(task_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS actions (
            id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            step_index INTEGER NOT NULL,
            tool TEXT NOT NULL,
            params_json TEXT NOT NULL,
            result_json TEXT,
            success INTEGER,
            duration_ms INTEGER,
            created_at INTEGER NOT NULL
        );
        ",
    )
}
```

- [ ] **Step 4: Implement store**

`crates/ck-memory/src/store.rs`:
```rust
use rusqlite::{params, Connection};
use crate::schema;

#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    Created,
    Planning,
    Planned,
    Executing,
    Verifying,
    Recovering,
    Completed,
    Failed,
    Escalated,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Planning => "planning",
            Self::Planned => "planned",
            Self::Executing => "executing",
            Self::Verifying => "verifying",
            Self::Recovering => "recovering",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Escalated => "escalated",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "created" => Some(Self::Created),
            "planning" => Some(Self::Planning),
            "planned" => Some(Self::Planned),
            "executing" => Some(Self::Executing),
            "verifying" => Some(Self::Verifying),
            "recovering" => Some(Self::Recovering),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "escalated" => Some(Self::Escalated),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskRecord {
    pub id: String,
    pub goal: String,
    pub status: TaskStatus,
    pub plan_json: Option<String>,
    pub current_step: i64,
    pub retry_budget_json: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct EventRecord {
    pub id: i64,
    pub task_id: String,
    pub event_type: String,
    pub payload_json: String,
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct CheckpointRecord {
    pub id: String,
    pub task_id: String,
    pub state_blob: Vec<u8>,
    pub step_index: i64,
    pub created_at: i64,
}

pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open(path: &str) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        schema::initialize(&conn)?;
        Ok(Self { conn })
    }

    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        schema::initialize(&conn)?;
        Ok(Self { conn })
    }

    pub fn create_task(&self, task: &TaskRecord) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT INTO tasks (id, goal, status, plan_json, current_step, retry_budget_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![task.id, task.goal, task.status.as_str(), task.plan_json, task.current_step, task.retry_budget_json, task.created_at, task.updated_at],
        )?;
        Ok(())
    }

    pub fn get_task(&self, id: &str) -> rusqlite::Result<Option<TaskRecord>> {
        let mut stmt = self.conn.prepare("SELECT id, goal, status, plan_json, current_step, retry_budget_json, created_at, updated_at FROM tasks WHERE id = ?1")?;
        let mut rows = stmt.query(params![id])?;
        match rows.next()? {
            Some(row) => {
                let status_str: String = row.get(2)?;
                Ok(Some(TaskRecord {
                    id: row.get(0)?,
                    goal: row.get(1)?,
                    status: TaskStatus::from_str(&status_str).unwrap_or(TaskStatus::Failed),
                    plan_json: row.get(3)?,
                    current_step: row.get(4)?,
                    retry_budget_json: row.get(5)?,
                    created_at: row.get(6)?,
                    updated_at: row.get(7)?,
                }))
            }
            None => Ok(None),
        }
    }

    pub fn update_task_status(&self, id: &str, status: TaskStatus, updated_at: i64) -> rusqlite::Result<()> {
        self.conn.execute(
            "UPDATE tasks SET status = ?1, updated_at = ?2 WHERE id = ?3",
            params![status.as_str(), updated_at, id],
        )?;
        Ok(())
    }

    pub fn append_event(&self, task_id: &str, event_type: &str, payload_json: &str, timestamp: i64) -> rusqlite::Result<u64> {
        self.conn.execute(
            "INSERT INTO events (task_id, event_type, payload_json, timestamp) VALUES (?1, ?2, ?3, ?4)",
            params![task_id, event_type, payload_json, timestamp],
        )?;
        Ok(self.conn.last_insert_rowid() as u64)
    }

    pub fn replay_events(&self, task_id: &str) -> rusqlite::Result<Vec<EventRecord>> {
        let mut stmt = self.conn.prepare("SELECT id, task_id, event_type, payload_json, timestamp FROM events WHERE task_id = ?1 ORDER BY timestamp")?;
        let rows = stmt.query_map(params![task_id], |row| {
            Ok(EventRecord {
                id: row.get(0)?,
                task_id: row.get(1)?,
                event_type: row.get(2)?,
                payload_json: row.get(3)?,
                timestamp: row.get(4)?,
            })
        })?;
        rows.collect()
    }

    pub fn save_checkpoint(&self, id: &str, task_id: &str, state_blob: &[u8], step_index: i64) -> rusqlite::Result<()> {
        let now = chrono::Utc::now().timestamp_millis();
        self.conn.execute(
            "INSERT INTO checkpoints (id, task_id, state_blob, step_index, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, task_id, state_blob, step_index, now],
        )?;
        Ok(())
    }

    pub fn load_latest_checkpoint(&self, task_id: &str) -> rusqlite::Result<Option<CheckpointRecord>> {
        let mut stmt = self.conn.prepare("SELECT id, task_id, state_blob, step_index, created_at FROM checkpoints WHERE task_id = ?1 ORDER BY created_at DESC LIMIT 1")?;
        let mut rows = stmt.query(params![task_id])?;
        match rows.next()? {
            Some(row) => Ok(Some(CheckpointRecord {
                id: row.get(0)?,
                task_id: row.get(1)?,
                state_blob: row.get(2)?,
                step_index: row.get(3)?,
                created_at: row.get(4)?,
            })),
            None => Ok(None),
        }
    }
}
```

- [ ] **Step 5: Implement checkpoint serialization**

`crates/ck-memory/src/checkpoint.rs`:
```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointData {
    pub task_id: String,
    pub goal: String,
    pub status: String,
    pub plan_json: Option<String>,
    pub current_step: usize,
    pub retry_count: u32,
    pub replan_count: u32,
}

impl CheckpointData {
    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }
}
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p ck-memory`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/ck-memory/
git commit -m "feat(memory): implement SQLite store with tasks, events, checkpoints"
```

---


### Task 4: IPC Protocol + Named Pipe Server (ck-ipc)

**Files:**
- Create: `crates/ck-ipc/src/types.rs`
- Create: `crates/ck-ipc/src/protocol.rs`
- Create: `crates/ck-ipc/src/server.rs`
- Test: `crates/ck-ipc/tests/protocol_test.rs`

- [ ] **Step 1: Write test for protocol framing**

`crates/ck-ipc/tests/protocol_test.rs`:
```rust
use ck_ipc::protocol::{encode_message, decode_message};
use ck_ipc::types::{CognitionRequest, CognitionResponse, ExecutionRequest, ExecutionResponse};
use std::collections::HashMap;

#[test]
fn test_encode_decode_cognition_request() {
    let req = CognitionRequest {
        request_type: "plan".into(),
        task_id: "t1".into(),
        objective: "create a file".into(),
        current_state: HashMap::new(),
        memory_context: HashMap::new(),
        failure_context: None,
    };
    let encoded = encode_message(&req).unwrap();
    let decoded: CognitionRequest = decode_message(&encoded).unwrap();
    assert_eq!(decoded.task_id, "t1");
    assert_eq!(decoded.request_type, "plan");
}

#[test]
fn test_encode_decode_execution_request() {
    let mut params = HashMap::new();
    params.insert("cmd".into(), serde_json::Value::String("echo hello".into()));
    let req = ExecutionRequest {
        task_id: "t1".into(),
        action_id: "a1".into(),
        tool: "shell".into(),
        params,
        timeout_ms: 30000,
    };
    let encoded = encode_message(&req).unwrap();
    let decoded: ExecutionRequest = decode_message(&encoded).unwrap();
    assert_eq!(decoded.tool, "shell");
    assert_eq!(decoded.timeout_ms, 30000);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ck-ipc`
Expected: FAIL

- [ ] **Step 3: Implement IPC types**

`crates/ck-ipc/src/types.rs`:
```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CognitionRequest {
    pub request_type: String, // "plan" | "replan" | "reflect" | "verify_complex"
    pub task_id: String,
    pub objective: String,
    pub current_state: HashMap<String, serde_json::Value>,
    pub memory_context: HashMap<String, serde_json::Value>,
    pub failure_context: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CognitionResponse {
    pub task_id: String,
    pub response_type: String, // "plan" | "decision"
    pub plan: Option<Vec<PlanStep>>,
    pub reasoning: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    pub description: String,
    pub tool: String,
    pub params: HashMap<String, serde_json::Value>,
    pub expected_outcome: String,
    pub verification_strategy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRequest {
    pub task_id: String,
    pub action_id: String,
    pub tool: String, // "shell" | "filesystem" | "browser" | "desktop"
    pub params: HashMap<String, serde_json::Value>,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResponse {
    pub task_id: String,
    pub action_id: String,
    pub success: bool,
    pub output: String,
    pub error: Option<String>,
    pub side_effects: Vec<String>,
    pub duration_ms: u64,
}
```

Add `serde_json` to ck-ipc Cargo.toml:
```toml
serde_json = { workspace = true }
```

- [ ] **Step 4: Implement protocol framing**

`crates/ck-ipc/src/protocol.rs`:
```rust
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("serialization error: {0}")]
    Serialize(#[from] rmp_serde::encode::Error),
    #[error("deserialization error: {0}")]
    Deserialize(#[from] rmp_serde::decode::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("incomplete message")]
    Incomplete,
}

/// Encode a message: [4 bytes length (big-endian)] + [msgpack payload]
pub fn encode_message<T: Serialize>(msg: &T) -> Result<Vec<u8>, ProtocolError> {
    let payload = rmp_serde::to_vec_named(msg)?;
    let len = payload.len() as u32;
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&payload);
    Ok(buf)
}

/// Decode a message from a framed buffer (length prefix already stripped or full frame)
pub fn decode_message<T: DeserializeOwned>(data: &[u8]) -> Result<T, ProtocolError> {
    if data.len() < 4 {
        return Err(ProtocolError::Incomplete);
    }
    let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() < 4 + len {
        return Err(ProtocolError::Incomplete);
    }
    let msg = rmp_serde::from_slice(&data[4..4 + len])?;
    Ok(msg)
}

/// Read one framed message from an async reader
pub async fn read_message<T: DeserializeOwned>(
    reader: &mut (impl tokio::io::AsyncReadExt + Unpin),
) -> Result<T, ProtocolError> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    let msg = rmp_serde::from_slice(&payload)?;
    Ok(msg)
}

/// Write one framed message to an async writer
pub async fn write_message<T: Serialize>(
    writer: &mut (impl tokio::io::AsyncWriteExt + Unpin),
    msg: &T,
) -> Result<(), ProtocolError> {
    let payload = rmp_serde::to_vec_named(msg)?;
    let len = payload.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}
```

- [ ] **Step 5: Implement Named Pipe server (Windows)**

`crates/ck-ipc/src/server.rs`:
```rust
use std::path::PathBuf;
use tokio::net::windows::named_pipe::{ServerOptions, NamedPipeServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::protocol::{read_message, write_message, ProtocolError};
use serde::{de::DeserializeOwned, Serialize};

pub struct PipeServer {
    pipe_name: String,
}

impl PipeServer {
    pub fn new(name: &str) -> Self {
        Self {
            pipe_name: format!(r"\\.\pipe\{}", name),
        }
    }

    pub fn pipe_name(&self) -> &str {
        &self.pipe_name
    }

    /// Create a named pipe server instance and wait for a client connection
    pub async fn accept(&self) -> Result<PipeConnection, ProtocolError> {
        let server = ServerOptions::new()
            .first_pipe_instance(false)
            .create(&self.pipe_name)
            .map_err(|e| ProtocolError::Io(e))?;
        server.connect().await.map_err(|e| ProtocolError::Io(e))?;
        Ok(PipeConnection { pipe: server })
    }
}

pub struct PipeConnection {
    pipe: NamedPipeServer,
}

impl PipeConnection {
    pub async fn read<T: DeserializeOwned>(&mut self) -> Result<T, ProtocolError> {
        read_message(&mut self.pipe).await
    }

    pub async fn write<T: Serialize>(&mut self, msg: &T) -> Result<(), ProtocolError> {
        write_message(&mut self.pipe, msg).await
    }
}
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p ck-ipc`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/ck-ipc/
git commit -m "feat(ipc): implement MessagePack protocol framing and Named Pipe server"
```

---


### Task 5: Verification Engine (ck-verify)

**Files:**
- Create: `crates/ck-verify/src/strategies.rs`
- Create: `crates/ck-verify/src/engine.rs`
- Test: `crates/ck-verify/tests/verify_test.rs`

- [ ] **Step 1: Write test for verification strategies**

`crates/ck-verify/tests/verify_test.rs`:
```rust
use ck_verify::engine::Verifier;
use ck_verify::strategies::{VerificationStrategy, VerificationResult};
use std::path::PathBuf;
use std::fs;

#[test]
fn test_file_exists_pass() {
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("test.txt");
    fs::write(&file_path, "hello").unwrap();

    let strategy = VerificationStrategy::FileExists {
        path: file_path,
        content_contains: None,
    };
    let result = Verifier::verify_strategy(&strategy);
    assert!(matches!(result, VerificationResult::Verified { .. }));
}

#[test]
fn test_file_exists_fail() {
    let strategy = VerificationStrategy::FileExists {
        path: PathBuf::from("/nonexistent/path/file.txt"),
        content_contains: None,
    };
    let result = Verifier::verify_strategy(&strategy);
    assert!(matches!(result, VerificationResult::Failed { .. }));
}

#[test]
fn test_file_content_contains() {
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("test.txt");
    fs::write(&file_path, "hello world foo bar").unwrap();

    let strategy = VerificationStrategy::FileExists {
        path: file_path,
        content_contains: Some("foo bar".into()),
    };
    let result = Verifier::verify_strategy(&strategy);
    assert!(matches!(result, VerificationResult::Verified { .. }));
}

#[test]
fn test_exit_code_zero() {
    let strategy = VerificationStrategy::ExitCodeZero;
    let result = Verifier::verify_with_exit_code(&strategy, 0);
    assert!(matches!(result, VerificationResult::Verified { .. }));
}

#[test]
fn test_exit_code_nonzero() {
    let strategy = VerificationStrategy::ExitCodeZero;
    let result = Verifier::verify_with_exit_code(&strategy, 1);
    assert!(matches!(result, VerificationResult::Failed { .. }));
}

#[test]
fn test_output_contains() {
    let strategy = VerificationStrategy::OutputContains {
        expected: "success".into(),
    };
    let result = Verifier::verify_with_output(&strategy, "operation success completed");
    assert!(matches!(result, VerificationResult::Verified { .. }));
}
```

Add `tempfile` to ck-verify dev-dependencies:
```toml
[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ck-verify`
Expected: FAIL

- [ ] **Step 3: Implement verification strategies**

`crates/ck-verify/src/strategies.rs`:
```rust
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VerificationStrategy {
    FileExists {
        path: PathBuf,
        content_contains: Option<String>,
    },
    ExitCodeZero,
    OutputContains {
        expected: String,
    },
    FileModified {
        path: PathBuf,
        after_ms: i64,
    },
    ProcessRunning {
        name: String,
    },
    CognitionVerify {
        context: String,
    },
}

#[derive(Debug, Clone)]
pub enum VerificationResult {
    Verified { evidence: String },
    Failed { reason: String, actual: String, expected: String },
}
```

- [ ] **Step 4: Implement verifier engine**

`crates/ck-verify/src/engine.rs`:
```rust
use crate::strategies::{VerificationStrategy, VerificationResult};
use std::fs;

pub struct Verifier;

impl Verifier {
    pub fn verify_strategy(strategy: &VerificationStrategy) -> VerificationResult {
        match strategy {
            VerificationStrategy::FileExists { path, content_contains } => {
                if !path.exists() {
                    return VerificationResult::Failed {
                        reason: "file does not exist".into(),
                        actual: "missing".into(),
                        expected: format!("file at {}", path.display()),
                    };
                }
                if let Some(expected_content) = content_contains {
                    match fs::read_to_string(path) {
                        Ok(content) => {
                            if content.contains(expected_content.as_str()) {
                                VerificationResult::Verified {
                                    evidence: format!("file exists and contains '{}'", expected_content),
                                }
                            } else {
                                VerificationResult::Failed {
                                    reason: "file exists but missing expected content".into(),
                                    actual: content[..content.len().min(200)].into(),
                                    expected: expected_content.clone(),
                                }
                            }
                        }
                        Err(e) => VerificationResult::Failed {
                            reason: format!("cannot read file: {}", e),
                            actual: "unreadable".into(),
                            expected: expected_content.clone(),
                        },
                    }
                } else {
                    VerificationResult::Verified {
                        evidence: format!("file exists at {}", path.display()),
                    }
                }
            }
            VerificationStrategy::ExitCodeZero => {
                // This variant is checked via verify_with_exit_code
                VerificationResult::Verified { evidence: "exit code check requires explicit call".into() }
            }
            VerificationStrategy::OutputContains { .. } => {
                // This variant is checked via verify_with_output
                VerificationResult::Verified { evidence: "output check requires explicit call".into() }
            }
            _ => VerificationResult::Failed {
                reason: "strategy not yet implemented".into(),
                actual: "N/A".into(),
                expected: "N/A".into(),
            },
        }
    }

    pub fn verify_with_exit_code(strategy: &VerificationStrategy, code: i32) -> VerificationResult {
        match strategy {
            VerificationStrategy::ExitCodeZero => {
                if code == 0 {
                    VerificationResult::Verified {
                        evidence: "exit code 0".into(),
                    }
                } else {
                    VerificationResult::Failed {
                        reason: "non-zero exit code".into(),
                        actual: format!("exit code {}", code),
                        expected: "exit code 0".into(),
                    }
                }
            }
            _ => Self::verify_strategy(strategy),
        }
    }

    pub fn verify_with_output(strategy: &VerificationStrategy, output: &str) -> VerificationResult {
        match strategy {
            VerificationStrategy::OutputContains { expected } => {
                if output.contains(expected.as_str()) {
                    VerificationResult::Verified {
                        evidence: format!("output contains '{}'", expected),
                    }
                } else {
                    VerificationResult::Failed {
                        reason: "output missing expected content".into(),
                        actual: output[..output.len().min(200)].into(),
                        expected: expected.clone(),
                    }
                }
            }
            _ => Self::verify_strategy(strategy),
        }
    }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p ck-verify`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/ck-verify/
git commit -m "feat(verify): implement verification engine with file/exit-code/output strategies"
```

---

### Task 6: Recovery Engine (ck-recovery)

**Files:**
- Create: `crates/ck-recovery/src/budget.rs`
- Create: `crates/ck-recovery/src/engine.rs`
- Test: `crates/ck-recovery/tests/recovery_test.rs`

- [ ] **Step 1: Write test for recovery decisions**

`crates/ck-recovery/tests/recovery_test.rs`:
```rust
use ck_recovery::budget::RetryBudget;
use ck_recovery::engine::{RecoveryEngine, RecoveryDecision, FailureContext};

#[test]
fn test_first_failure_retries() {
    let budget = RetryBudget::new(3, 2);
    let ctx = FailureContext {
        task_id: "t1".into(),
        action_id: "a1".into(),
        reason: "file not found".into(),
        retry_count: 0,
        replan_count: 0,
    };
    let decision = RecoveryEngine::decide(&ctx, &budget);
    assert!(matches!(decision, RecoveryDecision::Retry { .. }));
}

#[test]
fn test_exhausted_retries_replans() {
    let budget = RetryBudget::new(3, 2);
    let ctx = FailureContext {
        task_id: "t1".into(),
        action_id: "a1".into(),
        reason: "permission denied".into(),
        retry_count: 3,
        replan_count: 0,
    };
    let decision = RecoveryEngine::decide(&ctx, &budget);
    assert!(matches!(decision, RecoveryDecision::Replan { .. }));
}

#[test]
fn test_exhausted_replans_escalates() {
    let budget = RetryBudget::new(3, 2);
    let ctx = FailureContext {
        task_id: "t1".into(),
        action_id: "a1".into(),
        reason: "persistent failure".into(),
        retry_count: 3,
        replan_count: 2,
    };
    let decision = RecoveryEngine::decide(&ctx, &budget);
    assert!(matches!(decision, RecoveryDecision::Escalate { .. }));
}

#[test]
fn test_retry_backoff_increases() {
    let budget = RetryBudget::new(3, 2);
    let ctx1 = FailureContext { task_id: "t1".into(), action_id: "a1".into(), reason: "err".into(), retry_count: 0, replan_count: 0 };
    let ctx2 = FailureContext { task_id: "t1".into(), action_id: "a1".into(), reason: "err".into(), retry_count: 1, replan_count: 0 };

    if let RecoveryDecision::Retry { backoff_ms: b1 } = RecoveryEngine::decide(&ctx1, &budget) {
        if let RecoveryDecision::Retry { backoff_ms: b2 } = RecoveryEngine::decide(&ctx2, &budget) {
            assert!(b2 > b1, "backoff should increase");
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ck-recovery`
Expected: FAIL

- [ ] **Step 3: Implement retry budget**

`crates/ck-recovery/src/budget.rs`:
```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryBudget {
    pub max_retries: u32,
    pub max_replans: u32,
}

impl RetryBudget {
    pub fn new(max_retries: u32, max_replans: u32) -> Self {
        Self { max_retries, max_replans }
    }

    pub fn default_budget() -> Self {
        Self { max_retries: 3, max_replans: 2 }
    }
}
```

- [ ] **Step 4: Implement recovery engine**

`crates/ck-recovery/src/engine.rs`:
```rust
use crate::budget::RetryBudget;

#[derive(Debug, Clone)]
pub struct FailureContext {
    pub task_id: String,
    pub action_id: String,
    pub reason: String,
    pub retry_count: u32,
    pub replan_count: u32,
}

#[derive(Debug, Clone)]
pub enum RecoveryDecision {
    Retry { backoff_ms: u64 },
    Replan { failure_context: String },
    Rollback { checkpoint_id: String },
    Escalate { reason: String },
}

pub struct RecoveryEngine;

impl RecoveryEngine {
    pub fn decide(ctx: &FailureContext, budget: &RetryBudget) -> RecoveryDecision {
        if ctx.retry_count < budget.max_retries {
            RecoveryDecision::Retry {
                backoff_ms: Self::exponential_backoff(ctx.retry_count),
            }
        } else if ctx.replan_count < budget.max_replans {
            RecoveryDecision::Replan {
                failure_context: format!(
                    "Action {} failed after {} retries: {}",
                    ctx.action_id, ctx.retry_count, ctx.reason
                ),
            }
        } else {
            RecoveryDecision::Escalate {
                reason: format!(
                    "Exhausted {} retries and {} replans for task {}. Last error: {}",
                    budget.max_retries, budget.max_replans, ctx.task_id, ctx.reason
                ),
            }
        }
    }

    fn exponential_backoff(attempt: u32) -> u64 {
        // Base 500ms, doubles each attempt: 500, 1000, 2000, ...
        500 * 2u64.pow(attempt)
    }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p ck-recovery`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/ck-recovery/
git commit -m "feat(recovery): implement recovery engine with retry budget and exponential backoff"
```

---


### Task 7: Task Model + State Machine (ck-kernel)

**Files:**
- Create: `crates/ck-kernel/src/task.rs`
- Create: `crates/ck-kernel/src/config.rs`
- Test: `crates/ck-kernel/tests/task_test.rs`

- [ ] **Step 1: Write test for task state transitions**

`crates/ck-kernel/tests/task_test.rs`:
```rust
use ck_kernel::task::{Task, TaskStatus, Plan, PlanStep};

#[test]
fn test_task_creation() {
    let task = Task::new("test goal".into());
    assert_eq!(task.status(), TaskStatus::Created);
    assert!(task.plan().is_none());
}

#[test]
fn test_valid_transition_created_to_planning() {
    let mut task = Task::new("goal".into());
    assert!(task.transition_to(TaskStatus::Planning).is_ok());
    assert_eq!(task.status(), TaskStatus::Planning);
}

#[test]
fn test_valid_transition_planning_to_planned() {
    let mut task = Task::new("goal".into());
    task.transition_to(TaskStatus::Planning).unwrap();
    let plan = Plan {
        id: "p1".into(),
        steps: vec![PlanStep {
            id: "s1".into(),
            description: "do thing".into(),
            tool: "shell".into(),
            params: Default::default(),
            expected_outcome: "file exists".into(),
            verification_strategy: "file_exists".into(),
        }],
        generated_by: "gpt-4".into(),
        reasoning: "simple task".into(),
    };
    assert!(task.set_plan(plan).is_ok());
    assert_eq!(task.status(), TaskStatus::Planned);
}

#[test]
fn test_invalid_transition_created_to_executing() {
    let mut task = Task::new("goal".into());
    assert!(task.transition_to(TaskStatus::Executing).is_err());
}

#[test]
fn test_advance_step() {
    let mut task = Task::new("goal".into());
    task.transition_to(TaskStatus::Planning).unwrap();
    let plan = Plan {
        id: "p1".into(),
        steps: vec![
            PlanStep { id: "s1".into(), description: "step 1".into(), tool: "shell".into(), params: Default::default(), expected_outcome: "done".into(), verification_strategy: "exit_code_zero".into() },
            PlanStep { id: "s2".into(), description: "step 2".into(), tool: "shell".into(), params: Default::default(), expected_outcome: "done".into(), verification_strategy: "exit_code_zero".into() },
        ],
        generated_by: "gpt-4".into(),
        reasoning: "two steps".into(),
    };
    task.set_plan(plan).unwrap();
    task.transition_to(TaskStatus::Executing).unwrap();
    assert_eq!(task.current_step(), 0);
    task.advance_step();
    assert_eq!(task.current_step(), 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ck-kernel`
Expected: FAIL

- [ ] **Step 3: Implement task model with state machine**

`crates/ck-kernel/src/task.rs`:
```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Created,
    Planning,
    Planned,
    Executing,
    Verifying,
    Recovering,
    Completed,
    Failed,
    Escalated,
}

impl TaskStatus {
    /// Returns valid next states from current state
    fn valid_transitions(&self) -> &[TaskStatus] {
        match self {
            Self::Created => &[Self::Planning, Self::Failed],
            Self::Planning => &[Self::Planned, Self::Failed],
            Self::Planned => &[Self::Executing, Self::Failed],
            Self::Executing => &[Self::Verifying, Self::Failed],
            Self::Verifying => &[Self::Executing, Self::Recovering, Self::Completed, Self::Failed],
            Self::Recovering => &[Self::Executing, Self::Planned, Self::Escalated, Self::Failed],
            Self::Completed => &[],
            Self::Failed => &[],
            Self::Escalated => &[Self::Executing, Self::Failed],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub id: String,
    pub steps: Vec<PlanStep>,
    pub generated_by: String,
    pub reasoning: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    pub id: String,
    pub description: String,
    pub tool: String,
    pub params: HashMap<String, serde_json::Value>,
    pub expected_outcome: String,
    pub verification_strategy: String,
}

#[derive(Debug, Error)]
pub enum TaskError {
    #[error("invalid transition from {from:?} to {to:?}")]
    InvalidTransition { from: TaskStatus, to: TaskStatus },
    #[error("cannot set plan in state {0:?}")]
    InvalidPlanState(TaskStatus),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    id: String,
    goal: String,
    status: TaskStatus,
    plan: Option<Plan>,
    current_step: usize,
    retry_count: u32,
    replan_count: u32,
    created_at: i64,
    updated_at: i64,
}

impl Task {
    pub fn new(goal: String) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            id: Ulid::new().to_string(),
            goal,
            status: TaskStatus::Created,
            plan: None,
            current_step: 0,
            retry_count: 0,
            replan_count: 0,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn id(&self) -> &str { &self.id }
    pub fn goal(&self) -> &str { &self.goal }
    pub fn status(&self) -> TaskStatus { self.status }
    pub fn plan(&self) -> Option<&Plan> { self.plan.as_ref() }
    pub fn current_step(&self) -> usize { self.current_step }
    pub fn retry_count(&self) -> u32 { self.retry_count }
    pub fn replan_count(&self) -> u32 { self.replan_count }

    pub fn transition_to(&mut self, new_status: TaskStatus) -> Result<(), TaskError> {
        if self.status.valid_transitions().contains(&new_status) {
            self.status = new_status;
            self.updated_at = chrono::Utc::now().timestamp_millis();
            Ok(())
        } else {
            Err(TaskError::InvalidTransition { from: self.status, to: new_status })
        }
    }

    pub fn set_plan(&mut self, plan: Plan) -> Result<(), TaskError> {
        if self.status != TaskStatus::Planning {
            return Err(TaskError::InvalidPlanState(self.status));
        }
        self.plan = Some(plan);
        self.status = TaskStatus::Planned;
        self.current_step = 0;
        self.updated_at = chrono::Utc::now().timestamp_millis();
        Ok(())
    }

    pub fn advance_step(&mut self) {
        self.current_step += 1;
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }

    pub fn increment_replan(&mut self) {
        self.replan_count += 1;
        self.retry_count = 0; // reset retries on replan
    }

    pub fn current_plan_step(&self) -> Option<&PlanStep> {
        self.plan.as_ref()?.steps.get(self.current_step)
    }

    pub fn is_plan_complete(&self) -> bool {
        match &self.plan {
            Some(plan) => self.current_step >= plan.steps.len(),
            None => false,
        }
    }
}
```

- [ ] **Step 4: Implement config**

`crates/ck-kernel/src/config.rs`:
```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelConfig {
    pub db_path: String,
    pub cognition_pipe: String,
    pub worker_pipe: String,
    pub max_concurrent_tasks: usize,
    pub max_retries: u32,
    pub max_replans: u32,
    pub default_timeout_ms: u64,
}

impl Default for KernelConfig {
    fn default() -> Self {
        Self {
            db_path: "cognition_kernel.db".into(),
            cognition_pipe: "ck-cognition".into(),
            worker_pipe: "ck-worker".into(),
            max_concurrent_tasks: 10,
            max_retries: 3,
            max_replans: 2,
            default_timeout_ms: 30_000,
        }
    }
}
```

- [ ] **Step 5: Update ck-kernel lib.rs to export modules**

Add `crates/ck-kernel/src/lib.rs`:
```rust
pub mod task;
pub mod config;
```

Update `crates/ck-kernel/Cargo.toml` to add `[lib]` section:
```toml
[lib]
name = "ck_kernel"
path = "src/lib.rs"
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p ck-kernel`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/ck-kernel/
git commit -m "feat(kernel): implement task model with validated state machine transitions"
```

---

### Task 8: Runtime Kernel Main Loop (ck-kernel)

**Files:**
- Create: `crates/ck-kernel/src/runtime.rs`
- Modify: `crates/ck-kernel/src/main.rs`
- Modify: `crates/ck-kernel/src/lib.rs`

- [ ] **Step 1: Implement runtime loop**

`crates/ck-kernel/src/runtime.rs`:
```rust
use crate::task::{Task, TaskStatus};
use crate::config::KernelConfig;
use ck_events::bus::EventBus;
use ck_events::types::KernelEvent;
use ck_ipc::protocol::{read_message, write_message};
use ck_ipc::types::{CognitionRequest, CognitionResponse, ExecutionRequest, ExecutionResponse};
use ck_memory::store::{Store, TaskRecord, TaskStatus as DbTaskStatus};
use ck_memory::checkpoint::CheckpointData;
use ck_verify::engine::Verifier;
use ck_verify::strategies::{VerificationStrategy, VerificationResult};
use ck_recovery::engine::{RecoveryEngine, RecoveryDecision, FailureContext};
use ck_recovery::budget::RetryBudget;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{info, warn, error};

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
    pub fn new(
        config: KernelConfig,
        cmd_rx: mpsc::Receiver<RuntimeCommand>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let store = Store::open(&config.db_path)?;
        let event_bus = EventBus::new(1024);
        Ok(Self {
            config,
            store,
            event_bus,
            tasks: HashMap::new(),
            cmd_rx,
        })
    }

    pub fn event_bus(&self) -> &EventBus {
        &self.event_bus
    }

    pub async fn run(&mut self) {
        info!("Runtime loop starting");

        loop {
            // Check for commands (non-blocking)
            match self.cmd_rx.try_recv() {
                Ok(RuntimeCommand::CreateTask { goal }) => {
                    self.handle_create_task(goal).await;
                }
                Ok(RuntimeCommand::PauseTask { task_id }) => {
                    info!(task_id = %task_id, "Pausing task");
                    // Remove from active processing
                    if let Some(task) = self.tasks.get_mut(&task_id) {
                        let _ = task.transition_to(TaskStatus::Escalated);
                        self.save_checkpoint(&task_id).await;
                    }
                }
                Ok(RuntimeCommand::CancelTask { task_id }) => {
                    info!(task_id = %task_id, "Cancelling task");
                    if let Some(task) = self.tasks.get_mut(&task_id) {
                        let _ = task.transition_to(TaskStatus::Failed);
                    }
                }
                Ok(RuntimeCommand::Shutdown) => {
                    info!("Shutdown requested");
                    break;
                }
                Ok(RuntimeCommand::ResumeTask { task_id }) => {
                    self.handle_resume_task(&task_id).await;
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }

            // Process active tasks
            let task_ids: Vec<String> = self.tasks.keys().cloned().collect();
            for task_id in task_ids {
                self.step_task(&task_id).await;
            }

            // Small yield to prevent busy-spinning
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        info!("Runtime loop exited");
    }

    async fn handle_create_task(&mut self, goal: String) {
        let task = Task::new(goal.clone());
        let task_id = task.id().to_string();
        info!(task_id = %task_id, goal = %goal, "Task created");

        // Persist
        let record = TaskRecord {
            id: task_id.clone(),
            goal: goal.clone(),
            status: DbTaskStatus::Created,
            plan_json: None,
            current_step: 0,
            retry_budget_json: None,
            created_at: chrono::Utc::now().timestamp_millis(),
            updated_at: chrono::Utc::now().timestamp_millis(),
        };
        let _ = self.store.create_task(&record);

        // Emit event
        self.event_bus.emit(KernelEvent::TaskCreated {
            task_id: task_id.clone(),
            goal,
            timestamp: chrono::Utc::now().timestamp_millis(),
        });

        self.tasks.insert(task_id, task);
    }

    async fn handle_resume_task(&mut self, task_id: &str) {
        if let Ok(Some(cp)) = self.store.load_latest_checkpoint(task_id) {
            if let Ok(data) = CheckpointData::deserialize(&cp.state_blob) {
                info!(task_id = %task_id, step = data.current_step, "Resuming from checkpoint");
                // Reconstruct task from checkpoint
                let mut task = Task::new(data.goal);
                // Task reconstruction would need more fields — simplified for V1
                self.tasks.insert(task_id.to_string(), task);
            }
        }
    }

    async fn step_task(&mut self, task_id: &str) {
        let task = match self.tasks.get(task_id) {
            Some(t) => t,
            None => return,
        };

        match task.status() {
            TaskStatus::Created => {
                // Transition to planning — request plan from cognition
                if let Some(task) = self.tasks.get_mut(task_id) {
                    let _ = task.transition_to(TaskStatus::Planning);
                }
                self.request_plan(task_id).await;
            }
            TaskStatus::Planned | TaskStatus::Executing => {
                // Execute next step
                self.execute_next_step(task_id).await;
            }
            TaskStatus::Verifying => {
                // Already handled inline after execution
            }
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Escalated => {
                // Terminal states — remove from active
                self.tasks.remove(task_id);
            }
            _ => {}
        }
    }

    async fn request_plan(&mut self, task_id: &str) {
        let task = match self.tasks.get(task_id) {
            Some(t) => t,
            None => return,
        };

        let request = CognitionRequest {
            request_type: "plan".into(),
            task_id: task_id.into(),
            objective: task.goal().into(),
            current_state: HashMap::new(),
            memory_context: HashMap::new(),
            failure_context: None,
        };

        // Send to cognition via IPC (implementation connects to pipe)
        // For now, this is the interface point — actual IPC wiring in Task 9
        info!(task_id = %task_id, "Plan requested from cognition engine");
    }

    async fn execute_next_step(&mut self, task_id: &str) {
        let task = match self.tasks.get(task_id) {
            Some(t) => t,
            None => return,
        };

        if task.is_plan_complete() {
            if let Some(task) = self.tasks.get_mut(task_id) {
                let _ = task.transition_to(TaskStatus::Completed);
                self.event_bus.emit(KernelEvent::TaskCompleted {
                    task_id: task_id.into(),
                    duration_ms: 0, // TODO: calculate from created_at
                    steps_executed: task.current_step(),
                });
            }
            return;
        }

        let step = match task.current_plan_step() {
            Some(s) => s.clone(),
            None => return,
        };

        // Dispatch to worker
        let action_id = ulid::Ulid::new().to_string();
        self.event_bus.emit(KernelEvent::ActionDispatched {
            task_id: task_id.into(),
            action_id: action_id.clone(),
            tool: step.tool.clone(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        });

        info!(task_id = %task_id, step = %step.description, tool = %step.tool, "Executing step");
    }

    async fn save_checkpoint(&self, task_id: &str) {
        if let Some(task) = self.tasks.get(task_id) {
            let data = CheckpointData {
                task_id: task_id.into(),
                goal: task.goal().into(),
                status: format!("{:?}", task.status()),
                plan_json: task.plan().map(|p| serde_json::to_string(p).unwrap_or_default()),
                current_step: task.current_step(),
                retry_count: task.retry_count(),
                replan_count: task.replan_count(),
            };
            if let Ok(blob) = data.serialize() {
                let cp_id = ulid::Ulid::new().to_string();
                let _ = self.store.save_checkpoint(&cp_id, task_id, &blob, task.current_step() as i64);
                self.event_bus.emit(KernelEvent::CheckpointSaved {
                    task_id: task_id.into(),
                    checkpoint_id: cp_id,
                });
            }
        }
    }
}
```

- [ ] **Step 2: Update main.rs to bootstrap runtime**

`crates/ck-kernel/src/main.rs`:
```rust
mod runtime;

use crate::runtime::{Runtime, RuntimeCommand};
use ck_kernel::config::KernelConfig;
use tokio::sync::mpsc;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .init();

    tracing::info!("Cognition Kernel starting");

    let config = KernelConfig::default();
    let (cmd_tx, cmd_rx) = mpsc::channel::<RuntimeCommand>(64);

    let mut runtime = match Runtime::new(config, cmd_rx) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Failed to initialize runtime: {}", e);
            return;
        }
    };

    // Example: create a task for testing
    let _ = cmd_tx.send(RuntimeCommand::CreateTask {
        goal: "test task".into(),
    }).await;

    runtime.run().await;
}
```

- [ ] **Step 3: Update lib.rs**

`crates/ck-kernel/src/lib.rs`:
```rust
pub mod task;
pub mod config;
pub mod runtime;
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build -p ck-kernel`
Expected: Compiles successfully

- [ ] **Step 5: Commit**

```bash
git add crates/ck-kernel/
git commit -m "feat(kernel): implement runtime main loop with task lifecycle orchestration"
```

---


### Task 9: Worker Supervisor (ck-kernel)

**Files:**
- Create: `crates/ck-kernel/src/supervisor.rs`
- Modify: `crates/ck-kernel/src/lib.rs`

- [ ] **Step 1: Implement worker supervisor**

`crates/ck-kernel/src/supervisor.rs`:
```rust
use std::process::{Command, Child};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use tracing::{info, warn, error};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkerType {
    Cognition,
    ToolWorker,
}

impl WorkerType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cognition => "cognition",
            Self::ToolWorker => "tool-worker",
        }
    }
}

struct WorkerProcess {
    worker_type: WorkerType,
    child: Child,
    command: String,
    args: Vec<String>,
    restart_count: u32,
    max_restarts: u32,
}

pub struct Supervisor {
    workers: HashMap<WorkerType, WorkerProcess>,
    pipe_prefix: String,
}

impl Supervisor {
    pub fn new(pipe_prefix: &str) -> Self {
        Self {
            workers: HashMap::new(),
            pipe_prefix: pipe_prefix.into(),
        }
    }

    pub fn spawn_cognition(&mut self, python_path: &str, script_path: &str, pipe_name: &str) -> std::io::Result<u32> {
        let child = Command::new(python_path)
            .arg(script_path)
            .arg("--pipe")
            .arg(pipe_name)
            .spawn()?;

        let pid = child.id();
        info!(worker = "cognition", pid = pid, "Spawned cognition worker");

        self.workers.insert(WorkerType::Cognition, WorkerProcess {
            worker_type: WorkerType::Cognition,
            child,
            command: python_path.into(),
            args: vec![script_path.into(), "--pipe".into(), pipe_name.into()],
            restart_count: 0,
            max_restarts: 5,
        });

        Ok(pid)
    }

    pub fn spawn_tool_worker(&mut self, binary_path: &str, pipe_name: &str) -> std::io::Result<u32> {
        let child = Command::new(binary_path)
            .arg("--pipe")
            .arg(pipe_name)
            .spawn()?;

        let pid = child.id();
        info!(worker = "tool-worker", pid = pid, "Spawned tool worker");

        self.workers.insert(WorkerType::ToolWorker, WorkerProcess {
            worker_type: WorkerType::ToolWorker,
            child,
            command: binary_path.into(),
            args: vec!["--pipe".into(), pipe_name.into()],
            restart_count: 0,
            max_restarts: 5,
        });

        Ok(pid)
    }

    /// Check all workers, restart any that have died
    pub fn check_and_restart(&mut self) -> Vec<(WorkerType, u32)> {
        let mut restarted = Vec::new();

        let types: Vec<WorkerType> = self.workers.keys().cloned().collect();
        for wtype in types {
            let worker = self.workers.get_mut(&wtype).unwrap();
            match worker.child.try_wait() {
                Ok(Some(status)) => {
                    warn!(
                        worker = worker.worker_type.as_str(),
                        exit_code = ?status.code(),
                        "Worker exited"
                    );

                    if worker.restart_count < worker.max_restarts {
                        // Restart
                        let cmd = worker.command.clone();
                        let args = worker.args.clone();
                        match Command::new(&cmd).args(&args).spawn() {
                            Ok(new_child) => {
                                let pid = new_child.id();
                                worker.child = new_child;
                                worker.restart_count += 1;
                                info!(worker = worker.worker_type.as_str(), pid = pid, restarts = worker.restart_count, "Worker restarted");
                                restarted.push((wtype, pid));
                            }
                            Err(e) => {
                                error!(worker = worker.worker_type.as_str(), error = %e, "Failed to restart worker");
                            }
                        }
                    } else {
                        error!(worker = worker.worker_type.as_str(), "Max restarts exceeded");
                    }
                }
                Ok(None) => {} // Still running
                Err(e) => {
                    error!(worker = worker.worker_type.as_str(), error = %e, "Error checking worker status");
                }
            }
        }

        restarted
    }

    /// Gracefully shutdown all workers
    pub fn shutdown_all(&mut self) {
        for (wtype, worker) in self.workers.iter_mut() {
            info!(worker = wtype.as_str(), "Shutting down worker");
            let _ = worker.child.kill();
            let _ = worker.child.wait();
        }
        self.workers.clear();
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        self.shutdown_all();
    }
}
```

- [ ] **Step 2: Update lib.rs**

`crates/ck-kernel/src/lib.rs`:
```rust
pub mod task;
pub mod config;
pub mod runtime;
pub mod supervisor;
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p ck-kernel`
Expected: Compiles

- [ ] **Step 4: Commit**

```bash
git add crates/ck-kernel/src/supervisor.rs crates/ck-kernel/src/lib.rs
git commit -m "feat(kernel): implement worker supervisor with health check and auto-restart"
```

---

### Task 10: Go Tool Workers

**Files:**
- Create: `workers/go.mod`
- Create: `workers/go.sum`
- Create: `workers/pkg/protocol/types.go`
- Create: `workers/internal/ipc/client.go`
- Create: `workers/internal/shell/executor.go`
- Create: `workers/internal/filesystem/worker.go`
- Create: `workers/cmd/ck-worker/main.go`
- Test: `workers/internal/shell/executor_test.go`
- Test: `workers/internal/filesystem/worker_test.go`

- [ ] **Step 1: Initialize Go module and install dependencies**

```bash
cd workers
go mod init github.com/cognition-kernel/workers
go get github.com/vmihailenco/msgpack/v5
```

- [ ] **Step 2: Implement protocol types**

`workers/pkg/protocol/types.go`:
```go
package protocol

type ExecutionRequest struct {
	TaskID    string                 `msgpack:"task_id"`
	ActionID  string                 `msgpack:"action_id"`
	Tool      string                 `msgpack:"tool"`
	Params    map[string]interface{} `msgpack:"params"`
	TimeoutMs uint64                 `msgpack:"timeout_ms"`
}

type ExecutionResponse struct {
	TaskID     string   `msgpack:"task_id"`
	ActionID   string   `msgpack:"action_id"`
	Success    bool     `msgpack:"success"`
	Output     string   `msgpack:"output"`
	Error      *string  `msgpack:"error"`
	SideEffects []string `msgpack:"side_effects"`
	DurationMs uint64   `msgpack:"duration_ms"`
}
```

- [ ] **Step 3: Implement IPC client**

`workers/internal/ipc/client.go`:
```go
package ipc

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/vmihailenco/msgpack/v5"
)

type Client struct {
	conn net.Conn
}

func Connect(pipePath string) (*Client, error) {
	conn, err := net.Dial("unix", pipePath)
	if err != nil {
		// On Windows, try named pipe
		conn, err = dialNamedPipe(pipePath)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to pipe %s: %w", pipePath, err)
		}
	}
	return &Client{conn: conn}, nil
}

func (c *Client) ReadMessage(v interface{}) error {
	// Read 4-byte length prefix
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, lenBuf); err != nil {
		return fmt.Errorf("read length: %w", err)
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)

	// Read payload
	payload := make([]byte, msgLen)
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return fmt.Errorf("read payload: %w", err)
	}

	return msgpack.Unmarshal(payload, v)
}

func (c *Client) WriteMessage(v interface{}) error {
	payload, err := msgpack.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	// Write 4-byte length prefix
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
	if _, err := c.conn.Write(lenBuf); err != nil {
		return fmt.Errorf("write length: %w", err)
	}
	if _, err := c.conn.Write(payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// Windows named pipe dialer
func dialNamedPipe(path string) (net.Conn, error) {
	// On Windows, named pipes are accessed via file path
	return net.Dial("unix", path) // Fallback — real Windows impl uses winio
}
```

- [ ] **Step 4: Implement shell executor**

`workers/internal/shell/executor.go`:
```go
package shell

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"time"
)

type Result struct {
	Output   string
	Error    string
	ExitCode int
	Duration time.Duration
}

func Execute(command string, workDir string, timeoutMs uint64) Result {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "cmd", "/C", command)
	} else {
		cmd = exec.CommandContext(ctx, "sh", "-c", command)
	}

	if workDir != "" {
		cmd.Dir = workDir
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	start := time.Now()
	err := cmd.Run()
	duration := time.Since(start)

	result := Result{
		Output:   stdout.String(),
		Duration: duration,
	}

	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			result.Error = "timeout exceeded"
			result.ExitCode = -1
		} else if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
			result.Error = stderr.String()
		} else {
			result.Error = fmt.Sprintf("exec error: %v", err)
			result.ExitCode = -1
		}
	}

	return result
}
```

- [ ] **Step 5: Implement filesystem worker**

`workers/internal/filesystem/worker.go`:
```go
package filesystem

import (
	"fmt"
	"os"
	"path/filepath"
)

type Result struct {
	Success     bool
	Output      string
	Error       string
	SideEffects []string
}

func Execute(action string, params map[string]interface{}) Result {
	switch action {
	case "create_dir":
		return createDir(params)
	case "write_file":
		return writeFile(params)
	case "read_file":
		return readFile(params)
	case "delete":
		return deleteFile(params)
	default:
		return Result{Success: false, Error: fmt.Sprintf("unknown filesystem action: %s", action)}
	}
}

func createDir(params map[string]interface{}) Result {
	path, ok := params["path"].(string)
	if !ok {
		return Result{Success: false, Error: "missing 'path' parameter"}
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		return Result{Success: false, Error: fmt.Sprintf("mkdir failed: %v", err)}
	}
	return Result{Success: true, Output: fmt.Sprintf("created directory: %s", path), SideEffects: []string{path}}
}

func writeFile(params map[string]interface{}) Result {
	path, ok := params["path"].(string)
	if !ok {
		return Result{Success: false, Error: "missing 'path' parameter"}
	}
	content, ok := params["content"].(string)
	if !ok {
		return Result{Success: false, Error: "missing 'content' parameter"}
	}

	// Ensure parent directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return Result{Success: false, Error: fmt.Sprintf("mkdir parent failed: %v", err)}
	}

	// Atomic write: write to temp, then rename
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(content), 0644); err != nil {
		return Result{Success: false, Error: fmt.Sprintf("write failed: %v", err)}
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return Result{Success: false, Error: fmt.Sprintf("rename failed: %v", err)}
	}

	return Result{Success: true, Output: fmt.Sprintf("wrote %d bytes to %s", len(content), path), SideEffects: []string{path}}
}

func readFile(params map[string]interface{}) Result {
	path, ok := params["path"].(string)
	if !ok {
		return Result{Success: false, Error: "missing 'path' parameter"}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Result{Success: false, Error: fmt.Sprintf("read failed: %v", err)}
	}
	return Result{Success: true, Output: string(data)}
}

func deleteFile(params map[string]interface{}) Result {
	path, ok := params["path"].(string)
	if !ok {
		return Result{Success: false, Error: "missing 'path' parameter"}
	}
	if err := os.RemoveAll(path); err != nil {
		return Result{Success: false, Error: fmt.Sprintf("delete failed: %v", err)}
	}
	return Result{Success: true, Output: fmt.Sprintf("deleted: %s", path), SideEffects: []string{path}}
}
```

- [ ] **Step 6: Implement worker main entry**

`workers/cmd/ck-worker/main.go`:
```go
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/cognition-kernel/workers/internal/ipc"
	"github.com/cognition-kernel/workers/internal/shell"
	"github.com/cognition-kernel/workers/internal/filesystem"
	"github.com/cognition-kernel/workers/pkg/protocol"
)

func main() {
	pipePath := flag.String("pipe", "", "Named pipe path for IPC")
	flag.Parse()

	if *pipePath == "" {
		log.Fatal("--pipe argument required")
	}

	client, err := ipc.Connect(*pipePath)
	if err != nil {
		log.Fatalf("Failed to connect to kernel: %v", err)
	}
	defer client.Close()

	log.Printf("Worker connected to kernel via %s", *pipePath)

	// Main worker loop: read request, execute, respond
	for {
		var req protocol.ExecutionRequest
		if err := client.ReadMessage(&req); err != nil {
			log.Printf("Read error: %v", err)
			break
		}

		start := time.Now()
		resp := handleRequest(req)
		resp.DurationMs = uint64(time.Since(start).Milliseconds())

		if err := client.WriteMessage(&resp); err != nil {
			log.Printf("Write error: %v", err)
			break
		}
	}
}

func handleRequest(req protocol.ExecutionRequest) protocol.ExecutionResponse {
	resp := protocol.ExecutionResponse{
		TaskID:   req.TaskID,
		ActionID: req.ActionID,
	}

	switch req.Tool {
	case "shell":
		cmd, _ := req.Params["cmd"].(string)
		workDir, _ := req.Params["work_dir"].(string)
		result := shell.Execute(cmd, workDir, req.TimeoutMs)
		resp.Success = result.ExitCode == 0
		resp.Output = result.Output
		if result.Error != "" {
			resp.Error = &result.Error
		}

	case "filesystem":
		action, _ := req.Params["action"].(string)
		result := filesystem.Execute(action, req.Params)
		resp.Success = result.Success
		resp.Output = result.Output
		if result.Error != "" {
			resp.Error = &result.Error
		}
		resp.SideEffects = result.SideEffects

	default:
		errMsg := fmt.Sprintf("unknown tool: %s", req.Tool)
		resp.Success = false
		resp.Error = &errMsg
	}

	return resp
}
```

- [ ] **Step 7: Write shell executor test**

`workers/internal/shell/executor_test.go`:
```go
package shell

import (
	"runtime"
	"testing"
)

func TestExecuteEcho(t *testing.T) {
	var cmd string
	if runtime.GOOS == "windows" {
		cmd = "echo hello"
	} else {
		cmd = "echo hello"
	}
	result := Execute(cmd, "", 5000)
	if result.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", result.ExitCode, result.Error)
	}
	if len(result.Output) == 0 {
		t.Fatal("expected output")
	}
}

func TestExecuteTimeout(t *testing.T) {
	var cmd string
	if runtime.GOOS == "windows" {
		cmd = "ping -n 10 127.0.0.1"
	} else {
		cmd = "sleep 10"
	}
	result := Execute(cmd, "", 100) // 100ms timeout
	if result.ExitCode != -1 {
		t.Fatalf("expected timeout exit code -1, got %d", result.ExitCode)
	}
}

func TestExecuteFailure(t *testing.T) {
	result := Execute("exit 1", "", 5000)
	if result.ExitCode == 0 {
		t.Fatal("expected non-zero exit code")
	}
}
```

- [ ] **Step 8: Write filesystem worker test**

`workers/internal/filesystem/worker_test.go`:
```go
package filesystem

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateDir(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "ck-test-dir")
	defer os.RemoveAll(dir)

	result := Execute("create_dir", map[string]interface{}{"path": dir})
	if !result.Success {
		t.Fatalf("create_dir failed: %s", result.Error)
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Fatal("directory was not created")
	}
}

func TestWriteAndReadFile(t *testing.T) {
	path := filepath.Join(os.TempDir(), "ck-test-file.txt")
	defer os.Remove(path)

	result := Execute("write_file", map[string]interface{}{
		"path":    path,
		"content": "hello world",
	})
	if !result.Success {
		t.Fatalf("write_file failed: %s", result.Error)
	}

	result = Execute("read_file", map[string]interface{}{"path": path})
	if !result.Success {
		t.Fatalf("read_file failed: %s", result.Error)
	}
	if result.Output != "hello world" {
		t.Fatalf("expected 'hello world', got '%s'", result.Output)
	}
}

func TestDeleteFile(t *testing.T) {
	path := filepath.Join(os.TempDir(), "ck-test-delete.txt")
	os.WriteFile(path, []byte("temp"), 0644)

	result := Execute("delete", map[string]interface{}{"path": path})
	if !result.Success {
		t.Fatalf("delete failed: %s", result.Error)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file still exists after delete")
	}
}
```

- [ ] **Step 9: Run Go tests**

Run: `cd workers && go test ./...`
Expected: PASS

- [ ] **Step 10: Build Go worker binary**

Run: `cd workers && go build -o bin/ck-worker ./cmd/ck-worker`
Expected: Binary created at `workers/bin/ck-worker`

- [ ] **Step 11: Commit**

```bash
git add workers/
git commit -m "feat(workers): implement Go tool workers with shell and filesystem execution"
```

---


### Task 11: Python Cognition Engine

**Files:**
- Create: `cognition/pyproject.toml`
- Create: `cognition/cognition_kernel/__init__.py`
- Create: `cognition/cognition_kernel/models.py`
- Create: `cognition/cognition_kernel/ipc.py`
- Create: `cognition/cognition_kernel/context.py`
- Create: `cognition/cognition_kernel/planner.py`
- Create: `cognition/cognition_kernel/reasoner.py`
- Create: `cognition/cognition_kernel/engine.py`
- Test: `cognition/tests/test_planner.py`
- Test: `cognition/tests/test_ipc.py`

- [ ] **Step 1: Create pyproject.toml**

`cognition/pyproject.toml`:
```toml
[project]
name = "cognition-kernel"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "litellm>=1.40.0",
    "msgpack>=1.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
]

[project.scripts]
ck-cognition = "cognition_kernel.engine:main"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

- [ ] **Step 2: Implement data models**

`cognition/cognition_kernel/models.py`:
```python
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class PlanStep:
    description: str
    tool: str  # "shell" | "filesystem" | "browser" | "desktop"
    params: dict
    expected_outcome: str
    verification_strategy: str  # "file_exists" | "exit_code_zero" | "output_contains"

@dataclass
class CognitionRequest:
    request_type: str  # "plan" | "replan" | "reflect" | "verify_complex"
    task_id: str
    objective: str
    current_state: dict = field(default_factory=dict)
    memory_context: dict = field(default_factory=dict)
    failure_context: Optional[dict] = None

@dataclass
class CognitionResponse:
    task_id: str
    response_type: str  # "plan" | "decision"
    plan: Optional[list[PlanStep]] = None
    reasoning: str = ""
```

- [ ] **Step 3: Implement IPC client**

`cognition/cognition_kernel/ipc.py`:
```python
import struct
import msgpack

class PipeClient:
    """MessagePack IPC client over Named Pipes (Windows) or Unix sockets."""

    def __init__(self, pipe_path: str):
        self.pipe_path = pipe_path
        self._conn = None

    def connect(self):
        """Connect to the kernel's named pipe."""
        import socket
        # Try Unix socket first (Linux/Mac), fall back to Windows named pipe
        try:
            self._conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._conn.connect(self.pipe_path)
        except (OSError, AttributeError):
            # Windows: open named pipe as file
            self._conn = open(self.pipe_path, "r+b", buffering=0)

    def read_message(self) -> dict:
        """Read one length-prefixed msgpack message."""
        len_bytes = self._read_exact(4)
        msg_len = struct.unpack(">I", len_bytes)[0]
        payload = self._read_exact(msg_len)
        return msgpack.unpackb(payload, raw=False)

    def write_message(self, msg: dict):
        """Write one length-prefixed msgpack message."""
        payload = msgpack.packb(msg, use_bin_type=True)
        length = struct.pack(">I", len(payload))
        self._write(length + payload)

    def _read_exact(self, n: int) -> bytes:
        data = b""
        while len(data) < n:
            if hasattr(self._conn, "recv"):
                chunk = self._conn.recv(n - len(data))
            else:
                chunk = self._conn.read(n - len(data))
            if not chunk:
                raise ConnectionError("pipe closed")
            data += chunk
        return data

    def _write(self, data: bytes):
        if hasattr(self._conn, "sendall"):
            self._conn.sendall(data)
        else:
            self._conn.write(data)
            self._conn.flush()

    def close(self):
        if self._conn:
            if hasattr(self._conn, "close"):
                self._conn.close()
            self._conn = None
```

- [ ] **Step 4: Implement context assembly**

`cognition/cognition_kernel/context.py`:
```python
from cognition_kernel.models import CognitionRequest

SYSTEM_PROMPT = """You are the planning engine for Cognition Kernel, an autonomous runtime.
Your job is to decompose a goal into concrete executable steps.

Each step must specify:
- description: what this step does
- tool: one of "shell", "filesystem", "browser", "desktop"
- params: tool-specific parameters
- expected_outcome: what success looks like
- verification_strategy: how to verify (file_exists, exit_code_zero, output_contains)

For shell tool, params must include "cmd" (the command to run).
For filesystem tool, params must include "action" (create_dir, write_file, read_file, delete) and relevant params (path, content).

Respond with a JSON array of steps. No explanation outside the JSON."""

def build_plan_prompt(request: CognitionRequest) -> list[dict]:
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
    ]

    user_content = f"Goal: {request.objective}"

    if request.current_state:
        user_content += f"\n\nCurrent progress: {request.current_state}"

    if request.failure_context:
        user_content += f"\n\nPrevious attempt failed: {request.failure_context}"
        user_content += "\nPlease create a revised plan that avoids the previous failure."

    messages.append({"role": "user", "content": user_content})
    return messages


def build_replan_prompt(request: CognitionRequest) -> list[dict]:
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"""Goal: {request.objective}

Previous plan failed with error: {request.failure_context}

Create a new plan that works around this failure. Consider alternative approaches."""},
    ]
    return messages
```

- [ ] **Step 5: Implement planner**

`cognition/cognition_kernel/planner.py`:
```python
import json
import litellm
from cognition_kernel.models import CognitionRequest, PlanStep
from cognition_kernel.context import build_plan_prompt, build_replan_prompt

DEFAULT_MODEL = "gpt-4o-mini"

async def generate_plan(request: CognitionRequest, model: str = DEFAULT_MODEL) -> tuple[list[PlanStep], str]:
    """Generate an execution plan from a goal. Returns (steps, reasoning)."""
    if request.request_type == "replan":
        messages = build_replan_prompt(request)
    else:
        messages = build_plan_prompt(request)

    response = await litellm.acompletion(
        model=model,
        messages=messages,
        temperature=0.2,
        response_format={"type": "json_object"},
    )

    content = response.choices[0].message.content
    reasoning = f"Model: {model}, tokens: {response.usage.total_tokens}"

    # Parse JSON response
    try:
        data = json.loads(content)
        # Handle both {"steps": [...]} and direct [...]
        steps_data = data if isinstance(data, list) else data.get("steps", [])
    except json.JSONDecodeError:
        # Fallback: try to extract JSON array from response
        import re
        match = re.search(r'\[.*\]', content, re.DOTALL)
        if match:
            steps_data = json.loads(match.group())
        else:
            raise ValueError(f"Could not parse plan from LLM response: {content[:200]}")

    steps = []
    for s in steps_data:
        steps.append(PlanStep(
            description=s.get("description", ""),
            tool=s.get("tool", "shell"),
            params=s.get("params", {}),
            expected_outcome=s.get("expected_outcome", ""),
            verification_strategy=s.get("verification_strategy", "exit_code_zero"),
        ))

    return steps, reasoning
```

- [ ] **Step 6: Implement reasoner (reflection)**

`cognition/cognition_kernel/reasoner.py`:
```python
import litellm
from cognition_kernel.models import CognitionRequest

REFLECT_PROMPT = """You are evaluating the execution of an autonomous task.
Given the current state and history, determine if the approach is working or needs adjustment.
Respond with JSON: {"assessment": "on_track"|"needs_adjustment", "reasoning": "..."}"""

async def reflect(request: CognitionRequest, model: str = "gpt-4o-mini") -> dict:
    """Reflect on current execution state."""
    messages = [
        {"role": "system", "content": REFLECT_PROMPT},
        {"role": "user", "content": f"Objective: {request.objective}\nState: {request.current_state}"},
    ]

    response = await litellm.acompletion(
        model=model,
        messages=messages,
        temperature=0.1,
    )

    import json
    try:
        return json.loads(response.choices[0].message.content)
    except json.JSONDecodeError:
        return {"assessment": "on_track", "reasoning": "could not parse reflection"}
```

- [ ] **Step 7: Implement main engine loop**

`cognition/cognition_kernel/engine.py`:
```python
import asyncio
import argparse
import logging
from cognition_kernel.ipc import PipeClient
from cognition_kernel.models import CognitionRequest, CognitionResponse, PlanStep
from cognition_kernel.planner import generate_plan
from cognition_kernel.reasoner import reflect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [cognition] %(message)s")
logger = logging.getLogger(__name__)


async def handle_request(request: CognitionRequest) -> CognitionResponse:
    """Route a cognition request to the appropriate handler."""
    if request.request_type in ("plan", "replan"):
        try:
            steps, reasoning = await generate_plan(request)
            return CognitionResponse(
                task_id=request.task_id,
                response_type="plan",
                plan=steps,
                reasoning=reasoning,
            )
        except Exception as e:
            logger.error(f"Plan generation failed: {e}")
            return CognitionResponse(
                task_id=request.task_id,
                response_type="plan",
                plan=None,
                reasoning=f"error: {e}",
            )

    elif request.request_type == "reflect":
        result = await reflect(request)
        return CognitionResponse(
            task_id=request.task_id,
            response_type="decision",
            reasoning=result.get("reasoning", ""),
        )

    else:
        return CognitionResponse(
            task_id=request.task_id,
            response_type="decision",
            reasoning=f"unknown request type: {request.request_type}",
        )


def response_to_dict(resp: CognitionResponse) -> dict:
    """Serialize response for IPC."""
    d = {
        "task_id": resp.task_id,
        "response_type": resp.response_type,
        "reasoning": resp.reasoning,
    }
    if resp.plan:
        d["plan"] = [
            {
                "description": s.description,
                "tool": s.tool,
                "params": s.params,
                "expected_outcome": s.expected_outcome,
                "verification_strategy": s.verification_strategy,
            }
            for s in resp.plan
        ]
    return d


def dict_to_request(d: dict) -> CognitionRequest:
    """Deserialize request from IPC."""
    return CognitionRequest(
        request_type=d["request_type"],
        task_id=d["task_id"],
        objective=d["objective"],
        current_state=d.get("current_state", {}),
        memory_context=d.get("memory_context", {}),
        failure_context=d.get("failure_context"),
    )


async def run(pipe_path: str):
    """Main cognition engine loop."""
    logger.info(f"Connecting to kernel via {pipe_path}")
    client = PipeClient(pipe_path)
    client.connect()
    logger.info("Connected to kernel")

    while True:
        try:
            msg = client.read_message()
            request = dict_to_request(msg)
            logger.info(f"Received {request.request_type} request for task {request.task_id}")

            response = await handle_request(request)
            client.write_message(response_to_dict(response))
            logger.info(f"Sent response for task {request.task_id}")

        except ConnectionError:
            logger.warning("Kernel connection lost, exiting")
            break
        except Exception as e:
            logger.error(f"Error in cognition loop: {e}")
            continue

    client.close()


def main():
    parser = argparse.ArgumentParser(description="Cognition Kernel - Cognition Engine")
    parser.add_argument("--pipe", required=True, help="Named pipe path for IPC")
    args = parser.parse_args()

    asyncio.run(run(args.pipe))


if __name__ == "__main__":
    main()
```

- [ ] **Step 8: Write planner test**

`cognition/tests/test_planner.py`:
```python
import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from cognition_kernel.planner import generate_plan
from cognition_kernel.models import CognitionRequest

@pytest.mark.asyncio
async def test_generate_plan_parses_response():
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = '''[
        {"description": "create directory", "tool": "filesystem", "params": {"action": "create_dir", "path": "test-project"}, "expected_outcome": "directory exists", "verification_strategy": "file_exists"},
        {"description": "create main.py", "tool": "filesystem", "params": {"action": "write_file", "path": "test-project/main.py", "content": "print('hello')"}, "expected_outcome": "file exists", "verification_strategy": "file_exists"}
    ]'''
    mock_response.usage = MagicMock(total_tokens=100)

    with patch("cognition_kernel.planner.litellm.acompletion", new_callable=AsyncMock, return_value=mock_response):
        request = CognitionRequest(
            request_type="plan",
            task_id="t1",
            objective="create a python project",
        )
        steps, reasoning = await generate_plan(request)

    assert len(steps) == 2
    assert steps[0].tool == "filesystem"
    assert steps[0].params["action"] == "create_dir"
    assert steps[1].params["path"] == "test-project/main.py"

@pytest.mark.asyncio
async def test_generate_plan_handles_wrapped_json():
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = '{"steps": [{"description": "echo", "tool": "shell", "params": {"cmd": "echo hi"}, "expected_outcome": "output", "verification_strategy": "exit_code_zero"}]}'
    mock_response.usage = MagicMock(total_tokens=50)

    with patch("cognition_kernel.planner.litellm.acompletion", new_callable=AsyncMock, return_value=mock_response):
        request = CognitionRequest(request_type="plan", task_id="t2", objective="echo hi")
        steps, _ = await generate_plan(request)

    assert len(steps) == 1
    assert steps[0].tool == "shell"
```

- [ ] **Step 9: Write IPC test**

`cognition/tests/test_ipc.py`:
```python
import struct
import msgpack
import pytest
from unittest.mock import MagicMock, patch
from cognition_kernel.ipc import PipeClient

def test_write_message_format():
    """Verify message framing: 4-byte length + msgpack payload."""
    client = PipeClient("test")
    mock_conn = MagicMock()
    client._conn = mock_conn
    mock_conn.sendall = MagicMock()

    msg = {"task_id": "t1", "type": "plan"}
    client.write_message(msg)

    # Verify the written data
    written = mock_conn.sendall.call_args[0][0]
    length = struct.unpack(">I", written[:4])[0]
    payload = msgpack.unpackb(written[4:], raw=False)
    assert payload["task_id"] == "t1"
    assert length == len(written) - 4
```

- [ ] **Step 10: Run Python tests**

Run: `cd cognition && uv run pytest tests/ -v`
Expected: PASS

- [ ] **Step 11: Commit**

```bash
git add cognition/
git commit -m "feat(cognition): implement Python cognition engine with LiteLLM planning"
```

---


### Task 12: CLI Interface (ck-cli)

**Files:**
- Modify: `crates/ck-cli/Cargo.toml`
- Create: `crates/ck-cli/src/commands.rs`
- Modify: `crates/ck-cli/src/main.rs`

- [ ] **Step 1: Update ck-cli dependencies**

`crates/ck-cli/Cargo.toml`:
```toml
[package]
name = "ck-cli"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "ck"
path = "src/main.rs"

[dependencies]
ck-kernel = { path = "../ck-kernel" }
ck-events = { path = "../ck-events" }
ck-memory = { path = "../ck-memory" }
clap = { version = "4", features = ["derive"] }
tokio = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
```

- [ ] **Step 2: Implement CLI commands**

`crates/ck-cli/src/commands.rs`:
```rust
use ck_kernel::config::KernelConfig;
use ck_kernel::runtime::{Runtime, RuntimeCommand};
use ck_memory::store::Store;
use tokio::sync::mpsc;

pub async fn cmd_start(goal: String) {
    println!("Starting task: {}", goal);

    let config = KernelConfig::default();
    let (cmd_tx, cmd_rx) = mpsc::channel::<RuntimeCommand>(64);

    let mut runtime = match Runtime::new(config, cmd_rx) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to start kernel: {}", e);
            return;
        }
    };

    // Send create task command
    let _ = cmd_tx.send(RuntimeCommand::CreateTask { goal }).await;

    // Run the runtime (blocks until shutdown)
    runtime.run().await;
}

pub fn cmd_status(task_id: Option<String>) {
    let config = KernelConfig::default();
    let store = match Store::open(&config.db_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Cannot open database: {}", e);
            return;
        }
    };

    match task_id {
        Some(id) => {
            match store.get_task(&id) {
                Ok(Some(task)) => {
                    println!("Task: {}", task.id);
                    println!("  Goal: {}", task.goal);
                    println!("  Status: {:?}", task.status);
                    println!("  Step: {}", task.current_step);
                    println!("  Created: {}", task.created_at);
                }
                Ok(None) => println!("Task not found: {}", id),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        None => {
            // List all tasks — query all from DB
            println!("Task listing requires full table scan — implement with list_tasks()");
        }
    }
}

pub fn cmd_trace(task_id: String) {
    let config = KernelConfig::default();
    let store = match Store::open(&config.db_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Cannot open database: {}", e);
            return;
        }
    };

    match store.replay_events(&task_id) {
        Ok(events) => {
            println!("Event trace for task {}:", task_id);
            println!("{:-<60}", "");
            for event in events {
                println!("[{}] {} | {}", event.timestamp, event.event_type, event.payload_json);
            }
        }
        Err(e) => eprintln!("Error: {}", e),
    }
}
```

- [ ] **Step 3: Update main.rs with full CLI**

`crates/ck-cli/src/main.rs`:
```rust
mod commands;

use clap::Parser;

#[derive(Parser)]
#[command(name = "ck", about = "Cognition Kernel - Autonomous Runtime CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Start a new task from a goal
    Start {
        /// The goal to accomplish
        goal: String,
    },
    /// Show task status
    Status {
        /// Optional task ID (shows all if omitted)
        task_id: Option<String>,
    },
    /// Pause a running task
    Pause {
        /// Task ID to pause
        task_id: String,
    },
    /// Resume a paused task
    Resume {
        /// Task ID to resume
        task_id: String,
    },
    /// Cancel a task
    Cancel {
        /// Task ID to cancel
        task_id: String,
    },
    /// Show execution trace
    Trace {
        /// Task ID
        task_id: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start { goal } => commands::cmd_start(goal).await,
        Commands::Status { task_id } => commands::cmd_status(task_id),
        Commands::Trace { task_id } => commands::cmd_trace(task_id),
        Commands::Pause { task_id } => {
            println!("Pause not yet wired to running kernel (requires daemon mode)");
        }
        Commands::Resume { task_id } => {
            println!("Resume not yet wired to running kernel (requires daemon mode)");
        }
        Commands::Cancel { task_id } => {
            println!("Cancel not yet wired to running kernel (requires daemon mode)");
        }
    }
}
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build -p ck-cli`
Expected: Compiles, produces `ck` binary

- [ ] **Step 5: Test CLI help output**

Run: `cargo run -p ck-cli -- --help`
Expected: Shows usage with start, status, pause, resume, cancel, trace commands

- [ ] **Step 6: Commit**

```bash
git add crates/ck-cli/
git commit -m "feat(cli): implement CLI with start, status, trace commands"
```

---

### Task 13: Integration Test — Full End-to-End Loop

**Files:**
- Create: `tests/integration_test.rs` (workspace-level)
- Create: `tests/helpers.rs`

This test verifies the full flow: task creation → cognition (mocked) → execution → verification → completion.

- [ ] **Step 1: Create workspace-level integration test**

Add to root `Cargo.toml`:
```toml
[[test]]
name = "integration"
path = "tests/integration_test.rs"

[dev-dependencies]
ck-kernel = { path = "crates/ck-kernel" }
ck-events = { path = "crates/ck-events" }
ck-memory = { path = "crates/ck-memory" }
ck-verify = { path = "crates/ck-verify" }
ck-recovery = { path = "crates/ck-recovery" }
tokio = { version = "1", features = ["full"] }
tempfile = "3"
```

- [ ] **Step 2: Write integration test**

`tests/integration_test.rs`:
```rust
use ck_kernel::task::{Task, TaskStatus, Plan, PlanStep};
use ck_kernel::config::KernelConfig;
use ck_kernel::runtime::{Runtime, RuntimeCommand};
use ck_events::bus::EventBus;
use ck_events::types::KernelEvent;
use ck_memory::store::{Store, TaskRecord, TaskStatus as DbStatus};
use ck_memory::checkpoint::CheckpointData;
use ck_verify::engine::Verifier;
use ck_verify::strategies::{VerificationStrategy, VerificationResult};
use ck_recovery::engine::{RecoveryEngine, RecoveryDecision, FailureContext};
use ck_recovery::budget::RetryBudget;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// Test: Full task lifecycle without IPC (in-process simulation)
#[tokio::test]
async fn test_full_task_lifecycle() {
    // Create task
    let mut task = Task::new("create test-project directory".into());
    assert_eq!(task.status(), TaskStatus::Created);

    // Transition to planning
    task.transition_to(TaskStatus::Planning).unwrap();
    assert_eq!(task.status(), TaskStatus::Planning);

    // Set plan (simulating cognition response)
    let plan = Plan {
        id: "plan-1".into(),
        steps: vec![
            PlanStep {
                id: "s1".into(),
                description: "create directory".into(),
                tool: "filesystem".into(),
                params: {
                    let mut m = HashMap::new();
                    m.insert("action".into(), serde_json::json!("create_dir"));
                    m.insert("path".into(), serde_json::json!("test-project"));
                    m
                },
                expected_outcome: "directory exists".into(),
                verification_strategy: "file_exists".into(),
            },
            PlanStep {
                id: "s2".into(),
                description: "create main.py".into(),
                tool: "filesystem".into(),
                params: {
                    let mut m = HashMap::new();
                    m.insert("action".into(), serde_json::json!("write_file"));
                    m.insert("path".into(), serde_json::json!("test-project/main.py"));
                    m.insert("content".into(), serde_json::json!("print('hello world')"));
                    m
                },
                expected_outcome: "file exists with content".into(),
                verification_strategy: "file_exists".into(),
            },
        ],
        generated_by: "test".into(),
        reasoning: "simple two-step plan".into(),
    };
    task.set_plan(plan).unwrap();
    assert_eq!(task.status(), TaskStatus::Planned);

    // Begin execution
    task.transition_to(TaskStatus::Executing).unwrap();
    assert_eq!(task.current_step(), 0);

    // Simulate step 1 execution + verification
    let dir = tempfile::tempdir().unwrap();
    let project_dir = dir.path().join("test-project");
    std::fs::create_dir(&project_dir).unwrap();

    let strategy = VerificationStrategy::FileExists {
        path: project_dir.clone(),
        content_contains: None,
    };
    let result = Verifier::verify_strategy(&strategy);
    assert!(matches!(result, VerificationResult::Verified { .. }));

    task.advance_step();
    assert_eq!(task.current_step(), 1);

    // Simulate step 2 execution + verification
    let file_path = project_dir.join("main.py");
    std::fs::write(&file_path, "print('hello world')").unwrap();

    let strategy = VerificationStrategy::FileExists {
        path: file_path,
        content_contains: Some("hello world".into()),
    };
    let result = Verifier::verify_strategy(&strategy);
    assert!(matches!(result, VerificationResult::Verified { .. }));

    task.advance_step();
    assert!(task.is_plan_complete());

    // Complete
    task.transition_to(TaskStatus::Verifying).unwrap();
    task.transition_to(TaskStatus::Completed).unwrap();
    assert_eq!(task.status(), TaskStatus::Completed);
}

/// Test: Recovery triggers on verification failure
#[test]
fn test_recovery_on_failure() {
    let budget = RetryBudget::new(3, 2);

    // First failure → retry
    let ctx = FailureContext {
        task_id: "t1".into(),
        action_id: "a1".into(),
        reason: "file not created".into(),
        retry_count: 0,
        replan_count: 0,
    };
    let decision = RecoveryEngine::decide(&ctx, &budget);
    assert!(matches!(decision, RecoveryDecision::Retry { .. }));

    // After max retries → replan
    let ctx = FailureContext {
        task_id: "t1".into(),
        action_id: "a1".into(),
        reason: "persistent failure".into(),
        retry_count: 3,
        replan_count: 0,
    };
    let decision = RecoveryEngine::decide(&ctx, &budget);
    assert!(matches!(decision, RecoveryDecision::Replan { .. }));
}

/// Test: Event bus delivers events to subscribers
#[tokio::test]
async fn test_event_bus_integration() {
    let bus = EventBus::new(64);
    let mut rx = bus.subscribe();

    bus.emit(KernelEvent::TaskCreated {
        task_id: "t1".into(),
        goal: "test".into(),
        timestamp: 1000,
    });

    let event = rx.recv().await.unwrap();
    match event {
        KernelEvent::TaskCreated { task_id, .. } => assert_eq!(task_id, "t1"),
        _ => panic!("wrong event"),
    }
}

/// Test: Checkpoint save and restore
#[test]
fn test_checkpoint_roundtrip() {
    let data = CheckpointData {
        task_id: "t1".into(),
        goal: "test goal".into(),
        status: "executing".into(),
        plan_json: Some(r#"{"steps":[]}"#.into()),
        current_step: 2,
        retry_count: 1,
        replan_count: 0,
    };

    let blob = data.serialize().unwrap();
    let restored = CheckpointData::deserialize(&blob).unwrap();

    assert_eq!(restored.task_id, "t1");
    assert_eq!(restored.current_step, 2);
    assert_eq!(restored.retry_count, 1);
}

/// Test: Store persists and retrieves tasks
#[test]
fn test_store_persistence() {
    let store = Store::open_in_memory().unwrap();

    let record = TaskRecord {
        id: "t1".into(),
        goal: "persist test".into(),
        status: DbStatus::Executing,
        plan_json: Some("{}".into()),
        current_step: 3,
        retry_budget_json: None,
        created_at: 1000,
        updated_at: 2000,
    };
    store.create_task(&record).unwrap();

    let fetched = store.get_task("t1").unwrap().unwrap();
    assert_eq!(fetched.goal, "persist test");
    assert_eq!(fetched.current_step, 3);
}
```

- [ ] **Step 3: Run integration tests**

Run: `cargo test --test integration`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add tests/
git commit -m "test: add integration tests for full task lifecycle, recovery, events, checkpoints"
```

---

### Task 14: Checkpoint Resume Test

**Files:**
- Create: `tests/checkpoint_resume_test.rs`

This test verifies that a task can be checkpointed and resumed from that checkpoint.

- [ ] **Step 1: Write checkpoint resume test**

`tests/checkpoint_resume_test.rs`:
```rust
use ck_memory::store::Store;
use ck_memory::checkpoint::CheckpointData;
use ck_kernel::task::{Task, TaskStatus, Plan, PlanStep};
use std::collections::HashMap;

/// Simulate: task runs 2 of 4 steps, checkpoint, "crash", resume from checkpoint
#[test]
fn test_checkpoint_and_resume() {
    let store = Store::open_in_memory().unwrap();

    // Create a task with 4 steps
    let mut task = Task::new("four step task".into());
    task.transition_to(TaskStatus::Planning).unwrap();

    let plan = Plan {
        id: "p1".into(),
        steps: (0..4).map(|i| PlanStep {
            id: format!("s{}", i),
            description: format!("step {}", i),
            tool: "shell".into(),
            params: HashMap::new(),
            expected_outcome: "done".into(),
            verification_strategy: "exit_code_zero".into(),
        }).collect(),
        generated_by: "test".into(),
        reasoning: "test plan".into(),
    };
    task.set_plan(plan).unwrap();
    task.transition_to(TaskStatus::Executing).unwrap();

    // Execute steps 0 and 1
    task.advance_step(); // now at step 1
    task.advance_step(); // now at step 2

    // Checkpoint at step 2
    let checkpoint = CheckpointData {
        task_id: task.id().into(),
        goal: task.goal().into(),
        status: format!("{:?}", task.status()),
        plan_json: Some(serde_json::to_string(task.plan().unwrap()).unwrap()),
        current_step: task.current_step(),
        retry_count: 0,
        replan_count: 0,
    };
    let blob = checkpoint.serialize().unwrap();
    store.save_checkpoint("cp1", task.id(), &blob, task.current_step() as i64).unwrap();

    // "Crash" — drop the task
    let task_id = task.id().to_string();
    drop(task);

    // Resume from checkpoint
    let loaded = store.load_latest_checkpoint(&task_id).unwrap().unwrap();
    let restored = CheckpointData::deserialize(&loaded.state_blob).unwrap();

    assert_eq!(restored.task_id, task_id);
    assert_eq!(restored.current_step, 2); // Should resume at step 2
    assert_eq!(restored.goal, "four step task");

    // Reconstruct task and continue from step 2
    let mut resumed_task = Task::new(restored.goal);
    resumed_task.transition_to(TaskStatus::Planning).unwrap();
    let plan: Plan = serde_json::from_str(restored.plan_json.as_ref().unwrap()).unwrap();
    resumed_task.set_plan(plan).unwrap();
    resumed_task.transition_to(TaskStatus::Executing).unwrap();

    // Fast-forward to step 2
    for _ in 0..restored.current_step {
        resumed_task.advance_step();
    }
    assert_eq!(resumed_task.current_step(), 2);

    // Continue execution: steps 2 and 3
    resumed_task.advance_step(); // step 3
    resumed_task.advance_step(); // step 4 (past end)
    assert!(resumed_task.is_plan_complete());

    resumed_task.transition_to(TaskStatus::Verifying).unwrap();
    resumed_task.transition_to(TaskStatus::Completed).unwrap();
    assert_eq!(resumed_task.status(), TaskStatus::Completed);
}
```

- [ ] **Step 2: Run checkpoint resume test**

Run: `cargo test --test checkpoint_resume_test`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/checkpoint_resume_test.rs
git commit -m "test: add checkpoint resume test verifying task continuity across crashes"
```

- [ ] **Step 4: Run full test suite**

Run: `cargo test --workspace`
Expected: ALL PASS

- [ ] **Step 5: Final commit — tag V1 milestone**

```bash
git add -A
git commit -m "milestone: Cognition Kernel V1 complete — full vertical slice"
git tag v0.1.0
```

---

## Execution Notes

**Build order matters:** Tasks 1-6 can be built independently. Tasks 7-8 depend on 2-6. Task 9 depends on 8. Tasks 10-11 are independent of each other but need Task 4 (IPC types). Task 12 depends on 7-8. Tasks 13-14 depend on everything.

**Parallel opportunities:**
- Tasks 2, 3, 4, 5, 6 can all be built in parallel after Task 1
- Tasks 10 (Go) and 11 (Python) can be built in parallel
- Task 12 (CLI) can start once Task 8 compiles

**Testing strategy:**
- Each task has its own tests that pass independently
- Integration tests (13-14) verify the full system works together
- Go tests run via `go test`, Python via `pytest`, Rust via `cargo test`

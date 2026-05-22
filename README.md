# Cognition Kernel

> A local-first autonomous cognitive runtime that continuously translates human intent into reliable real-world computer execution.

Cognition Kernel is not a chatbot wrapper, a workflow engine, or an AI assistant. It is a **persistent execution substrate** — a runtime that accepts goals, maintains state across sessions, plans execution using LLMs, operates tools on your machine, verifies outcomes, and recovers from failures autonomously.

The runtime itself is the product. Not the UI. Not the API. Not the prompts.

---

## Quick Start

**Prerequisites:** Rust 1.75+, Python 3.12+, Go 1.22+

```powershell
# 1. Clone
git clone https://github.com/chandankumar123456/Cognition-Kernel.git
cd "Cognition Kernel"

# 2. Set up Python venv (one time)
cd cognition
uv venv .venv
uv pip install -r requirements.txt
cd ..

# 3. Build Go worker (one time — ck start also does this automatically)
cd workers && go build -o bin/ck-worker.exe ./cmd/ck-worker && cd ..

# 4. Set your LLM API key
$env:OPENAI_API_KEY = "sk-..."   # PowerShell
# export OPENAI_API_KEY=sk-...   # bash

# 5. Run
cargo run -p ck-cli -- start "create a file called hello.txt with the content hello world"
```

That's it. `ck start` spawns the Go worker and Python cognition engine automatically, connects everything, runs the task end-to-end, and exits cleanly.

**Watch live execution in a second terminal:**

```powershell
cargo run -p ck-cli -- watch
```

**Check task history:**

```powershell
cargo run -p ck-cli -- status
cargo run -p ck-cli -- trace <task-id>
```

---

## Table of Contents

- [Quick Start](#quick-start)
- [What It Is](#what-it-is)
- [Architecture](#architecture)
- [Language Allocation](#language-allocation)
- [Project Structure](#project-structure)
- [Core Components](#core-components)
- [Execution Flow](#execution-flow)
- [Data Model](#data-model)
- [IPC Protocol](#ipc-protocol)
- [Configuration](#configuration)
- [Building](#building)
- [Running Tests](#running-tests)
- [CLI Reference](#cli-reference)
- [Development Status](#development-status)

---

## What It Is

Traditional software requires humans to orchestrate execution — open apps, click buttons, sequence steps, handle errors, track state. Cognition Kernel replaces that manual orchestration with a continuously running intelligent runtime.

```
OLD:  Human → manual steps → result
NEW:  Human → intent → Cognition Kernel → result
```

The system operates a continuous loop:

```
observe → decide → act → verify → recover → persist → continue
```

It never truly ends. It becomes idle when objectives are satisfied.

| Problem | Typical AI agents | Cognition Kernel |
|---------|------------------|-----------------|
| State across restarts | Lost | Checkpointed to SQLite |
| Long-running tasks | Fail mid-way | Resume from checkpoint |
| Tool failure handling | Break permanently | Retry → replan → escalate |
| Execution continuity | Per-prompt | Persistent runtime loop |
| Recovery | None | First-class engine |

---

## Architecture

```
USER / GOAL
    │
    ▼
┌─────────────────────────────────────┐
│  CLI  (Rust — ck-cli)               │
└──────────────┬──────────────────────┘
               │ in-process
               ▼
╔═══════════════════════════════════════════════════════╗
║           COGNITION KERNEL  (Rust process)            ║
║                                                       ║
║  Runtime Kernel                                       ║
║    ├── Task State Machine (type-safe FSM)             ║
║    ├── Scheduler (10ms tick, priority queue)          ║
║    ├── Worker Supervisor (spawn/monitor/restart)      ║
║    ├── Verification Engine (in-process)               ║
║    ├── Recovery Engine (retry/replan/escalate)        ║
║    ├── Memory System (SQLite WAL + bincode)           ║
║    └── Event Bus (tokio broadcast, lock-free)         ║
║                                                       ║
╚═══════════════════════════════════════════════════════╝
        │  Named Pipe + MessagePack    │
        ▼                              ▼
┌──────────────────┐        ┌─────────────────────────┐
│ COGNITION        │        │ TOOL WORKERS  (Go)       │
│ (Python)         │        │                         │
│ LiteLLM          │        │  shell — subprocess     │
│ Planner          │        │  filesystem — atomic I/O │
│ Reasoner         │        │                         │
└──────────────────┘        └──────────────┬──────────┘
                                           │
                                           ▼
                                ┌──────────────────┐
                                │ COMPUTER / WORLD │
                                └──────────────────┘
```

The Rust kernel process never blocks on Python's GIL or Go's GC. Workers can crash without killing the kernel — the supervisor restarts them and re-dispatches pending actions.

---

## Language Allocation

| Component | Language | Reason |
|-----------|----------|--------|
| Runtime kernel, FSM, scheduler | Rust | Deterministic execution, no GC pauses, memory safety, tokio async |
| Event bus | Rust | Lock-free broadcast channels, zero-copy |
| Memory / SQLite | Rust | Direct C FFI via rusqlite, WAL mode, bincode serialization |
| IPC protocol | Rust | Shared framing layer |
| Verification engine | Rust | In-process, microsecond latency |
| Recovery engine | Rust | Deterministic decision logic |
| CLI | Rust | Native binary, instant startup |
| Cognition engine | Python | LLM ecosystem (LiteLLM), rapid prompt iteration |
| Tool workers | Go | Goroutines for concurrent tool execution, excellent subprocess management |

---

## Project Structure

```
cognition-kernel/
├── Cargo.toml                     # Rust workspace (7 crates)
├── crates/
│   ├── ck-kernel/                 # Runtime kernel binary + library
│   │   └── src/
│   │       ├── main.rs            # Entry point, tokio bootstrap
│   │       ├── runtime.rs         # Main execution loop
│   │       ├── task.rs            # Task model + type-safe FSM
│   │       ├── supervisor.rs      # Worker process management
│   │       └── config.rs         # KernelConfig
│   ├── ck-events/                 # Event bus + event types
│   │   └── src/
│   │       ├── bus.rs             # tokio broadcast wrapper
│   │       ├── types.rs          # KernelEvent enum (12 variants)
│   │       └── log.rs            # EventLog trait
│   ├── ck-memory/                 # SQLite persistence
│   │   └── src/
│   │       ├── store.rs          # CRUD: tasks, events, checkpoints
│   │       ├── schema.rs         # Schema init + WAL pragma
│   │       └── checkpoint.rs     # bincode CheckpointData
│   ├── ck-ipc/                    # IPC protocol
│   │   └── src/
│   │       ├── protocol.rs       # MessagePack length-framing
│   │       ├── server.rs         # Named Pipe server (Windows)
│   │       └── types.rs          # Request/Response types
│   ├── ck-verify/                 # Verification engine
│   │   └── src/
│   │       ├── engine.rs         # Verifier dispatch
│   │       └── strategies.rs     # FileExists, ExitCodeZero, OutputContains...
│   ├── ck-recovery/               # Recovery engine
│   │   └── src/
│   │       ├── engine.rs         # RecoveryEngine::decide()
│   │       └── budget.rs         # RetryBudget
│   └── ck-cli/                    # CLI interface
│       └── src/
│           ├── main.rs            # clap command definitions
│           └── commands.rs        # start, status, trace handlers
├── cognition/                     # Python cognition engine
│   └── cognition_kernel/
│       ├── engine.py             # Main loop (IPC ↔ LLM)
│       ├── planner.py            # Plan generation via LiteLLM
│       ├── reasoner.py           # Reflection and evaluation
│       ├── context.py            # Prompt assembly
│       ├── ipc.py                # MessagePack pipe client
│       └── models.py             # PlanStep, CognitionRequest/Response
├── workers/                       # Go tool workers
│   ├── cmd/ck-worker/main.go     # Worker entry + tool dispatch
│   ├── internal/
│   │   ├── shell/executor.go     # Shell execution with timeout
│   │   ├── filesystem/worker.go  # File operations (atomic writes)
│   │   └── ipc/client.go         # MessagePack pipe client
│   └── pkg/protocol/types.go     # Shared ExecutionRequest/Response
└── tests/
    ├── integration_test.rs        # Full lifecycle tests (5 tests)
    └── checkpoint_resume_test.rs  # Crash + resume simulation
```


---

## Core Components

### Runtime Kernel

**Crate:** `ck-kernel` | **File:** `src/runtime.rs`

The kernel owns the main execution loop and coordinates all other components. It is the single source of truth for runtime state.

**Responsibilities:**
- Drives the `while runtime_alive` loop at 10ms ticks
- Creates and transitions tasks through their lifecycle
- Dispatches to cognition and execution on each tick
- Saves checkpoints after every state change
- Routes all events through the event bus
- Manages the worker supervisor (spawn/restart Python and Go processes)

**Command interface:**

```rust
pub enum RuntimeCommand {
    CreateTask { goal: String },
    PauseTask  { task_id: String },
    ResumeTask { task_id: String },
    CancelTask { task_id: String },
    Shutdown,
}
```

Commands are sent through a `tokio::sync::mpsc` channel. The kernel polls it on every tick, which means it is non-blocking and never stalls on command arrival.

**Concurrency model:** Single tokio runtime. The loop is logically sequential per task (one step at a time), but all I/O (SQLite, IPC, event dispatch) is async. Multiple tasks can be in flight simultaneously — each advances one step per tick.

---

### Task State Machine

**Crate:** `ck-kernel` | **File:** `src/task.rs`

Tasks are the primary unit of work. Every task has an identity, a goal, a plan (once generated), and a position in the execution graph.

**States and valid transitions:**

```
Created ──→ Planning ──→ Planned ──→ Executing ──→ Verifying
                                                       │
                                          ┌────────────┤
                                          ▼            │
                                      Recovering ──────┤
                                          │            │
                                ┌─────────┤            ▼
                                ▼         ▼        Completed
                            Escalated  Planned
                                │
                                ▼
                             Failed
```

State transitions are validated at runtime — calling `transition_to()` with an invalid next state returns `Err(TaskError::InvalidTransition)`. This makes illegal states unrepresentable at the logic level.

**Task structure:**

```rust
pub struct Task {
    id: String,          // ULID — sortable, unique
    goal: String,
    status: TaskStatus,
    plan: Option<Plan>,
    current_step: usize,
    retry_count: u32,
    replan_count: u32,
    created_at: i64,     // Unix ms
    updated_at: i64,
}
```

**Plan structure:**

```rust
pub struct Plan {
    pub id: String,
    pub steps: Vec<PlanStep>,
    pub generated_by: String,  // model name, e.g. "gpt-4o-mini"
    pub reasoning: String,
}

pub struct PlanStep {
    pub id: String,
    pub description: String,
    pub tool: String,          // "shell" | "filesystem" | "browser" | "desktop"
    pub params: HashMap<String, serde_json::Value>,
    pub expected_outcome: String,
    pub verification_strategy: String,
}
```

---

### Event Bus

**Crate:** `ck-events` | **File:** `src/bus.rs`

All internal communication flows through the event bus. It uses `tokio::sync::broadcast` — a lock-free MPSC channel. Subscribers receive a clone of every event emitted after they subscribe.

**Usage:**

```rust
let bus = EventBus::new(1024);  // capacity 1024 events
let mut rx = bus.subscribe();

bus.emit(KernelEvent::TaskCreated { task_id: "...", goal: "...", timestamp: 0 });

let event = rx.recv().await.unwrap();
```

**Event types (12 variants):**

| Event | Emitted when |
|-------|-------------|
| `TaskCreated` | A new task is accepted |
| `PlanGenerated` | Cognition returns a plan |
| `ActionDispatched` | A step is sent to the tool worker |
| `ActionCompleted` | Worker responds with result |
| `VerificationPassed` | Outcome matches expected state |
| `VerificationFailed` | Outcome does not match |
| `RecoveryTriggered` | Recovery engine takes action |
| `TaskCompleted` | All steps verified, task done |
| `TaskFailed` | Task failed after exhausting recovery |
| `CheckpointSaved` | State written to SQLite |
| `WorkerSpawned` | Supervisor started a worker process |
| `WorkerCrashed` | Worker process exited unexpectedly |

**Event sourcing:** All events are persisted to an append-only `events` table in SQLite. Current state can be reconstructed by replaying events from any checkpoint.

---

### Memory System

**Crate:** `ck-memory` | **Files:** `src/store.rs`, `src/schema.rs`, `src/checkpoint.rs`

All persistence goes through the memory system. No other component writes to SQLite directly.

**SQLite schema (WAL mode):**

```sql
-- Task lifecycle state
tasks (id, goal, status, plan_json, current_step, retry_budget_json, created_at, updated_at)

-- Append-only event log
events (id AUTOINCREMENT, task_id, event_type, payload_json, timestamp)
INDEX idx_events_task ON events(task_id, timestamp)

-- Checkpoint blobs for resume
checkpoints (id, task_id, state_blob BLOB, step_index, created_at)
INDEX idx_checkpoints_task ON checkpoints(task_id, created_at DESC)

-- Per-action execution history
actions (id, task_id, step_index, tool, params_json, result_json, success, duration_ms, created_at)
```

**WAL mode benefits:** Multiple readers can query the database while the kernel is writing. The kernel never blocks on reads from the CLI.

**Checkpoint serialization:** Checkpoints use `bincode` (Rust binary format) — not JSON. This gives compact, fast serialization with deterministic field ordering. The `CheckpointData` struct is:

```rust
pub struct CheckpointData {
    pub task_id: String,
    pub goal: String,
    pub status: String,
    pub plan_json: Option<String>,
    pub current_step: usize,
    pub retry_count: u32,
    pub replan_count: u32,
}
```

**Resume on restart:** On startup, the kernel scans for tasks with non-terminal status (`executing`, `planned`, `recovering`) and loads their latest checkpoint. Execution continues from `current_step`.

---

### IPC Protocol

**Crate:** `ck-ipc` | **Files:** `src/protocol.rs`, `src/server.rs`, `src/types.rs`

The kernel communicates with external processes (Python cognition, Go workers) via Named Pipes on Windows (Unix Domain Sockets on Linux/Mac) with MessagePack serialization.

**Framing format:**

```
┌──────────────────────┬─────────────────────────────┐
│  4 bytes             │  N bytes                    │
│  message length      │  MessagePack payload        │
│  (u32 big-endian)    │                             │
└──────────────────────┴─────────────────────────────┘
```

**Kernel → Cognition:**

```rust
pub struct CognitionRequest {
    pub request_type: String,   // "plan" | "replan" | "reflect" | "verify_complex"
    pub task_id: String,
    pub objective: String,
    pub current_state: HashMap<String, Value>,
    pub memory_context: HashMap<String, Value>,
    pub failure_context: Option<HashMap<String, Value>>,
}
```

**Cognition → Kernel:**

```rust
pub struct CognitionResponse {
    pub task_id: String,
    pub response_type: String,  // "plan" | "decision"
    pub plan: Option<Vec<PlanStep>>,
    pub reasoning: String,
}
```

**Kernel → Worker:**

```rust
pub struct ExecutionRequest {
    pub task_id: String,
    pub action_id: String,
    pub tool: String,           // "shell" | "filesystem"
    pub params: HashMap<String, Value>,
    pub timeout_ms: u64,
}
```

**Worker → Kernel:**

```rust
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

---

### Verification Engine

**Crate:** `ck-verify` | **Files:** `src/engine.rs`, `src/strategies.rs`

Verification runs in-kernel (no IPC overhead). After every tool execution, the kernel checks whether the world changed correctly.

**Strategies:**

```rust
pub enum VerificationStrategy {
    // File or directory exists, optionally contains expected string
    FileExists { path: PathBuf, content_contains: Option<String> },
    // Last action exited with code 0
    ExitCodeZero,
    // Output of last action contains a substring
    OutputContains { expected: String },
    // File was modified after a given timestamp
    FileModified { path: PathBuf, after_ms: i64 },
    // A named process is running
    ProcessRunning { name: String },
    // Delegate to cognition engine for semantic check
    CognitionVerify { context: String },
}
```

**Result:**

```rust
pub enum VerificationResult {
    Verified { evidence: String },
    Failed { reason: String, actual: String, expected: String },
}
```

Simple strategies (`FileExists`, `ExitCodeZero`, `OutputContains`) execute in microseconds in-process. `CognitionVerify` delegates to the Python process via IPC for semantic checks ("did the output make logical sense?").

---

### Recovery Engine

**Crate:** `ck-recovery` | **Files:** `src/engine.rs`, `src/budget.rs`

Recovery is not optional — it is part of the normal execution path. Every verification failure flows through the recovery engine before the kernel decides what to do next.

**Decision logic:**

```
VerificationFailed
    │
    ▼
retry_count < max_retries?
    ├─ yes → Retry { backoff_ms: 500 × 2^attempt }
    └─ no
        │
        ▼
    replan_count < max_replans?
        ├─ yes → Replan { failure_context: "..." }
        └─ no → Escalate { reason: "..." }
```

**Backoff schedule:**

| Attempt | Wait |
|---------|------|
| 0 | 500ms |
| 1 | 1000ms |
| 2 | 2000ms |
| 3 | → Replan |

**Default budget:** 3 retries per action, 2 replans per task. Both are configurable via `KernelConfig`.

**Recovery decisions:**

| Decision | Effect |
|----------|--------|
| `Retry` | Re-dispatch same action to worker after backoff |
| `Replan` | Send failure context to cognition for a revised plan |
| `Rollback` | Restore last good checkpoint, discard subsequent actions |
| `Escalate` | Pause task, emit `TaskEscalated` event, wait for human via CLI |

---

### Cognition Engine

**Language:** Python | **Package:** `cognition/cognition_kernel/`

The cognition engine runs as a separate process. It connects to the kernel's Named Pipe on startup, then loops — reading `CognitionRequest` messages, calling an LLM, and writing `CognitionResponse` messages back.

**Components:**

| File | Responsibility |
|------|---------------|
| `engine.py` | IPC loop, request routing, serialization |
| `planner.py` | `generate_plan()` — calls LiteLLM, parses JSON plan |
| `reasoner.py` | `reflect()` — evaluates current execution state |
| `context.py` | Assembles system + user prompts |
| `ipc.py` | `PipeClient` — length-prefixed msgpack over Named Pipe |
| `models.py` | `PlanStep`, `CognitionRequest`, `CognitionResponse` |

**Plan generation:**

The planner sends a structured prompt asking the LLM to decompose the goal into steps. Each step must specify a tool, parameters, expected outcome, and verification strategy. Response is parsed from JSON.

```python
@dataclass
class PlanStep:
    description: str
    tool: str                   # "shell" | "filesystem" | "browser" | "desktop"
    params: dict
    expected_outcome: str
    verification_strategy: str  # "file_exists" | "exit_code_zero" | "output_contains"
```

**LLM abstraction:** LiteLLM is used as the provider layer. Switch models by changing the `model` parameter — OpenAI, Anthropic, Gemini, and local models (Ollama) are all supported without code changes.

**Windows IPC:** Uses `pywin32` (`win32file.CreateFile` + `win32file.ReadFile/WriteFile`) to open Named Pipes as file handles, wrapped in `asyncio.run_in_executor` for non-blocking I/O.

**Startup:**

```bash
python -m cognition_kernel.engine --pipe \\.\pipe\ck-cognition
```

> In normal use, `ck start` spawns this process automatically. Manual startup is only needed for development.

---

### Tool Workers

**Language:** Go | **Module:** `workers/`

Tool workers run as a separate process. They connect to the kernel's Named Pipe, loop reading `ExecutionRequest` messages, execute the requested action, and write `ExecutionResponse` back.

**Supported tools:**

**Shell (`tool: "shell"`):**

```json
{
  "tool": "shell",
  "params": { "command": "echo hello", "work_dir": "C:\\projects" },
  "timeout_ms": 30000
}
```

Uses `os/exec` with `context.WithTimeout`. On Windows: `cmd /C <command>`. On Linux/Mac: `sh -c <command>`. Captures stdout and stderr separately. Returns exit code, output, and elapsed duration.

**Filesystem (`tool: "filesystem"`):**

```json
{ "tool": "filesystem", "params": { "action": "write_file", "path": "out.txt", "content": "hello" } }
{ "tool": "filesystem", "params": { "action": "create_dir", "path": "mydir" } }
{ "tool": "filesystem", "params": { "action": "read_file",  "path": "out.txt" } }
{ "tool": "filesystem", "params": { "action": "delete",     "path": "out.txt" } }
```

`write_file` is atomic: writes to `<path>.tmp`, then renames. This prevents partial writes from being visible to verification.

**Startup:**

```bash
./ck-worker --pipe \\.\pipe\ck-worker
```

> In normal use, `ck start` spawns this process automatically and builds the binary if needed.

---

### CLI

**Crate:** `ck-cli` | **Files:** `src/main.rs`, `src/commands.rs`

The CLI is a thin adapter — it creates a `Runtime` instance and sends commands through the `mpsc` channel. It owns no business logic.

**Implementation note:** In the current V1, `ck start` runs the kernel inline (blocking the terminal). A future daemon mode will allow the kernel to run as a background process and all CLI commands will communicate with it via IPC.

---

### TUI — `ck watch`

**Crate:** `ck-cli` | **File:** `src/tui.rs`

A live ratatui terminal view that polls SQLite every 500ms and renders two panels:

- **Top — Tasks table:** ID (first 8 chars), Goal (truncated), Status (color-coded), Step index
- **Bottom — Event stream:** Last 20 events in chronological order with timestamp and task ID

Status colors: `Executing` = cyan, `Completed` = green, `Failed` = red, `Escalated` = yellow.

Press `q` or `Ctrl+C` to exit. Reads directly from the SQLite database — no extra IPC connection required.


---

## Execution Flow

A complete goal execution from CLI input to task completion:

```
1. User: ck start "create a directory called test-project with main.py"
   │ (Rust binary, <10ms startup)
   │
2. CLI → Kernel (in-process): RuntimeCommand::CreateTask { goal }
   │
3. Kernel:
   │  task_id = ULID::new()
   │  TaskStatus: Created
   │  persist to SQLite
   │  emit(TaskCreated)
   │
4. Kernel loop tick → request_plan(task_id)
   │  TaskStatus: Planning
   │  build CognitionRequest { type: "plan", objective, ... }
   │  → Named Pipe → Python process
   │
5. Python cognition:
   │  assemble prompt
   │  LiteLLM call → LLM response (JSON array of steps)
   │  parse PlanSteps
   │  → Named Pipe → Kernel: CognitionResponse { plan: [...] }
   │
6. Kernel:
   │  TaskStatus: Planned
   │  persist plan to SQLite
   │  checkpoint()
   │  emit(PlanGenerated)
   │
7. FOR EACH STEP:
   │
   ├─ Kernel loop tick → execute_next_step(task_id)
   │    action_id = ULID::new()
   │    emit(ActionDispatched)
   │    → Named Pipe → Go worker: ExecutionRequest { tool: "filesystem", params }
   │
   ├─ Go worker:
   │    execute action (e.g. os.MkdirAll or atomic file write)
   │    → Named Pipe → Kernel: ExecutionResponse { success, output, duration_ms }
   │
   ├─ Kernel: Verifier::verify_strategy(FileExists { path })
   │    Result: Verified { evidence: "file exists at ..." }
   │
   ├─ IF VerificationFailed:
   │    RecoveryEngine::decide(failure, budget)
   │    → Retry (backoff) / Replan (→ cognition) / Escalate (→ human)
   │
   ├─ persist action result
   │  checkpoint()
   │  emit(VerificationPassed)
   │
8. task.is_plan_complete() == true
   │  TaskStatus: Completed
   │  final checkpoint
   │  emit(TaskCompleted)
   │  CLI prints: "Task completed"
```

---

## Data Model

### Task

```
id          ULID (sortable unique identifier)
goal        Natural language string from user
status      Created | Planning | Planned | Executing | Verifying |
            Recovering | Completed | Failed | Escalated
plan        Optional<Plan>
current_step Index into plan.steps
retry_count  Attempts on current action (reset on replan)
replan_count Replans issued for this task
created_at  Unix timestamp ms
updated_at  Unix timestamp ms
```

### Plan

```
id           ULID
steps        Vec<PlanStep>
generated_by Model name (e.g. "gpt-4o-mini")
reasoning    LLM's explanation of the approach
```

### PlanStep

```
id                   ULID
description          Human-readable step description
tool                 "shell" | "filesystem" | "browser" | "desktop"
params               Tool-specific parameters (JSON object)
expected_outcome     What success looks like
verification_strategy How to verify: "file_exists" | "exit_code_zero" | "output_contains"
```

### Checkpoint

Stored as a `bincode`-serialized blob in the `checkpoints` table. Contains full task state at the time of saving, including plan JSON and step index. On restart, the kernel loads the latest checkpoint for any non-terminal task and resumes from that point.

---

## IPC Protocol

### Transport

- **Windows:** Named Pipes (`\\.\pipe\<name>`)
- **Linux/Mac:** Unix Domain Sockets

### Framing

Every message is prefixed with a 4-byte big-endian length, followed by a MessagePack-encoded payload:

```
[u32: length][msgpack bytes...]
```

This framing is implemented in both the Rust kernel (`ck-ipc`) and the Go/Python clients.

### Pipe Names (default)

| Pipe | Purpose |
|------|---------|
| `ck-cognition` | Kernel ↔ Python cognition engine |
| `ck-worker` | Kernel ↔ Go tool workers |

### Kernel → Cognition request types

| `request_type` | When sent |
|---------------|-----------|
| `"plan"` | New task needs initial plan |
| `"replan"` | Previous plan exhausted retries |
| `"reflect"` | Kernel wants evaluation of current state |
| `"verify_complex"` | Verify outcome semantically (not just structurally) |

---

## Configuration

`KernelConfig` is loaded at startup. Currently defaults are used; file-based config is planned for V2.

```rust
pub struct KernelConfig {
    pub db_path: String,            // default: "cognition_kernel.db"
    pub cognition_pipe: String,     // default: "ck-cognition"
    pub worker_pipe: String,        // default: "ck-worker"
    pub max_concurrent_tasks: usize, // default: 10
    pub max_retries: u32,           // default: 3  (per action)
    pub max_replans: u32,           // default: 2  (per task)
    pub default_timeout_ms: u64,    // default: 30000
}
```

**Recovery budget** controls how many times the kernel will retry a failing action before escalating to a replan, and how many replans it will request before giving up and escalating to the human operator.

---

## Building

### Prerequisites

- **Rust** 1.75+ with cargo
- **Python** 3.12+ with `uv`
- **Go** 1.22+
- **pywin32** — installed automatically via the venv setup below

### One-time setup

**1. Python venv**

```powershell
cd cognition
uv venv .venv
uv pip install -r requirements.txt
```

This creates `.venv/` with all dependencies (litellm, msgpack, pywin32). The kernel uses `.venv/Scripts/python.exe` automatically — no system Python needed.

**2. Go worker binary**

```powershell
cd workers
go build -o bin/ck-worker.exe ./cmd/ck-worker
```

Or skip this — `ck start` will build it automatically on first run.

### Rust workspace

```powershell
# Build all crates
cargo build

# Build release binaries
cargo build --release

# CLI binary only
cargo build -p ck-cli
# Output: C:\cargo-targets\cognition-kernel\debug\ck.exe
```

### Starting the full system

All components start automatically with a single command:

```powershell
# Set your LLM API key
$env:OPENAI_API_KEY = "sk-..."          # or ANTHROPIC_API_KEY, etc.

# Start — spawns Go worker + Python cognition, then runs kernel
cargo run -p ck-cli -- start "create a file called hello.txt with the content hello world"
```

`ck start` automatically:
1. Builds the Go worker binary if it doesn't exist yet (`workers/bin/ck-worker.exe`)
2. Spawns the Go tool worker process
3. Spawns the Python cognition engine process (using `.venv/Scripts/python.exe`)
4. Creates pipe endpoints, waits for both workers to connect
5. Creates and runs the task to completion
6. Kills worker processes on exit

**Optional — live TUI view (second terminal):**

```powershell
cargo run -p ck-cli -- watch
```

---

## Running Tests

### Rust (all crates)

```bash
# All crates
cargo test --workspace

# Individual crates
cargo test -p ck-events
cargo test -p ck-memory
cargo test -p ck-ipc
cargo test -p ck-verify
cargo test -p ck-recovery
cargo test -p ck-kernel

# Integration tests
cargo test --test integration_test
cargo test --test checkpoint_resume_test
```

### Go workers

```bash
cd workers
go test ./...

# Individual packages
go test ./internal/shell/...
go test ./internal/filesystem/...
```

### Python cognition

```bash
cd cognition

# With uv
uv run pytest tests/ -v

# With pip
pytest tests/ -v
```

### Test coverage summary

| Suite | Tests | What it covers |
|-------|-------|---------------|
| `ck-events` | 2 | Event emission, multiple subscribers |
| `ck-memory` | 4 | Task CRUD, event append/replay, checkpoint save/load |
| `ck-ipc` | 2 | MessagePack encode/decode roundtrip |
| `ck-verify` | 6 | File exists/missing, content match, exit code, output match |
| `ck-recovery` | 4 | Retry, replan, escalate decisions, backoff curve |
| `ck-kernel` | 5 | Task creation, valid/invalid transitions, set_plan, advance_step |
| Integration | 5 | Full lifecycle, recovery flow, event bus, checkpoint roundtrip, store |
| Checkpoint resume | 1 | Crash + resume from mid-task checkpoint |
| Go shell | 3 | Echo command, timeout, non-zero exit |
| Go filesystem | 3 | create_dir, write+read, delete |
| Python planner | 2 | Mocked LLM parse, wrapped JSON parse |
| Python IPC | 1 | Write message framing |

---

## CLI Reference

```
ck <command> [args]
```

| Command | Description |
|---------|-------------|
| `ck start "<goal>"` | Create a task from a natural language goal and start the runtime (auto-spawns workers) |
| `ck status` | List all tasks and their current status |
| `ck status <task_id>` | Show detailed state for a specific task |
| `ck trace <task_id>` | Print the full event log for a task (timestamped) |
| `ck watch` | Open live TUI — real-time task table and event stream |
| `ck pause <task_id>` | Pause a running task (saves checkpoint) |
| `ck resume <task_id>` | Resume a paused task from its last checkpoint |
| `ck cancel <task_id>` | Cancel a task and mark it as Failed |

**Examples:**

```bash
# Start a task (workers auto-spawned)
ck start "create a Python script at ~/scripts/backup.py that zips the Desktop folder"

# Check what's running
ck status

# Live TUI view
ck watch

# View execution trace
ck trace 01HX1ABCDEF2345678GHJKM

# Pause and resume
ck pause 01HX1ABCDEF2345678GHJKM
ck resume 01HX1ABCDEF2345678GHJKM
```

**Task IDs** are [ULIDs](https://github.com/ulid/spec) — lexicographically sortable, unique, and timestamp-prefixed. They appear in all log output and event traces.

---

## Development Status

**V1 — Implemented**

- [x] Rust workspace with 7 crates
- [x] Task model with validated state machine (9 states)
- [x] Event bus (tokio broadcast, 12 event types)
- [x] SQLite persistence (WAL mode) — tasks, events, checkpoints, actions
- [x] Bincode checkpoint serialization + resume on restart
- [x] MessagePack IPC protocol with Named Pipe server
- [x] Verification engine (FileExists, ExitCodeZero, OutputContains, FileModified)
- [x] Recovery engine (retry with exponential backoff, replan, escalate)
- [x] Runtime kernel main loop (10ms tick, task stepping, checkpoint on state change)
- [x] Worker supervisor (spawn, health check, auto-restart)
- [x] Go tool workers — shell (cross-platform, timeout) + filesystem (atomic writes)
- [x] Python cognition engine — LiteLLM planning, replan, reflection, IPC loop
- [x] End-to-end IPC wiring — kernel ↔ cognition ↔ workers over Named Pipes
- [x] Event persistence — all events written to SQLite for full replay
- [x] Replan flow — `Task::start_replan()` resets FSM cleanly on recovery
- [x] CLI — start, status, trace, watch, pause, resume, cancel
- [x] `ck watch` — ratatui live TUI (task table + event stream)
- [x] Single command startup — `ck start` auto-spawns all workers
- [x] Windows Named Pipe IPC — Go (go-winio), Python (pywin32 win32file), Rust (tokio)
- [x] 38 tests across all layers

**V1.1 — Implemented**

- [x] Real resume from checkpoint (plan + step restored, not just loaded)
- [x] Rollback restores from last good checkpoint instead of escalating
- [x] retry_count, replan_count, plan, current_step all persisted to SQLite
- [x] Supervisor health check every 5s — dead workers restarted, connections reset
- [x] Interrupted tasks from previous session marked failed on startup (clean slate)
- [x] Step outputs propagated as `current_state` to LLM on each planning request
- [x] Filesystem side effects auto-verified post-execution
- [x] Path sandbox — absolute paths outside `work_dir` blocked before dispatch
- [x] `ck trace` shows plan steps with ✓/○ completion markers + readable event summaries
- [x] uv venv setup with `requirements.txt` for reproducible Python environment
- [x] 9/9 integration tests pass (4 new tests for V1.1 fixes)

**V1.2 — Implemented**

- [x] Action outputs persisted to SQLite `actions` table
- [x] `ck status <id>` shows command output previews (tool, duration, output)
- [x] `ck trace <id>` shows action outputs inline with events
- [x] Failure context passed to LLM on replan requests (`request_type: "replan"`)
- [x] Playwright browser tool worker via Python bridge (navigate, screenshot, click, fill_form)
- [x] Daemon mode — `ck daemon` starts kernel in background, `ck stop` shuts it down
- [x] OS-aware system prompt (Windows: PowerShell/cmd; Linux: sh)
- [x] 10 critical/high bugs fixed: sandbox escape, nil slice deserialization, replan handler, resume ID mismatch, timestamp consistency, daemon worker lifecycle, pipe reconnect after crash

**V2 — Planned**

- [ ] Desktop tool worker (screen capture, input simulation)
- [ ] Long-term memory (vector search for task history)
- [ ] Config file support (`cognition-kernel.toml`)
- [ ] `ck approve` / `ck reject` for escalated tasks
- [ ] Linux/Mac Unix socket fallback
- [ ] Multi-task parallelism (spawn step_task per task)

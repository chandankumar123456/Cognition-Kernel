# Cognition Kernel — System Design Specification

> **Local-first autonomous cognitive runtime that continuously translates human intent into reliable real-world computer execution.**

**Date:** 2026-05-22
**Status:** Draft
**Codename:** Cognition Kernel (formerly AgentOS)

---

## 1. System Identity

Cognition Kernel is:
- A persistent autonomous runtime
- A stateful cognitive execution system
- A local-first machine-level execution substrate
- An observe → decide → act → verify → recover runtime
- A runtime that continuously transforms environment state toward objective completion

Cognition Kernel is NOT:
- A chatbot, web app, or API-first platform
- A LangGraph/LangChain wrapper
- A browser automation tool
- A microservices experiment
- A workflow engine pretending to be autonomy

The runtime itself is the product. Not the UI. Not the API. Not the prompts.

---

## 2. Core Problem

There is no reliable runtime infrastructure for autonomous intelligent execution.

Current AI systems fail because they are:
- Stateless (every prompt starts over)
- Short-lived (no execution continuity)
- Weak at recovery (break once, fail permanently)
- Environment-unaware (live inside chat windows)
- Request-response shaped (not continuously running)

Cognition Kernel bridges LLM cognition with runtime continuity to create a persistent autonomous execution system.

---

## 3. Execution Model

Single persistent runtime kernel running a continuous loop:

```
while runtime_alive:
    observe_world_state()
    synchronize_internal_state()
    evaluate_objectives()
    determine_next_action()
    execute_action()
    verify_environment_changes()
    recover_if_needed()
    persist_state()
```

Characteristics:
- Stateful, interruptible, recoverable
- Deterministic where needed, autonomous where needed
- Event-driven internally
- Local-first with persistent memory
- Tool-isolated, supervision-aware

**Note on "observe":** In V1, observation is implicit — the kernel observes via verification results and tool outputs. Explicit environment observation (screenshots, process enumeration) is a V2 concern for desktop-native awareness.

---

## 4. Language Allocation

| Component | Language | Rationale |
|-----------|----------|-----------|
| Runtime Kernel | Rust | Deterministic execution, memory safety, zero-cost abstractions, no GC pauses, true concurrency via tokio |
| Event Bus | Rust | Lock-free channels, nanosecond routing, zero-copy |
| State/Memory System | Rust | Direct SQLite FFI, deterministic checkpoint serialization |
| Scheduler | Rust | Precise timing, priority queues, preemption |
| Verification Engine | Rust | In-kernel, type-safe, deterministic |
| Recovery Engine | Rust | In-kernel, deterministic state transitions |
| CLI/TUI | Rust | Instant startup, native binary, ratatui |
| Cognition Engine | Python | LLM ecosystem (LiteLLM), rapid prompt iteration |
| Tool Workers | Go | Lightweight goroutines, excellent subprocess mgmt, cross-platform |
| Browser Automation | Python (via Go worker) | Playwright bindings maturity |
| Desktop Automation | Go + Rust lib | Go for orchestration, Rust for screen capture/image processing |

---

## 5. Architecture Overview

```
USER / GOAL
    │
    ▼
┌─────────────────────────────────────┐
│  CLI/TUI (Rust - ratatui)           │
│  Instant startup, real-time viz     │
└──────────────┬──────────────────────┘
               │ (in-process)
               ▼
╔═══════════════════════════════════════════════════════════════╗
║              COGNITION KERNEL (Rust Process)                  ║
║                                                               ║
║  Runtime Kernel ←→ Event Bus ←→ Scheduler ←→ State Machine   ║
║       │                                                       ║
║       ├── Verification Engine (in-process)                    ║
║       ├── Recovery Engine (in-process)                        ║
║       ├── Memory System (SQLite, in-process)                  ║
║       └── Worker Supervisor (process management)              ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
        │                              │
        │ Named Pipes + MessagePack    │ Named Pipes + MessagePack
        ▼                              ▼
┌──────────────────────┐    ┌──────────────────────────────┐
│ COGNITION (Python)   │    │ TOOL WORKERS (Go)            │
│ - LiteLLM            │    │ - Shell Executor             │
│ - Planning           │    │ - Filesystem Worker          │
│ - Reasoning          │    │ - Browser Worker             │
│ - Reflection         │    │ - Desktop Worker             │
└──────────────────────┘    └──────────────────────────────┘
                                       │
                                       ▼
                            ┌──────────────────────┐
                            │  COMPUTER / WORLD    │
                            └──────────────────────┘
```

---

## 6. Component Specifications


### 6.1 Runtime Kernel (Rust)

**Responsibility:** Owns the main execution loop, task lifecycle, scheduling, and coordination of all engines.

**Owns:**
- Main `while runtime_alive` loop (tokio async runtime)
- Task lifecycle as a finite state machine
- Deterministic scheduling via priority queue
- Interrupt handling (OS signals, user commands)
- Process supervision (spawn/monitor/restart workers)
- Checkpoint coordination
- Event routing

**Task State Machine:**
```
Created → Planning → Planned → Executing → Verifying
    ↑                                         │
    │         ┌───────────────────────────────┘
    │         ▼
    │    Recovering ──→ Replanning ──→ Planned
    │         │
    │         ▼
    └── Failed/Escalated
              │
              ▼
         Completed
```

State transitions are enforced at compile time via Rust's type system. Invalid transitions are impossible to express.

**Concurrency model:** tokio multi-threaded runtime. The kernel loop is single-threaded logically (one task step at a time per task), but I/O (IPC, SQLite, event dispatch) is async.

---

### 6.2 Event Bus (Rust)

**Responsibility:** Internal communication, state propagation, observability.

**Implementation:** tokio broadcast channels (lock-free MPSC).

**Event types:**
```rust
enum KernelEvent {
    TaskCreated { task_id, goal, timestamp },
    PlanGenerated { task_id, plan_id, step_count },
    ActionDispatched { task_id, action_id, tool, params },
    ActionCompleted { task_id, action_id, success, duration_ms },
    VerificationPassed { task_id, action_id, evidence },
    VerificationFailed { task_id, action_id, reason, expected, actual },
    RecoveryTriggered { task_id, strategy, attempt },
    TaskCompleted { task_id, duration_ms, steps_executed },
    TaskFailed { task_id, reason, last_state },
    CheckpointSaved { task_id, checkpoint_id },
    WorkerSpawned { worker_type, pid },
    WorkerCrashed { worker_type, pid, exit_code },
}
```

**Subscribers:**
- Logger (structured JSON logs)
- State updater (persists transitions)
- CLI/TUI notifier (real-time display)
- Event log (append-only SQLite table for replay)

**Event sourcing:** All events are persisted to an append-only log. Current state can be reconstructed by replaying events from any checkpoint.

---

### 6.3 State Machine & Task Model (Rust)

**Task representation:**
```rust
struct Task {
    id: TaskId,
    goal: String,
    status: TaskStatus,
    plan: Option<Plan>,
    current_step: usize,
    retry_budget: RetryBudget,
    created_at: Timestamp,
    updated_at: Timestamp,
    checkpoint_id: Option<CheckpointId>,
}

struct Plan {
    id: PlanId,
    steps: Vec<PlanStep>,
    generated_by: String,  // model name
    reasoning: String,
}

struct PlanStep {
    id: StepId,
    description: String,
    tool: ToolType,
    params: HashMap<String, Value>,
    expected_outcome: ExpectedOutcome,
    status: StepStatus,
}

enum ToolType {
    Shell,          // → Go Shell Worker
    Filesystem,     // → Go Filesystem Worker
    Browser,        // → Go Browser Worker (bridges to Playwright)
    Desktop,        // → Go Desktop Worker
    CodeExecution,  // → Go Shell Worker (sandboxed subprocess)
}
```

**Type-safe state transitions (compile-time enforcement):**
```rust
impl Task<Created> {
    fn start_planning(self) -> Task<Planning> { ... }
}
impl Task<Planning> {
    fn plan_ready(self, plan: Plan) -> Task<Planned> { ... }
    fn plan_failed(self, err: Error) -> Task<Failed> { ... }
}
impl Task<Planned> {
    fn begin_execution(self) -> Task<Executing> { ... }
}
// Invalid: Task<Created>.begin_execution() — won't compile
```


---

### 6.4 Memory System (Rust + SQLite)

**Responsibility:** All persistence — checkpoints, execution history, task state, artifacts.

**Storage backend:** SQLite via rusqlite (direct C FFI). WAL mode for concurrent reads.

**Schema:**
```sql
-- Core task state
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    goal TEXT NOT NULL,
    status TEXT NOT NULL,
    plan_json TEXT,
    current_step INTEGER DEFAULT 0,
    retry_budget_json TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Append-only event log (event sourcing)
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    timestamp INTEGER NOT NULL
);
CREATE INDEX idx_events_task ON events(task_id, timestamp);

-- Checkpoints for resume
CREATE TABLE checkpoints (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    state_blob BLOB NOT NULL,  -- bincode serialized full state
    step_index INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_checkpoints_task ON checkpoints(task_id, created_at);

-- Execution history
CREATE TABLE actions (
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

-- Artifacts produced by tasks
CREATE TABLE artifacts (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    path TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
```

**Checkpoint strategy:**
- Checkpoint after every successful step verification
- Checkpoint before risky actions (destructive operations)
- On restart: load latest checkpoint, resume from that step
- Checkpoints use bincode serialization (fast, compact binary)

**Working memory:** In-process HashMap for hot task context. Flushed to SQLite on checkpoint.

---

### 6.5 Verification Engine (Rust, in-kernel)

**Responsibility:** Determine whether an action produced the intended result.

**Lives inside the kernel process** — no IPC overhead for simple checks.

**Verification strategies:**
```rust
enum VerificationStrategy {
    // File exists at path with optional content check
    FileExists { path: PathBuf, content_contains: Option<String> },
    // Command exit code check
    ExitCodeZero,
    // Output contains expected string
    OutputContains { expected: String },
    // File was modified after action
    FileModified { path: PathBuf, after: Timestamp },
    // Process is running
    ProcessRunning { name: String },
    // Custom: delegate to cognition engine for complex checks
    CognitionVerify { context: String },
}
```

**Interface:**
```rust
fn verify(action: &CompletedAction, strategy: &VerificationStrategy) -> VerificationResult;

enum VerificationResult {
    Verified { evidence: String },
    Failed { reason: String, actual: String, expected: String },
}
```

**Simple verifications** (file exists, exit code, output match) run in-kernel in microseconds.
**Complex verifications** (did the UI look right, is the output semantically correct) delegate to the cognition engine via IPC.

---

### 6.6 Recovery Engine (Rust, in-kernel)

**Responsibility:** Handle failures deterministically.

**Recovery decision tree:**
```rust
fn decide_recovery(failure: &VerificationFailure, context: &RecoveryContext) -> RecoveryDecision {
    if context.retry_count < context.max_retries {
        RecoveryDecision::Retry {
            backoff: exponential_backoff(context.retry_count),
        }
    } else if context.replan_count < context.max_replans {
        RecoveryDecision::Replan {
            failure_context: failure.to_context(),
        }
    } else {
        RecoveryDecision::Escalate {
            reason: format!("Exhausted {} retries and {} replans", ...),
        }
    }
}

enum RecoveryDecision {
    Retry { backoff: Duration },
    Replan { failure_context: FailureContext },
    Rollback { to_checkpoint: CheckpointId },
    Escalate { reason: String },
}
```

**Recovery budget (per task):**
- Max 3 retries per action
- Max 2 replans per task
- Configurable via task creation params

**Rollback:** Restore last known-good checkpoint state. Discard actions after that point.

**Escalation:** Pause task, emit `TaskEscalated` event, wait for human intervention via CLI.


---

### 6.7 Cognition Engine (Python)

**Responsibility:** LLM-powered reasoning, planning, decomposition, reflection, replanning.

**Runs as a separate process.** Communicates with kernel via Named Pipes + MessagePack.

**Components:**
- **Planner:** Receives goal + context → produces execution plan (list of steps with tool assignments)
- **Reasoner:** Evaluates current state, decides if plan needs adjustment
- **Reflector:** Post-action analysis — was the approach effective?
- **Replanner:** Receives failure context → produces revised plan

**LLM integration:** LiteLLM (model-agnostic). Supports OpenAI, Anthropic, Gemini, local models.

**Context assembly:**
```python
def assemble_context(task_state, memory, failure_history):
    return {
        "objective": task_state.goal,
        "current_progress": task_state.completed_steps,
        "remaining_steps": task_state.pending_steps,
        "available_tools": tool_registry.list(),
        "failure_history": failure_history,
        "environment": observe_environment(),
    }
```

**Plan output format:**
```python
@dataclass
class PlanStep:
    description: str
    tool: str  # "shell" | "filesystem" | "browser" | "desktop"
    params: dict
    expected_outcome: str
    verification_strategy: str
```

**Protocol (receives from kernel):**
```
CognitionRequest:
  request_type: "plan" | "replan" | "reflect" | "verify_complex"
  task_id: str
  objective: str
  current_state: dict
  memory_context: dict
  failure_context: dict | null
```

**Protocol (sends to kernel):**
```
CognitionResponse:
  task_id: str
  response_type: "plan" | "decision"
  plan: list[PlanStep] | null
  reasoning: str
```

---

### 6.8 Tool Workers (Go)

**Responsibility:** Execute actions in the real world. Isolated from kernel state.

**Runs as a separate process.** Communicates with kernel via Named Pipes + MessagePack.

**Workers:**

**Shell Executor:**
- Runs commands via `os/exec`
- Captures stdout, stderr, exit code
- Enforces timeout (configurable per action)
- Supports working directory specification
- Returns structured result

**Filesystem Worker:**
- Create/read/write/delete files and directories
- Atomic operations where possible (write-to-temp + rename)
- Built-in verification (stat after write)
- Path sanitization

**Browser Worker:**
- Bridges to Playwright (spawns Python subprocess for Playwright)
- Navigation, element interaction, content extraction
- Screenshot capture for verification
- Page state observation

**Desktop Worker:**
- Screen capture (platform-native: Win32 API on Windows)
- Input simulation (keyboard, mouse)
- Window management (enumerate, focus, resize)
- OCR/image matching for verification (via Rust library or OpenCV)

**Worker protocol (receives from kernel):**
```
ExecutionRequest:
  task_id: str
  action_id: str
  tool: "shell" | "filesystem" | "browser" | "desktop"
  params: map[string]any
  timeout_ms: int
```

**Worker protocol (sends to kernel):**
```
ExecutionResponse:
  task_id: str
  action_id: str
  success: bool
  output: str
  error: str | null
  side_effects: list[str]
  duration_ms: int
```

---

### 6.9 IPC Protocol

**Transport:** Named Pipes (Windows) / Unix Domain Sockets (Linux/Mac)

**Serialization:** MessagePack (binary, fast, schema-flexible, native support in Rust/Python/Go)

**Connection model:**
- Kernel starts first, creates pipe endpoints
- Kernel spawns worker processes, passes pipe path as argument
- Workers connect on startup
- Kernel monitors worker health via heartbeat (every 5s)
- If worker dies, kernel re-spawns it and re-dispatches pending actions

**Message framing:**
```
[4 bytes: message length (u32 big-endian)] [N bytes: MessagePack payload]
```

**Flow control:**
- Kernel sends one request at a time per worker (sequential per task)
- Workers can handle concurrent requests for different tasks (Go goroutines)
- Kernel enforces timeout — if worker doesn't respond within timeout, mark action failed

---

### 6.10 CLI/TUI Interface (Rust)

**Responsibility:** User interaction adapter. Does NOT own runtime logic.

**Implementation:** ratatui for TUI, clap for argument parsing.

**Commands:**
```
ck start <goal>          # Create and start a task
ck status                # Show all tasks and their states
ck status <task_id>      # Detailed task view
ck pause <task_id>       # Pause execution
ck resume <task_id>      # Resume from checkpoint
ck cancel <task_id>      # Cancel task
ck logs <task_id>        # Show execution log
ck trace <task_id>       # Show full event trace
ck approve <task_id>     # Approve escalated action
ck reject <task_id>      # Reject and replan
```

**TUI mode (`ck watch`):**
- Real-time execution visualization
- Task state, current step, progress
- Live event stream
- Error/recovery indicators
- Subscribes to kernel event bus via IPC


---

## 7. Data Flow — Complete Execution Cycle

```
1. User: ck start "Create a Python project with tests"
   │ Rust CLI binary — instant startup (<10ms)
   │
2. CLI → Kernel (in-process): create_task(goal)
   │
3. Kernel:
   │  - Generate task_id (ULID)
   │  - TaskState: Created
   │  - Persist to SQLite
   │  - emit(TaskCreated)
   │
4. Kernel → Cognition (IPC/Named Pipe):
   │  CognitionRequest { type: Plan, goal, context }
   │
5. Cognition (Python):
   │  - Assemble context
   │  - LiteLLM call (OpenAI/Anthropic/etc)
   │  - Parse response into PlanSteps
   │  - Return CognitionResponse { plan: [...] }
   │
6. Kernel:
   │  - TaskState: Planned
   │  - Persist plan
   │  - Checkpoint
   │  - emit(PlanGenerated)
   │
7. FOR EACH STEP in plan:
   │
   ├─ Kernel → Worker (IPC/Named Pipe):
   │    ExecutionRequest { tool: Shell, params: {cmd: "mkdir project"} }
   │
   ├─ Worker (Go):
   │    - Execute subprocess
   │    - Capture output
   │    - Return ExecutionResponse { success: true, output: "..." }
   │
   ├─ Kernel (Rust, in-process):
   │    - Verification: verify(FileExists { path: "project/" })
   │    - Result: Verified
   │
   ├─ IF VerificationFailed:
   │    - Recovery: decide(failure, retry_count)
   │    - IF Retry: re-dispatch to worker (with backoff)
   │    - IF Replan: send to cognition engine
   │    - IF Escalate: pause, notify user
   │
   ├─ Kernel:
   │    - Persist action result
   │    - Checkpoint
   │    - emit(StepCompleted)
   │
   └─ Continue to next step
   │
8. All steps verified:
   │  - TaskState: Completed
   │  - Final checkpoint
   │  - emit(TaskCompleted)
   │  - CLI displays success
```

---

## 8. Error Handling Model

### Failure Categories

| Category | Example | First Response | Escalation |
|----------|---------|---------------|------------|
| Tool failure | Command timeout, crash | Retry (max 3) | Replan |
| Verification failure | File not created, wrong content | Retry action | Replan |
| Cognition failure | LLM API error, malformed plan | Retry LLM call, fallback model | Escalate |
| Worker crash | Go process dies | Supervisor restarts worker, re-dispatch | Escalate |
| Kernel crash | Power loss, OOM | Resume from last checkpoint on restart | N/A |

### Recovery Flow

```
Action fails
    │
    ▼
Verification detects failure
    │
    ▼
Recovery engine evaluates:
    ├─ retry_count < max_retries? → RETRY (exponential backoff)
    ├─ replan_count < max_replans? → REPLAN (send failure context to cognition)
    ├─ rollback possible? → ROLLBACK (restore checkpoint)
    └─ else → ESCALATE (pause task, notify human)
```

### Checkpoint & Resume

- Every successful step = checkpoint saved
- Checkpoint contains: task state, plan, current step index, working memory
- On process restart: scan for incomplete tasks, load latest checkpoint, resume
- Checkpoint format: bincode (Rust binary serialization) — fast, compact, versioned

---

## 9. Project Structure

```
cognition-kernel/
├── Cargo.toml                          # Rust workspace
├── crates/
│   ├── ck-kernel/                      # Runtime kernel
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs                 # Entry point, tokio runtime
│   │       ├── runtime.rs              # Main execution loop
│   │       ├── task.rs                 # Task model + type-safe FSM
│   │       ├── scheduler.rs            # Priority scheduling
│   │       ├── supervisor.rs           # Worker process management
│   │       └── config.rs              # Runtime configuration
│   ├── ck-events/                      # Event bus + event sourcing
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── bus.rs                  # Broadcast channels
│   │       ├── types.rs               # Event enum definitions
│   │       └── log.rs                 # Append-only event persistence
│   ├── ck-memory/                      # State persistence
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── store.rs               # SQLite operations
│   │       ├── schema.rs             # Schema + migrations
│   │       └── checkpoint.rs          # Bincode serialization
│   ├── ck-ipc/                         # Inter-process communication
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── protocol.rs           # MessagePack framing
│   │       ├── server.rs             # Kernel-side pipe server
│   │       └── types.rs              # Shared message types
│   ├── ck-verify/                      # Verification engine
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── engine.rs             # Verification dispatch
│   │       └── strategies.rs         # File, output, process checks
│   ├── ck-recovery/                    # Recovery engine
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── engine.rs             # Recovery decision logic
│   │       └── budget.rs             # Retry/replan budget tracking
│   └── ck-cli/                         # CLI/TUI
│       ├── Cargo.toml
│       └── src/
│           ├── main.rs               # CLI entry point
│           ├── commands.rs           # Command handlers
│           └── tui.rs                # ratatui live view
├── cognition/                          # Python cognition engine
│   ├── pyproject.toml
│   ├── cognition_kernel/
│   │   ├── __init__.py
│   │   ├── engine.py                 # Main cognition loop
│   │   ├── planner.py               # Plan generation
│   │   ├── reasoner.py              # Reasoning/reflection
│   │   ├── context.py               # Context assembly
│   │   ├── models.py                # Data models
│   │   └── ipc.py                   # MessagePack IPC client
│   └── tests/
│       ├── test_planner.py
│       ├── test_reasoner.py
│       └── test_integration.py
├── workers/                            # Go tool workers
│   ├── go.mod
│   ├── go.sum
│   ├── cmd/
│   │   └── ck-worker/main.go        # Worker process entry
│   ├── internal/
│   │   ├── shell/executor.go        # Shell command execution
│   │   ├── filesystem/worker.go     # File operations
│   │   ├── browser/bridge.go        # Playwright bridge
│   │   ├── desktop/worker.go        # Desktop automation
│   │   └── ipc/client.go            # MessagePack IPC client
│   └── pkg/
│       └── protocol/types.go        # Shared message types
├── docs/
│   └── superpowers/
│       └── specs/
└── README.md
```

---

## 10. Technology Stack

| Layer | Technology | Version Strategy |
|-------|-----------|-----------------|
| Kernel runtime | Rust + tokio | Latest stable |
| Serialization | bincode (state), rmp-serde (IPC) | Pinned |
| Database | SQLite via rusqlite | WAL mode |
| Event channels | tokio::sync::broadcast | In-process |
| CLI framework | clap | Latest stable |
| TUI | ratatui + crossterm | Latest stable |
| IDs | ulid | Sortable, unique |
| Cognition | Python 3.12+ | Via uv |
| LLM | LiteLLM | Latest |
| Prompt mgmt | Jinja2 templates | Simple |
| Tool workers | Go 1.22+ | Latest stable |
| Browser | Playwright (Python, called from Go) | Latest |
| Desktop capture | Windows: win32 API / Linux: X11 | Platform-native |
| Image processing | Rust: image crate + imageproc | For verification |
| Build | cargo (Rust), uv (Python), go build (Go) | Workspace |
| Testing | cargo test, pytest, go test | Per-language |

---

## 11. Non-Functional Requirements

| Requirement | Target |
|-------------|--------|
| CLI startup time | < 10ms |
| Event bus throughput | > 100k events/sec |
| Checkpoint save | < 50ms |
| Checkpoint resume | < 100ms |
| IPC round-trip | < 5ms (excluding LLM latency) |
| SQLite write | < 1ms per operation |
| Worker restart | < 500ms |
| Memory (kernel idle) | < 20MB |
| Concurrent tasks | Up to 10 (scheduler-limited) |

---

## 12. Acceptance Criteria (V1)

V1 is complete when:

1. `ck start "create a directory called test-project with a main.py that prints hello world"` executes successfully through the full loop: plan → execute → verify → complete
2. Killing the kernel mid-task and restarting resumes from the last checkpoint
3. Injecting a tool failure (e.g., permission denied) triggers retry → replan → eventual success or escalation
4. `ck status` shows task state accurately
5. `ck trace <id>` shows the full event history
6. `ck pause` / `ck resume` works correctly
7. The cognition engine produces valid plans from natural language goals
8. All verification strategies (file exists, exit code, output contains) work
9. Worker crash is detected and worker is restarted by supervisor
10. Full execution trace is persisted and queryable

---

## 13. What V1 Does NOT Include

- GUI/web interface
- Multi-machine distribution
- Plugin marketplace
- Agent swarms
- Cloud scaling
- Complex browser automation flows
- Advanced desktop automation (OCR, complex image matching)
- Long-term memory / semantic search
- Multi-user support

---

## 14. Design Principles

1. **One kernel owns execution** — single source of truth
2. **State transitions are the product** — everything else is secondary
3. **Recovery is foundational** — not bolted on
4. **Cognition ≠ execution** — reasoning and acting are separate processes
5. **Interfaces are adapters** — CLI/GUI never own runtime logic
6. **Tools are isolated** — workers can crash without killing the kernel
7. **Persistence is mandatory** — every important transition is saved
8. **Local-first** — the machine is the environment
9. **No fake abstractions** — no wrappers around wrappers
10. **Execution continuity over feature count** — reliability first

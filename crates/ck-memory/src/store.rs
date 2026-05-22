use std::path::Path;
use rusqlite::{params, Connection};
use crate::schema;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        schema::initialize(&conn)?;
        Ok(Self { conn })
    }

    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        schema::initialize(&conn)?;
        Ok(Self { conn })
    }

    pub fn create_task(&self, id: &str, goal: &str) -> rusqlite::Result<()> {
        let now = chrono::Utc::now().timestamp();
        self.conn.execute(
            "INSERT INTO tasks (id, goal, status, current_step, created_at, updated_at) VALUES (?1, ?2, ?3, 0, ?4, ?5)",
            params![id, goal, TaskStatus::Created.as_str(), now, now],
        )?;
        Ok(())
    }

    pub fn get_task(&self, id: &str) -> rusqlite::Result<Option<TaskRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, goal, status, plan_json, current_step, retry_budget_json, created_at, updated_at FROM tasks WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            let status_str: String = row.get(2)?;
            Ok(TaskRecord {
                id: row.get(0)?,
                goal: row.get(1)?,
                status: TaskStatus::from_str(&status_str).unwrap_or(TaskStatus::Created),
                plan_json: row.get(3)?,
                current_step: row.get(4)?,
                retry_budget_json: row.get(5)?,
                created_at: row.get(6)?,
                updated_at: row.get(7)?,
            })
        })?;
        match rows.next() {
            Some(r) => Ok(Some(r?)),
            None => Ok(None),
        }
    }

    pub fn update_task_status(&self, id: &str, status: TaskStatus) -> rusqlite::Result<()> {
        let now = chrono::Utc::now().timestamp();
        self.conn.execute(
            "UPDATE tasks SET status = ?1, updated_at = ?2 WHERE id = ?3",
            params![status.as_str(), now, id],
        )?;
        Ok(())
    }

    pub fn append_event(&self, task_id: &str, event_type: &str, payload_json: &str) -> rusqlite::Result<i64> {
        let now = chrono::Utc::now().timestamp();
        self.conn.execute(
            "INSERT INTO events (task_id, event_type, payload_json, timestamp) VALUES (?1, ?2, ?3, ?4)",
            params![task_id, event_type, payload_json, now],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn replay_events(&self, task_id: &str) -> rusqlite::Result<Vec<EventRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, task_id, event_type, payload_json, timestamp FROM events WHERE task_id = ?1 ORDER BY timestamp ASC",
        )?;
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
        let now = chrono::Utc::now().timestamp();
        self.conn.execute(
            "INSERT INTO checkpoints (id, task_id, state_blob, step_index, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, task_id, state_blob, step_index, now],
        )?;
        Ok(())
    }

    pub fn load_latest_checkpoint(&self, task_id: &str) -> rusqlite::Result<Option<CheckpointRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, task_id, state_blob, step_index, created_at FROM checkpoints WHERE task_id = ?1 ORDER BY created_at DESC LIMIT 1",
        )?;
        let mut rows = stmt.query_map(params![task_id], |row| {
            Ok(CheckpointRecord {
                id: row.get(0)?,
                task_id: row.get(1)?,
                state_blob: row.get(2)?,
                step_index: row.get(3)?,
                created_at: row.get(4)?,
            })
        })?;
        match rows.next() {
            Some(r) => Ok(Some(r?)),
            None => Ok(None),
        }
    }
}

use std::collections::HashMap;
use std::io;
use std::process::{Child, Command};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    #[allow(dead_code)]
    worker_type: WorkerType,
    child: Child,
    command: String,
    args: Vec<String>,
    restart_count: u32,
    max_restarts: u32,
}

pub struct Supervisor {
    workers: HashMap<WorkerType, WorkerProcess>,
    #[allow(dead_code)]
    pipe_prefix: String,
}

impl Supervisor {
    pub fn new(pipe_prefix: impl Into<String>) -> Self {
        Self {
            workers: HashMap::new(),
            pipe_prefix: pipe_prefix.into(),
        }
    }

    pub fn spawn_cognition(&mut self, python_path: &str, script_path: &str, pipe_name: &str) -> io::Result<u32> {
        let args = vec![script_path.to_string(), "--pipe".to_string(), pipe_name.to_string()];
        let child = Command::new(python_path).args(&args).spawn()?;
        let pid = child.id();
        self.workers.insert(WorkerType::Cognition, WorkerProcess {
            worker_type: WorkerType::Cognition,
            child,
            command: python_path.to_string(),
            args,
            restart_count: 0,
            max_restarts: 3,
        });
        Ok(pid)
    }

    /// Spawn cognition engine as a Python module (`python -m cognition_kernel.engine`)
    /// from the given working directory. This avoids relative import errors.
    pub fn spawn_cognition_module(&mut self, python_path: &str, work_dir: &str, pipe_name: &str) -> io::Result<u32> {
        let args = vec![
            "-m".to_string(),
            "cognition_kernel.engine".to_string(),
            "--pipe".to_string(),
            pipe_name.to_string(),
        ];
        let child = Command::new(python_path)
            .args(&args)
            .current_dir(work_dir)
            .spawn()?;
        let pid = child.id();
        self.workers.insert(WorkerType::Cognition, WorkerProcess {
            worker_type: WorkerType::Cognition,
            child,
            command: python_path.to_string(),
            args,
            restart_count: 0,
            max_restarts: 3,
        });
        Ok(pid)
    }

    pub fn spawn_tool_worker(&mut self, binary_path: &str, pipe_name: &str) -> io::Result<u32> {
        let args = vec!["--pipe".to_string(), pipe_name.to_string()];
        let child = Command::new(binary_path).args(&args).spawn()?;
        let pid = child.id();
        self.workers.insert(WorkerType::ToolWorker, WorkerProcess {
            worker_type: WorkerType::ToolWorker,
            child,
            command: binary_path.to_string(),
            args,
            restart_count: 0,
            max_restarts: 3,
        });
        Ok(pid)
    }

    pub fn check_and_restart(&mut self) -> Vec<(WorkerType, u32)> {
        let mut restarted = Vec::new();
        let dead: Vec<WorkerType> = self.workers.iter_mut()
            .filter_map(|(wt, wp)| match wp.child.try_wait() {
                Ok(Some(_)) => Some(*wt),
                _ => None,
            })
            .collect();

        for wt in dead {
            if let Some(wp) = self.workers.get_mut(&wt) {
                if wp.restart_count < wp.max_restarts {
                    if let Ok(child) = Command::new(&wp.command).args(&wp.args).spawn() {
                        let pid = child.id();
                        wp.child = child;
                        wp.restart_count += 1;
                        restarted.push((wt, pid));
                    }
                }
            }
        }
        restarted
    }

    pub fn shutdown_all(&mut self) {
        for (_, wp) in self.workers.iter_mut() {
            let _ = wp.child.kill();
            let _ = wp.child.wait();
        }
        self.workers.clear();
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        self.shutdown_all();
    }
}

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CognitionRequest {
    pub request_type: String,
    pub task_id: String,
    pub objective: String,
    pub current_state: HashMap<String, serde_json::Value>,
    pub memory_context: HashMap<String, serde_json::Value>,
    pub failure_context: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CognitionResponse {
    pub task_id: String,
    pub response_type: String,
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
    pub tool: String,
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

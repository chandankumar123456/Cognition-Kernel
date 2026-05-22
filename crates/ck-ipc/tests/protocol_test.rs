use ck_ipc::protocol::{decode_message, encode_message};
use ck_ipc::types::{CognitionRequest, ExecutionRequest};
use std::collections::HashMap;

#[test]
fn roundtrip_cognition_request() {
    let req = CognitionRequest {
        request_type: "plan".into(),
        task_id: "task-001".into(),
        objective: "build feature X".into(),
        current_state: HashMap::from([("key".into(), serde_json::json!("value"))]),
        memory_context: HashMap::new(),
        failure_context: None,
    };
    let encoded = encode_message(&req).unwrap();
    let decoded: CognitionRequest = decode_message(&encoded).unwrap();
    assert_eq!(decoded.task_id, req.task_id);
    assert_eq!(decoded.objective, req.objective);
    assert_eq!(decoded.request_type, req.request_type);
}

#[test]
fn roundtrip_execution_request() {
    let req = ExecutionRequest {
        task_id: "task-002".into(),
        action_id: "act-001".into(),
        tool: "shell".into(),
        params: HashMap::from([("cmd".into(), serde_json::json!("echo hello"))]),
        timeout_ms: 5000,
    };
    let encoded = encode_message(&req).unwrap();
    let decoded: ExecutionRequest = decode_message(&encoded).unwrap();
    assert_eq!(decoded.task_id, req.task_id);
    assert_eq!(decoded.action_id, req.action_id);
    assert_eq!(decoded.tool, req.tool);
    assert_eq!(decoded.timeout_ms, 5000);
}

package protocol

type ExecutionRequest struct {
	TaskID    string                 `msgpack:"task_id"`
	ActionID  string                 `msgpack:"action_id"`
	Tool      string                 `msgpack:"tool"`
	Params    map[string]interface{} `msgpack:"params"`
	TimeoutMs uint64                 `msgpack:"timeout_ms"`
}

type ExecutionResponse struct {
	TaskID      string   `msgpack:"task_id"`
	ActionID    string   `msgpack:"action_id"`
	Success     bool     `msgpack:"success"`
	Output      string   `msgpack:"output"`
	Error       *string  `msgpack:"error"`
	SideEffects []string `msgpack:"side_effects"`
	DurationMs  uint64   `msgpack:"duration_ms"`
}

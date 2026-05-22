import json
from .models import CognitionRequest

SYSTEM_PROMPT = """You are a planning engine for an autonomous agent. Given an objective and context, produce a JSON array of plan steps. Each step must have:
- "description": what the step does
- "tool": which tool to invoke
- "params": parameters dict for the tool
- "expected_outcome": what success looks like
- "verification_strategy": how to verify the step succeeded

Respond ONLY with a valid JSON array."""


def build_plan_prompt(request: CognitionRequest) -> str:
    return (
        f"Objective: {request.objective}\n"
        f"Current state: {json.dumps(request.current_state)}\n"
        f"Memory context: {json.dumps(request.memory_context)}"
    )


def build_replan_prompt(request: CognitionRequest) -> str:
    return (
        f"Objective: {request.objective}\n"
        f"Current state: {json.dumps(request.current_state)}\n"
        f"Failure context: {json.dumps(request.failure_context)}\n"
        "Generate a revised plan that avoids the previous failure."
    )

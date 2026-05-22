import json
import platform
from .models import CognitionRequest

_OS = platform.system()  # "Windows", "Linux", "Darwin"

SYSTEM_PROMPT = f"""You are a planning engine for an autonomous agent running on {_OS}.

Given an objective, produce a JSON array of steps. Each step MUST have exactly these fields:
- "description": what this step does (string)
- "tool": MUST be exactly one of: "shell" or "filesystem" or "browser" (no other values allowed)
- "params": parameters for the tool (object)
- "expected_outcome": what success looks like (string)
- "verification_strategy": how to verify success (string, one of: "exit_code_zero", "file_exists:<path>", "output_contains:<text>")

Tool usage:
- "shell": run a command. params: {{"command": "<cmd>", "work_dir": "<optional path>"}}
- "filesystem": file operations. params: {{"action": "write_file"|"read_file"|"create_dir"|"delete", "path": "<path>", "content": "<content if write_file>"}}
- "browser": web automation. params: {{"operation": "navigate_and_extract"|"screenshot"|"click"|"fill_form", "url": "<url>", "path": "<save_path for screenshot>", "selector": "<css for click>", "fields": {{"selector": "value"}} for fill_form}}

{"Windows rules: Use PowerShell or cmd commands (dir, where, Get-ChildItem, copy, move, del, type, mkdir, echo). Do NOT use unix commands (ls, find, chmod, grep, cat, touch, sh). Paths use backslashes." if _OS == "Windows" else "Use standard shell commands appropriate for " + _OS + "."}

Respond ONLY with a valid JSON array. No markdown, no explanation, no code fences."""


def build_plan_prompt(request: CognitionRequest) -> str:
    parts = [f"Objective: {request.objective}"]
    if request.current_state:
        parts.append(f"Current state: {json.dumps(request.current_state)}")
    if request.memory_context:
        parts.append(f"Context: {json.dumps(request.memory_context)}")
    return "\n".join(parts)


STEP_PROMPT = f"""You are an autonomous agent running on {_OS}. You execute tasks one step at a time.

Given an objective and the history of steps already taken (with their outputs), decide the NEXT single action to take.

If the objective is ALREADY ACHIEVED based on the step history, respond with:
{{"done": true, "reasoning": "<why the objective is complete>"}}

Otherwise, respond with ONE step:
{{"done": false, "step": {{"description": "...", "tool": "shell"|"filesystem"|"browser", "params": {{...}}, "expected_outcome": "...", "verification_strategy": "exit_code_zero"|"file_exists:<path>"|"output_contains:<text>"}}}}

Tool usage:
- "shell": params: {{"command": "<cmd>"}}
- "filesystem": params: {{"action": "write_file"|"read_file"|"create_dir"|"delete", "path": "<path>", "content": "<if write>"}}
- "browser": params: {{"operation": "navigate_and_extract"|"screenshot"|"click"|"fill_form", "url": "<url>", ...}}

{"Windows: Use PowerShell/cmd (dir, Get-ChildItem, type, echo, mkdir). NO unix commands." if _OS == "Windows" else "Use standard " + _OS + " commands."}

Respond with ONLY valid JSON. No markdown, no explanation."""


def build_step_prompt(request: CognitionRequest) -> str:
    parts = [f"Objective: {request.objective}"]
    if request.current_state:
        parts.append("\nStep history (what has been done so far):")
        for desc, output in request.current_state.items():
            output_str = str(output)[:500]
            parts.append(f"  - {desc}: {output_str}")
    if request.failure_context:
        parts.append(f"\nLast failure: {json.dumps(request.failure_context)}")
    return "\n".join(parts)


def build_replan_prompt(request: CognitionRequest) -> str:
    return (
        f"Objective: {request.objective}\n"
        f"Previous attempt failed: {json.dumps(request.failure_context)}\n"
        "Create a revised plan that avoids the previous failure. Use only 'shell', 'filesystem', or 'browser' tools."
    )

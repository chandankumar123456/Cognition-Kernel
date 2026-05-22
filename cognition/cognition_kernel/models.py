from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PlanStep:
    description: str
    tool: str
    params: dict
    expected_outcome: str
    verification_strategy: str


@dataclass
class CognitionRequest:
    request_type: str
    task_id: str
    objective: str
    current_state: dict = field(default_factory=dict)
    memory_context: dict = field(default_factory=dict)
    failure_context: Optional[dict] = None


@dataclass
class CognitionResponse:
    task_id: str
    response_type: str
    plan: Optional[list] = None
    reasoning: str = ""

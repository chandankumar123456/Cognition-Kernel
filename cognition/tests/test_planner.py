import json
from unittest.mock import AsyncMock, patch, MagicMock
import pytest
from cognition_kernel.models import CognitionRequest, PlanStep
from cognition_kernel.planner import generate_plan


@pytest.mark.asyncio
async def test_generate_plan_parses_response():
    mock_steps = [
        {
            "description": "Read file",
            "tool": "file_read",
            "params": {"path": "/tmp/test.txt"},
            "expected_outcome": "File contents returned",
            "verification_strategy": "Check non-empty response",
        }
    ]
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = json.dumps(mock_steps)

    with patch("cognition_kernel.planner.litellm.acompletion", new_callable=AsyncMock, return_value=mock_response):
        request = CognitionRequest(request_type="plan", task_id="t1", objective="Read a file")
        result = await generate_plan(request)

    assert len(result) == 1
    assert isinstance(result[0], PlanStep)
    assert result[0].tool == "file_read"
    assert result[0].params == {"path": "/tmp/test.txt"}

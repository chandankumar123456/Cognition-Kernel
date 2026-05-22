import json
import litellm
from .models import CognitionRequest, PlanStep
from .context import SYSTEM_PROMPT, build_plan_prompt, build_replan_prompt


async def generate_plan(request: CognitionRequest, model: str = "gpt-4o-mini") -> list[PlanStep]:
    prompt = build_replan_prompt(request) if request.failure_context else build_plan_prompt(request)
    response = await litellm.acompletion(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    )
    content = response.choices[0].message.content
    steps_data = json.loads(content)
    return [
        PlanStep(
            description=s["description"],
            tool=s["tool"],
            params=s["params"],
            expected_outcome=s["expected_outcome"],
            verification_strategy=s["verification_strategy"],
        )
        for s in steps_data
    ]

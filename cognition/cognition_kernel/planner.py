import json
from .models import CognitionRequest, PlanStep
from .context import SYSTEM_PROMPT, build_plan_prompt, build_replan_prompt


async def generate_plan(request: CognitionRequest, model: str = "gpt-4o-mini") -> list[PlanStep]:
    import litellm  # lazy import — avoids 3s startup delay

    prompt = build_replan_prompt(request) if request.failure_context else build_plan_prompt(request)
    response = await litellm.acompletion(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    )
    content = response.choices[0].message.content

    # Strip markdown code fences if present
    content = content.strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[-1]
        content = content.rsplit("```", 1)[0].strip()

    steps_data = json.loads(content)
    if isinstance(steps_data, dict):
        steps_data = steps_data.get("steps", [])

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



async def generate_next_step(request: CognitionRequest, model: str = "gpt-4o-mini") -> dict:
    """Generate the next single step (or declare done). Returns raw dict."""
    import litellm
    from .context import STEP_PROMPT, build_step_prompt

    prompt = build_step_prompt(request)
    response = await litellm.acompletion(
        model=model,
        messages=[
            {"role": "system", "content": STEP_PROMPT},
            {"role": "user", "content": prompt},
        ],
    )
    content = response.choices[0].message.content.strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    return json.loads(content)

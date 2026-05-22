import litellm
from .models import CognitionRequest


async def reflect(request: CognitionRequest, model: str = "gpt-4o-mini") -> str:
    response = await litellm.acompletion(
        model=model,
        messages=[
            {"role": "system", "content": "You are a reasoning engine. Evaluate the situation and provide analysis."},
            {"role": "user", "content": f"Objective: {request.objective}\nState: {request.current_state}\nEvaluate progress and suggest next actions."},
        ],
    )
    return response.choices[0].message.content

from .models import CognitionRequest


async def reflect(request: CognitionRequest, model: str = "gpt-4o-mini") -> str:
    import litellm  # lazy import

    response = await litellm.acompletion(
        model=model,
        messages=[
            {"role": "system", "content": "Evaluate whether the current execution is on track. Reply with one sentence."},
            {"role": "user", "content": f"Objective: {request.objective}\nState: {request.current_state}"},
        ],
    )
    return response.choices[0].message.content

import argparse
import asyncio
from dataclasses import asdict

from .models import CognitionRequest, CognitionResponse
from .planner import generate_plan
from .reasoner import reflect
from .ipc import PipeClient


def dict_to_request(d: dict) -> CognitionRequest:
    return CognitionRequest(
        request_type=d["request_type"],
        task_id=d["task_id"],
        objective=d["objective"],
        current_state=d.get("current_state", {}),
        memory_context=d.get("memory_context", {}),
        failure_context=d.get("failure_context"),
    )


def response_to_dict(resp: CognitionResponse) -> dict:
    return asdict(resp)


async def handle_request(request: CognitionRequest) -> CognitionResponse:
    if request.request_type == "plan":
        steps = await generate_plan(request)
        return CognitionResponse(
            task_id=request.task_id,
            response_type="plan",
            plan=[asdict(s) for s in steps],
        )
    elif request.request_type == "reflect":
        reasoning = await reflect(request)
        return CognitionResponse(
            task_id=request.task_id,
            response_type="reflection",
            reasoning=reasoning,
        )
    else:
        return CognitionResponse(
            task_id=request.task_id,
            response_type="error",
            reasoning=f"Unknown request_type: {request.request_type}",
        )


async def run(pipe_path: str):
    client = PipeClient(pipe_path)
    await client.connect()
    try:
        while True:
            msg = await client.read_message()
            request = dict_to_request(msg)
            response = await handle_request(request)
            await client.write_message(response_to_dict(response))
    except (asyncio.IncompleteReadError, ConnectionError):
        pass
    finally:
        await client.close()


def main():
    parser = argparse.ArgumentParser(description="Cognition Kernel Engine")
    parser.add_argument("--pipe", required=True, help="Path to named pipe")
    args = parser.parse_args()
    asyncio.run(run(args.pipe))


if __name__ == "__main__":
    main()

import argparse
import asyncio
import sys
import os
from dataclasses import asdict

# Suppress LiteLLM's noisy startup warnings about optional AWS dependencies
os.environ.setdefault("LITELLM_LOG", "ERROR")

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
    try:
        if request.request_type in ("plan", "replan"):
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
    except Exception as e:
        print(f"[cognition] ERROR handling {request.request_type}: {e}", flush=True)
        return CognitionResponse(
            task_id=request.task_id,
            response_type="error",
            plan=None,
            reasoning=f"cognition error: {e}",
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
    except (asyncio.IncompleteReadError, ConnectionError, EOFError, OSError):
        pass  # kernel closed the pipe — normal shutdown
    except Exception as e:
        # Catch pywintypes.error and any other pipe errors on Windows
        err_name = type(e).__name__
        if "pywintypes" in err_name or "win32" in err_name.lower() or "pipe" in str(e).lower():
            pass  # pipe closed — normal shutdown
        else:
            print(f"[cognition] unexpected error: {e}", flush=True)
    finally:
        await client.close()


def main():
    parser = argparse.ArgumentParser(description="Cognition Kernel Engine")
    parser.add_argument("--pipe", required=True, help="Path to named pipe")
    args = parser.parse_args()

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    asyncio.run(run(args.pipe))


if __name__ == "__main__":
    main()

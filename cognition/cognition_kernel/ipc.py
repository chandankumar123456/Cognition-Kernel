import struct
import asyncio
import sys
import msgpack


class PipeClient:
    """Async MessagePack IPC client.
    - Windows: uses asyncio with ProactorEventLoop (default on Windows) via open_connection with pipe path
    - Unix: uses asyncio.open_unix_connection
    """

    def __init__(self, pipe_path: str):
        self._path = pipe_path
        self._reader = None
        self._writer = None

    async def connect(self):
        if sys.platform == "win32":
            # On Windows, asyncio ProactorEventLoop supports named pipes via open_connection
            # The path must be the full pipe path: \\.\pipe\name
            self._reader, self._writer = await asyncio.open_connection(self._path)
        else:
            self._reader, self._writer = await asyncio.open_unix_connection(self._path)

    async def read_message(self) -> dict:
        header = await self._reader.readexactly(4)
        length = struct.unpack(">I", header)[0]
        data = await self._reader.readexactly(length)
        return msgpack.unpackb(data, raw=False)

    async def write_message(self, msg: dict):
        data = msgpack.packb(msg, use_bin_type=True)
        header = struct.pack(">I", len(data))
        self._writer.write(header + data)
        await self._writer.drain()

    async def close(self):
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass

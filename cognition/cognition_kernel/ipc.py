import struct
import asyncio
import sys
import msgpack


class PipeClient:
    """Async MessagePack IPC client over Named Pipes (Windows) or Unix sockets."""

    def __init__(self, pipe_path: str):
        self._path = pipe_path
        self._reader = None
        self._writer = None

    async def connect(self):
        if sys.platform == "win32":
            await self._connect_windows()
        else:
            self._reader, self._writer = await asyncio.open_unix_connection(self._path)

    async def _connect_windows(self):
        import time
        import win32file
        import win32pipe
        import pywintypes

        loop = asyncio.get_event_loop()
        deadline = time.monotonic() + 10.0  # 10 second total timeout

        def _connect():
            while time.monotonic() < deadline:
                try:
                    # Wait for pipe to exist (500ms timeout per attempt)
                    win32pipe.WaitNamedPipe(self._path, 500)
                    # Pipe exists — open it
                    handle = win32file.CreateFile(
                        self._path,
                        win32file.GENERIC_READ | win32file.GENERIC_WRITE,
                        0, None,
                        win32file.OPEN_EXISTING,
                        0, None
                    )
                    return handle
                except pywintypes.error as e:
                    if e.args[0] == 2:  # ERROR_FILE_NOT_FOUND — pipe doesn't exist yet
                        time.sleep(0.1)
                        continue
                    raise  # any other error is fatal
            raise TimeoutError(f"Timed out waiting for pipe: {self._path}")

        self._win_handle = await loop.run_in_executor(None, _connect)

    async def read_message(self) -> dict:
        if sys.platform == "win32":
            header = await self._win_read_exact(4)
            length = struct.unpack(">I", header)[0]
            data = await self._win_read_exact(length)
        else:
            header = await self._reader.readexactly(4)
            length = struct.unpack(">I", header)[0]
            data = await self._reader.readexactly(length)
        return msgpack.unpackb(data, raw=False)

    async def write_message(self, msg: dict):
        data = msgpack.packb(msg, use_bin_type=True)
        framed = struct.pack(">I", len(data)) + data
        if sys.platform == "win32":
            await self._win_write(framed)
        else:
            self._writer.write(framed)
            await self._writer.drain()

    async def _win_read_exact(self, n: int) -> bytes:
        loop = asyncio.get_event_loop()

        def _read():
            import win32file
            buf = b""
            while len(buf) < n:
                _, chunk = win32file.ReadFile(self._win_handle, n - len(buf))
                buf += chunk
            return buf

        return await loop.run_in_executor(None, _read)

    async def _win_write(self, data: bytes):
        loop = asyncio.get_event_loop()

        def _write():
            import win32file
            win32file.WriteFile(self._win_handle, data)

        await loop.run_in_executor(None, _write)

    async def close(self):
        if sys.platform == "win32":
            if hasattr(self, "_win_handle") and self._win_handle is not None:
                import win32file
                win32file.CloseHandle(self._win_handle)
        else:
            if self._writer:
                self._writer.close()
                try:
                    await self._writer.wait_closed()
                except Exception:
                    pass

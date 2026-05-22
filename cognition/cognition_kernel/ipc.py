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
        """Connect to a Windows Named Pipe using ProactorEventLoop's pipe support."""
        loop = asyncio.get_event_loop()

        # ProactorEventLoop on Windows supports create_pipe_connection
        # which is the correct way to connect to a named pipe.
        import io

        class _PipeStream:
            """Wrap a synchronous pipe handle for async I/O via executor."""
            def __init__(self, handle):
                self._handle = handle

            def read(self, n):
                import win32file
                _, data = win32file.ReadFile(self._handle, n)
                return data

            def write(self, data):
                import win32file
                win32file.WriteFile(self._handle, data)

            def close(self):
                import win32file
                win32file.CloseHandle(self._handle)

        # Use win32file to open the named pipe
        try:
            import win32file
            import win32pipe
            import pywintypes

            # Wait for pipe to be available (up to 5s)
            win32pipe.WaitNamedPipe(self._path, 5000)
            handle = win32file.CreateFile(
                self._path,
                win32file.GENERIC_READ | win32file.GENERIC_WRITE,
                0, None,
                win32file.OPEN_EXISTING,
                0, None
            )
            self._win_handle = handle
            self._loop = loop

        except ImportError:
            # pywin32 not available — fall back to open() which works on
            # Windows named pipes as regular files
            self._win_handle = None
            # Try raw open as file (works for some pipe configurations)
            self._file = open(self._path, "r+b", buffering=0)

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
        if hasattr(self, "_win_handle") and self._win_handle is not None:
            def _read():
                import win32file
                buf = b""
                while len(buf) < n:
                    _, chunk = win32file.ReadFile(self._win_handle, n - len(buf))
                    buf += chunk
                return buf
            return await loop.run_in_executor(None, _read)
        else:
            def _read():
                buf = b""
                while len(buf) < n:
                    chunk = self._file.read(n - len(buf))
                    if not chunk:
                        raise ConnectionError("pipe closed")
                    buf += chunk
                return buf
            return await loop.run_in_executor(None, _read)

    async def _win_write(self, data: bytes):
        loop = asyncio.get_event_loop()
        if hasattr(self, "_win_handle") and self._win_handle is not None:
            def _write():
                import win32file
                win32file.WriteFile(self._win_handle, data)
            await loop.run_in_executor(None, _write)
        else:
            def _write():
                self._file.write(data)
                self._file.flush()
            await loop.run_in_executor(None, _write)

    async def close(self):
        if sys.platform == "win32":
            if hasattr(self, "_win_handle") and self._win_handle is not None:
                import win32file
                win32file.CloseHandle(self._win_handle)
            elif hasattr(self, "_file"):
                self._file.close()
        else:
            if self._writer:
                self._writer.close()
                try:
                    await self._writer.wait_closed()
                except Exception:
                    pass

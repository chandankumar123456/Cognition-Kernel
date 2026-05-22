import struct
import msgpack
import pytest
from unittest.mock import AsyncMock, MagicMock
from cognition_kernel.ipc import PipeClient


@pytest.mark.asyncio
async def test_write_message_framing():
    client = PipeClient("test_pipe")
    client._writer = MagicMock()
    client._writer.write = MagicMock()
    client._writer.drain = AsyncMock()

    msg = {"task_id": "t1", "type": "plan"}
    await client.write_message(msg)

    written = client._writer.write.call_args[0][0]
    length = struct.unpack(">I", written[:4])[0]
    payload = msgpack.unpackb(written[4:], raw=False)

    assert length == len(written) - 4
    assert payload == msg


@pytest.mark.asyncio
async def test_read_message():
    client = PipeClient("test_pipe")
    msg = {"hello": "world"}
    data = msgpack.packb(msg, use_bin_type=True)
    header = struct.pack(">I", len(data))

    client._reader = AsyncMock()
    client._reader.readexactly = AsyncMock(side_effect=[header, data])

    result = await client.read_message()
    assert result == msg

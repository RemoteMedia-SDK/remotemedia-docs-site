"""Send two inputs, receive their two echoes across output kinds, and close.

This graph has a known one-output-per-input contract. Input completion alone
does not establish that the installed runtime will close every output channel.
"""

import asyncio
from pathlib import Path

import remotemedia.runtime as rt


async def send_inputs(session):
    for text in ("hello", "again"):
        await session.send_input(text)
    await session.signal_input_complete()


async def receive_outputs(session):
    received = []
    for _ in range(2):
        output = await session.recv_output()
        if output is None:
            raise RuntimeError("Echo session ended before both outputs arrived")
        kind, data = output
        print(kind, data)
        received.append((kind, data))
    assert received == [("data", "hello"), ("data", "again")]


async def exchange(session):
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(send_inputs(session))
        tasks.create_task(receive_outputs(session))


async def main():
    manifest = Path(__file__).with_name("echo.json").read_text(encoding="utf-8")
    session = await asyncio.wait_for(rt.create_streaming_session(manifest), timeout=10)
    try:
        await asyncio.wait_for(exchange(session), timeout=10)
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())

"""Read a real performance snapshot and separate it from buffer copy reports."""

import asyncio
import json
import os
from pathlib import Path

import remotemedia.runtime as rt
from remotemedia.control import attach_inproc


async def main():
    os.environ["REMOTEMEDIA_PERF_TAP"] = "1"
    manifest = Path(__file__).with_name("echo.json").read_text()
    session = await rt.create_streaming_session(manifest)
    try:
        async with attach_inproc(session) as ctrl:
            snapshots = await ctrl.subscribe("__perf__.out")
            try:
                for index in range(4):
                    await session.send_input(f"sample-{index}")
                    assert (await asyncio.wait_for(session.recv_output(), 5))[1] == f"sample-{index}"
                snapshot = await asyncio.wait_for(anext(snapshots), 5)
                print(json.dumps({"performance": snapshot.json_value,
                                  "input_copies": session.input_copy_report(),
                                  "node_copies": session.node_copy_reports()}, indent=2))
            finally:
                await snapshots.aclose()
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())

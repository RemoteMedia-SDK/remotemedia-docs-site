"""Load a plugin directory and provision its declared Python dependencies."""

import asyncio
import json
import os
from pathlib import Path

import remotemedia.runtime as rt
from remotemedia.control import attach_inproc


async def main():
    directory = Path(__file__).resolve().parents[1] / "plugins/docs_transform"
    manifest = {"version": "v1", "metadata": {"name": "docs-source-plugin"},
                "plugins": [str(directory)],
                "nodes": [{"id": "transform", "node_type": "DocsSourceTransform",
                           "params": {"prefix": "SOURCE: ", "__python_version__": "3.12"}}],
                "connections": []}
    session = await asyncio.wait_for(rt.create_streaming_session(json.dumps(manifest)), 120)
    details = None
    try:
        await session.send_input("hello")
        assert await asyncio.wait_for(session.recv_output(), 10) == ("data", "SOURCE: HELLO")
        async with attach_inproc(session) as ctrl:
            details = await ctrl.transform.describe(_timeout=5)
        assert details["pid"] != os.getpid()
        assert details["dependency"] == "25.0", details
        assert details["prefix"] == "SOURCE: ", details
        print(json.dumps(details, indent=2))
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())

"""Execute DocsUppercase in a real worker using the current environment explicitly."""

import asyncio
import json
import os
import sys
from pathlib import Path

import remotemedia.runtime as rt
from remotemedia.control import attach_inproc


async def main():
    # The worker imports this on-disk module; it does not inherit this process's registry.
    module_dir = str(Path(__file__).resolve().parent)
    os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [module_dir, os.environ.get("PYTHONPATH")]))
    rt.register_python_node_type("DocsUppercase", python_class="node_transform.DocsUppercase")
    manifest = {
        "version": "v1",
        "metadata": {"name": "docs-worker"},
        "python_env": {"explicit_venv": sys.prefix},
        "nodes": [{"id": "formatter", "node_type": "DocsUppercase", "params": {"prefix": "WORKER: "}}],
        "connections": [],
    }
    session = await asyncio.wait_for(rt.create_streaming_session(json.dumps(manifest)), timeout=45)
    try:
        await session.send_input("hello")
        kind, value = await asyncio.wait_for(session.recv_output(), timeout=10)
        assert (kind, value) == ("data", "WORKER: HELLO"), (kind, value)
        async with attach_inproc(session) as ctrl:
            description = await asyncio.wait_for(ctrl.formatter.describe(), timeout=10)
        assert description["prefix"] == "WORKER: "
        assert description["pid"] != os.getpid(), "The node must execute in a separate process"
        print(json.dumps({"output": value, "worker_pid": description["pid"], "host_pid": os.getpid()}))
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())

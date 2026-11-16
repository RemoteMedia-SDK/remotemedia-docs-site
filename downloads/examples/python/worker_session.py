"""Open one explicitly configured worker; shared by the advanced examples."""

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import remotemedia.runtime as rt


@asynccontextmanager
async def open_worker(class_name, *, module="advanced_nodes", params=None):
    directory = str(Path(__file__).resolve().parent)
    os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [directory, os.environ.get("PYTHONPATH")]))
    rt.register_python_node_type(class_name, python_class=f"{module}.{class_name}", multi_output=True)
    manifest = {"version": "v1", "metadata": {"name": "docs-advanced-worker"},
                "python_env": {"explicit_venv": sys.prefix},
                "nodes": [{"id": "worker", "node_type": class_name, "params": params or {}}], "connections": []}
    session = await asyncio.wait_for(rt.create_streaming_session(json.dumps(manifest)), 45)
    try:
        yield session
    finally:
        await session.close()

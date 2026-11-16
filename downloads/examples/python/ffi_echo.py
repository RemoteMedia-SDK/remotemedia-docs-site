"""Execute one request through the native FFI. Requires client + native runtime."""

import asyncio
import json
from pathlib import Path

from remotemedia import execute_pipeline_with_input


async def main():
    manifest = json.loads(Path(__file__).with_name("echo.json").read_text(encoding="utf-8"))
    result = await asyncio.wait_for(
        execute_pipeline_with_input(manifest, ["hello"]), timeout=10
    )
    assert result == "hello", result
    print(result)


if __name__ == "__main__":
    asyncio.run(main())

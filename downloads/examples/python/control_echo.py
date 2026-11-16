"""Tap and publish through an in-process control attach. Requires native FFI."""

import asyncio
from pathlib import Path

import remotemedia.runtime as rt
from remotemedia.control import attach_inproc


async def main():
    manifest = Path(__file__).with_name("echo.json").read_text(encoding="utf-8")
    session = await asyncio.wait_for(rt.create_streaming_session(manifest), timeout=10)
    try:
        async with attach_inproc(session) as ctrl:
            tap = await ctrl.subscribe("echo.out")
            try:
                await ctrl.publish("echo.in", "hello")
                item = await asyncio.wait_for(anext(tap), timeout=5)
                print(item.text)
            finally:
                await tap.aclose()
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())

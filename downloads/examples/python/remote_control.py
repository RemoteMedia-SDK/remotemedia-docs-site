"""Observe one new output on an existing session via a local/private admin listener.

Required environment: REMOTEMEDIA_CONTROL_ADDRESS (host:port), REMOTEMEDIA_SESSION_ID.
Optional: REMOTEMEDIA_CONTROL_TOKEN, REMOTEMEDIA_TAP (default echo.out).
Configure grpc channel credentials when using a TLS-terminated deployment.
"""

import asyncio
import os

from remotemedia.control import attach


async def main():
    address = os.environ["REMOTEMEDIA_CONTROL_ADDRESS"]
    session_id = os.environ["REMOTEMEDIA_SESSION_ID"]
    token = os.environ.get("REMOTEMEDIA_CONTROL_TOKEN")
    output_address = os.environ.get("REMOTEMEDIA_TAP", "echo.out")
    async with attach(address, session_id=session_id, token=token) as ctrl:
        tap = await ctrl.subscribe(output_address)
        try:
            print(f"Waiting for a new output at {output_address}...")
            item = await asyncio.wait_for(anext(tap), timeout=30)
            print(item)
        finally:
            await tap.aclose()


if __name__ == "__main__":
    asyncio.run(main())

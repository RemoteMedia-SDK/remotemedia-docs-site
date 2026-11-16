"""Call a real worker with reply, stream, fire-and-forget, error, and deadline modes."""

import asyncio
import json
import os

from remotemedia.control import attach_inproc
from remotemedia.control.errors import RpcError, RpcTimeoutError
from worker_session import open_worker


async def main():
    async with open_worker("DocsRpcWorker", params={"prefix": "RPC: "}) as session:
        async with attach_inproc(session) as ctrl:
            info = await asyncio.wait_for(ctrl.worker.describe(), 5)
            assert info["pid"] != os.getpid() and info["prefix"] == "RPC: "
            words = []
            async with asyncio.timeout(5):
                stream = aiter(ctrl.worker.words("one two three"))
                try:
                    async for word in stream:
                        words.append(word)
                finally:
                    await stream.aclose()
            assert words == ["RPC: one", "RPC: two", "RPC: three"]
            await ctrl.worker.configure("CHANGED: ")
            assert (await asyncio.wait_for(ctrl.worker.describe(), 5))["prefix"] == "CHANGED: "
            try:
                await ctrl.worker.fail(_timeout=2)
            except RpcError as error:
                assert "example failure" in str(error), error
            else:
                raise AssertionError("Worker exception did not reach the caller")
            try:
                await ctrl.worker.slow(_timeout=0.02)
            except RpcTimeoutError:
                pass
            else:
                raise AssertionError("The short caller deadline should expire")
            # A caller timeout alone does not certify remote cancellation.
            assert (await ctrl.worker.describe(_timeout=2))["prefix"] == "CHANGED: "
            print(json.dumps({"rpc_modes": "passed", "words": words, "worker_pid": info["pid"]}))


if __name__ == "__main__":
    asyncio.run(main())

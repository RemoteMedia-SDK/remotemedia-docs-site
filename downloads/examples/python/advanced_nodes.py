"""Small CPU-only worker nodes used by the MS2 RPC and endpoint examples."""

import asyncio

from node_transform import DocsUppercase
from remotemedia.core.multiprocessing.data import RuntimeData
from remotemedia.rpc import rpc


class DocsRpcWorker(DocsUppercase):
    @rpc
    async def configure(self, prefix: str):
        self.prefix = prefix

    @rpc(stream=True)
    async def words(self, text: str):
        for word in text.split():
            yield self.prefix + word
            await asyncio.sleep(0.01)

    @rpc(reply=True)
    async def fail(self):
        raise ValueError("example failure")

    @rpc(reply=True)
    async def slow(self):
        await asyncio.sleep(0.2)
        return "finished after the caller's deadline"


class DocsEndpointWorker(DocsUppercase):
    async def process(self, data: RuntimeData) -> RuntimeData:
        result = await self.call_endpoint("application", "decorate", text=data.as_text(), deadline_ms=3000)
        return RuntimeData.text(result["text"])

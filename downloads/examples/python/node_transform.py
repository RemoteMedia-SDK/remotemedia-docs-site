"""Direct worker-node contract check; does not launch a host or worker process."""

import asyncio
import os

from remotemedia.core.multiprocessing import MultiprocessNode, NodeConfig, register_node
from remotemedia.core.multiprocessing.data import RuntimeData
from remotemedia.rpc import rpc


@register_node("DocsUppercase")
class DocsUppercase(MultiprocessNode):
    def __init__(self, config: NodeConfig):
        if not isinstance(config, NodeConfig):
            raise TypeError("DocsUppercase requires NodeConfig with manifest parameters")
        super().__init__(config)
        self.prefix = str(config.params.get("prefix", ""))

    async def initialize(self):
        # Load model or device resources here when your node needs them.
        pass

    async def process(self, data: RuntimeData) -> RuntimeData:
        if not data.is_text():
            raise TypeError("DocsUppercase expects text input")
        return RuntimeData.text(self.prefix + data.as_text().upper())

    @rpc(reply=True)
    async def describe(self) -> dict:
        return {"operation": "uppercase", "prefix": self.prefix, "pid": os.getpid()}

    async def cleanup(self):
        # Release any resources allocated during initialize().
        pass


async def main():
    try:
        DocsUppercase("formatter")
    except TypeError:
        pass
    else:
        raise AssertionError("A bare node ID must not discard manifest parameters")
    node = DocsUppercase(NodeConfig("formatter", "DocsUppercase", {"prefix": "DOCS: "}))
    try:
        await node.initialize()
        output = await node.process(RuntimeData.text("hello"))
        print(output.as_text())
        assert output.as_text() == "DOCS: HELLO"
        assert await node.describe() == {"operation": "uppercase", "prefix": "DOCS: ", "pid": os.getpid()}
    finally:
        await node.cleanup()


if __name__ == "__main__":
    asyncio.run(main())

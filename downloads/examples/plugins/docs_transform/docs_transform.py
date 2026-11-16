"""A source plugin with explicit configuration and a pinned dependency."""

import os
import sys

from packaging.version import Version
from remotemedia.core.multiprocessing import MultiprocessNode, NodeConfig, register_node
from remotemedia.core.multiprocessing.data import RuntimeData
from remotemedia.rpc import rpc


@register_node("DocsSourceTransform")
class DocsSourceTransform(MultiprocessNode):
    def __init__(self, config: NodeConfig):
        if not isinstance(config, NodeConfig):
            raise TypeError("DocsSourceTransform requires NodeConfig")
        super().__init__(config)
        self.prefix = config.params.get("prefix", "PLUGIN: ")
        self.call_application = bool(config.params.get("call_application", False))

    async def initialize(self):
        self.version = str(Version("0.1.0"))

    async def process(self, data: RuntimeData):
        if self.call_application:
            result = await self.call_endpoint("application", "decorate", text=data.as_text(), deadline_ms=5000)
            return RuntimeData.text(result["text"])
        return RuntimeData.text(self.prefix + data.as_text().upper())

    @rpc(reply=True)
    async def describe(self):
        import packaging
        return {"pid": os.getpid(), "python": sys.executable, "prefix": self.prefix,
                "plugin_version": self.version, "dependency": packaging.__version__}

    async def cleanup(self):
        pass

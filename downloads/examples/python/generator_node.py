"""A deterministic stateful text generator with explicit cancellation semantics."""

import asyncio
import numpy as np

from node_transform import DocsUppercase
from remotemedia.core.cancel import AudioGenerator, CallInfo, Cancellation, CancelStrategy, Retention, TextGenerator
from remotemedia.core.multiprocessing.data import RuntimeData
from remotemedia.rpc import rpc


class DocsGenerator(TextGenerator, DocsUppercase):
    cancel_strategy = CancelStrategy.OBSERVES_TOKEN

    async def initialize(self):
        self.history = []
        self.partial = None
        self.cancellations = []

    async def process(self, data: RuntimeData):
        token = self.cancel_token
        partial = []
        self.partial = partial
        for index in range(30):
            if token.is_cancelled():
                return
            word = f"h{len(self.history)}:{data.as_text()}:{index}"
            partial.append(word)
            yield RuntimeData.text(word)
            await asyncio.sleep(0.02)
        self.history.append(" ".join(partial))
        self.partial = None

    async def cancel(self, call: CallInfo, cancellation: Cancellation) -> None:
        partial, self.partial = self.partial, None
        if partial and cancellation.disposition.partial_output is Retention.RETAIN:
            self.history.append(" ".join(partial) + " [interrupted]")
        self.cancellations.append(cancellation.to_dict())

    @rpc(reply=True)
    async def generation_state(self):
        return {"history_count": len(self.history), "cancellations": self.cancellations}


class DocsAudioGenerator(AudioGenerator, DocsUppercase):
    cancel_strategy = CancelStrategy.OBSERVES_TOKEN

    async def initialize(self):
        self.cancellations = []

    async def process(self, data):
        token = self.cancel_token
        samples = (0.1 * np.sin(2 * np.pi * 440 * np.arange(480) / 24000)).astype(np.float32)
        for _ in range(30):
            if token.is_cancelled():
                return
            yield RuntimeData.audio(samples, sample_rate=24000, channels=1)
            await asyncio.sleep(0.02)

    async def cancel(self, call, cancellation):
        self.cancellations.append(cancellation.to_dict())

    @rpc(reply=True)
    async def generation_state(self):
        return {"cancellations": self.cancellations}

"""Check interrupt/abort conformance and stop a real worker's generated text."""

import asyncio
import json
import time

from generator_node import DocsGenerator
from remotemedia.control import Data, attach_inproc
from remotemedia.core.cancel import Disposition
from remotemedia.core.multiprocessing import NodeConfig
from remotemedia.core.multiprocessing.data import RuntimeData
from remotemedia.testing.cancellation import assert_generator_cancels_async
from worker_session import open_worker


async def main():
    report = await assert_generator_cancels_async(
        lambda: DocsGenerator(NodeConfig("generator", "DocsGenerator", {})),
        lambda label: RuntimeData.text(label), stateful=True, timeout=5,
    )
    assert not report.failures
    outcomes = []
    for disposition in (Disposition.INTERRUPT, Disposition.ABORT):
        async with open_worker("DocsGenerator", module="generator_node") as session:
            outputs = []
            first = asyncio.Event()

            async def receive():
                while (item := await session.recv_output()) is not None:
                    kind, value = item
                    if kind == "data" and isinstance(value, str) and value.startswith("h"):
                        outputs.append((time.monotonic(), value))
                        first.set()

            receiver = asyncio.create_task(receive())
            try:
                async with attach_inproc(session) as ctrl:
                    await session.send_input("cancelled")
                    await asyncio.wait_for(first.wait(), 5)
                    sent = time.monotonic()
                    await ctrl.publish("worker.in.barge_in", Data.from_json({"disposition": disposition.to_dict()}))
                    status = await ctrl.worker.generation_state(_timeout=5)
                    assert status["cancellations"], status
                    assert status["cancellations"][-1]["disposition"] == disposition.to_dict(), status
                    await asyncio.sleep(0.3)
                    late = [text for at, text in outputs if at > sent + 0.25]
                    assert not late, late
                    expected_history = 1 if disposition == Disposition.INTERRUPT else 0
                    assert status["history_count"] == expected_history, status
                    before = len(outputs)
                    await session.send_input("after")
                    async with asyncio.timeout(5):
                        while len(outputs) == before:
                            await asyncio.sleep(0.01)
                    assert outputs[before][1].startswith(f"h{expected_history}:after:"), outputs[before]
                    # Finish the short next call before closing the session.
                    async with asyncio.timeout(5):
                        while len(outputs) < before + 30:
                            await asyncio.sleep(0.01)
                    outcomes.append({"disposition": disposition.to_dict(), "cancelled_output_count": before,
                                     "outputs_after_250ms": len(late), "next_call": outputs[before][1]})
            finally:
                receiver.cancel()
                await asyncio.gather(receiver, return_exceptions=True)
    print(json.dumps({"in_memory_conformance": "passed", "real_worker": outcomes}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

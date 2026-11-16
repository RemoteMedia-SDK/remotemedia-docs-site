"""Bound the synthetic audio tail at FFI receive; no speaker or device is used."""

import asyncio
import json
import time

from remotemedia.control import Data, attach_inproc
from remotemedia.core.cancel import Disposition
from worker_session import open_worker


async def main():
    async with open_worker("DocsAudioGenerator", module="generator_node") as session:
        chunks, first = [], asyncio.Event()

        async def receive():
            while (item := await session.recv_output()) is not None:
                if item[0] == "audio":
                    chunks.append((time.monotonic(), len(item[1]["samples"])))
                    first.set()

        receiver = asyncio.create_task(receive())
        try:
            async with attach_inproc(session) as ctrl:
                await session.send_input("tone")
                await asyncio.wait_for(first.wait(), 5)
                sent = time.monotonic()
                await ctrl.publish("worker.in.barge_in", Data.from_json({"disposition": Disposition.INTERRUPT.to_dict()}))
                state = await ctrl.worker.generation_state(_timeout=5)
                assert state["cancellations"]
                await asyncio.sleep(0.35)
                late = [size for at, size in chunks if at > sent + 0.25]
                assert not late, late
                print(json.dumps({"sample_rate": 24000, "chunk_samples": 480,
                                  "received_chunks": len(chunks), "samples_after_250ms": sum(late),
                                  "observation_window_ms": 350, "boundary": "FFI receive"}))
        finally:
            receiver.cancel()
            await asyncio.gather(receiver, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())

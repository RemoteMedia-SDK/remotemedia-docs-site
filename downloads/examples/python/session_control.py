"""Observe pre-intercept taps and verify the actual downstream result."""

import asyncio
import json

import remotemedia.runtime as rt
from remotemedia.control import Data, NodeState, attach_inproc


async def main():
    manifest = {"version": "v1", "metadata": {"name": "docs-control"}, "nodes": [
        {"id": "source", "node_type": "PassThrough"},
        {"id": "sink", "node_type": "PassThrough"},
    ], "connections": [{"from": "source", "to": "sink"}]}
    session = await rt.create_streaming_session(json.dumps(manifest))
    try:
        async with attach_inproc(session) as ctrl:
            incoming = await ctrl.subscribe_in("source.in")
            original = await ctrl.subscribe("source.out")
            downstream = await ctrl.subscribe("sink.out")
            try:
                async with ctrl.intercept("source.out", deadline_ms=2000) as requests:
                    await ctrl.publish("source.in", "original", metadata={"request_id": "docs-1"})
                    observed = await asyncio.wait_for(anext(incoming), 5)
                    assert observed.text == "original"
                    metadata_observed = observed.metadata
                    request = await asyncio.wait_for(anext(requests), 5)
                    await request.replace(Data.from_text("rewritten"))
                    assert (await asyncio.wait_for(anext(original), 5)).text == "original"
                    assert (await asyncio.wait_for(anext(downstream), 5)).text == "rewritten"
                    await ctrl.publish("source.in", "discard")
                    await (await asyncio.wait_for(anext(requests), 5)).drop()
                    # A sentinel proves the downstream channel still works after the drop.
                    await ctrl.publish("source.in", "sentinel")
                    await (await asyncio.wait_for(anext(requests), 5)).pass_through()
                    assert (await asyncio.wait_for(anext(downstream), 5)).text == "sentinel"
                await ctrl.set_node_state("source", NodeState.DISABLED)
                await ctrl.publish("source.in", "disabled")
                pending = asyncio.create_task(anext(downstream))
                try:
                    done, _ = await asyncio.wait({pending}, timeout=0.2)
                    assert not done, "Disabled input unexpectedly reached the downstream tap"
                    await ctrl.set_node_state("source", NodeState.BYPASS)
                    await ctrl.publish("source.in", "bypassed")
                    assert (await asyncio.wait_for(pending, 5)).text == "bypassed"
                finally:
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                await ctrl.clear_node_state("source")
                await ctrl.publish("source.in", "enabled again")
                assert (await asyncio.wait_for(anext(downstream), 5)).text == "enabled again"
                # Output publication is control-bus delivery to subscribers.
                # It does not enqueue a new execution at the downstream node.
                external = await ctrl.subscribe("source.out.external")
                try:
                    await ctrl.publish_out("source.out.external", "application event")
                    assert (await asyncio.wait_for(anext(external), 5)).text == "application event"
                finally:
                    await external.aclose()
                print(json.dumps({"control": "passed", "input_tap_metadata": metadata_observed,
                                  "expected_publish_metadata": {"request_id": "docs-1"}}))
            finally:
                for tap in (incoming, original, downstream):
                    await tap.aclose()
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())

"""Join two participants, retain attribution, and close the final membership."""

import asyncio
import json
import uuid
from pathlib import Path

import remotemedia.runtime as rt


async def main():
    manifest = Path(__file__).with_name("echo.json").read_text()
    key = "docs-room-" + uuid.uuid4().hex
    handles = []
    try:
        for participant in ("alice", "bob"):
            handles.append(await rt.create_shared_streaming_session(
                manifest, key, participant, "user", modality="text", display_name=participant.title()
            ))
        alice, bob = handles
        assert alice.session_id == bob.session_id
        initial_id = alice.session_id
        for sender, name in ((alice, "alice"), (bob, "bob")):
            await sender.send_input("hello from " + name)
            for receiver in (alice, bob):
                event = await asyncio.wait_for(receiver.recv_event(), 5)
                assert event["participant"]["id"] == name, event
                assert event["data"] == "hello from " + name, event
        await alice.close()
        await bob.send_input("still here")
        event = await asyncio.wait_for(bob.recv_event(), 5)
        assert event["data"] == "still here"
        await bob.close()
        assert await asyncio.wait_for(bob.recv_event(), 2) is None
        replacement = await rt.create_shared_streaming_session(manifest, key, "carol", "user")
        handles.append(replacement)
        assert replacement.session_id != initial_id
        await replacement.send_input("fresh session")
        assert (await asyncio.wait_for(replacement.recv_event(), 5))["data"] == "fresh session"
        print(json.dumps({"shared_id": initial_id, "replacement_id": replacement.session_id,
                          "attribution": "passed", "last_member_cleanup": "passed"}))
    finally:
        for handle in reversed(handles):
            await handle.close()


if __name__ == "__main__":
    asyncio.run(main())

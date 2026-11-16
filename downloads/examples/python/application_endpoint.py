"""Let a real worker await an application-owned service while processing input."""

import asyncio
import json
import socket
from contextlib import asynccontextmanager

import grpc
import remotemedia.runtime as rt
from remotemedia.control import EndpointCall, EndpointError, attach, attach_inproc
from worker_session import open_worker


@asynccontextmanager
async def control(session, transport):
    if transport == "inproc":
        async with attach_inproc(session) as ctrl:
            yield ctrl
    else:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            address = f"127.0.0.1:{sock.getsockname()[1]}"
        async with rt.GrpcServer(address):
            async with grpc.aio.insecure_channel(address) as channel:
                await asyncio.wait_for(channel.channel_ready(), 5)
            async with attach(address, session_id=session.session_id) as ctrl:
                yield ctrl


async def main(transport="inproc"):
    async with open_worker("DocsEndpointWorker") as session:
        async with control(session, transport) as ctrl:
            host = await ctrl.host_endpoint("application", methods={"decorate": "reply"})
            try:
                assert await ctrl.has_endpoint("application")
                if transport == "inproc":
                    try:
                        await ctrl.host_endpoint("application", methods={"decorate": "reply"})
                    except EndpointError as error:
                        assert error.kind == "already_hosted", error
                    else:
                        raise AssertionError("Duplicate endpoint registration should fail")
                await session.send_input("hello")
                event = await asyncio.wait_for(anext(host), 5)
                assert isinstance(event, EndpointCall), event
                assert event.method == "decorate" and event.kwargs["text"] == "hello"
                ack = await event.reply({"text": "APP: " + event.kwargs["text"].upper()})
                assert ack, ack
                kind, value = await asyncio.wait_for(session.recv_output(), 5)
                assert (kind, value) == ("data", "APP: HELLO"), (kind, value)
                print(json.dumps({"transport": transport, "output": value, "reply_accepted": bool(ack)}))
            finally:
                await host.close()
            # Remote unhosting completes after the stream-close reaches the server.
            async with asyncio.timeout(3):
                while await ctrl.has_endpoint("application"):
                    await asyncio.sleep(0.02)


if __name__ == "__main__":
    asyncio.run(main())

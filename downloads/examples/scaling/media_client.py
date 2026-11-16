"""Exchange synthetic audio with a Scaling room using an Entry admission ticket."""

import asyncio
import fractions
import json
import time

import numpy as np
from aiortc import AudioStreamTrack, RTCBundlePolicy, RTCConfiguration, RTCPeerConnection, RTCSessionDescription
from aiortc.sdp import candidate_from_sdp
from av import AudioFrame
from websockets.asyncio.client import connect


class SineTrack(AudioStreamTrack):
    """A bounded test sends this 440 Hz tone; no microphone is opened."""

    def __init__(self):
        super().__init__()
        self.position = 0
        self.started = None

    async def recv(self):
        if self.started is None:
            self.started = time.monotonic()
        await asyncio.sleep(max(0, self.started + self.position / 48000 - time.monotonic()))
        indices = np.arange(self.position, self.position + 960)
        samples = (6000 * np.sin(2 * np.pi * 440 * indices / 48000)).astype(np.int16)
        frame = AudioFrame.from_ndarray(samples.reshape(1, -1), format="s16", layout="mono")
        frame.sample_rate, frame.pts, frame.time_base = 48000, self.position, fractions.Fraction(1, 48000)
        self.position += 960
        return frame


async def exchange_audio(admission):
    pc = RTCPeerConnection(RTCConfiguration(iceServers=[], bundlePolicy=RTCBundlePolicy.MAX_BUNDLE))
    tone = SineTrack()
    received = asyncio.Queue()
    tasks, candidates = [], []
    channel_open = asyncio.Event()
    pc.addTrack(tone)
    channel = pc.createDataChannel("data")
    channel.on("open", channel_open.set)

    @pc.on("track")
    def track_received(track):
        async def consume():
            while True:
                frame = await track.recv()
                if track.kind == "audio":
                    await received.put(frame)
        tasks.append(asyncio.create_task(consume()))

    async def notification(message):
        if message.get("method") != "peer.ice_candidate":
            return
        params = message["params"]
        raw = params.get("candidate", "")
        if not raw:
            return
        candidate = candidate_from_sdp(raw.removeprefix("candidate:"))
        candidate.sdpMid = params.get("sdp_mid", "0")
        candidate.sdpMLineIndex = params.get("sdp_m_line_index", 0)
        if pc.remoteDescription is None:
            candidates.append(candidate)
        else:
            await pc.addIceCandidate(candidate)

    async def call(ws, identifier, method, params):
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": identifier, "method": method, "params": params}))
        async with asyncio.timeout(10):
            while True:
                message = json.loads(await ws.recv())
                if message.get("id") == identifier:
                    if "error" in message:
                        raise RuntimeError(f"{method}: {message['error']}")
                    return message["result"]
                await notification(message)

    try:
        async with connect(admission["signal_url"], open_timeout=10) as ws:
            announced = await call(ws, "announce", "peer.announce", {
                "peer_id": "docs-python", "ticket": admission["ticket"], "capabilities": ["audio", "data"], "user_data": {},
            })
            await pc.setLocalDescription(await pc.createOffer())
            answer = await call(ws, "offer", "peer.offer", {
                "from": announced["peer_id"], "to": "remotemedia-server", "sdp": pc.localDescription.sdp,
            })
            await pc.setRemoteDescription(RTCSessionDescription(sdp=answer["sdp"], type="answer"))
            for candidate in candidates:
                await pc.addIceCandidate(candidate)

            async def signaling():
                async for raw in ws:
                    await notification(json.loads(raw))
            tasks.append(asyncio.create_task(signaling()))
            try:
                await asyncio.wait_for(channel_open.wait(), 15)
            except TimeoutError:
                for task in tasks:
                    if task.done() and not task.cancelled() and task.exception():
                        raise task.exception()
                print({"connection": pc.connectionState, "ice": pc.iceConnectionState,
                       "local_candidate_count": sum(line.startswith("a=candidate") for line in pc.localDescription.sdp.splitlines()),
                       "early_remote_candidates": len(candidates)}, flush=True)
                raise
            frames = []
            async with asyncio.timeout(15):
                while len(frames) < 20:
                    frame = await received.get()
                    array = frame.to_ndarray().astype(np.float32)
                    mono = array[0] if frame.format.is_planar else array.reshape(-1, len(frame.layout.channels))[:, 0]
                    frames.append(mono)
            rms = float(np.sqrt(np.mean(np.concatenate([frame.ravel() for frame in frames]) ** 2)))
            assert rms > 100, f"Expected an echoed tone, received RMS {rms}"
            samples = np.concatenate(frames)
            spectrum = np.abs(np.fft.rfft(samples * np.hanning(len(samples))))
            peak_hz = float(np.fft.rfftfreq(len(samples), 1 / frame.sample_rate)[np.argmax(spectrum)])
            assert abs(peak_hz - 440) < 10, f"Expected 440 Hz, received {peak_hz} Hz"
            assert pc.connectionState == "connected"
            return {"connection": pc.connectionState, "data_channel": channel.readyState,
                    "received_audio_frames": len(frames), "pcm_rms": rms, "dominant_hz": peak_hz,
                    "received_sample_rate": frame.sample_rate, "sent_samples": tone.position,
                    "member": announced["peer_id"]}
    finally:
        tone.stop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await pc.close()

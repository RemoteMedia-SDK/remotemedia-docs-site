"""Send typed data through a native echo session and inspect owning output views."""

import asyncio
import json
from pathlib import Path

import numpy as np
import remotemedia.runtime as rt
from remotemedia.control import Data


async def main():
    manifest = (Path(__file__).with_name("echo.json")).read_text()
    samples = np.linspace(-0.1, 0.1, 480, dtype=np.float32)
    audio = Data.from_audio_numpy(samples, sample_rate=24000, channels=1)
    assert audio.as_runtime_data()["samples"] is samples
    pixels = bytes([255, 0, 0] * 4)  # A packed 2 x 2 RGB24 frame.
    tensor = np.arange(6, dtype=np.float32).reshape(2, 3)
    cases = [
        ("text", "hello"),
        ("json", Data.from_json({"labels": ["voice", "test"]}).as_runtime_data()),
        ("binary", Data.from_buffer(b"bytes").as_runtime_data()),
        ("audio", audio.as_runtime_data()),
        ("tensor", {"type": "tensor", "data": tensor.tobytes(), "shape": [2, 3], "dtype": 0}),
        ("video", {"type": "video", "pixel_data": pixels, "width": 2, "height": 2,
                   "format": "Rgb24", "codec": None, "frame_number": 0, "timestamp_us": 0, "is_keyframe": True}),
        ("image", {"type": "image", "data": pixels, "width": 2, "height": 2,
                   "format": {"Raw": {"pixel_format": "Rgb24"}}}),
        ("file", {"type": "file", "path": "example.wav", "filename": "example.wav", "mime_type": "audio/wav"}),
    ]
    session = await rt.create_streaming_session(manifest)
    received = {}
    try:
        for name, payload in cases:
            await session.send_input(payload)
            kind, value = await asyncio.wait_for(session.recv_output(), 5)
            if name == "text":
                assert value == "hello"
            elif name == "json":
                assert value["value"] == {"labels": ["voice", "test"]}
            elif name == "binary":
                assert bytes(value) == b"bytes"
            elif name == "audio":
                np.testing.assert_array_equal(value["samples"], samples)
                assert value["sample_rate"] == 24000 and value["channels"] == 1
                owned_audio = np.asarray(value["samples"])
                received["audio_export"] = type(value["samples"]).__name__
                received["audio_dtype"] = str(owned_audio.dtype)
                received["audio_writable"] = bool(owned_audio.flags.writeable)
            elif name == "tensor":
                assert value["shape"] == [2, 3] and value["dtype"] == 0
                np.testing.assert_array_equal(np.frombuffer(value["data"], dtype=np.float32).reshape(2, 3), tensor)
            elif name in ("video", "image"):
                assert value["width"] == 2 and value["height"] == 2
                assert bytes(value["pixel_data" if name == "video" else "data"]) == pixels
            else:
                assert value["path"] == "example.wav"
            received[name] = kind
        try:
            await session.send_input({"type": "audio", "samples": samples.tolist(),
                                      "sample_rate": 24000, "channels": 1}, require_zero_copy=True)
        except BufferError as error:
            received["strict_list_rejected"] = str(error)
        else:
            raise AssertionError("A Python list requires audio materialization")
        received["input_copy_report"] = session.input_copy_report()
    finally:
        await session.close()
    # The exported owner keeps samples alive after the session closes.
    np.testing.assert_array_equal(owned_audio, samples)
    detached = np.array(owned_audio, copy=True)
    detached[0] = 0.5
    assert owned_audio[0] != detached[0]
    print(json.dumps(received, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

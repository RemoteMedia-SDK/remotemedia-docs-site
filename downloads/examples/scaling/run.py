"""Start an authenticated loopback Entry + runner, echo through control, and stop.

Requires the checkout Python client, pyjwt[crypto], psutil, and built Scaling binaries.
Public-key configuration and logs stay under --output; private credentials stay in memory.
"""

import argparse
import asyncio
import hashlib
import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import jwt
import psutil
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from remotemedia.control import attach


def free_ports(count):
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def request(base, method, path, token, body=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            payload = response.read()
    except urllib.error.HTTPError as error:
        error.add_note(f"{method} {path}: {error.read().decode('utf-8', errors='replace')}")
        raise
    return json.loads(payload) if payload else None


def wait_ready(base, token, processes):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if any(process.poll() is not None for process in processes):
            raise RuntimeError("An owned server exited during startup; inspect the run logs")
        try:
            request(base, "GET", "/pipelines", token)
            return
        except (urllib.error.URLError, TimeoutError):
            time.sleep(0.1)
    raise TimeoutError("Entry did not become ready within 30 seconds")


def stop_processes(processes):
    owned = []
    for process in processes:
        if process.poll() is None:
            parent = psutil.Process(process.pid)
            owned.extend(parent.children(recursive=True))
            owned.append(parent)
    for process in reversed(owned):
        try:
            process.terminate()
        except psutil.NoSuchProcess:
            pass
    _, alive = psutil.wait_procs(owned, timeout=5)
    for process in alive:
        process.kill()
    _, alive = psutil.wait_procs(alive, timeout=5)
    for process in processes:
        process.wait(timeout=5)
    if alive:
        raise RuntimeError("An owned server process did not terminate")


async def exchange(address, session_id, token, tools=False):
    async with attach(address, session_id=session_id, token=token) as ctrl:
        if tools:
            details = await ctrl.echo.describe(_timeout=10)
            host = await ctrl.host_endpoint("application", methods={"decorate": "reply"})
        else:
            details, host = None, None
        tap = await ctrl.subscribe("echo.out")
        try:
            await ctrl.publish("echo.in", "hello through Entry")
            if host is not None:
                event = await asyncio.wait_for(anext(host), 10)
                assert event.method == "decorate"
                assert (await event.reply({"text": event.kwargs["text"]})).accepted
            result = await asyncio.wait_for(anext(tap), timeout=10)
            assert result.text == "hello through Entry", result
            return {"text": result.text, "worker_rpc": details, "application_endpoint": host is not None}
        finally:
            await tap.aclose()
            if host is not None:
                await host.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=Path(".run/scaling"))
    parser.add_argument("--media", action="store_true", help="Also negotiate WebRTC and echo synthetic audio")
    parser.add_argument("--tools", action="store_true", help="Run source-plugin RPC and an application callback through Entry")
    args = parser.parse_args()
    if args.tools and args.media:
        parser.error("Run --tools and --media separately: their example graphs differ")
    run_dir = args.output.resolve() / uuid.uuid4().hex[:10]
    run_dir.mkdir(parents=True)
    suffix = ".exe" if os.name == "nt" else ""
    binaries = {name: (args.bin_dir / f"remotemedia-scaling-{name}{suffix}").resolve() for name in ("entry", "runner")}
    for path in binaries.values():
        if not path.is_file():
            parser.error(f"Missing binary: {path}")
    admin_port, public_port, runner_port, member_port = free_ports(4)
    address = f"127.0.0.1:{admin_port}"
    admin, public = "http://" + address, f"http://127.0.0.1:{public_port}"
    name, session_key = "docs-echo-" + uuid.uuid4().hex[:8], "example"
    key = Ed25519PrivateKey.generate()
    public_key = run_dir / "developer.pub"
    public_key.write_bytes(key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo))
    expiry = int(time.time()) + 300
    backend_token = jwt.encode({"exp": expiry, "rm": {"role": "backend"}}, key, algorithm="EdDSA")
    client_token = jwt.encode({"exp": expiry, "rm": {"role": "client", "pipeline": name, "session": session_key, "member": "docs-reader"}}, key, algorithm="EdDSA")
    discovery = run_dir / "runners.toml"
    discovery.write_text(
        '[[runner]]\nid = "docs-runner"\n'
        f'control_addr = "127.0.0.1:{runner_port}"\n'
        'webrtc_endpoints = []\n'
        f'labels = {{ "remotemedia.room_signal_url" = "ws://127.0.0.1:{member_port}/ws" }}\n'
        'capacity = { slots_total = 16, slots_available = 16 }\n', encoding="utf-8"
    )
    processes, logs = [], []
    deployed = joined = False
    report = {"binaries": {name: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for name, path in binaries.items()}}
    try:
        commands = [
            ("runner", [str(binaries["runner"]), "--id", "docs-runner", "--listen", f"127.0.0.1:{runner_port}", "--room-signal-port", str(member_port), "--discovery-config", str(discovery), "--slots", "16"]),
            ("entry", [str(binaries["entry"]), "--listen", address, "--signaling-port", str(public_port), "--developer-key", str(public_key), "--discovery-config", str(discovery), "--session-grace-ms", "60000"]),
        ]
        for label, command in commands:
            log = (run_dir / f"{label}.log").open("wb")
            logs.append(log)
            process = subprocess.Popen(command, cwd=run_dir, stdout=log, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            processes.append(process)
        wait_ready(admin, backend_token, processes)
        try:
            request(admin, "GET", "/pipelines", None)
        except urllib.error.HTTPError as error:
            assert error.code in (401, 403), error.code
            report["unauthenticated_admin_status"] = error.code
        else:
            raise AssertionError("The admin listener accepted a request without a token")
        manifest = json.loads((Path(__file__).parents[1] / "python/echo.json").read_text(encoding="utf-8"))
        manifest["metadata"]["name"] = name
        if args.media:
            manifest["nodes"].append({"id": "audio", "node_type": "FastResampleNode", "params": {
                "target_rate": 48000, "output_stream_id": "default"}})
            manifest["connections"].append({"from": "echo", "to": "audio"})
        if args.tools:
            manifest["plugins"] = [str((Path(__file__).parents[1] / "plugins/docs_transform").resolve())]
            manifest["nodes"][0].update(node_type="DocsSourceTransform", params={
                "prefix": "ENTRY: ", "call_application": True, "__python_version__": "3.12"})
        request(admin, "PUT", f"/pipelines/{name}", backend_token, {"manifest": manifest})
        deployed = True
        admission = request(public, "POST", "/join", client_token, {})
        joined = True
        assert {"signal_url", "ticket", "member"} <= admission.keys(), admission.keys()
        result = asyncio.run(asyncio.wait_for(exchange(address, f"{name}/{session_key}", backend_token, args.tools), timeout=30))
        report.update(output=result, backend_control="passed", client_admission="passed", server_pids=[p.pid for p in processes])
        if args.media:
            from media_client import exchange_audio
            report["media"] = asyncio.run(asyncio.wait_for(exchange_audio(admission), timeout=45))
        print(result, flush=True)
    finally:
        try:
            if joined:
                request(admin, "DELETE", f"/sessions/{name}/{session_key}", backend_token)
            if deployed:
                request(admin, "DELETE", f"/pipelines/{name}", backend_token)
        finally:
            stop_processes(processes)
            for log in logs:
                log.close()
    report["owned_processes_stopped"] = True
    (run_dir / "result.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"PASS: authenticated Entry control, runner echo, admission, and teardown. Evidence: {run_dir / 'result.json'}")


if __name__ == "__main__":
    main()

"""Identify the installed client/native runtime; fail clearly when FFI is required."""

import argparse
import importlib.metadata
import json
import platform
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-ffi", action="store_true")
    args = parser.parse_args()
    report = {"python": platform.python_version(), "platform": platform.platform(), "executable": sys.executable}
    for package in ("remotemedia-client", "remotemedia-ffi", "iceoryx2"):
        try:
            report[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            report[package] = "not installed"
    try:
        import remotemedia
        report["client_path"] = remotemedia.__file__
    except ImportError as error:
        print(f"Cannot import the client: {error}. Install remotemedia-client in {sys.executable}.", file=sys.stderr)
        return 2
    try:
        import remotemedia.runtime as rt
        report["native_path"] = rt.__file__
        report["native_runtime_version"] = rt.get_runtime_version()
    except ImportError as error:
        report["ffi_error"] = str(error)
        if args.require_ffi:
            print(json.dumps(report, indent=2))
            print("Local execution requires a compatible remotemedia-ffi wheel in this environment. See the Installation page; the client alone is sufficient for remote control.", file=sys.stderr)
            return 2
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

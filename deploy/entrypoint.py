#!/usr/bin/env python3
"""Supervise vLLM and the outbound Anvil Ring tether in one rental image."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from collections.abc import Sequence

UPSTREAM = "http://127.0.0.1:8000"
SHUTDOWN_GRACE_SECONDS = 10.0


def _error(message: str) -> int:
    print(f"anvil-ring-entrypoint: {message}", file=sys.stderr)
    return 2


def _validate(argv: Sequence[str], env: dict[str, str]) -> int | None:
    if not env.get("ANVIL_RING_HUB_URL"):
        return _error("ANVIL_RING_HUB_URL is required")
    if not (env.get("ANVIL_RING_CRED_FILE") or env.get("ANVIL_RING_CREDENTIAL")):
        return _error(
            "set ANVIL_RING_CRED_FILE (preferred) or ANVIL_RING_CREDENTIAL"
        )
    if env.get("ANVIL_RING_UPSTREAM", UPSTREAM) != UPSTREAM:
        return _error(
            f"ANVIL_RING_UPSTREAM must remain {UPSTREAM} in the combined image"
        )

    protected = ("--host", "--port")
    for arg in argv:
        if arg in protected or any(arg.startswith(f"{name}=") for name in protected):
            return _error(
                f"{arg.split('=', 1)[0]} is fixed by the image so the engine stays loopback-only"
            )
    return None


def _stop(children: Sequence[subprocess.Popen[bytes]]) -> None:
    live = [child for child in children if child.poll() is None]
    for child in live:
        child.terminate()

    deadline = time.monotonic() + SHUTDOWN_GRACE_SECONDS
    for child in live:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            child.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            child.kill()
    for child in live:
        try:
            child.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    env = dict(os.environ)
    if invalid := _validate(args, env):
        return invalid
    env["ANVIL_RING_UPSTREAM"] = UPSTREAM
    # The model engine has no role in tunnel authentication. Do not copy tether
    # credentials, hub location, or caller configuration into its environment.
    engine_env = {
        key: value for key, value in env.items() if not key.startswith("ANVIL_RING_")
    }

    children: list[subprocess.Popen[bytes]] = []
    received_signal: int | None = None

    def forward_signal(signum: int, _frame: object) -> None:
        nonlocal received_signal
        received_signal = signum
        for child in children:
            if child.poll() is None:
                child.send_signal(signum)

    signal.signal(signal.SIGTERM, forward_signal)
    signal.signal(signal.SIGINT, forward_signal)

    try:
        engine = subprocess.Popen(
            ["vllm", "serve", *args, "--host", "127.0.0.1", "--port", "8000"],
            env=engine_env,
        )
        children.append(engine)
        tether = subprocess.Popen(["anvil-ring", "tether"], env=env)
        children.append(tether)

        while True:
            for child in children:
                if (code := child.poll()) is not None:
                    if received_signal is not None:
                        return 128 + received_signal
                    return code
            time.sleep(0.2)
    except OSError as exc:
        return _error(f"failed to start a child process: {exc}")
    finally:
        _stop(children)


if __name__ == "__main__":
    raise SystemExit(main())

"""Verify that an idle local tether remains authorized without reconnecting.

The harness starts a loopback engine, the real hub binary, and the real tether
binary. It sends no caller requests during the observation window. Any repeated
authorization, WebSocket reset, dial failure, or tunnel error therefore indicates
idle connection churn rather than request handling or deliberate shutdown.

Usage:
    python3 scripts/soak_tunnel.py 100 "$PWD/target/debug/anvil-ring"

Exit 0 means one authorization and no churn. Invalid startup or observed churn
returns exit 1.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HOST = "127.0.0.1"


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind((HOST, 0))
        return listener.getsockname()[1]


def spawn(
    command: list[str],
    env: dict[str, str],
    log_path: Path,
    processes: list[tuple[subprocess.Popen, object]],
) -> subprocess.Popen:
    log_file = log_path.open("w")
    process = subprocess.Popen(
        command,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        text=True,
    )
    processes.append((process, log_file))
    return process


def wait_for_log(
    log_path: Path,
    expected: str,
    processes: list[subprocess.Popen],
    timeout: float = 5.0,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for process in processes:
            return_code = process.poll()
            if return_code is not None:
                raise RuntimeError(
                    f"child process exited with status {return_code} before {expected!r}"
                )
        if expected in log_path.read_text(errors="replace"):
            return
        time.sleep(0.025)
    raise TimeoutError(f"log did not contain {expected!r} within {timeout}s")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "window",
        nargs="?",
        type=int,
        default=100,
        help="idle observation window in seconds (default: %(default)s)",
    )
    parser.add_argument("binary", type=Path, help="freshly built anvil-ring binary")
    args = parser.parse_args()
    if args.window < 1:
        parser.error("window must be at least one second")

    binary = args.binary.expanduser().resolve()
    if not binary.is_file():
        parser.error(f"binary does not exist: {binary}")

    hub_port, front_port, upstream_port = free_port(), free_port(), free_port()
    while len({hub_port, front_port, upstream_port}) != 3:
        hub_port, front_port, upstream_port = free_port(), free_port(), free_port()

    credential = f"local-soak-{time.time_ns()}"
    env = dict(os.environ, TZ="UTC")
    processes: list[tuple[subprocess.Popen, object]] = []

    engine_program = (
        "import socket,threading\n"
        "s=socket.socket()\n"
        f"s.bind(('{HOST}',{upstream_port}));s.listen(8)\n"
        "print('engine bound',flush=True)\n"
        "def handle(c):\n"
        "    try:\n"
        "        c.recv(65536)\n"
        "    except Exception:\n"
        "        pass\n"
        "while True:\n"
        "    c,_=s.accept()\n"
        "    threading.Thread(target=handle,args=(c,),daemon=True).start()\n"
    )

    try:
        with tempfile.TemporaryDirectory(prefix="anvil-ring-soak-") as temp_dir:
            temp = Path(temp_dir)
            engine_log = temp / "engine.log"
            hub_log = temp / "hub.log"
            tether_log = temp / "tether.log"

            engine = spawn(
                [sys.executable, "-c", engine_program], env, engine_log, processes
            )
            wait_for_log(engine_log, "engine bound", [engine])

            hub = spawn(
                [str(binary), "hub"],
                dict(
                    env,
                    ANVIL_RING_DEMO_CREDENTIAL=credential,
                    ANVIL_RING_HUB_LISTEN=f"{HOST}:{hub_port}",
                    ANVIL_RING_FRONTEND_LISTEN=f"{HOST}:{front_port}",
                    ANVIL_RING_CALLER_TOKEN="local-caller-token",
                ),
                hub_log,
                processes,
            )
            wait_for_log(hub_log, f"hub on {HOST}:{hub_port}", [engine, hub])

            tether = spawn(
                [str(binary), "tether"],
                dict(
                    env,
                    ANVIL_RING_HUB_URL=f"ws://{HOST}:{hub_port}/ring",
                    ANVIL_RING_UPSTREAM=f"http://{HOST}:{upstream_port}",
                    ANVIL_RING_CREDENTIAL=credential,
                ),
                tether_log,
                processes,
            )
            wait_for_log(tether_log, "tunnel #1 authorized", [engine, hub, tether])

            time.sleep(args.window)

            hub_text = hub_log.read_text(errors="replace")
            tether_text = tether_log.read_text(errors="replace")
            authorizations = hub_text.count("authorized from")
            resets = hub_text.count("reset without closing handshake")
            refusals = hub_text.count("refused tether")
            dial_failures = tether_text.count("dial failed")
            tunnel_errors = tether_text.count("tunnel error")
            alive = all(process.poll() is None for process, _ in processes)

            steady = (
                authorizations == 1
                and resets == 0
                and refusals == 0
                and dial_failures == 0
                and tunnel_errors == 0
                and alive
            )
            print(f"Idle observation: {args.window}s with no caller requests")
            print(f"  hub authorizations   : {authorizations}")
            print(f"  WebSocket resets     : {resets}")
            print(f"  authorization refusals: {refusals}")
            print(f"  tether dial failures : {dial_failures}")
            print(f"  tether tunnel errors : {tunnel_errors}")
            print(f"  all processes alive  : {alive}")
            if steady:
                print("RESULT: PASSED — one authorization and no idle churn.")
                return 0
            print("RESULT: FAILED — the idle connection changed or a process exited.")
            return 1
    except (OSError, RuntimeError, TimeoutError) as exc:
        print(f"RESULT: INVALID — {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        for process, _ in processes:
            if process.poll() is None:
                process.kill()
        for process, log_file in processes:
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            log_file.close()


if __name__ == "__main__":
    sys.exit(main())

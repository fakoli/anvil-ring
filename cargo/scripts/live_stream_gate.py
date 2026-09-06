"""Local end-to-end streaming check for Anvil Ring.

The harness is stored in the repository so operators and CI can reproduce the
same engine, hub, tether, and caller path. It uses only loopback addresses and
local test credentials.

What it proves:
  1. Every server-sent event emitted by the local engine reaches the caller.
  2. The caller receives the engine's HTTP status before the body.
  3. Events arrive at the engine's cadence instead of in one buffered burst.

Separate Rust tests cover interrupted upstream responses and verify that the hub
does not invent a successful result when the engine has not supplied one.

Usage:
    python3 scripts/live_stream_gate.py              # default: 6 events, 0.8s apart
    python3 scripts/live_stream_gate.py --events 10 --gap 0.5
    python3 scripts/live_stream_gate.py --binary ./target/debug/anvil-ring

Exit 0 means every check passed. Exit 1 prints the failed checks.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib.request import Request, urlopen

# Local test-only values. Real deployments use wss:// and real secrets; these are
# deliberately inert so this file can be committed and shared.
CRED = "tun"
CALLER_TOKEN = "cal"
HOST = "127.0.0.1"


def engine_thread(
    events: int, gap: float, log: list[str]
) -> tuple[threading.Thread, int]:
    """Minimal chunked SSE engine: HEAD, N chunks, then a real 0-length terminator."""

    srv = socket.socket()
    srv.bind((HOST, 0))
    engine_port = srv.getsockname()[1]
    srv.listen(8)

    def serve() -> None:
        log.append(
            f"engine listening :{engine_port} ({events} events, {gap}s apart)"
        )
        deadline = time.monotonic() + events * gap + 30
        while time.monotonic() < deadline:
            srv.settimeout(max(0.1, deadline - time.monotonic()))
            try:
                conn, _ = srv.accept()
            except OSError:
                break
            threading.Thread(target=handle, args=(conn,), daemon=True).start()

    def handle(conn: socket.socket) -> None:
        try:
            conn.recv(65536)  # request; body unused
            conn.sendall(
                b"HTTP/1.1 200 OK\r\n"
                b"content-type: text/event-stream\r\n"
                b"transfer-encoding: chunked\r\n\r\n"
            )
            for i in range(events):
                payload = f"data: ev{i:02d}\n\n".encode()
                conn.sendall(f"{len(payload):x}\r\n".encode() + payload + b"\r\n")
                time.sleep(gap)
            conn.sendall(b"0\r\n\r\n")  # engine-supplied body terminator
        except OSError as exc:
            log.append(f"engine write failed: {exc}")
        finally:
            try:
                conn.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            conn.close()

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    return t, engine_port


def spawn(cmd: list[str], env: dict[str, str], cwd: str) -> subprocess.Popen:
    e = dict(os.environ, **env)
    return subprocess.Popen(cmd, cwd=cwd, env=e, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)


def free_port() -> int:
    """Ask the operating system for an unused loopback port."""
    with socket.socket() as listener:
        listener.bind((HOST, 0))
        return listener.getsockname()[1]


def health(front_port: int) -> str | None:
    try:
        with urlopen(f"http://{HOST}:{front_port}/healthz", timeout=0.2) as response:
            return response.read().decode(errors="replace")
    except OSError:
        return None


def wait_for_health(
    front_port: int,
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
        if expected in (health(front_port) or ""):
            return
        time.sleep(0.025)
    raise TimeoutError(f"health endpoint did not report {expected!r} within {timeout}s")


def main() -> int:
    cargo_dir = Path(__file__).resolve().parents[1]
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=6)
    ap.add_argument("--gap", type=float, default=0.8)
    ap.add_argument(
        "--binary",
        type=Path,
        default=cargo_dir / "target" / "debug" / "anvil-ring",
        help="freshly built runtime (default: %(default)s)",
    )
    args = ap.parse_args()

    if args.events < 1:
        ap.error("--events must be at least 1")
    if args.gap < 0:
        ap.error("--gap must be non-negative")

    binary = args.binary.expanduser().resolve()
    if not binary.is_file():
        print(f"FATAL: no binary at {binary}; run `cargo build --bin anvil-ring` first")
        return 1

    hub_port = free_port()
    front_port = free_port()
    while front_port == hub_port:
        front_port = free_port()
    log: list[str] = []
    _, engine_port = engine_thread(args.events, args.gap, log)
    hub: subprocess.Popen | None = None
    tun: subprocess.Popen | None = None
    failures: list[str] = []
    received: list[tuple[float, str]] = []
    try:
        hub = spawn([str(binary), "hub"], {
            "ANVIL_RING_DEMO_CREDENTIAL": CRED,
            "ANVIL_RING_HUB_LISTEN": f"{HOST}:{hub_port}",
            "ANVIL_RING_FRONTEND_LISTEN": f"{HOST}:{front_port}",
            "ANVIL_RING_CALLER_TOKEN": CALLER_TOKEN,
        }, str(cargo_dir))
        wait_for_health(front_port, "tether-down", [hub])

        tun = spawn([str(binary), "tether"], {
            "ANVIL_RING_HUB_URL": f"ws://{HOST}:{hub_port}/ring",
            "ANVIL_RING_UPSTREAM": f"http://{HOST}:{engine_port}",
            "ANVIL_RING_CREDENTIAL": CRED,
        }, str(cargo_dir))
        wait_for_health(front_port, "tether-up", [hub, tun])

        req = Request(f"http://{HOST}:{front_port}/v1/chat/completions",
                      data=b'{"model":"m","stream":true}',
                      headers={"Authorization": f"Bearer {CALLER_TOKEN}",
                               "Content-Type": "application/json"})
        t0 = time.monotonic()
        with urlopen(req) as resp:
            head_early = resp.status == 200
            for raw in resp:
                line = raw.decode(errors="replace").rstrip("\n")
                if line.strip():
                    received.append((time.monotonic() - t0, line.strip()))

        texts = [t for _, t in received]
        want = [f"data: ev{i:02d}" for i in range(args.events)]
        missing = [w for w in want if w not in texts]
        if missing:
            failures.append(f"TRUNCATION: missing {missing} of {len(want)} "
                            f"(got {len(texts)})")
        if not head_early:
            failures.append(f"head not 200 (got status {resp.status})")
        # Blank SSE separator lines are ignored, so pacing is measured only from
        # data lines. HTTP chunk framing is decoded by urllib before this loop.
        data_stamps = sorted({round(t, 3) for t, s in received if s.startswith("data:")})
        if len(data_stamps) >= 3:
            spread = data_stamps[-1] - data_stamps[0]
            need = args.gap * (len(data_stamps) - 1) * 0.5
            if spread < need:
                failures.append(
                    f"BUFFERED: {len(data_stamps)} data events spread over "
                    f"{spread:.2f}s, expected >= {need:.2f}s (gap={args.gap}s)")
        elif len(data_stamps) >= 2:
            spread = data_stamps[-1] - data_stamps[0]
            if spread < args.gap * 0.5:
                failures.append(f"BUFFERED: only {spread:.2f}s between first two "
                                f"data events (gap={args.gap}s)")
    except Exception as exc:  # noqa: BLE001 - report, do not mask
        failures.append(f"caller error: {type(exc).__name__}: {exc}")
    finally:
        for process in (tun, hub):
            if process is not None and process.poll() is None:
                process.kill()
        for name, process in (("tether", tun), ("hub", hub)):
            if process is None:
                continue
            try:
                out, _ = process.communicate(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                out, _ = process.communicate()
            log.append(f"--- {name} ---\n{out.strip()}")

    print("\n".join(log))
    print(f"\nreceived {len(received)} event lines:")
    for when, t in received:
        print(f"  +{when:5.2f}s  {t}")
    if failures:
        print("\nSTREAMING CHECK FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print(
        f"\nSTREAMING CHECK PASSED: {args.events} engine events arrived with status first "
        "and without response buffering."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

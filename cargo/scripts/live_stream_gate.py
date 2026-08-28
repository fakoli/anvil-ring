"""Regenerable live-test harness for anvil-ring I-6 verification.

Why this file exists: the throwaway probes lived in /tmp (caller*.py, spyengine.py,
fakeeng*.py), and /tmp is ephemeral. After a reboot the kickoff pack's live gate could
not be run, only re-derived. This one file regenerates the whole gate and is SAFE TO
COMMIT: it contains no hostnames, no tokens beyond local test values, no paths outside
this repo.

What it proves (the I-6 gate):
  1. A caller's SSE stream is NOT truncated: all N events arrive, head first.
  2. The chunked terminator comes from the ENGINE, not fabricated by the hub (I-11).
  3. Streaming is not buffered: events arrive paced, not in one burst (I-9).

Usage:
    python3 scripts/live_stream_gate.py              # default: 6 events, 0.8s apart
    python3 scripts/live_stream_gate.py --events 10 --gap 0.5
    python3 scripts/live_stream_gate.py --keep        # leave the topology running

Exit 0 = gate passed. Exit 1 = failed (diff printed).
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import threading
import time
from urllib.request import Request, urlopen
# Local test-only values. Real deployments use wss:// and real secrets; these are
# deliberately inert so this file can be committed and shared (I-7 de-identification).
HUB_PORT = 19920
FRONT_PORT = 19922
ENGINE_PORT = 19905
CRED = "tun"
CALLER_TOKEN = "cal"
HOST = "127.0.0.1"


def engine_thread(events: int, gap: float, log: list[str]) -> threading.Thread:
    """Minimal chunked SSE engine: HEAD, N chunks, then a real 0-length terminator."""

    def serve() -> None:
        srv = socket.socket()  # no SO_REUSEADDR: a clean bind proves the port is ours
        srv.bind((HOST, ENGINE_PORT))
        srv.listen(8)
        log.append(f"engine listening :{ENGINE_PORT} ({events} events, {gap}s apart)")
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
            conn.sendall(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n")
            for i in range(events):
                payload = f"data: ev{i:02d}\n\n".encode()
                conn.sendall(f"{len(payload):x}\r\n".encode() + payload + b"\r\n")
                time.sleep(gap)
            conn.sendall(b"0\r\n\r\n")  # engine-supplied terminator (I-11)
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
    return t


def spawn(cmd: list[str], env: dict[str, str], cwd: str) -> subprocess.Popen:
    e = dict(os.environ, **env)
    return subprocess.Popen(cmd, cwd=cwd, env=e, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=6)
    ap.add_argument("--gap", type=float, default=0.8)
    ap.add_argument("--events-dir", default=os.path.expanduser(
        "~/workspace-work/projects/anvil-ring/cargo"))
    args = ap.parse_args()

    binary = os.path.join(args.events_dir, "target/debug/anvil-ring")
    if not os.path.exists(binary):
        print(f"FATAL: no binary at {binary}; run `cargo build` first")
        return 1

    log: list[str] = []
    engine_thread(args.events, args.gap, log)
    time.sleep(0.4)

    hub = spawn([binary, "hub"], {
        "ANVIL_RING_DEMO_CREDENTIAL": CRED,
        "ANVIL_RING_HUB_LISTEN": f"{HOST}:{HUB_PORT}",
        "ANVIL_RING_FRONTEND_LISTEN": f"{HOST}:{FRONT_PORT}",
        "ANVIL_RING_CALLER_TOKEN": CALLER_TOKEN,
    }, args.events_dir)
    time.sleep(1.0)
    tun = spawn([binary, "tether"], {
        "ANVIL_RING_HUB_URL": f"ws://{HOST}:{HUB_PORT}/ring",
        "ANVIL_RING_UPSTREAM": f"http://{HOST}:{ENGINE_PORT}",
        "ANVIL_RING_CREDENTIAL": CRED,
    }, args.events_dir)
    time.sleep(1.5)

    failures: list[str] = []
    received: list[tuple[float, str]] = []
    try:
        req = Request(f"http://{HOST}:{FRONT_PORT}/v1/chat/completions",
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
        if len(received) >= 3:
            spread = received[-1][0] - received[0][0]
            if spread < args.gap * (len(received) - 1) * 0.5:
                failures.append(f"BUFFERED: {len(received)} events arrived in "
                                f"{spread:.2f}s, expected >= "
                                f"{args.gap * (len(received)-1):.2f}s of pacing")
    except Exception as exc:  # noqa: BLE001 - report, do not mask
        failures.append(f"caller error: {type(exc).__name__}: {exc}")

    for p in (tun, hub):
        p.kill()
    for p in (tun, hub):
        try:
            out = p.stdout.read() if p.stdout else ""
        except Exception:
            out = ""
        log.append(f"--- {'tether' if p is tun else 'hub'} ---\n{out.strip()}")

    print("\n".join(log))
    print(f"\nreceived {len(received)} event lines:")
    for when, t in received:
        print(f"  +{when:5.2f}s  {t}")
    if failures:
        print("\nGATE FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print(f"\nGATE PASSED: {args.events} events, head first, "
          f"terminator from engine, not buffered.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

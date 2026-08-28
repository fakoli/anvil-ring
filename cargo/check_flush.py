#!/usr/bin/env python3
"""Measure whether anvil-ring flushes SSE incrementally or buffers (I-9).

Deliberately independent of the Rust test suite: this is the check to trust when
you do not believe your own test harness. The upstream emits one chunk every GAP
seconds, so a flushing proxy yields arrivals spread across that window while a
buffering proxy delivers one burst at the end.

Run:  RING_BIN=./target/debug/anvil-ring python3 check_flush.py
"""
import os
import socket
import subprocess
import threading
import time

ENGINE_PORT = 18511
PROXY_PORT = 18512
TOKEN = "sekret"
GAP = 0.4
CHUNKS = ["data: one\n\n", "data: two\n\n", "data: three\n\n", "data: [DONE]\n\n"]


def engine():
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", ENGINE_PORT))
    srv.listen(8)
    while True:
        conn, _ = srv.accept()

        def handle(c=conn):
            try:
                buf = b""
                while b"\r\n\r\n" not in buf:
                    d = c.recv(4096)
                    if not d:
                        break
                    buf += d
                c.sendall(
                    b"HTTP/1.1 200 OK\r\n"
                    b"content-type: text/event-stream\r\n"
                    b"transfer-encoding: chunked\r\n\r\n"
                )
                for ch in CHUNKS:
                    c.sendall(b"%x\r\n%s\r\n" % (len(ch), ch.encode()))
                    time.sleep(GAP)
                c.sendall(b"0\r\n\r\n")
            except Exception as exc:  # noqa: BLE001
                print("engine err", repr(exc))
            finally:
                c.close()

        threading.Thread(target=handle, daemon=True).start()


threading.Thread(target=engine, daemon=True).start()
time.sleep(0.5)

env = dict(os.environ)
env.update(
    {
        "ANVIL_RING_LISTEN": "127.0.0.1:%d" % PROXY_PORT,
        "ANVIL_RING_UPSTREAM": "http://127.0.0.1:%d" % ENGINE_PORT,
        "ANVIL_RING_TOKEN": TOKEN,
    }
)
BIN = os.environ.get("RING_BIN", "./target/debug/anvil-ring")
proxy = subprocess.Popen([BIN, "proxy"], env=env, stdout=subprocess.DEVNULL)

for _ in range(120):
    try:
        socket.create_connection(("127.0.0.1", PROXY_PORT), 0.1).close()
        break
    except OSError:
        time.sleep(0.05)

path = "/v1/chat/completions"
body = b"{}"
req = (
    "POST %s HTTP/1.1\r\n"
    "Host: x\r\n"
    "authorization: Bearer %s\r\n"
    "content-type: application/json\r\n"
    "content-length: %d\r\n"
    "\r\n" % (path, TOKEN, len(body))
).encode() + body

s = socket.create_connection(("127.0.0.1", PROXY_PORT), 10)
s.settimeout(8)
t0 = time.monotonic()
s.sendall(req)

arrivals = []
try:
    while True:
        d = s.recv(4096)
        if not d:
            break
        arrivals.append((time.monotonic() - t0, d))
except socket.timeout:
    pass
s.close()

print("upstream emission span: %.2fs" % (GAP * len(CHUNKS)))
print("tcp reads observed: %d" % len(arrivals))
for at, d in arrivals:
    print("  t+%5.2fs  %4dB  %r" % (at, len(d), d[:56]))

payload = b"".join(d for _, d in arrivals)
missing = [c for c in CHUNKS if c.replace("data: ", "").strip().encode() not in payload]
if missing:
    print("FAIL: missing chunks %r" % missing)
    proxy.terminate()
    raise SystemExit(1)

if len(arrivals) < 2:
    print("FAIL: single TCP read -> proxy BUFFERED the stream (I-9 violation)")
    proxy.terminate()
    raise SystemExit(1)

spread = arrivals[-1][0] - arrivals[0][0]
ok = spread >= GAP * 0.5
print("arrival spread: %.2fs -> %s" % (spread, "PASS (flushing)" if ok else "FAIL (buffered)"))
proxy.terminate()
raise SystemExit(0 if ok else 1)

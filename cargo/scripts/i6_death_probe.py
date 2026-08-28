"""I-6 step-1 probe — MEASUREMENT ONLY. Changes no product code.

Question (STATE.md step 1): when a tether process dies mid-stream, (a) does the HUB
observe the death, and (b) is the CALLER's stream ended?

Raw sockets on purpose: urllib buffers and would hide the exact timing measured here.
Topology: engine(19915) <- tether <- hub(19930) ; caller -> hub frontend(19932).

Reaping is deliberate. An earlier version of this probe died with an exception in its
cleanup and left the HUB listening on 19930/19932; the next run then received
`HTTP/1.1 502 Bad Gateway` at +0.00s -- a stale hub with no tether registered, which
looks exactly like a product defect and is purely an orphan of the previous run. So:
refuse to start unless the ports are free, and always reap in `finally`.

Run:  python3 i6_death_probe.py <abs path to anvil-ring binary> [runs]
"""
import os, signal, socket, subprocess, sys, threading, time

H, F, E = 19930, 19932, 19915
HOST, CRED, TOK = "127.0.0.1", "tun", "cal"
T_OBSERVE = 20.0          # watch the caller this long after the kill
STREAM_GAP = 0.25         # engine pacing


def port_free(p):
    s = socket.socket()
    try:
        s.bind((HOST, p))
        return True
    except OSError:
        return False
    finally:
        s.close()


def engine():
    srv = socket.socket(); srv.bind((HOST, E)); srv.listen(8)

    def handle(c):
        try:
            c.recv(65536)
            c.sendall(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n")
            for i in range(400):
                p = f"data: ev{i:03d}\n\n".encode()
                c.sendall(f"{len(p):x}\r\n".encode() + p + b"\r\n")
                time.sleep(STREAM_GAP)
        except OSError:
            pass
        finally:
            try:
                c.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            c.close()

    def serve():
        while True:
            try:
                c, _ = srv.accept()
            except OSError:
                return
            threading.Thread(target=handle, args=(c,), daemon=True).start()
    threading.Thread(target=serve, daemon=True).start()
    return srv


def spawn(args, env):
    p = subprocess.Popen([BIN] + args, env=dict(os.environ, **env),
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         bufsize=1, text=True)
    lines = []
    threading.Thread(target=lambda: [lines.append(l.rstrip()) for l in p.stdout],
                     daemon=True).start()
    return p, lines


def caller():
    s = socket.create_connection((HOST, F), timeout=T_OBSERVE + 5)
    body = b'{"model":"m","stream":true}'
    preq = (f"POST /v1/chat/completions HTTP/1.1\r\nhost: x\r\n"
            f"authorization: Bearer {TOK}\r\ncontent-type: application/json\r\n"
            f"content-length: {len(body)}\r\n\r\n").encode()
    s.sendall(preq + body)
    return s


def one_run():
    """Returns a dict of measured facts. Never raises for an expected failure mode."""
    out = {"ok": False}
    hub = tun = srv = None
    hub_log = []
    try:
        srv = engine()
        time.sleep(0.3)
        hub, hub_log = spawn(["hub"], {
            "ANVIL_RING_DEMO_CREDENTIAL": CRED,
            "ANVIL_RING_HUB_LISTEN": f"{HOST}:{H}",
            "ANVIL_RING_FRONTEND_LISTEN": f"{HOST}:{F}",
            "ANVIL_RING_CALLER_TOKEN": TOK})
        time.sleep(1.2)
        tun, _ = spawn(["tether"], {
            "ANVIL_RING_HUB_URL": f"ws://{HOST}:{H}/ring",
            "ANVIL_RING_UPSTREAM": f"http://{HOST}:{E}",
            "ANVIL_RING_CREDENTIAL": CRED})
        time.sleep(1.5)

        sock = caller()
        t0 = time.monotonic()
        head = b""
        while b"\r\n\r\n" not in head:
            b_ = sock.recv(1)
            if not b_:
                break
            head += b_
        out["head"] = head.split(b"\r\n", 1)[0].decode(errors="replace")
        # A hub with no registered tether answers 502 immediately. That is an orphan
        # from a previous run, not a measurement -- flag it and discard the sample.
        out["stale"] = "502" in out["head"] and (time.monotonic() - t0) < 0.25
        if out["stale"]:
            return out

        tkill, reads = None, 0
        while tkill is None:
            try:
                chunk = sock.recv(4096)
            except OSError:
                break
            if not chunk:
                break
            reads += 1
            if time.monotonic() - t0 > 2.0:
                os.kill(tun.pid, signal.SIGKILL)
                tkill = time.monotonic()
                out["killed_after_reads"] = reads

        after, closed_at, why = 0, None, ""
        while tkill and time.monotonic() - tkill < T_OBSERVE:
            try:
                chunk = sock.recv(4096)
            except OSError as exc:
                closed_at, why = time.monotonic() - tkill, type(exc).__name__
                break
            if not chunk:
                closed_at, why = time.monotonic() - tkill, "EOF"
                break
            after += len(chunk)
        out["bytes_after_kill"] = after
        # A socket timeout means the read was STILL BLOCKED when our own ceiling
        # expired -- that is "the stream stayed open", the opposite of termination.
        # An earlier version matched only "Timeout" (capital T, the socket module's
        # spelling) while this path yields lowercase "timeout", so a wedged caller was
        # reported as TERMINATED and the probe contradicted its own reason field.
        # Decide on the reason, and make the two fields impossible to disagree:
        timed_out = "timeout" in why.lower()
        out["caller_terminated"] = bool(closed_at) and not timed_out
        out["stayed_open"] = timed_out or closed_at is None
        out["closed_at"] = closed_at
        out["close_reason"] = why or "never (stream still open at T_OBSERVE)"

        # "ended" is the hub's own emission; match it narrowly so a stray word in an
        # unrelated line cannot produce a false positive.
        out["hub_observed"] = [l for l in hub_log if "tether" in l and "ended:" in l]
        out["ok"] = True
        return out
    finally:
        # Order matters: kill the hub/tether BEFORE dropping engine sockets so nothing
        # survives to poison the next run.
        for proc in (tun, hub):
            if proc is not None:
                try:
                    proc.kill()
                except Exception:
                    pass
        if srv is not None:
            try:
                srv.close()
            except Exception:
                pass


if __name__ == "__main__":
    BIN = sys.argv[1] if len(sys.argv) > 1 else ""
    RUNS = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    if not os.path.exists(BIN):
        sys.exit(f"no binary at {BIN!r}; run `cargo build` and pass its absolute path")
    busy = [p for p in (H, F, E) if not port_free(p)]
    if busy:
        sys.exit(f"ports {busy} busy -- an orphan from a previous run is holding them. "
                 f"Clear it (`pkill -f target/debug/anvil-ring`) and re-run; measuring "
                 f"against a stale hub produces a confident wrong answer.")

    good = []
    for i in range(RUNS):
        r = one_run()
        if not r["ok"]:
            print(f"[run {i+1}] probe error (see traceback above)")
            continue
        if r["stale"]:
            print(f"[run {i+1}] DISCARDED -- instant 502, an orphaned hub answered")
            continue
        good.append(r)
        print(f"[run {i+1}] head={r['head']!r}")
        print(f"          bytes after kill = {r['bytes_after_kill']}")
        print(f"          caller terminated = {r['caller_terminated']} "
              f"({r['close_reason']})   stayed_open = {r['stayed_open']}")
        print(f"          hub logged tether end = {len(r['hub_observed'])}")
        time.sleep(1.0)

    if good:
        hub_yes = sum(1 for r in good if r["hub_observed"])
        cal_yes = sum(1 for r in good if r["caller_terminated"])
        # Self-check: no sample may claim termination AND a timeout reason.
        assert not any(r["caller_terminated"] and r["stayed_open"] for r in good), \
            "probe bug: a sample claims termination and a stayed-open reason"
        open_yes = sum(1 for r in good if r["stayed_open"])
        print(f"\nVERDICT over {len(good)} valid run(s): hub observed death in "
              f"{hub_yes}/{len(good)}; caller terminated in {cal_yes}/{len(good)}")
        print("I-6 (caller never terminated) is CONFIRMED only if caller "
              "terminated = 0 in every valid run.")
    else:
        print("\nno valid samples -- nothing concluded")

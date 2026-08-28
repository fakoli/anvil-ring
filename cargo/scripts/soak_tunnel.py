"""Soak: hold one tunnel open, leave it alone, count re-authorizations.

Why this exists: a field log showed repeated `authorized -> Up -> reset ->
authorized` cycles for ONE tether, and I had claimed twice that such resets were
only ever teardown noise. This measures it honestly: hub + tether run with nothing
killed and no requests sent, so any churn is NOT teardown.

Harness traps found the hard way (all three produced a bogus 'unauthorized' run):
  * The hub registers its one demo tether from ANVIL_RING_DEMO_CREDENTIAL. The
    tether must dial with EXACTLY that value in ANVIL_RING_CREDENTIAL. Mismatching
    them yields `refused tether` + `ended: unauthorized`, which is I-5 working --
    the hub never reveals which half was wrong, so a mismatch looks like a protocol
    bug. A PONG (0x08) arrives for HELLO because the hub replies to the stream with
    its keepalive while refusing the tunnel.
  * A leftover server can answer on the port; bind the fixture WITHOUT SO_REUSEADDR
    so a successful bind proves the port was free.
  * Verify the log names the expected listening socket before trusting any count.
"""
import os
import subprocess
import sys
import time

WINDOW = int(sys.argv[1]) if len(sys.argv) > 1 else 100
BIN = os.path.abspath(sys.argv[2] if len(sys.argv) > 2 else "NOT_SET")

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HUB_PORT, FRONT_PORT, UP_PORT = 19350, 19352, 19351
# ONE value used on BOTH sides -- this is the whole point.
CRED = "soak-cred-..." + str(int(time.time()))
ENV = dict(os.environ, TZ="UTC",
           PATH="/opt/homebrew/bin:" + os.environ.get("PATH", ""))

if not os.path.isfile(BIN):
    sys.exit("pass the absolute path to the freshly built anvil-ring binary")

procs = []


def spawn(cmd, env, path):
    f = open(path, "w")
    procs.append((subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT,
                                   cwd=REPO, env=env), f))


ENG_SNIPPET = (
    "import socket,threading\n"
    "s=socket.socket()\n"
    f"s.bind(('127.0.0.1',{UP_PORT}));s.listen(8)\n"
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
    spawn([sys.executable, "-c", ENG_SNIPPET], ENV, "/tmp/soak_eng.log")
    time.sleep(0.8)
    spawn([BIN, "hub"],
          dict(ENV, ANVIL_RING_DEMO_CREDENTIAL=CRED,
               ANVIL_RING_HUB_LISTEN=f"127.0.0.1:{HUB_PORT}",
               ANVIL_RING_FRONTEND_LISTEN=f"127.0.0.1:{FRONT_PORT}",
               ANVIL_RING_CALLER_TOKEN="***"),
          "/tmp/soak_hub.log")
    time.sleep(1.5)
    spawn([BIN, "tether"],
          dict(ENV, ANVIL_RING_HUB_URL=f"ws://127.0.0.1:{HUB_PORT}/ring",
               ANVIL_RING_UPSTREAM=f"http://127.0.0.1:{UP_PORT}",
               ANVIL_RING_CREDENTIAL=CRED),
          "/tmp/soak_tun.log")

    time.sleep(WINDOW)

    hub_txt = open("/tmp/soak_hub.log").read()
    tun_txt = open("/tmp/soak_tun.log").read()
    hub_ok = f"hub on 127.0.0.1:{HUB_PORT}" in hub_txt
    tun_up = "tunnel #1 authorized" in tun_txt
    auth = hub_txt.count("authorized from")
    resets = hub_txt.count("reset without closing handshake")
    refused = hub_txt.count("refused tether")
    dials = tun_txt.count("dial failed")
    terrs = tun_txt.count("tunnel error")
    alive = all(p.poll() is None for p, _ in procs)

    print(f"SOAK {WINDOW}s, nothing killed, no requests sent")
    print(f"  credential shared by both sides : {CRED}")
    print(f"  expected hub listening in log   : {hub_ok}")
    print(f"  tunnel reached Up               : {tun_up}")
    print(f"  hub authorizations              : {auth}")
    print(f"  hub resets                      : {resets}")
    print(f"  hub refusals                    : {refused}")
    print(f"  tether dial failures            : {dials}")
    print(f"  tether tunnel errors            : {terrs}")
    print(f"  all processes alive             : {alive}")
    if not (hub_ok and tun_up):
        print("  VERDICT: INVALID RUN -- hub or tunnel never came up; do not "
              "interpret the counts.")
    elif auth == 1 and resets == 0 and dials == 0 and terrs == 0 and refused == 0 and alive:
        print("  VERDICT: STEADY STATE -- one authorization, zero resets. The link "
              "does not flap on its own; field resets were teardown.")
    else:
        print("  VERDICT: REAL CHURN -- not teardown, since nothing was killed. "
              "Investigate.")
finally:
    for p, f in procs:
        try:
            p.kill()
        except Exception:
            pass
        f.close()

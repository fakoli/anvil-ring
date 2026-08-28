"""Provider egress probe — evidence for ADR-0002 (transport choice).

Run this ON A REAL RENTAL HOST, unprivileged, inside the container you would
actually deploy. It answers one question with recorded facts: **which outbound
ports does this provider permit?** That answer decides chisel (WSS/443) vs.
`ssh -R` (22) vs. writing our own tunnel.

Design constraints, and why:

* **Zero secrets, zero dependencies, no config.** It must be copy-pastable onto a
  machine you have not configured and will not keep. stdlib only (I-7).
* **Outbound only.** It opens no listening socket, so it cannot itself violate I-1
  while it investigates the network.
* **Writes findings to a file, prints a table.** The ADR should cite recorded
  output, not this operator's recollection six weeks from now.
* **TLS probes verify the handshake, not just TCP connect.** A port that accepts
  SYN but MITMs or drops TLS is useless for a WSS tunnel, and that distinction is
  exactly what the transport decision turns on.

It deliberately does NOT probe the hub or send any identifier. It is a port
tester, not a registration client.
"""

from __future__ import annotations

import argparse
import json
import platform
import socket
import ssl
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

# Targets are public, anonymous infrastructure chosen so that a *successful*
# connect proves egress is permitted. Each entry may be overridden with --target
# to test a provider-specific relay or the hub itself.
DEFAULT_TARGETS: list[tuple[str, int, str]] = [
    ("api.github.com", 443, "tls"),
    ("registry.npmjs.org", 443, "tls"),
    ("github.com", 22, "ssh-banner"),
    ("1.1.1.1", 443, "tls"),
    ("1.1.1.1", 53, "tcp"),
]

TCP_TIMEOUT = 8.0
TLS_TIMEOUT = 8.0


@dataclass
class Probe:
    target: str
    port: int
    mode: str
    tcp: bool = False
    tls: bool = False
    detail: str = ""
    ms: float = 0.0

    @property
    def verdict(self) -> str:
        if not self.tcp:
            return "BLOCKED"
        if self.mode == "tls" and not self.tls:
            return "TCP-ONLY"  # SYN accepted, TLS failed -> useless for WSS tunnel
        if self.mode == "ssh-banner" and "SSH" not in self.detail.upper():
            return "TCP-ONLY"
        return "OPEN"


def _probe_tcp(target: str, port: int, mode: str) -> Probe:
    p = Probe(target=target, port=port, mode=mode)
    start = time.monotonic()
    try:
        # getaddrinfo first: a DNS failure is a *different* finding than a
        # firewalled port, and conflating them has misled people before.
        try:
            socket.getaddrinfo(target, port, type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            p.detail = f"DNS FAILED: {exc}"
            p.ms = (time.monotonic() - start) * 1000
            return p

        with socket.create_connection((target, port), timeout=TCP_TIMEOUT) as sock:
            p.tcp = True
            p.ms = (time.monotonic() - start) * 1000

            if mode == "ssh-banner":
                sock.settimeout(TLS_TIMEOUT)
                try:
                    banner = sock.recv(64).decode(errors="replace").strip()
                    p.detail = banner[:48]
                except (TimeoutError, OSError) as exc:
                    p.detail = f"no banner: {type(exc).__name__}"
            elif mode == "tls":
                ctx = ssl.create_default_context()
                try:
                    with ctx.wrap_socket(sock, server_hostname=target) as tls:
                        p.tls = True
                        p.detail = f"TLS {tls.version().upper()}"
                except ssl.SSLError as exc:
                    p.detail = f"TLS FAILED: {exc.strerror or exc}"[:70]
                except ssl.SSLEOFError:
                    p.detail = "TLS FAILED: clean close (port likely intercepted)"
                except OSError as exc:
                    p.detail = f"TLS FAILED: {type(exc).__name__}"
    except TimeoutError:
        p.detail = "timed out (dropped)"
        p.ms = (time.monotonic() - start) * 1000
    except OSError as exc:
        p.detail = f"refused/reset: {type(exc).__name__}"
        p.ms = (time.monotonic() - start) * 1000
    return p


def _usable(p: Probe) -> bool:
    """Whether a probe proves the transport that port would carry actually WORKS.

    Separate from Probe.verdict because `tls` is only populated in tls mode: a
    port's mode and its success signal are different axes, and conflating them
    made the ssh-banner rows read as failures when they returned a real banner.
    """
    if not p.tcp:
        return False
    if p.mode == "tls":
        return p.tls
    if p.mode == "ssh-banner":
        return "SSH" in p.detail.upper()
    return True


def _transport_hint(probes: list[Probe]) -> str:
    """Turn raw ports into the ADR-0002 decision, stated conservatively."""
    open_443 = any(p.port == 443 and p.tcp for p in probes)
    tls_443 = any(p.port == 443 and p.mode == "tls" and _usable(p) for p in probes)
    ssh_22 = any(p.port == 22 and _usable(p) for p in probes)

    if tls_443 and ssh_22:
        return "Both viable. chisel-over-WSS(443) preferred: uniform, survives tightening."
    if tls_443 and not ssh_22:
        return "443-only egress. This RULES OUT ssh -R -> ADR-0002 resolves to chisel or own WSS tunnel."
    if open_443 and not tls_443:
        return "443 accepts TCP but TLS fails -> MITM/proxy likely. A WSS tunnel may still work THROUGH the proxy; needs -v test."
    return "No usable 443 or 22. This host cannot host a tether at all -> provider is unusable, no transport fixes that."


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="anvil-ring-probe-egress",
        description="Record which outbound ports a rental host permits (ADR-0002 evidence).",
    )
    ap.add_argument("--out", default="egress-probe.json", help="write findings here")
    ap.add_argument(
        "--target",
        action="append",
        default=[],
        metavar="HOST:PORT[:MODE]",
        help="extra probe target; MODE in tls|tcp|ssh-banner (default tcp). Repeatable.",
    )
    ap.add_argument(
        "--include-hostname",
        action="store_true",
        help="record the local hostname in the output JSON. OFF by default: this "
        "file is meant to be attached to an ADR and possibly published, and "
        "rental hostnames are often operator-chosen. Only your local copy keeps it.",
    )
    # Deliberately NO token argument: this probe authenticates to nothing (I-8).
    args = ap.parse_args(argv)

    targets = list(DEFAULT_TARGETS)
    for spec in args.target:
        parts = spec.split(":")
        if len(parts) < 2 or not parts[1].isdigit():
            sys.stderr.write(f"bad --target {spec!r}, expected HOST:PORT[:MODE]\n")
            return 2
        host, port = parts[0], int(parts[1])
        mode = parts[2] if len(parts) > 2 else "tcp"
        targets.append((host, port, mode))

    print(f"anvil-ring egress probe :: {platform.node()} :: {platform.system()} "
          f"{platform.release()} :: {datetime.now(UTC).isoformat()}")
    print(f"{'TARGET':<24} {'PORT':>5} {'MODE':<11} {'VERDICT':<9} {'MS':>7}  DETAIL")
    print("-" * 92)

    probes = [_probe_tcp(h, p, m) for h, p, m in targets]
    for p in probes:
        print(f"{p.target:<24} {p.port:>5} {p.mode:<11} {p.verdict:<9} {p.ms:>7.0f}  {p.detail}")

    hint = _transport_hint(probes)
    print("-" * 92)
    print(f"ADR-0002 HINT: {hint}")

    out = Path(args.out)
    payload = {
        "probe_version": "1",
        # Redacted by default; see --include-hostname.
        "host": platform.node() if args.include_hostname else "<redacted>",
        "system": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "observed_at": datetime.now(UTC).isoformat(),
        "transport_hint": hint,
        "probes": [asdict(p) | {"verdict": p.verdict, "usable": _usable(p)} for p in probes],
    }
    out.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"\nwrote {out}  -- attach this file to ADR-0002. Hostname is redacted "
          f"unless --include-hostname was passed.")

    # Non-zero if nothing usable, so this is usable in a CI/pre-deploy gate.
    return 0 if any(_usable(p) for p in probes) else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

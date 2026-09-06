"""Provider egress probe — deployment qualification evidence.

Run this on a real rental host, unprivileged, inside the container intended for
deployment. It records whether the environment can complete the outbound TLS
connection required by the shipped Anvil Ring WSS transport.

Design constraints, and why:

* **Zero secrets and zero project dependencies.** It uses only Python's standard
  library so it can run in the final rental image without installing a package.
* **Outbound only.** It opens no listening socket while it checks the provider's
  outbound network access.
* **Writes findings to a file and prints a table.** Provider qualification should
  cite a recorded result rather than operator recollection.
* **TLS probes verify negotiation, not only a TCP connection.** A port that
  accepts a network connection but blocks or replaces TLS cannot carry the
  required secure WebSocket session.

It sends no Anvil Ring identifier or credential. With an explicit `--target`, it
may test the intended hub's public TLS endpoint, but it is never a registration
client.
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
DOCS_URL = "https://fakoli.github.io/anvil-ring/"
DOCS_SOURCE_URL = "https://github.com/fakoli/anvil-ring/tree/main/docs"
PROJECT_URL = "https://github.com/fakoli/anvil-ring"
VALID_MODES = {"tls", "tcp", "ssh-banner"}


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
    """Turn raw ports into a conservative deployment hint."""
    open_443 = any(p.port == 443 and p.tcp for p in probes)
    tls_443 = any(p.port == 443 and p.mode == "tls" and _usable(p) for p in probes)
    tls_any = any(p.mode == "tls" and _usable(p) for p in probes)
    ssh_22 = any(p.port == 22 and _usable(p) for p in probes)

    if tls_443 and ssh_22:
        return (
            "TLS/WSS egress on port 443 is usable for Anvil Ring. "
            "SSH is also reachable but is not required."
        )
    if tls_443:
        return "TLS/WSS egress on port 443 is usable for Anvil Ring."
    if tls_any:
        return (
            "A configured TLS target is usable. Verify that it is the intended "
            "Anvil Ring hub before rollout."
        )
    if open_443 and not tls_443:
        return (
            "TCP port 443 is reachable, but TLS verification failed. The Anvil "
            "Ring WSS path is not qualified; inspect proxy and CA policy."
        )
    if ssh_22:
        return (
            "SSH egress is reachable, but no usable TLS target was observed. "
            "The shipped Anvil Ring WSS transport is not qualified."
        )
    return (
        "No usable TLS target was observed. This environment is not qualified "
        "for a routable Anvil Ring tether."
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="anvil-ring-probe-egress",
        description="Record which outbound ports a rental host permits.",
        epilog=(
            f"Documentation portal: {DOCS_URL}\n"
            f"Documentation source: {DOCS_SOURCE_URL}\n"
            f"Main project portal: {PROJECT_URL}"
        ),
    )
    ap.add_argument("--out", default="egress-probe.json", help="write findings here")
    ap.add_argument(
        "--target",
        action="append",
        default=[],
        metavar="HOST:PORT[:MODE]",
        help="probe target; MODE in tls|tcp|ssh-banner (default tcp). Repeatable.",
    )
    ap.add_argument(
        "--no-defaults",
        action="store_true",
        help="probe only explicitly supplied --target values",
    )
    ap.add_argument(
        "--include-hostname",
        action="store_true",
        help="include the local hostname in console and JSON output. OFF by default.",
    )
    # Deliberately no token argument: this network diagnostic authenticates to
    # nothing and must not accept a deployment secret.
    args = ap.parse_args(argv)

    targets = [] if args.no_defaults else list(DEFAULT_TARGETS)
    for spec in args.target:
        parts = spec.split(":")
        if len(parts) not in {2, 3} or not parts[1].isdigit():
            sys.stderr.write(f"bad --target {spec!r}, expected HOST:PORT[:MODE]\n")
            return 2
        host, port = parts[0], int(parts[1])
        mode = parts[2] if len(parts) > 2 else "tcp"
        if not host or not 1 <= port <= 65535 or mode not in VALID_MODES:
            sys.stderr.write(
                f"bad --target {spec!r}, expected HOST:PORT[:MODE] with "
                "MODE in tls|tcp|ssh-banner\n"
            )
            return 2
        targets.append((host, port, mode))

    if not targets:
        sys.stderr.write("no targets: remove --no-defaults or add --target\n")
        return 2

    shown_host = platform.node() if args.include_hostname else "<redacted>"
    print(f"anvil-ring egress probe :: {shown_host} :: {platform.system()} "
          f"{platform.release()} :: {datetime.now(UTC).isoformat()}")
    print(f"{'TARGET':<24} {'PORT':>5} {'MODE':<11} {'VERDICT':<9} {'MS':>7}  DETAIL")
    print("-" * 92)

    probes = [_probe_tcp(h, p, m) for h, p, m in targets]
    for p in probes:
        print(f"{p.target:<24} {p.port:>5} {p.mode:<11} {p.verdict:<9} {p.ms:>7.0f}  {p.detail}")

    hint = _transport_hint(probes)
    print("-" * 92)
    print(f"TRANSPORT HINT: {hint}")

    out = Path(args.out)
    payload = {
        "probe_version": "2",
        # Redacted by default; see --include-hostname.
        "host": platform.node() if args.include_hostname else "<redacted>",
        "system": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "observed_at": datetime.now(UTC).isoformat(),
        "transport_hint": hint,
        "probes": [asdict(p) | {"verdict": p.verdict, "usable": _usable(p)} for p in probes],
    }
    out.write_text(json.dumps(payload, indent=2) + "\n")
    print(
        f"\nwrote {out}  -- attach this file to the provider qualification record. "
        "Hostname is redacted unless --include-hostname was passed."
    )

    # Raw TCP/SSH reachability is evidence, but only verified TLS qualifies the
    # shipped WSS transport for a continuous-integration or pre-deployment check.
    return 0 if any(p.mode == "tls" and _usable(p) for p in probes) else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

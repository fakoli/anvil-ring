"""anvil-ring CLI skeleton.

The executable is ``anvil-ring`` -- always with the ``anvil-`` prefix. This module
contains argument parsing and honest NotImplemented guards ONLY. There is no
tunnel implementation here yet: the transport is undecided (ADR-0002) and this
project does not ship stubs that look like features.
"""

from __future__ import annotations

import argparse
import os
import sys

PROG = "anvil-ring"

__all__ = ["main"]


def _not_implemented(feature: str) -> int:
    sys.stderr.write(
        f"{PROG}: '{feature}' is not implemented yet -- see STATE.md "
        f"(transport decision ADR-0002 is still open).\n"
    )
    return 2


def cmd_up(args: argparse.Namespace) -> int:
    """Run on the remote host: dial out and tether a local port."""
    # I-8: the token comes from the environment, never argv.
    if not os.environ.get("ANVIL_RING_TOKEN"):
        sys.stderr.write(
            f"{PROG}: ANVIL_RING_TOKEN is not set. Tokens are never accepted as "
            "command-line arguments (they leak via `ps` and shell history on a "
            "shared rental host).\n"
        )
        return 2
    return _not_implemented("up")


def cmd_list(_args: argparse.Namespace) -> int:
    """Run on the hub: list live tethers and their mapped endpoints."""
    return _not_implemented("list")


def cmd_revoke(args: argparse.Namespace) -> int:
    """Run on the hub: revoke a tether's credential (I-3: must take effect fast)."""
    return _not_implemented(f"revoke {args.tether}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog=PROG,
        description=(
            "Outbound-initiated tether exposing a rented host's model-serving "
            "port to the Anvil family."
        ),
    )
    p.add_argument("--version", action="version", version=f"{PROG} 0.1.0-scaffold")
    sub = p.add_subparsers(dest="command", required=True)

    up = sub.add_parser("up", help="on the remote host: dial out, tether a port")
    up.add_argument(
        "--serve",
        default="http://127.0.0.1:8000",
        help="local upstream to expose (default: %(default)s)",
    )
    up.add_argument("--name", default=None, help="tether name on the hub")
    up.set_defaults(func=cmd_up)

    ls = sub.add_parser("list", help="on the hub: list live tethers")
    ls.set_defaults(func=cmd_list)

    rv = sub.add_parser("revoke", help="on the hub: revoke a tether credential")
    rv.add_argument("tether")
    rv.set_defaults(func=cmd_revoke)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

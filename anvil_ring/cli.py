"""Compatibility import for the standalone egress probe.

The request-transport runtime is the Rust binary in ``cargo/``. Keeping the Python
package focused on diagnostics prevents it from shadowing the ``anvil-ring``
executable.
"""

from __future__ import annotations

from anvil_ring.probe_egress import main

__all__ = ["main"]


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

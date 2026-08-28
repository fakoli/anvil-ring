# STATE

**Last updated:** 2026-08-27
**Phase:** scaffold — design contract only, **no working tunnel**.

## What exists

| Item | File | Verified |
|---|---|---|
| Origin story (de-identified) | `docs/origin-story.md` | prose reviewed |
| Invariants I-1…I-8 | `docs/invariants.md` | prose reviewed |
| ADR-0001 outbound-tether-not-mesh | `docs/adr/0001-…` | **Accepted** |
| ADR-0002 transport | `docs/adr/0002-…` | **OPEN — blocking** |
| Wire shape draft | `schemas/tether-v1.json` | schema lint pending |
| CLI skeleton (`up`/`list`/`revoke`) | `anvil_ring/cli.py` | argparse only; every subcommand returns exit 2 with "not implemented" |

The CLI has **no** transport flag and **no** implementation. Subcommands fail
loudly rather than pretending. This is intentional: an early stub that looks like
a feature is worse than an explicit gap.

## Blocking decision

**ADR-0002** — transport. Needs two facts, not opinions:

1. A **provider egress probe** from a real rental: is outbound 443 only, or is 22
   open too? Record per-provider. This decides chisel vs. `ssh -R`.
2. A **ruling on I-7's scope**: does "stdlib-only" forbid a third-party binary in
   the tunnel path, or only forbid third-party Python deps in the client? This
   decides chisel vs. writing our own.

## Next steps, in order

1. **Decide ADR-0002.** Everything downstream depends on framing + authZ surface.
2. Write **ADR-0003 (identity & revocation)** — registration, lease interval,
   revocation propagation path. I-3 and I-4 are assertions until this exists.
3. Egress probe harness as a tiny stdlib script (no secrets, no tokens) so the
   ADR-0002 evidence is reproducible rather than remembered.
4. Hub design: it must satisfy the fleet's two-runtime pattern (native launchd
   where there's no Docker, thin container where there is) — see anvil-events
   ADR-0002 for the precedent.
5. First end-to-end test against a **known-good local control** before any real
   rental: loop → tether → reach the mapped port. Only then a rental.

## Explicitly out of scope (v1)

- Bulk artifact / model-weight transfer (bounded by a single tunneled conn).
- Many-to-many routing between rentals.
- Any privileged or TUN-based mode, ever (I-2).

## Naming rule (operator directive, do not revisit casually)

Binary and every invocation: **`anvil-ring`**. No bare `ring`, no short alias.
The prefix is the namespace — it is also the mitigation for the collision between
the word "ring" and everything else named ring.

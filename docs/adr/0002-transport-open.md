# ADR-0002: Transport for the tether — NOT DECIDED

- **Status:** **Open / blocked on a decision**
- **Date:** 2026-08-27

This is deliberately left un-decided. Nothing in this repo is allowed to
"temporarily pick one" — code that quietly assumes a transport is how projects
like this end up with a migration they never scheduled.

## Candidates

### A. `chisel` (TCP-over-WebSocket, reverse mode)
- **For:** single static binary, no root, no TUN, and because it rides WSS/443 it
  survives egress that permits only HTTPS — the scenario most likely to bite on a
  random provider. Built-in reverse mode is exactly I-1.
- **Against:** adds a third-party binary to the tether path (supply-chain surface
  on someone else's GPU box, which is the argument behind I-7); authZ is thin, so
  our hub must do the real authorization work around it.

### B. `ssh -R` / autossh to a hub sshd
- **For:** no new dependency, mature auth (keys, certs, revoke-by-authorized_keys),
  everyone has already operationalized sshd.
- **Against:** port 22 egress is *less* likely to be open on a restrictive
  provider than 443. Key management for ephemeral nodes is fiddly, and
  `GatewayPorts` semantics are a footgun.

### C. Own stdlib WebSocket/HTTP multiplexed tunnel in the client
- **For:** satisfies I-7 literally — no third-party binary in the path. Full
  control of framing, so health, backpressure, and I-6 detection are first-class
  rather than inferred.
- **Against:** we are then writing and owning a tunnel. That is the real cost, and
  it is paid in correctness bugs at 3 a.m.

## What would settle it

Both of these must be true before choosing:

1. **Which transports do the target providers actually permit?** Not "usually" —
   run a probe from a real rental and record the answer. 443-only egress is the
   common case; if 22 is reliably open, B gets much stronger.
2. **Does I-7 (stdlib-only) bind the *tunnel* or only the *client logic*?** If an
   external binary is disallowed anywhere in the tether path, A is out and the
   real choice is B vs. C.

## Interim

The CLI in this repo exposes no transport flag and implements none. The first
implementation PR must either resolve this ADR or add a probe that produces
evidence for it — see STATE.md next-steps.

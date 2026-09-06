# ADR-0002: Transport candidates (superseded)

- **Status:** **Superseded by ADR-0004**
- **Date:** 2026-08-27

ADR-0004 selected an owned binary WebSocket tunnel implemented in Rust. The
candidate analysis below is retained as decision history; no implementation
decision remains open. Provider network probes still verify that a rental image
can reach the selected hub, but they do not reopen the transport choice.

At the time of this decision, the transport was deliberately left open. The
team did not allow an interim implementation to become an accidental permanent
choice. That constraint is historical; ADR-0004 subsequently selected the owned
Rust WebSocket transport.

## Candidates

### A. `chisel` (TCP-over-WebSocket, reverse mode)
- **For:** single static binary, no root, no TUN, and because it rides WSS/443 it
  survives egress that permits only HTTPS — the scenario most likely to bite on a
  random provider. Built-in reverse mode preserves the outbound-only rental
  requirement.
- **Against:** adds a separately sourced third-party binary to the rental, which
  conflicts with the self-contained-executable requirement. Its authorization
  model would still require the hub to enforce Anvil Ring registrations and
  routes.

### B. `ssh -R` / autossh to a hub sshd
- **For:** no new dependency, mature auth (keys, certs, revoke-by-authorized_keys),
  everyone has already operationalized sshd.
- **Against:** port 22 egress is *less* likely to be open on a restrictive
  provider than 443. Key management for ephemeral nodes is operationally
  complex, and `GatewayPorts` can unintentionally widen exposure.

### C. Own stdlib WebSocket/HTTP multiplexed tunnel in the client
- **For:** requires no third-party binary in the path. Full control of framing
  makes health, bounded flow control, and tether-disconnection detection
  rather than inferred.
- **Against:** the project assumes full ownership of tunnel correctness,
  interoperability, and incident response.

## Historical selection criteria

Both of these must be true before choosing:

1. **Which transports do the target providers actually permit?** Not "usually" —
   run a probe from a real rental and record the answer. 443-only egress is the
   common case; if 22 is reliably open, B gets much stronger.
2. **Must every tunnel dependency be compiled into the shipped executable?** If an
   external binary is disallowed anywhere in the tether path, A is out and the
   real choice is B vs. C.

## Historical interim state

At the time of this ADR the CLI exposed no transport flag and implemented none.
That statement describes the 2026-08-27 repository only; the current Rust
runtime ships the WebSocket transport documented in ADR-0004.

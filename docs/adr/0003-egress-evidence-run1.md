# ADR-0003: Egress evidence from the first probe run

- **Status:** Historical evidence; transport later selected by ADR-0004
- **Date:** 2026-08-28

## What was measured

`anvil-ring-probe-egress` ran on a macOS workstation using ordinary
residential/office egress. It did not run on a GPU rental because no rental was
active at the time.

| Target | Port | Mode | Verdict | Detail |
|---|---:|---|---|---|
| api.github.com | 443 | tls | OPEN | TLSv1.3 |
| registry.npmjs.org | 443 | tls | OPEN | TLSv1.2 |
| github.com | 22 | ssh-banner | OPEN | `SSH-2.0-20b2056` |
| 1.1.1.1 | 443 | tls | OPEN | TLSv1.3 |
| 1.1.1.1 | 53 | tcp | OPEN | — |

The version-1 tool printed: *"Both viable. chisel-over-WSS(443) preferred:
uniform, survives tightening."* That output is retained as historical evidence.
The current diagnostic reports qualification for the shipped Anvil Ring
TLS/WSS transport and no longer presents Chisel or SSH as live product choices.

## What the run proves

The run showed that the diagnostic could resolve DNS, establish TCP and TLS,
read an SSH banner, distinguish verdict from usability, and write a
machine-readable JSON record with its hostname redacted by default.

It proves nothing about a GPU provider. It is a control result from an
unrestricted network, not provider qualification.

## Historical interpretation

Port 22 reaching `github.com`, with a real SSH banner, showed that `ssh -R` was
viable on that ordinary network. At the time, that kept candidate B in ADR-0002
open. ADR-0004 later selected the owned Rust WebSocket transport for reasons
beyond this one egress observation.

## Current provider check

Run one probe from the final rental image, as the unprivileged runtime user, for
every provider and image under consideration. The intended `wss://` hub target
must complete a verified TLS handshake. SSH reachability is supplementary
evidence and does not qualify the shipped transport.

```bash
anvil-ring-probe-egress \
  --no-defaults \
  --target hub.example:443:tls \
  --out egress-<provider>.json
```

No credentials are accepted by this tool. Review the artifact before attaching
it to a change record, and keep the hostname redacted unless disclosure is
intentional. See the
[production rollout guide](../production-rollout.md#network-and-tls).

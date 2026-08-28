# ADR-0003: Egress evidence from the first probe run

- **Status:** Data point 1 of N — **does not yet resolve ADR-0002**
- **Date:** 2026-08-28

## What was measured

`anvil-ring probe-egress` run on a **macOS workstation on ordinary residential/
office egress** — deliberately *not* a rental, since no rental was running at the
time. Recorded rather than asserted:

| Target | Port | Mode | Verdict | Detail |
|---|---|---|---|---|
| api.github.com | 443 | tls | OPEN | TLSv1.3 |
| registry.npmjs.org | 443 | tls | OPEN | TLSv1.2 |
| github.com | 22 | ssh-banner | OPEN | `SSH-2.0-20b2056` |
| 1.1.1.1 | 443 | tls | OPEN | TLSv1.3 |
| 1.1.1.1 | 53 | tcp | OPEN | — |

Tool hint: *"Both viable. chisel-over-WSS(443) preferred: uniform, survives
tightening."*

## What this proves, and what it does not

**Proves:** the probe works, all five code paths (DNS, TCP, TLS success, banner
read, verdict/`usable` separation) are exercised against real endpoints, and the
JSON artifact is machine-readable with the hostname redacted by default.

**Does not prove anything about any GPU provider.** This is the *baseline* — what
an unrestricted network looks like. It is the control in the experiment, not a
result. Recording it matters anyway, because without a control a later "443
works!" from a rental is unfalsifiable: you cannot tell provider permission from
tool malfunction.

## The finding that changed my recommendation

Port 22 reaching `github.com` here, with a real SSH banner, means **`ssh -R` is
viable on an ordinary network** — so the choice is genuinely open rather than
already forced by 443-only egress. That strengthens candidate B in ADR-0002
considerably, and I had been leaning A partly on an assumption that 22 is usually
blocked. On my own network it plainly is not.

What is still unknown, and is the *only* thing gating ADR-0002: whether the
specific providers in question restrict egress. That cannot be reasoned about; it
has to be measured from a real rental.

## Required before ADR-0002 can be accepted

One probe JSON **from an actual rental container**, per provider under
consideration, run as the unprivileged user in the image that would really
deploy. Then either:

- **443 + 22 open** → candidate B (`ssh -R`) is acceptable; A still preferred on
  uniformity grounds.
- **443 only** → B is eliminated; ADR-0002 resolves to chisel vs. own WSS tunnel,
  which then turns on the I-7 scope question (does stdlib-only forbid a
  third-party *binary* in the tether path?).
- **Neither** → that provider cannot host a tether; record it and move on. This is
  the outcome worth discovering *before* spending money on a rental, which is the
  main practical argument for the probe being a pre-deploy gate (exit 1 when
  nothing usable).

## Reproduce

```bash
anvil-ring probe-egress --out egress-<provider>.json
# optional: --target hub.example:22:ssh-banner
```

No credentials are accepted by this tool by design, so the artifact is safe to
attach to a PR.

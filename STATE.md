# STATE

**Last updated:** 2026-08-28
**Phase:** data-plane proxy implemented and tested; **tunnel client not started**.

## Language

**Rust** for the data plane (ADR-0004). The Python scaffold was superseded within
a day: token streaming is per-chunk latency-sensitive work, which is exactly where
the GIL and GC cost show up, and the earlier "stdlib-only" invariant was imported
from `anvil-events` by pattern-match rather than reasoned about. The Python docs
(invariants, ADRs, origin story) are retained and remain the contract.

## What exists

| Item | File | Verified how |
|---|---|---|
| Origin story (de-identified) | `docs/origin-story.md` | prose |
| Invariants I-1…I-10 | `docs/invariants.md` | I-8/I-9/I-10 now have tests |
| ADR-0001 outbound-tether-not-mesh | `docs/adr/0001-…` | Accepted |
| ADR-0002 transport | `docs/adr/0002-…` | **open**, but largely mooted by ADR-0004 |
| ADR-0003 egress evidence, run 1 | `docs/adr/0003-…` | real probe output; baseline only |
| ADR-0004 Rust data plane + fused image | `docs/adr/0004-…` | Accepted |
| Flushing reverse proxy | `cargo/src/proxy.rs` | 9 unit + 5 e2e tests |
| Hop-by-hop header stripping | `cargo/src/headers.rs` | 4 unit tests, incl. `Connection:`-named fields |
| Egress probe tool | `cargo/check_flush.py`, Python hub tooling | run for real; see below |
| Wire shape draft | `schemas/tether-v1.json` | schema validates |

**Test suite:** 14 passing (9 unit, 5 end-to-end spawning the real binary).

## The proof that matters most

`check_flush.py` measures TCP-level arrival times against a slow-emitting engine:

```
upstream emission span: 1.60s
tcp reads observed: 5
  t+ 0.00s   133B   (headers)
  t+ 0.53s    16B   data: two
  t+ 1.01s    18B   data: three
  t+ 1.49s    19B   data: [DONE]
  t+ 1.99s     5B   terminator
arrival spread: 1.98s -> PASS (flushing)
```

That cadence matching the emission interval is what distinguishes real flushing
from a proxy that buffers and presents as "the model is slow" — I-9's failure mode
is latency, not error, so `proxy_e2e::streaming_arrives_incrementally…` asserts on
**arrival timing**, never on eventual body equality.

**Negative control:** `src/bin/buffering_canary.rs` is a deliberately-buffering
binary; `tests/negative_control.rs` drives the same timing assertion against it and
requires it to FAIL. A guard that cannot go red is decoration. If that test ever
passes, the streaming assertion has been weakened and needs tightening.

## Blocking decision

**Per-provider egress policy** (ADR-0003). Measured from a real rental container,
not reasoned about. It no longer gates the *implementation* — we now own the proxy
and speak HTTP through our own tunnel — but it still gates *deployment*.

## Next steps, in order

1. **Tunnel client + hub** — this is the actual product. Everything so far is the
   proxy half; nothing dials out yet.
2. **Fused image `Dockerfile`** (`FROM vllm/vllm` + `COPY anvil-ring`), plus a
   loopback bind for the engine (I-10).
3. **ADR: identity & revocation** — registration, lease interval, revocation
   propagation. I-3/I-4 are assertions until this exists.
4. **musl static release build** in CI, so I-7 is enforced by a build rather than
   by intent.
5. **Egress probe from a real rental** → close ADR-0002/0003.

## Naming rule (operator directive — do not revisit casually)

Binary and every invocation: **`anvil-ring`**. No bare `ring`, no short alias.
The prefix is the namespace and is also the mitigation for "ring" collisions.
Enforced by `tests/proxy_e2e.rs` (spawns the binary by name) and by the
`CARGO_BIN_EXE_anvil-ring` reference in the test harness.

## Harness lessons (recorded so they are not rediscovered)

1. **Ports must be reserved per-test.** Sharing one pair made results depend on
   execution order.
2. **Never trust a bare `connect()` readiness probe.** It succeeds when *any*
   process holds the port — including a leaked proxy from a previous run. That
   produced a bewildering "proxy returned no bytes" which was really `AddrInUse`.
   The harness now reads the proxy's own log line to confirm *our* child bound it.
3. **Kill AND reap** test children (`kill()` + `wait()`); a reaped-less child keeps
   the listening socket alive into the next test.
4. **A test double must drain the request body.** Reading only headers left
   `Content-Length` bytes on the socket, hyper blocked forever, and the test saw a
   200 with an empty body.
5. **Parse the body the way the wire does it.** Taking the *last* `\r\n\r\n`
   segment of a chunked response yields the `0\r\n\r\n` terminator, so a correct
   proxied response read as empty. Split on the first boundary; de-frame chunks.

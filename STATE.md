# STATE

**Last updated:** 2026-08-28
**Phase:** tunnel + hub + client implemented and **verified end-to-end on one
machine**. Revocation verified at the authorization layer. **The hub cannot yet
forward a live request** — see "What is NOT done".

## Components

| Component | File | Verified |
|---|---|---|
| Flushing reverse proxy | `cargo/src/proxy.rs` | 9 unit + 5 e2e; TCP-level flush timing |
| Wire frame codec | `cargo/src/frames.rs` | 9 unit tests, incl. all 8 frame types |
| Tunnel client (rental side) | `cargo/src/tunnel.rs` | live handshake, lease, state machine |
| Hub + registry (authority) | `cargo/src/hub.rs` | 11 unit tests, incl. SHA-256 known-answer tests |
| `anvil-ring hub` / `tether` | `cargo/src/main.rs` | ran both binaries, real WSS |

`cargo test`: **41 passing**, zero warnings.

## What was actually observed (not asserted)

Two real processes, real WebSocket:

```
hub:    hub on 127.0.0.1:18900; tether demo-1 registered (cred 3316c9a97a9a...)
hub:    tether demo-1 authorized from 127.0.0.1:49288, lease 900s
hub:    event demo-1 Up
tether: tunnel #1 authorized; lease 900s
```

Invariants checked against the live processes:

- **I-1 (outbound only)** — the tether owns **exactly one** socket, and it is
  outbound: `anvil-rin 37433 TCP 127.0.0.1:49288->127.0.0.1:18900 (ESTABLISHED)`.
  Zero listeners. The hub, by contrast, shows its `LISTEN`.
- **I-8 (no secret in argv/logs)** — credential passed by env, absent from `ps`;
  hub logs only `cred 3316c9a97a9a...` (hash prefix).
- **I-10 (loopback-only upstream)** — `ANVIL_RING_UPSTREAM=http://example.com:80`
  → `Error: "upstream must be loopback; the tether only ever proxies locally (I-10)"`
  at startup *and* re-checked per stream in `StreamCtx::open`.
- **I-3 (revocation)** — `Registry::authorize` returns `None` the instant `revoke`
  is called; a live session additionally gets `GOAWAY`.

## What is NOT done — the honest gap

**The hub accepts a tunnel but cannot push a request through it.** `LiveSession.tx`
exists and the hub's session loop *can* write frames, but nothing constructs the
`OPEN` frame from an inbound HTTP request. So the data path
`caller -> hub -> tunnel -> vLLM -> back` does not close yet; the control path
(auth, lease, teardown) does.

This is why `tx` drew a dead-code warning: it is genuinely not written to yet. I
kept the field with an explanatory `#[allow(dead_code)]` rather than deleting the
warning by deleting the plumbing.

Also unbuilt: hub-side TLS termination (the test used `ws://` on loopback, which
`dial()` *permits only for loopback* and refuses otherwise), stream-id allocation
with reuse control, and the hub's caller-facing HTTP frontend.

## Bugs found and fixed during this phase

1. **`select!` cannot borrow one struct twice.** `hb.tick()` + `hb.dead()` in one
   `select!` is two `&mut` method calls on `hb` — opaque to the borrow checker.
   Fixed by borrowing the two timer *fields* directly, which it can prove disjoint.
2. **A `tick()` future pins its `Interval`.** `Box::pin(interval.tick())` holds
   `interval` borrowed, so re-arming is impossible; switched to a re-armed
   `Pin<Box<Sleep>>`.
3. **Hand-rolled SHA-256 failed its own known-answer test.** `finish_hex()` called
   `update()` for padding, which increments the byte counter, so the encoded
   message length was wrong. Replaced with the `sha2` crate. Recording this
   because the *reason* I hand-rolled it — "fewer deps for auditability" — is
   exactly the reasoning that produces vulnerable crypto. `sha2` is the audited
   choice; I-7 constrains the *artifact*, not the crate count.
4. **My test asserted a contract the code didn't have** (revoke returning false on
   a second call). Fixed the test, then fixed the code to the clearer contract:
   `true` = state changed, `false` = nothing changed.

## Naming rule (operator directive — do not revisit casually)

Binary and every invocation: **`anvil-ring`**. No bare `ring`, no short alias.

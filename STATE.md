# STATE

**Last updated:** 2026-08-28
**Phase:** control plane and data plane are both implemented. The data plane
forwards and streams, but **does not yet deliver a full multi-event response
through the tunnel.** Not production-ready.

## Verified working (observed on real processes, not asserted in a test)

1. **Handshake / authorization.** `tether` dials `hub`, presents a credential,
   receives a WELCOME lease. Hub logs `tether demo-1 authorized; lease 900s`.

2. **I-1 (thesis) on the OS.** The tether owns exactly one socket and it is
   outbound (`lsof -nP -a -p <pid> -i`): no listener, ever. The hub has one.

3. **I-10.** Non-loopback upstream refused at startup *and* per stream.

4. **Standalone proxy streams (I-9), through the new chunk decoder.** Four reads
   at 0.003 / 0.454 / 0.903 / 1.279 s against an engine emitting one event per
   300 ms, payload `data: one`, `data: two`, `data: done`. Streaming is real.

5. **Chunk decoding engages correctly.** Tether-side trace showed the engine's
   `transfer-encoding: chunked` detected and each event de-chunked.

6. **Caller auth is enforced before tunnel use** (401, and the tunnel is never
   touched); a live caller with no tether gets 502, not 401.

7. **No fabricated success (I-11).** With the engine dead the caller now gets
   `502 Bad Gateway`. See the bug below.

## Bugs found and fixed (each verified, each found by running)

- **Fabricated 200 for a dead engine (I-11) — the serious one.** The frontend
  built its response with `.status(head.map(status).unwrap_or(StatusCode::OK))`.
  Killing the engine produced `200 OK` with a valid chunked terminator and an
  empty body: indistinguishable from an engine that legitimately streamed
  nothing. Every caller treats 200 as "the model answered", so there is no retry,
  no failover, no surfaced error. Fixed by treating an absent head as failure and
  extracting the decision into `frontend::caller_status_for` so it is testable
  without a hub/tether/engine (`tests/no_fabricated_success.rs`).

- **`parse_head` stripped `transfer-encoding` from the inbound engine->tether
  response.** Hop-by-hop filtering is correct for a *forwarded* message and wrong
  for the message that tells you how to decode a body. A response literally
  declaring chunked parsed as if it did not.

- **`split_head` passed `end + 4`** where `find_header_end` already returns an
  offset past the CRLFs, starting the body 4 bytes early.

- **The tether dropped the engine's head when chunked**, forwarding only decoded
  body, so the hub's `parse_head` returned `None` and every chunk was dropped.

- **`transfer-encoding` was stripped from the caller's response** (correct per
  RFC) while the payload was forwarded in raw chunk framing, so hyper framed an
  already-chunked body a second time. Root of the "chunk length disagrees with
  its data" symptom.

- **My hand-rolled SHA-256 failed its own empty-input KAT.** Written to "reduce
  dependencies for auditability"; replaced with `sha2`. I-7 constrains the
  artifact, not the crate count.

## The bug that is still open

The tunnel delivers the **first** event and then ends the stream. Reproduced
against a fresh hub, one tether, one verified engine. The leading theory, from a
tether-side trace, is in `tunnel.rs`'s first-read branch: it forwards the head as
frame 1 and one decoded chunk as frame 2, and the `continue` after that skips the
`saw_end`/terminator handling, so a coalesced read contributes one event.
The engine's own response is complete (verified directly against it: 131 bytes,
three events). The standalone proxy delivers all three.

## Do not trust the earlier "live" results in git history

Two measurement errors contaminated most live testing on 2026-08-28 and are worth
remembering as method, not as this project's state:

- **Stale processes.** `process kill` reported success while the process survived;
  tethers from 16 minutes earlier kept holding hub registrations while new ones
  launched. Always `pkill -9 -f anvil-ring` and verify with `ps` before a run.
- **`| tee` buffers stderr**, so an absence of trace lines meant "buffered", not
  "not reached".

Also: byte-identical responses appearing across logically different builds was
the tell that a "live" run was executing in-process test code, not the binaries.

## Test inventory

- lib: 38 tests — frames, proxy, headers, hub registry/leases, chunked decoder.
- `no_fabricated_success`: 3 (I-11 regression).
- `chunked_decoder`: 1. `proxy_e2e`: 9.
- `forward_e2e`: 4 — 2 pass (auth), 2 fail (open data-path bug).
- clippy: the `is_hop_by_hop` "never used" warning on the bin target is the
  unused-linter's known false positive.

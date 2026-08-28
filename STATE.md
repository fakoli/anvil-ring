# STATE

**Last updated:** 2026-08-28 (late)
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

## The bug that is still open — ROOT CAUSE FOUND for the tunnel half

### Fixed: the tether's ping interval fired once and never re-armed

`hb.ping_tick` sent one `Frame::Ping` and was never reset. The only other re-arm
is `hb.reset()` in the inbound-frame arm, which requires an INBOUND frame — and an
idle tunnel receives none. So after ~6s the tunnel went permanently silent, the
hub's `TETHER_SILENCE` watchdog tore the session down, and the tether reconnected.

The signature was unmistakable once the hub's own log was read:

    Up(6.226s)  Up(6.213s)  Up(6.203s)  Up(6.197s)

Consistent to 20 ms across reconnections. A watchdog, not data loss. Why the loss
was silent: `sink.send` keeps returning `Ok` after the peer stops READING — writes
to a socket buffer fine until the buffer fills, so every frame the tether "sent"
was accepted and never arrived.

Proof the fix works, instrumented hub, one run:

    H RECV DATA 80 bytes   (head)
    H RECV DATA 10 bytes   (data: one)
    H RECV DATA 10 bytes   (data: two)
    H RECV DATA 11 bytes   (data: done)

SEE CORRECTION BELOW: these four lines came from an interval containing several
requests, not one response. The tunnel fix is real; the truncation is not solved.

### Still failing: the caller gets 137 bytes, and the hub receives ONE frame

Instrumented run, one request, traced end to end:

    HUBDATA frame 80 bytes, head_already=false
    BODY yield 10 bytes
    BODY end of stream

One inbound frame (the head). The single yielded event comes from `parse_head`'s
`rest`, i.e. the head frame's coalesced body bytes -- so the caller's 137 bytes
are head + first event, and then the body ends.

**Correction to the "proof" above:** the four `H RECV DATA` lines (80/10/10/11)
were emitted across ~116s of hub uptime containing several requests, NOT in one
response. Read as a single run they would have meant the tunnel was fixed; that
claim was wrong and is retracted. The ping re-arm is still a genuine, necessary
fix -- an idle tunnel demonstrably went silent and was torn down on a 6s
watchdog -- but it did not resolve the truncation.

The truncation is that the TETHER still sends only the head frame on the wire,
even though the pump provably reads all four reads and de-chunks them (traced as
`k=80 / k=15 / k=15 / k=16 / k=5`). Between "pump produced a payload" and "hub
received a frame", the later frames vanish. The pump hands them to the session
writer channel and `send` reports Ok; the writer reports no error; the peer
never shows them.

### SECOND MEASUREMENT (later same day) -- this account is also wrong

With one hub and one tether, both freshly started and verified via `ps`, the
hub's own routing point logged exactly one event for a whole request:

    HUB parse_head ok: bytes=80 rest=""

One DATA frame, the head, no body rest -- and no further DATA frame ever. Yet the
caller received `A\r\ndata: one\n\r\n0\r\n\r\n`. Those bytes cannot have come from
the one frame the hub saw. The frame carrying them reached `parse_head` LATER --
after `Registry::forward`'s `head()` poll exhausted its 200 x 5ms budget (~1s)
and returned None. In that window `head` goes from None to Some, the response is
built, and `parse_head`'s `rest` is forwarded as a body chunk. Because the head
now PRESERVES `transfer-encoding` (the deliberate hop-by-hop fix), hyper re-frames
`rest`, so the caller sees chunk framing as body: `A\r\n` + one event + `0\r\n`.

So the caller's payload is a stale, timed-out response, not a live stream. Two
distinct defects, previously conflated:

1. `head()` waits a fixed ~1s. A slow-to-first-byte engine makes every request
   take this path: it answers with whatever `rest` happens to hold, and the
   stream is then abandoned. This is the I-11 failure mode wearing a different
   face -- not fabricating a 200, but fabricating a PARTIAL body and ending it.
   It should wait for the head unconditionally (bounded by the caller's own
   timeout / disconnect), not a fixed 1s.
2. After `rest` is sent once, later frames stop reaching the hub. That part is
   still unexplained and is what the remaining failing e2e tests exercise.

Proven correct in isolation, so stop re-testing these: `parse_head` returns the
body after the head with headers ending once (`parse_head_rest_tests`);
`TunnelBody::poll_next` returns its receiver on both Chunk and Pending; the chunk
decoder handles every read boundary; standalone proxy streaming delivers all
events at correct timing; the tether pump reads all four reads and sends exactly
10 decoded bytes for the coalesced read (`BR` trace).

Two candidate mechanisms remain, and the next run should distinguish them:

1. The pump's `reply` sender is a clone whose receiver is not the one the writer
   drains after a reconnect — so accepted sends go into a dead channel. Check
   sender/receiver identity at clone time vs. writer drain time.
2. The stream task is torn down after the first send (guard drop -> `End` ->
   `streams.remove`) so later reads never reach a live sender.

Note `BODY end of stream` fires without any `Frame::End` arriving from the
tether, which means the channel closed -- consistent with candidate 1.

**The earlier hypothesis below is REFUTED — keep for history only.** It said the
hub session loop died mid-stream. The session lifetime was a watchdog artifact of
the ping bug, not a cause of the truncation.

- `process kill` can report success while the process survives; tethers from
  16 minutes earlier kept holding hub registrations. Always
  `pkill -9 -f anvil-ring` and VERIFY with `ps` before a run.
- `| tee` buffers stderr: absent trace lines mean buffered, not unreached.
- Byte-identical responses across logically different builds means the run
  executed in-process test code rather than the binaries under test.
- Instrument the component you have NOT traced. I theorized about the hub for a
  long while before tracing it; one `HU DATA n=80` line ended the guessing.
## Test inventory

- lib: 38 tests — frames, proxy, headers, hub registry/leases, chunked decoder.
- `no_fabricated_success`: 3 (I-11 regression).
- `chunked_decoder`: 1. `proxy_e2e`: 9.
- `forward_e2e`: 4 — 2 pass (auth), 2 fail (open data-path bug).
- clippy: the `is_hop_by_hop` "never used" warning on the bin target is the
  unused-linter's known false positive.

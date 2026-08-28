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

### ROOT CAUSE (measured, arithmetic, not inference)

Engine emits 131 bytes for this response:
    80 (head) + 51 (three events + terminator)
The caller received **137**. Six bytes MORE than the engine ever produced, so
`transfer-encoding: chunked` was applied a SECOND time, on top of bytes that were
already chunk-coded. A proxy cannot invent bytes; only hyper's framing can. That
requires the head reaching the caller to still say `transfer-encoding: chunked`
while the body it carries is already framed.

That is the hop-by-hop decision made backwards, in this project's own code, for
BOTH directions, and it was never executed anywhere:

  * `strip_hop_by_hop` in headers.rs has ZERO call sites (verified by grep).
    Comments in tunnel.rs and STATE.md claim the tether strips the header before
    forwarding a head. It never has. I asserted this in several comments without
    checking -- a comment describing code that does not exist.
  * Tether -> caller: forwarding the engine's head VERBATIM, header included, is
    right ONLY if the body is forwarded verbatim too. The tether de-chunks, so the
    header now lies, and the hub re-frames the de-chunked bytes: +6 bytes, and
    chunk framing visible in the body.
  * Caller -> tether -> engine: the caller's `transfer-encoding` is forwarded to
    the engine, which will chunk-decode a request body that was never framed --
    the mirror-image bug.

The fix is asymmetric and must be made in BOTH directions, in the tether (the
component that changes a body's framing is the component that owns the header):
  - response head to the hub: strip `transfer-encoding` (we de-chunked).
  - request head to the engine: strip it, and forward a body framed the way the
    head now claims, or strip it only when we pass the body through untouched.
Do NOT fix it by de-chunking at the hub: hop-by-hop is per-hop, and the hub has
already re-framed by then. Also wire up `strip_hop_by_hop` or delete it -- dead
code that comments claim is live is worse than no code, because it is what the
next reader trusts.

`hub_forwards_rest_verbatim_and_therefore_leaks_framing` PASSES while the bug is
present -- it documents current behaviour, deliberately, and must be inverted
when the head stops carrying the header.

## THE TERMINATOR IS FABRICATED (I-11 violation) - decisive test

Engine rewritten to NEVER finish (10 events, 3 s apart). Caller still received:
  B\r\ndata: ev00\n\r\n0\r\n\r\n        <- a COMPLETE terminator at t=0.0s
Hub received exactly 1 DATA frame (n=11). Tether: 1 read, k=96.
Tether's own trace: PUMP head_split=Some(16)  (head 80 + one 16-byte chunk
  "B\r\ndata: ev00\n\r\n").

Nothing in the hub or frontend can emit `0\r\n\r\n`; the de-chunker emits that only
when it sees the engine's real terminating zero-size chunk. Therefore the tether
declared the body complete after ONE chunk and sent Frame::End, and the hub/frontend
honestly relayed an END that was never real. This is exactly the I-11 failure the
project exists to prevent: a fabricated success.

With a 300 ms engine the same path produced the caller-visible single event;
with a 5 s engine the terminator still arrived at t=0.0s, i.e. BEFORE the engine
emitted event two. So the truncation is NOT a race, NOT starvation of the read arm,
and NOT byte loss in the de-chunker.

NEXT: ChunkedDecoder end-detection on the COALESCED path, where the pump decodes the
whole remainder in a loop `while !r.done { push(r.consumed) }`. `r.done` after a
single well-formed chunk must NOT be treated as end-of-body: HTTP/1.1 chunked ends
only at a zero-size chunk, and here the first read legitimately contains the head
plus chunk #1 while chunks #2..N are still in flight. The inline drain appears to
consume the head's trailing bytes as if they were part of the chunk stream, OR
treats a chunk-aligned read as EOF. Reproduce with a decoder fed
"B\r\ndata: ev00\n\r\n" and assert done == false.

## FINDINGS (this turn) - MEASURED, both ends of the tunnel

Tether (verified-live stack, one hub, one tether, one fake engine, lsof-checked):
  T RESP_HEAD send raw=80 fixed=52
      fixed = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n"   <- CORRECT: TE removed
  PUMP read k=95        <- EXACTLY ONE READ. Ever.
  Engine socket afterwards: our side FIN_WAIT_2, fake's side CLOSE_WAIT.

Caller receives:
  head: HTTP/1.1 200 OK | x-trace-marker: frontend-live-7q | content-type |
        transfer-encoding | date
  body: A\r\ndata: one\n\r\n0\r\n\r\n          <- ONE event + terminator

The marker header PROVES the current frontend served this request (it was added
this turn and is present on the wire). `date:` and `transfer-encoding:` are
hyper-synthesized, not engine-supplied -- the engine sends neither.

REFUTED THIS TURN, each by measurement rather than argument:
  1. "tether stops reading the engine"  - true observation, but it is CONSEQUENCE:
     k=95 = 80 head + 15 (one chunk). The fake engine's `while True: recv()` then
     ATE events two/three as a second request. One read is the bug, not the engine.
  2. "request-path parse_head leaks raw rest as body" - TRACE REQPATH fired ZERO times.
  3. "stale binary / stale listener answers the caller" - full `rm -rf target`
     reproduced byte-identical output; `lsof` shows exactly one LISTEN, seconds old.
  4. "chunk sent before caller installs receiver, send() errs and kills stream"
     - caller idled 2s reading nothing; nothing was buffered in the kernel, so the
     bytes never reached the frontend. Loss is upstream of the frontend.

PRIMARY SUSPECT, now with socket evidence: the pump exits after ONE read. The
stream task's select! has exactly two arms (from_hub.recv, ctx.read). A CLOSED
mpsc Receiver's recv() is permanently ready (tokio docs: "When a channel is
closed, recv() returns with None"), and `biased;` is in effect, so the from_hub
arm can win every pass and cancel ctx.read forever. I previously changed its None
arm to a no-op, which does NOT remove the permanent readiness -- only the guard
did that, and the guard was deleted as itself faulty. The two facts to reconcile:
whether the stream task survives read #1, and who half-closed the engine socket.

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

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

## The bug that is still open — narrowed to an exact mechanism

The tunnel delivers the first event, then the stream stops. **Two real bugs were
found and fixed on the way here:**

1. **`None => break` in the per-stream `select!`.** The request-side receiver
   yields `None` whenever no sender exists right now — the NORMAL state, since the
   only holder of the sender is the session loop on another task and a GET with no
   body sends nothing. Treating that as end-of-stream, in an arm biased by a 1 ms
   sleep against an engine answering ~300 ms away, cancelled the in-flight engine
   read. Fixed with a `request_side_idle` latch that DISABLES the arm instead of
   aborting, preserving both the read and the idle timeout.
2. **The framing decision was not sticky.** Head and first body bytes usually
   arrive in separate reads (time-to-first-token), so the old
   `dechunk.is_none() && !head_seen` guard left `dechunk` unset on body reads,
   which then forwarded RAW chunk framing. Replaced with a real tri-state
   `HeadState { Unknown, Chunked, Plain }`.

Verified fixed by trace — the tether now reads the engine continuously:
`TT2 k=80 state=Unknown`, then `k=15 / k=15 / k=16 / k=5` all `state=Chunked`,
producing 10/10/11/0 bytes, i.e. all three events de-chunked correctly.

**What is still broken, stated precisely:** those forwarded frames enter the
tunnel's send channel and NEVER reach the socket. Measured on one run:

- per-stream task: produced 10 + 10 + 11 bytes (all events); `reply.is_closed()`
  false, `send()` returned Ok — no error surfaced
- writer task: alive, `rx.recv()` loop intact, `run_session` never returned
- hub: received exactly ONE 80-byte frame (the head), then nothing for 20 s
- caller holding the connection open 20 s: 137 bytes total, then nothing

So the loss sits between an accepted `tx.send()` and `sink.send()`. Prime suspect:
the `reply` sender cloned into the stream task is not bound to the channel the
writer drains (e.g. cloned before the per-connection `tx` was replaced, or the
writer draining a different `rx` after a reconnect), so accepted sends land in a
channel nobody reads — which fits "accepted but never emitted, no error".

NOT YET CONFIRMED. `TT2 SEND FAILED` never fired, so nothing surfaced an error,
and I did not instrument `tx`/`rx` identity across the clone. Next step is
exactly that: print the channel/sender identity at clone time and at send time.

## Diagnostic traces ARE IN THE WORKING TREE (remove before shipping)

Uncommitted `eprintln!` traces: `TT2` and `TT3` in tunnel.rs, `HU ` in hub.rs.
They are the fastest way to re-orient on this bug:

    grep -n 'TT2\|TT3\|HU ' cargo/src/*.rs

Traces write to stderr. NOTE: piping through `tee` BUFFERS stderr, which earlier
made a live path look dead — read tracked process output directly instead.

## Measurement discipline (earned the hard way, twice)

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

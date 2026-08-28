# anvil-ring — STATE

## THE TRUNCATION BUG IS FOUND AND FIXED (2026-08-28)

**Root cause (measured, not inferred): the response body outlived the stream it
reads from.**

In `frontend::handle`, `forwarded: Forwarded` was a local. The handler built
`Response::builder().body(TunnelBody::live(rx))` and returned; `forwarded` was
dropped at the end of the function — **before hyper had written the body**.
`Forwarded` owns `StreamGuard`, whose `Drop` unregisters the stream from the
session map *and* releases the stream's channel sender. Once the map's handle was
gone too, every sender for the caller's chunk channel was dead, so `recv()`
returned `None`, and the body ended.

Traced directly:
```
GUARD drop id=1 completed=false     <-- guard torn down while the engine streamed
BODY poll_next -> Chunk  (exactly once)
BODY poll_next -> END               <-- body ended via None, not via the engine's END
```
There is only ONE `ChunkOrEnd::End` send site in the codebase (the hub's END
handler), so the early end could only come from `Poll::Ready(None)` = every sender
dropped.

That one teardown explains all three field symptoms at once: one event delivered,
a chunked terminator the engine never sent, and no FIN on the caller's socket.

**Fix:** tie the stream's lifetime to the response that consumes it.
`TunnelBody::Live` now carries `Arc<Forwarded>`, and `TunnelBody::from_forwarded`
builds the body by consuming the `Forwarded`. Nothing else about the data path
changed.

**Verification (clean build, engine emitting 6 events ~1 s apart):**
```
[+0.00s] head          [+3.44s] data: ev03
[+1.15s] data: ev01    [+4.45s] data: ev04
[+2.29s] data: ev02    [+5.60s] data: ev05
[+6.69s] 0\r\n\r\n     TOTAL events: 6
```
Each event arrives as the engine emits it (I-9 holds), and the terminator is the
engine's own, not synthesized (I-11). Drain-to-EOF: **6/6**.
`a_request_travels_the_whole_path` passes again — it had failed continuously
since `a1a07a2`.

## What was ALSO real earlier today (do not re-litigate)
- **Tunnel `select!` starvation — fixed, A/B-proven.** With `biased;` the pump did
  ONE read ever; without it, seven. The closed-channel `from_hub.recv()` arm was
  permanently ready and cancelled the engine-read arm every pass. Fixed by latching
  the arm off (`recv(), if !from_hub_done` + `None => from_hub_done = true`). This
  moved the hub from 1/6 to 6/6 DATA frames received.
- **`biased` is not itself the defect.** With the arm correctly disabled, biased
  only changes ordering. The defect was the permanently-ready arm.

## Wrong conclusions I recorded and later disproved (kept so they aren't re-trusted)
- `9f24d8e` "the terminator is fabricated by the tether; ChunkedDecoder end-detection
  misfires" — **FALSE.** The decoder's own unit test passes (`done=false` after one
  chunk). Its commit message still asserts this; read this file instead.
- "removing `biased` changes nothing" — measured on a broken no-op `None` arm, so
  the comparison was invalid. Corrected in `46b2ed1`.
- The `StreamGuard` "fix" `drop(self.chunk_tx.clone())` — a **no-op**: it drops the
  clone, not the field. Reverted in `81f6c37`.
- Stream-id reuse (`remove(&self.id)` in `StreamGuard::drop`) — a REAL latent sharp
  edge, but measured NOT to be tonight's bug: the id-reuse test passed (caller B got
  its full stream after caller A abandoned). Worth hardening (unregister by identity,
  not key), not urgent.

## REMAINING FAILURE: I-6 -- FULLY CHARACTERIZED (not a test artifact)

`tests/i6_tether_death_probe.rs` measures it. After killing the tether's task:
**bytes kept arriving for 65 s (3,287 B)** and the caller never terminated.

Mechanism: `tunnel::run_client` does `ws.split()` (tunnel.rs:175), so the writer
task owns the socket's sink half. Cancelling the client task leaves the TCP
connection OPEN, so the hub keeps reading -- the engine's PONGs refresh
`last_seen`, so the 45s liveness watchdog can never fire, because from the hub's
side the tether really IS alive. My cleanup sits at the session loop's exit, which
this path never reaches (instrumented: 74x `stream.next -> Some(Ok(..))`, never
None/Err, and `I-6 teardown:` never printed).

**Real defect: a wedged tether can stream to a caller indefinitely, because its
liveness proof is satisfied by traffic it is not processing.** In the fleet that
is a GPU rental whose tunnel is alive-but-stuck while the engine behind it is
gone -- the caller should be cut off, not fed.

Design fix, NOT yet implemented (do not rush it):
1. Keepalive must prove the peer is PROCESSING, not merely reachable. Ignore Pong
   for liveness, or require a bidirectional proof (hub Ping -> tether Pong within
   one interval) rather than counting any inbound byte.
2. Close the socket deterministically when the tether's session is ending: join or
   abort the writer task and drop the sink, so a leaked half cannot keep the
   connection alive.
3. Then end every stream that tether was serving (the drain already written at the
   loop exit handles that once the exit is reachable).
4. Add an explicit `tether_gone` signal so the frontend can end callers without
   waiting on the watchdog at all.

Also fixed while investigating, and it stays fixed: `decode` used to return
`Ok(None)` for `Message::Text`/`Message::Frame`, which would have made a real RST
look like a keepalive and the loop spin on a dead socket. Now terminal.

## Test topology currently in use
engine `spyengine.py` :19905 -> tether -> hub :19920 (frontend :19922), credential
`tun`, caller token `cal`. `/tmp/caller_paced.py` (timing truth),
`/tmp/caller_total.py` (drain-to-EOF count), `/tmp/spyengine.py` (what the engine
actually writes, with timestamps), `/tmp/reuse_test.py` (id reuse).

## Instrumentation currently in the tree (remove before shipping)
- `hub.rs::StreamGuard::drop` -> `GUARD drop id=.. completed=..`  <-- keep until I-6 is resolved
- `frontend.rs::poll_next` -> `BODY poll_next -> Chunk/END`        <-- same
- probe tests: `sender_teardown_probe`, `hub_to_caller_body`, `hyper_body_probe`,
  `chunk_backpressure_probe`, `coalesced_head_probe` — each is a proof of innocence
  for a component; keep them, they are cheap and they encode real contracts.

## COVERAGE GAP (do not read the probe suite as covering this)
The crashed-process case -- both socket halves gone, peer receives a real RST -- is
NOT tested. It IS detected: the hub logs 'Connection reset without closing
heartbeat', and after the `decode` fix an unexpected message is terminal rather
than swallowed. But the probe cannot exercise it, because `run_client` owns the
connection and the test harness cannot force SO_LINGER=0 / hard-close that socket.
Closing that gap needs either a test hook in `run_client` (e.g. accept an already
built WebSocket) or a fixture tether that exits without closing.

`tests/i6_tether_death_probe.rs` covers ONLY the leaked-socket case (task aborted,
sink retained by the writer task). Its docstring says so; keep it that way.

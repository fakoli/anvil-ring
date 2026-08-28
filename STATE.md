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

### I-6 fix plan -- staged, each step independently verifiable

Ordered so the riskiest reasoning is tested before it is relied on. Each step names
its own proof, and the suite must stay at 54 lib + forward_e2e 3/4 (plus the new
assertions) after every step, with `tether_death_midstream_ends_the_caller` turning
green only at step 4.

1. **Deterministic sink teardown.** The session currently ends with the reader half
   only; `ws.split()` means the writer task keeps the socket open, so the peer sees
   a live connection forever. Take explicit ownership: signal the writer task, await
   it, then drop the sink, so session end actually closes TCP.
   PROOF: a probe that aborts the tether's task and asserts the HUB observes death
   within ~2s (currently it observes none -- measured 74x `stream.next -> Some(Ok)`).
   EVIDENCE (observed, not theorized): a tether with no request in flight emitted
   ~700 WS frames (heartbeats/pings) before it was killed. Liveness measured as
   'any inbound traffic' therefore reports a stuck tether as alive forever, and the
   45s watchdog can never fire -- which is exactly the measured 65s-of-streaming
   failure. The keepalive must be answered by the CODE THAT PROCESSES STREAMS, not
   by the socket layer.
2. **Reachability is not liveness.** Stop letting any inbound byte refresh
   `last_seen`: keepalive must be a bidirectional proof (hub Ping -> tether Pong
   within one interval), because a tether stuck mid-loop still answers Pings.
   PROOF: a fixture tether that answers Pong but never processes OPEN, asserted to be
   declared dead within the window -- and NOT declared dead while genuinely working.
3. **Bounded command channel.** `LiveSession.tx` is an `UnboundedSender<Frame>`, so a
   wedged tether accumulates hub memory without limit (fleet-wide blast radius, see
   below). Bound it; refuse new streams when full and fail that caller fast rather
   than queueing toward an engine that cannot answer.
   PROOF: a probe that fills the bound and shows the caller gets a fast 503 while
   existing streams finish, instead of unbounded queueing.
4. **End the callers.** Only once 1-3 hold: on session exit, end every stream that
   tether was serving (the drain written at the loop exit works once the exit is
   reachable), and surface an explicit `tether_gone` so the frontend can cut callers
   off without waiting on the watchdog.
   PROOF: `tether_death_midstream_ends_the_caller` passes; caller sees the stream END
   rather than a fabricated terminator (I-11) or a hang.

Do NOT start at step 4: cutting callers off is only correct once the hub can actually
tell a dead tether from a busy one, which is exactly what steps 1-2 establish.

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

## PANIC FIX: bare-LF header terminator underflowed `reframe_head_for_tunnel`

A stale-tether panic report (`attempt to subtract with overflow`, src/tunnel.rs)
was from an OLD binary (its `TT k=` trace string is not in HEAD source) -- but the
same arithmetic existed at HEAD and was reproduced as a live crash.

`find_header_end` matches `\r\n\r\n` (returns i+4) OR bare `\n\n` (returns i+2).
`reframe_head_for_tunnel` sliced `&head[..end - 4]`, so a bare-LF head (end=2)
underflowed and panicked. A panic here kills the tether worker and every stream
multiplexed over the tunnel -- far worse than a wrong byte.

Fix: only run the CRLF reframe when the terminator really is CRLF-CRLF; a bare-LF
head passes through UNCHANGED (rewriting LF fields to CRLF could split a value
containing a bare LF, so normalization would be wrong, not just risky).
tests/bare_lf_head_panic_probe.rs pins both cases (2/2).

Gate after the fix: 54 lib, forward_e2e 3/4 (I-6 only), proxy_e2e 5/5, live
streaming 6/6 with `GUARD drop id=1 completed=true` after all six chunks.

## Link stability: MEASURED steady state (my reset-frequency worry was wrong twice)

A field log showed repeated `authorized -> Up -> reset -> authorized` cycles for one
tether, which made me raise a "tunnels reset after seconds" concern. A 100 s soak
(hub + tether, nothing killed, no requests) gives the answer:

    1 authorization, 0 resets, 0 refusals, 0 dial failures, tunnel Up, all alive
    VERDICT: STEADY STATE -- the link does not flap on its own.

So those resets WERE teardown (harness pkill), as first claimed. The soak initially
failed for reasons that were entirely harness bugs, and I proposed THREE wrong
explanations before finding it:
  1. "a stale /opt/homebrew/bin copy answered" -- FALSE: no such file exists.
  2. "an old installed binary lacking hub/tether subcommands" -- FALSE: the binary at
     the absolute path is current (`--help` lists all subcommands).
  3. "a zombie bound the port and answered first" -- FALSE: the fixture binds WITHOUT
     SO_REUSEADDR, so a successful bind proves the port was free.
The real cause: the hub registers its demo tether from ANVIL_RING_DEMO_CREDENTIAL,
and I dials with a DIFFERENT ANVIL_RING_CREDENTIAL. `refused tether` +
`ended: unauthorized` was I-5 working correctly. I-8 (authorize never says which half
was wrong) is why a credential typo is indistinguishable from a protocol bug -- which
is ALSO why the soak must use ONE unique value for both sides and assert `tunnel
reached Up` before interpreting any count.

## Minor robustness fix (defect real, urgency lower than I first assumed)

`Registry::detach(id)` was an unguarded `live.remove(id)`. On re-authorization the NEW
session is installed under the same tether id, so when the OLD session's loop exits it
deletes the NEW live session: a healthy tunnel left connected but unroutable (every
caller gets 502 NoTether) with nothing to repair it, since the survivor is never told.
Fixed with a per-session `generation` stamp and compare-before-remove.

Corrected urgency: the soak shows re-authorization is rare in steady state, so this is
a latent robustness fix, not the cause of the observed churn. Verified: 54 lib tests,
forward_e2e 3/4 (only I-6), and detach cannot be covered by a direct unit test -- it is
private and only reachable through a real re-auth race.

## I-6 DESIGN INPUT (measured, not assumed): the hub->tether command path is UNBOUNDED

Confirmed by grep, not inference:
  - hub.rs:79  `LiveSession.tx: mpsc::UnboundedSender<Frame>`   (hub -> tether commands)
  - hub.rs:772 `mpsc::unbounded_channel::<Frame>()`             (per-session)
  - tunnel.rs:176/216 unbounded writer task + per-stream senders on the tether side

Consequence for I-6: a tether that is connected-but-not-processing (the measured
65-seconds-of-streaming case) also means every OPEN/HEAD/BODY frame the hub pushes
into `LiveSession.tx` queues in hub RAM with NO ceiling. So the wedged-tether defect
is not only "a caller is never cut off" -- it is unbounded hub memory growth per
wedged tether. In the fleet: one rental whose loop wedges while callers keep
requesting can grow hub memory until the hub process dies, taking every OTHER
tether's tunnels with it. Blast radius is the whole fleet, not one call.

That makes a bounded command channel part of the I-6 fix, not a later hardening
step: bound it, and refuse new streams when full (fail the caller fast with 503 on
that tether) rather than queueing toward an engine that cannot answer. Response
bytes already bound correctly (`StreamState.chunk_tx` is a bounded `mpsc::Sender`,
and `pending` holds body bytes so a caller cannot outrun the tunnel) -- the gap is
only the outbound command direction.

## Confirmed design intent (operator, 2026-08-28): drop-and-redial is the design

Asked whether hub<->tunnel resets are routine in the fleet (which would shape how
strict the I-6 fix must be), the answer is: **no problem to design around -- the
tunnel is meant to drop and redial, and a redial is a normal, correct event.**

Two consequences the I-6 fix must respect:

1. Do NOT add reconnect damping, backoff growth, or 'too many reconnects -> alarm'
   logic. Backoff exists only for the DIAL side (tunnel.rs BACKOFF_MIN..MAX) and is
   correct there; do not extend that idea to session teardown.
2. Because re-authorization is EXPECTED to be routine, the `detach` clobber is more
   important than my soak implied. I downgraded it to 'latent' after measuring one
   authorization per 100s idle soak -- but that soak had no reason to reconnect, so
   it measured an idle tunnel, not a reconnecting one. Every real reconnect hits
   exactly the path where an old session's exit deletes the new live session under
   the same tether id. Keep the generation guard, and step 1 of the plan should show
   a RECONNECT (not just a teardown) leaving the tunnel routable.

Still worth a live check when convenient: how often the fleet's tethers actually
redial, so the reconnect path gets proportional test coverage. The soak script
(/tmp/soak_tunnel.py) is the tool; it asserted steady state on an idle tunnel.

## OPERATOR RULE (absolute): never touch ai-mbp25

Restated by the operator twice, so it is recorded here as well as in agent memory:
**ai-mbp25 is OFF LIMITS to me entirely.** No SSH to it, nothing read from it, nothing
run on it — no builds, containers, processes, config changes, probing, or read-only
"quick tests". It is the operator's active Mac and it HOSTS FLEET SERVICES (Docker
Desktop: serving containers, the Hermes gateway/dashboard, n8n app + postgres, webui),
so any action there risks interrupting his co-work or taking services down.

Consequences for this project:
- Do NOT propose mbp25 as a linux/amd64 / musl / `docker buildx` release-build host.
  That idea is explicitly rejected, not merely deferred.
- Do NOT install colima on Mini either (it cannot emulate x86_64).
- All anvil-ring build/test work stays on fakoli-mini (arm64) under ~/workspace-work.
  That means debug/CLI/test work only. A real linux/amd64 release artifact has NO
  authorized build host right now: if one is ever needed, STOP and ask the operator
  where it happens rather than picking a machine.
- This project is local-git only, no remote, and `anvil-ring` is the only binary name
  (no alias).

## MEMORY-HYGIENE LESSON (from tonight)

Durable memory has a hard char budget (~4k) and `replace` swaps the ENTIRE entry that
matches `old_text`. Consolidating the mbp25 rule into the anvil-ring entry therefore
DELETED the project facts (local-git-only, binary-name directive, "progress lives in
STATE.md") while looking like a clean success — the tool reported "Write saved" and the
usage went down, which reads as a good outcome.

So: never store a new rule by replacing an informative entry. Add a short standalone
entry, and only replace an entry with text that contains everything that entry already
said. Anything an entry merely *summarizes* should live in the repo (STATE.md), which is
versioned, auditable, and cannot be evicted by memory churn.

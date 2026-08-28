# anvil-ring I-6 — kickoff pack (paste-ready)

Context: I-6 is the one open defect in `~/workspace-work/projects/anvil-ring`
(local git only). Read `STATE.md` first — it holds the measurements, so this file is
only a launcher.

## Paste this to start

> Work on anvil-ring I-6, step 1 only: deterministic sink teardown. Read
> ~/workspace-work/projects/anvil-ring/STATE.md (I-6 section + staged plan) first.
> Do NOT skip ahead to step 4.

## The bug, in one line
A tether whose task has died keeps streaming to callers indefinitely (measured: 65 s,
74 iterations of `stream.next() -> Some(Ok(..))`), because `ws.split()` leaves the
writer half holding the socket open and the engine's Pongs keep `last_seen` fresh, so
the 45 s watchdog can never fire.

## Why the steps are ordered this way
Cutting callers off (step 4) is only correct once the hub can distinguish a DEAD tether
from a BUSY one. That is what steps 1–2 establish. Implementing 4 first would evict
callers on a plain transport hiccup — which is normal here: the operator confirmed
drop-and-redial is the intended steady behavior.

## The four steps and their proofs
1. Sink teardown — signal/await the writer task, drop the sink, so session end closes
   TCP. PROOF: abort the tether task, assert the HUB observes death in ~2 s (today: none).
2. Bidirectional keepalive — no inbound byte may refresh liveness; require hub Ping ->
   tether Pong. PROOF: a tether that Pongs but never processes OPEN is declared dead; a
   genuinely working one is NOT.
3. Bound `LiveSession.tx` (hub.rs:79 — the ONLY unbounded channel on the request path;
   response side is already `channel::<ChunkOrEnd>(64)` at hub.rs:324). PROOF: fill the
   bound, caller gets a fast 503 while existing streams finish.
4. End the callers + explicit `tether_gone`. PROOF: `tether_death_midstream_ends_the_caller`
   goes green with a real END — never a fabricated terminator (I-11).

## Gate that must hold after EVERY step
    cd ~/workspace-work/projects/anvil-ring/cargo
    cargo test --lib                                     # 54 pass
    cargo test --test forward_e2e -- --test-threads=1     # 3/4; only tether_death fails
Plus the live streaming gate — CANONICAL and committed (reboot-safe):
    python3 ~/workspace-work/projects/anvil-ring/cargo/scripts/live_stream_gate.py
    # regenerates engine+hub+tether+caller; asserts no-truncation, head-first-from-
    # engine, not-buffered. Exit 0 = pass. Ran green on 4 events x 0.4s.
    # (The old /tmp/*.py probes are ephemeral scratch history — do NOT depend on
    #  them; they are gone after a reboot. Prefer the committed gate.)


## If `cargo test` blocks on a build lock
Another cargo process can hold the lock for a long time (nightly-recent builds on this
box are slow). Rather than wait, run the ALREADY-BUILT test binary directly:

    cd ~/workspace-work/projects/anvil-ring/cargo
    ./$(ls -t target/debug/deps/forward_e2e-* | grep -v '\.d$' | head -1) --exact tether_death_midstream_ends_the_caller

Only trust this if the binary is newer than your last source edit (`ls -l` it); a stale
test binary is the trap that cost this project hours.

## Measured I-6 signature (fresh, 2026-08-28) — read this before designing the fix
The failing test's own trace shows the caller receiving chunks FOREVER after the tether
died, all at the same size, and the guard never completing:

    BODY poll_next -> Chunk 5B (total 0B)   x6+   (5B = "data:" -- the engine's events)
    GUARD drop id=1 completed=false
    panic: I-6: caller must be terminated when its tether dies, not left streaming

Two things that pin down the diagnosis:
  1. `total 0B` while chunks are delivered => the byte accounting is NOT incremented by
     the Live-body path. The StreamDeath signal, if it is ever produced, is dropped at a
     poll site that doesn't advance the count.
  2. `completed=false` => the stream was dropped, not ended. No END frame was produced,
     so a fabricated-vs-real END cannot even be discussed until death is observable.

So step 1 (deterministic sink teardown) genuinely comes first: until session end closes
TCP, the hub cannot see death, and steps 2-4 have nothing to react to.

## Traps already paid for (do not re-pay)
- Verify the BINARY is current before trusting any log: grep the trace string in src/.
  Half of tonight's false alarms were old binaries reporting fixed bugs.
- The hub registers its tether from ANVIL_RING_DEMO_CREDENTIAL; the tether must dial the
  SAME value in ANVIL_RING_CREDENTIAL. A mismatch gives `refused tether` + `unauthorized`
  — that is I-5 working, and because authorize() never says which half was wrong (I-8) it
  looks exactly like a protocol bug.
- Don't assert "clean/nothing running" from a check made before your last edit — verify
  in the same turn.
- Don't state a channel's behavior from memory; grep it (the response path claim was
  wrong until I checked).

## Boundaries
- Build/test ONLY on this host (the build host (arm64)). Do NOT touch the operator's daily-driver host in any form —
  it hosts fleet services and the n8n repo. If a linux/amd64 release artifact is needed,
  STOP and ask where it gets built; do not install colima (arm64 can't emulate x86_64).
- n8n / the CVE item: the operator's call, not mine. Do not open it.

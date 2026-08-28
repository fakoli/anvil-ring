## I-6 STEP 1 MEASURED (2026-08-28) — the hub DOES observe tether death; the caller is not terminated

Measured with `cargo/scripts/i6_death_probe.py` (raw sockets; urllib would buffer and
hide the timing). SIGKILL a tether mid-SSE-stream, 3 valid runs, no product code changed:

    head = 200 OK            in 3/3
    bytes received AFTER the kill = 0        in 3/3
    hub logged  "tether ... ended: WebSocket protocol error:
                 Connection reset without closing handshake"   in 3/3
    caller stream terminated              0/3   (stayed open at the 20s ceiling)

VERDICT: **hub observed death 3/3; caller terminated 0/3.**

### What this corrects
The prior claim "the hub never observes tether death" is **NOT supported**. The hub
observes the reset and logs it, every time. That claim had been inferred from
`forward_e2e`, which at the time *also* contained the response-body lifetime bug, so it
was measuring something other than what it appeared to.

The remaining, confirmed defect is narrower: an observed session end **is not propagated
to the caller**. Bytes stop, the stream is never ended, so the caller sits until its own
client timeout. Step 1 is therefore *not* "make death observable" — it already is — but
"turn an observed session end into a real END on every stream of that session."

This matches the probe's 0-bytes-after-kill: the hub is not withholding data, it is
withholding the *end*.

### Why this measurement is trustworthy when earlier ones were not
Every earlier probe run here was invalid, for reasons worth keeping:

1. **Orphaned hub.** A probe that dies before reaping leaves the hub listening on
   19930/19932; the next run then gets `HTTP/1.1 502 Bad Gateway` at +0.00s — a stale hub
   with no tether registered. That looks exactly like a product defect and is purely
   debris from the previous run. The probe now refuses to start unless all three ports
   are free, and reaps in `finally`. One such orphan (pid 95690, started 00:57) had to be
   cleared before valid runs; my own `ps | grep target/debug/anvil` check had reported
   "clean" around it because the command line was truncated past the match.
2. **A verdict that contradicted its own reason.** `caller terminated = True (timeout)`
   appeared because the code matched `"Timeout"` (the socket module's spelling) while
   this path yields lowercase `"timeout"` — so a wedged caller was scored as terminated
   and the run looked like it DISPROVED I-6. Fixed to match case-insensitively, plus an
   assertion that no sample may claim termination and a stayed-open reason at once.
3. **An instant-502 sample is now discarded, not counted.**

A probe that produces a convenient answer is more dangerous than no probe. Two of the
three bugs found here pointed toward "I-6 is fixed"; the correct result only appeared
once the checks were made capable of contradicting the expected answer.

### Still open
`forward_e2e::tether_death_midstream_ends_the_caller` remains red — correct, since
0/3 confirm the defect. Steps 2-4 of the plan are unchanged.

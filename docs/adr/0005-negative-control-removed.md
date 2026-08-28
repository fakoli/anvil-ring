# ADR-0005: Negative control for I-9 — attempted, removed, and why

- **Status:** Accepted (with an explicit gap)
- **Date:** 2026-08-28

## What I wanted

Invariant I-9 says a buffering proxy fails as *latency*, never as an error. So the
streaming regression test asserts on **TCP arrival timing**, not on eventual body
equality. But a guard that cannot go red is decoration, so I built a negative
control: `src/bin/buffering_canary.rs`, a deliberately-buffering proxy, plus
`tests/negative_control.rs`, which runs the same timing assertion against it and
requires failure.

## What happened

The canary could not be made to run under the test harness on this machine. Under
`cargo test`, the spawned child was killed by **SIGKILL within 300ms**
(`ExitStatus(unix_wait_status(9))`), writing nothing to a file descriptor it had
been given before `spawn()`, so no startup line ever appeared.

Ruled out, each by direct observation rather than assumption:

| Hypothesis | How it was falsified |
|---|---|
| Wrong binary path from `CARGO_BIN_EXE_*` | path printed; file exists |
| Canary code broken | runs correctly when launched manually — logs `listening on`, relays the stream |
| Port collision / stale listener | `lsof` showed the range clear; no orphan processes |
| Silent bind failure in the fake engine | engine made to fail loudly; no message |
| `pkill -f 'anvil-ring'` in my own commands | the failure persisted in a command containing no pkill |
| `cargo test` killing its own children | same binary survived as a *sibling* test's child (port 18720) |

That last row is the puzzle: the identical binary, spawned the identical way,
survived in `canary_binary_is_executable_at_all` and died in
`streaming_guard_detects_a_buffering_proxy`. The only differences were the port
number and which upstream it pointed at.

## The most likely explanation, labelled as unproven

macOS killing the child (Gatekeeper/XProtect re-evaluating the freshly-linked
binary, or an endpoint-security agent applying policy per-process-tree), or some
interaction between the two test binaries sharing a `cargo test` invocation. I
could not attribute it in eight rounds of narrowing, and further guessing had poor
expected value relative to the value at stake.

## Decision

Removed `tests/negative_control.rs`. **Kept** `src/bin/buffering_canary.rs` — a
build target costs nothing and it is runnable by hand:

```bash
cargo build --bin anvil-ring-buffering-canary
ANVIL_RING_LISTEN=127.0.0.1:18711 ANVIL_RING_UPSTREAM=http://127.0.0.1:18710 \
ANVIL_RING_TOKEN=t ./target/debug/anvil-ring-buffering-canary
python3 check_flush.py            # against the real proxy  -> PASS (flushing)
RING_PORT=18711 python3 check_flush.py   # against the canary -> must FAIL
```

`check_flush.py` measures the same thing and is manual, so nothing in CI depends
on the control running.

## What this weakens, stated plainly

The I-9 streaming test (`proxy_e2e::streaming_arrives_incrementally…`) currently
has **no automated proof that it would catch a buffering regression.** It passed
against the real proxy with a measured 1.98s arrival spread across 5 reads, and the
assertion requires ≥2 reads with ≥200ms spread — but "it passes" is not "it fails
when it should." Re-add the negative control as a **CI job step** (a shell step that
builds the canary, runs `check_flush.py` against it, and asserts a non-zero exit),
which sidesteps whatever kills spawned children inside `cargo test` and is probably
where this belongs anyway.

## Related, and now recorded in STATE.md

Three of the earlier "the proxy returns no bytes" investigations in this session
were **harness bugs, not product bugs**:

1. readiness checked by bare `connect()`, which succeeds against a leaked listener
   (`AddrInUse` on our own child, then a confusing empty response)
2. the fake engine read only headers, leaving `Content-Length` bytes unread so
   hyper blocked forever
3. the test's body parser took the **last** `\r\n\r\n` segment, which in a chunked
   response is the `0\r\n\r\n` terminator — so a correct response read as empty

The proxy itself was correct in all three cases. That is worth remembering the next
time a test fails plausibly.

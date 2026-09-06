# Testing and verification

## Required local verification

```bash
cd cargo
cargo fmt --check
cargo test --locked --all-targets -- --test-threads=1
cargo clippy --locked --all-targets -- -D warnings

cd ..
uv sync --extra dev
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .

uv run --with-requirements requirements-docs.txt mkdocs build --strict
```

Run Rust integration tests serially. They use loopback listeners, and parallel
test processes can turn a port collision into a misleading product failure.

## Test layers

| Layer | Primary coverage |
|---|---|
| Rust library tests | Binary frame codec, header filtering, constant-time credential comparison, URL and loopback restrictions, authorization renewal and revocation, parsing, chunk decoding, liveness messages, queue limits, and task cancellation |
| `proxy_e2e` | Authenticated local proxy, missing-token fail-closed behavior, streaming and status behavior |
| `transport_contract` | Fragmented and byte-valued headers, repeated headers, fixed-length and chunked bodies, early errors during upload, stream capacity, actual killed-process TCP reset, revocation, and lease expiry under blocked socket writes |
| `forward_e2e` | Caller → hub → tether → engine → caller, auth ordering, no-tether status, incremental streaming, tether cancellation, repeated stream lifecycle |
| Focused body and channel tests | Chunk ordering, explicit response end, sender teardown, response-body ownership, and bounded flow control |
| CLI contract | Real executable help, shipped modes, no secret flags, and rejection of unexpected process arguments |
| Durable administration | Private SQLite state, atomic credential delivery and audit, writer serialization, restart persistence, live rotation/revocation/expiry, store failure, and deterministic multi-tether routing |
| Python tests | Probe-only packaging, portal metadata, WSS-specific probe guidance, continuous-integration workflow contract, and combined-container child supervision |
| Documentation build | Navigation, internal links, Markdown rendering, and documentation portal configuration; warnings fail the build |
| Static release workflow | aarch64 and x86-64 Linux musl builds, architecture checks, absence of a dynamic loader, and the complete test suite |

## Durable hub evidence

The final local suite contains 149 Rust tests. It passes on macOS ARM64 with
Rust 1.96 and the minimum Rust 1.85, and on both Linux musl architectures with
Rust 1.85. The 16 Python tests and strict documentation build also pass.

`admin_contract` runs the shipped executable through issuance, rotation,
revocation, hub restarts, database loss/recovery, and expiry. It checks lexical
routing and sends work to another tether while the first response remains held.
`transport_contract::two_tethers_use_both_stream_budgets_before_returning_503`
keeps 128 responses open across two real tethers and requires the next call to
return 503. Expiry is moved into the past through test SQL; it does not wait for
the minimum 60-second credential lifetime. Live lease expiration has separate
transport coverage. The 16 storage tests include concurrent writers, bounded
snapshots/audit pages, private files, and forced commit-failure rollback.

Both rebuilt Linux artifacts are static and run their SQLite admin commands as
UID/GID 65534 with no network, no capabilities, and a read-only root filesystem
with private temporary state. These are local unsigned development builds;
physical power-loss behavior, hosted signing, provider egress, and a real GPU
model deployment remain outside this evidence.

## Regressions added in this remediation

### Tether cancellation ends callers

The original red test cancelled the tether and waited for TCP EOF. That mixed two
contracts: HTTP/1.1 may keep the caller connection open after a response, while a
chunked response body ends at its terminating chunk. The corrected regression
requires transport failure without a successful chunked terminator.

The code needed two lifecycle fixes before that test could pass:

1. Cancelling `serve_over` must abort its detached WebSocket writer so the hub
   observes the lost tether.
2. Any post-attach WebSocket/decode error must break through the common teardown
   block so every in-flight caller receives an independent failure notification.

The focused test is:

```bash
cargo test --test forward_e2e \
  tether_death_midstream_ends_the_caller -- --exact --test-threads=1
```

### Completed streams release capacity

The tether map originally retained a sender after an engine pump completed. The
concurrency cap therefore acted as a lifetime cap: requests 1–64 passed and
request 65 returned 502.

`completed_streams_release_tether_capacity` drives 66 sequential full-path calls
through one tether. It failed at request 65 before completion cleanup and now
passes.

### Queue bounds

Three tests fill each queue to its declared capacity and require the next
nonblocking send to return `Full`:

- hub command frames: 256;
- one tether stream's request input: 64;
- tether WebSocket writer frames: 256.

The hub also maps a saturated new-stream admission to `TetherBusy`, which
the frontend exposes as 503.

### Pump cancellation

`dropping_a_stream_handle_cancels_its_engine_pump` starts a never-ending task,
drops its owning stream handle, and requires the task's join error to be
cancelled. This prevents a caller abort or session teardown from detaching
upstream work.

### Bidirectional liveness proof

`only_a_pong_proves_bidirectional_hub_liveness` pins the distinction between
traffic and control-loop health: response `Data` and an unsolicited `Ping` do not
refresh the hub's proof; a tether `Pong` answering the hub does.

### CLI/package alignment

Tests first reproduced the collision between the Python and Rust command names
and the obsolete help text. They now
require:

- Rust help lists `proxy`, `hub`, `tether`, and `admin` with no scaffold copy;
- no token or credential CLI flags;
- extra process arguments are rejected instead of ignored;
- Python packages only `anvil-ring-probe-egress`;
- the historical JSON manifest is not installed as a live schema;
- runtime and diagnostics help expose the documentation and main project
  portals; and
- the egress probe reports qualification for the shipped TLS/WSS transport
  rather than the superseded Chisel/SSH decision.

### Deployment contract

`test_deploy_contract.py` proves that the Dockerfile uses the target platform,
non-root runtime, outbound-tether supervisor, and no engine-port exposure. A
fake `vllm` and fake `anvil-ring` then exercise the supervisor and assert the
exact child commands without requiring a GPU.

## Streaming harnesses

The in-repo live harness adds timing checks around a local fake engine that
emits server-sent events:

```bash
cd cargo
cargo build --bin anvil-ring
python3 scripts/live_stream_gate.py --events 6 --gap 0.8
```

It verifies that the response head comes from the engine, all events arrive,
and delivery is paced rather than buffered.

The idle soak keeps a local tunnel open and counts authorization/reset events:

```bash
python3 scripts/soak_tunnel.py 100 "$PWD/target/debug/anvil-ring"
```

An idle soak says whether a healthy connection flaps; it does not estimate
production reconnect frequency.

### Streaming timing negative control

`anvil-ring-buffering-canary` deliberately buffers a response and remains
test-only. `proxy_e2e` sends the same four-event response through the real proxy
and the canary. The real proxy must deliver reads across the engine's emission
window; the canary must return the complete body but fail that same timing
predicate. This proves the assertion detects buffering rather than merely
passing the current implementation. See
[ADR-0005](adr/0005-streaming-negative-control.md) for the earlier harness failure
and the restored comparison.

## Continuous integration

`.github/workflows/release-artifact.yml` has three responsibilities:

1. Build aarch64 and x86-64 musl binaries and reject the artifact if architecture
   or static-link checks fail.
2. Run every Rust target plus Python tests and Ruff. Tether-disconnection
   behavior is a required passing test; CI has no expected-failure exception.
3. Package executable archives, checksums, source identity, and target-specific
   Cargo SBOMs. Successful trusted main/tag runs sign provenance and SBOM
   attestations; pull requests never enter the signing job. See
   [Release verification](release-verification.md).

`.github/workflows/docs.yml` builds the documentation with MkDocs strict mode on
pull requests and publishes the portal from `main` through GitHub Pages. The
repository Pages source must be configured as **GitHub Actions** for deployment
to become active.

The local macOS suite cannot prove Linux musl linking or run the GPU image.
Those checks must run in the authorized Linux continuous-integration or build
environment.

## Verification snapshot

The current evidence is recorded in the repository's
[STATE snapshot](https://github.com/fakoli/anvil-ring/blob/main/STATE.md). Update
that snapshot only from fresh command output; do not copy counts from an older
session note.

## Transport completion review — 2026-09-05

The current transport uses Hyper for the engine HTTP exchange. The public
`chunked` and response-reframing helpers retain historical characterization tests;
they are no longer the runtime's engine parser. Full-path tests establish the
runtime guarantees.

`transport_contract` reproduces failures that the earlier suite missed:

- a response head split across reads leaked chunk framing;
- a coalesced fixed-length response delivered status-line bytes as its body;
- a completed chunked response waited for TCP EOF;
- truncated output was presented as a clean HTTP completion;
- an unfinished caller upload blocked an early 413 or tether failure;
- a killed tether needed to produce a real TCP reset and an HTTP body error;
- a full writer queue could postpone authorization expiry;
- excess concurrent requests returned 502 instead of 503;
- valid header octets and repeated fields needed preservation; and
- chunked GET bodies and unsupported transfer codings needed explicit handling.

Focused library regressions cover rotation, revocation during attachment,
per-caller queue saturation, late request completion, closed-session admission,
upload cancellation, literal loopback parsing, and concurrent final-chunk delivery.
The final-chunk test runs 10,000 producer/consumer interleavings. It is a stress
regression, not a formal proof of all possible schedules. A separate deterministic
test exhausts Tokio's cooperative budget before draining a completed response;
this catches a spurious `Pending` being mistaken for an empty queue. Completion
now closes the receiver and drains it until the receiver returns EOF.

The crash fixture is a child of the test executable running the real tether
session. Its sockets use abortive close. The parent kills the child and asserts
`ConnectionReset` on a witness socket, an HTTP body error, and the hub's `Down`
transition. This is separate from ordinary task cancellation.

Startup regressions additionally require secret-bearing upstream URLs to fail
before listening, without printing their contents. The local proxy connects to
the validated loopback address directly; it cannot re-resolve `localhost` after
validation or bypass validation through its socket helper.

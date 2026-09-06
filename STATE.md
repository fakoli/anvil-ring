# Anvil Ring current state

**Updated:** 2026-09-05

**Runtime:** Rust `anvil-ring` executable

**Python package:** diagnostics-only `anvil-ring-tools`

**Deployment status:** controlled single-rental pilot only; unattended and
multi-rental deployment are not approved

## Implemented request path

The complete single-tether path works in local end-to-end tests:

1. a rental-side tether creates an outbound WebSocket connection to the hub;
2. the hub authorizes it against durable registrations (or an explicit demo registration);
3. the caller frontend authenticates a bearer token before revealing tether
   state;
4. the hub sends the caller's HTTP request through the tether;
5. the tether opens one loopback connection to the configured model server;
6. the caller receives the model server's status, end-to-end headers, and body;
   and
7. completion, cancellation, revocation, overload, engine failure, or tether
   disconnection ends the affected stream and releases its tasks and queues.

The executable supports four modes:

- `anvil-ring proxy`: authenticated local reverse proxy without a tunnel;
- `anvil-ring hub`: tether WebSocket listener and optional authenticated caller
  frontend; and
- `anvil-ring tether`: outbound rental client that forwards only to loopback; and
- `anvil-ring admin`: local OS-authenticated registration and credential administration.

The Python package installs only `anvil-ring-probe-egress`; it cannot replace or
shadow the Rust runtime command.

## Verified behavior

- Caller and tether authentication are separate. Missing or incorrect caller
  authentication returns 401 before the frontend reports whether a tether is
  available.
- An authenticated caller with no available tether receives 502.
- Model-server 4xx and 5xx statuses and end-to-end headers reach the caller.
  Missing response headers never become an invented `200 OK`.
- Streaming body chunks arrive incrementally. The same timing predicate accepts
  the real proxy and rejects a deliberately buffering canary that returns an
  otherwise complete response.
- Cancelling a tether during a stream ends the caller response and records a hub
  state transition.
- Completed streams release their concurrency slots. A 66-request regression
  crosses the 64-concurrent-stream limit without turning that limit into a
  lifetime request cap.
- Hub command frames, per-stream request frames, tether WebSocket writes, and
  caller response chunks use bounded queues with tested saturation behavior.
- Only a tether `PONG` answering the hub's `PING` refreshes hub-side liveness.
  Unrelated model traffic cannot make an unresponsive control loop appear
  healthy.
- Routable plaintext `ws://` hub URLs and non-loopback serving upstreams are
  rejected. A routable deployment must terminate TLS before the Rust hub
  listener and use `wss://`.

## Test-suite review

The test suite retains behavior coverage while removing avoidable work:

- The shared HTTP chunk decoder moved under `tests/common`, so Cargo no longer
  compiles and launches it as a separate zero-test integration target.
- A duplicated full-topology tether-cancellation test was removed;
  `forward_e2e::tether_death_midstream_ends_the_caller` retains the stronger
  response-end and hub-state assertions.
- Raw-socket tests stop when the declared HTTP body is complete instead of
  waiting three, five, or eight seconds for a keep-alive connection to close.
- Proxy tests and live harnesses use operating-system-assigned loopback ports
  instead of project-wide fixed port numbers.
- Live harnesses wait for health or log readiness instead of fixed startup
  sleeps.
- The idle soak returns a failing exit status for invalid startup or observed
  connection churn and stores logs in a private temporary directory.

During the 2026-08-31 review, the complete Rust suite decreased from 19.87 seconds
at the start of the review to 10.92 seconds after the changes. The
`forward_e2e` target decreased from 9.66 to 1.68 seconds. The `proxy_e2e` target
now takes 2.77 seconds including the restored 1.2-second buffering comparison;
before the review it took 9.39 seconds without that comparison.

## Documentation review

The public documentation now:

- defines hub, tether, caller frontend, rental, serving upstream, loopback,
  registration, credentials, authorization lifetime, tunnel stream,
  server-sent events, bounded flow control, HTTP header categories, and both
  project portals in `docs/concepts.md`;
- describes architectural guarantees by behavior rather than identifier-only
  labels;
- explains how Anvil Ring carries requests while Anvil Serving owns model
  lifecycle and the mapping from stable capability names to model
  configurations;
- distinguishes the documentation portal from the main GitHub project portal
  and from a runtime administration interface;
- documents exact commands, environment variables, status codes, launch checks,
  rollback order, and the blockers for unattended or multi-rental deployment;
  and
- condenses obsolete investigation scratch notes into one current incident
  history page.

Portal checks on 2026-08-31 returned:

| URL | HTTP result | Meaning |
|---|---:|---|
| `https://github.com/fakoli/anvil-ring` | 200 | Main Anvil Ring project portal is available |
| `https://fakoli.github.io/anvil-ring/` | 404 | Documentation portal requires GitHub Pages enablement and a successful workflow run from `main` |
| `https://fakoli.github.io/anvil-serving/` | 200 | Anvil Serving documentation is available |
| `https://github.com/fakoli/anvil-serving` | 200 | Main Anvil Serving project portal is available |

A fresh check on 2026-09-05 still returned 404 for the Anvil Ring documentation
portal. The other portal results above remain historical.

Runtime and Python help include the intended documentation portal, the working
documentation-source fallback in GitHub, and the main project portal.

## Transport baseline verification (before durable administration)

The following results were produced before the durable administration changes on
2026-09-05. They remain historical transport evidence, not the final durable build. They qualify the local transport and packaging changes; they are
not evidence of a published release or an operated rental deployment.

| Command or check | Result |
|---|---|
| `cargo fmt --all -- --check` | Passed |
| `cargo test --locked --all-targets -- --test-threads=1` with Rust 1.96 | 124 passed, 0 failed, 0 ignored |
| `cargo +1.85.0 test --locked --all-targets -- --test-threads=1` | 124 passed on the declared minimum Rust version |
| Rust 1.85 full suite in Linux ARM64 Alpine | 124 passed, including the crashed-process TCP-reset fixture |
| Rust 1.85 full suite in Linux x86-64 Alpine under emulation | 124 passed, including the crashed-process TCP-reset fixture |
| `cargo clippy --locked --all-targets -- -D warnings` | Passed with no warnings |
| `RUSTDOCFLAGS='-D warnings' cargo doc --locked --no-deps` | Passed |
| Python 3.11 pytest, Ruff, lock validation | 16 tests passed; Ruff and lock validation passed |
| `actionlint` 1.7.12 | Both workflows passed |
| `uv build` | Source distribution and pure-Python wheel built |
| `mkdocs build --strict` | Passed after the release-documentation update |
| `live_stream_gate.py --events 6 --gap 0.4` | Passed; events at 0.00, 0.41, 0.81, 1.22, 1.63, and 2.03 seconds |
| `soak_tunnel.py 65 ...` | Passed; one authorization, zero resets/refusals/dial failures/tunnel errors; all processes alive |
| Both Linux musl release binaries | Correct ELF architecture; no dynamic loader; each starts as UID/GID 65534 with no network or Linux capabilities |

### Transport changes verified in this session

The tether uses Hyper's HTTP/1 framing for each engine exchange. Fragmented
headers, fixed lengths, chunked completion, early responses during stalled
uploads, repeated and byte-valued headers, and truncated responses have
full-path regression coverage. Unsupported transfer codings fail explicitly.

A failed stream produces an HTTP body error instead of a successful chunked
terminator. Independent completion/failure notification remains live when a
response queue is full. Owned tasks cancel on stream or session teardown.
Admission, completion, and upload-installation races have focused tests;
the final-chunk test exercises 10,000 producer/consumer interleavings.
A separate deterministic test exhausts Tokio's cooperative budget: `Pending`
cannot be treated as proof that the response queue is empty. Completed streams
close the receiver and drain it to its own EOF.

Startup rejects user-info and query-bearing upstream URLs without logging
secrets. Both proxy and tether connect to validated literal loopback addresses;
the proxy no longer performs a second hostname lookup after validation.

The hub enforces its lease deadline even with an unresponsive WebSocket writer.
Revocation and credential replacement cancel active sessions, and authorization
is rechecked when attaching a connection. A killed child tether with zero socket
linger produces a witnessed TCP reset and a failed caller response. This runs
on both macOS and Linux.

See [ADR-0006](docs/adr/0006-http-framing-and-terminal-state.md) and
[Testing](docs/testing.md) for the contracts and their limits.

### Historical transport-only Linux build identity

These are development binaries, built with Rust 1.85 in
`rust:1.85-alpine@sha256:4333721398de61f53ccbe53b0b855bcc4bb49e55828e8f652d7a8ac33dd0c118`.
ARM64 ran natively in the local Linux VM; x86-64 ran under emulation.
The final Linux suites and release builds used a frozen copy of the Cargo
sources; subsequent documentation edits could not overlap compilation.
They include uncommitted changes on top of `b180fe0` and are not represented as
artifacts of that commit alone.

| Target | Binary SHA-256 |
|---|---|
| `aarch64-unknown-linux-musl` | `8d5aca9f7fac4822e0d1d184ee7f11e2db4f807f57625f273481bd4ceda5e94c` |
| `x86_64-unknown-linux-musl` | `35c705bc625af6a32bb32657886e3595d48be80ad77269f4ff8a583892895bb3` |

The release workflow now prepares a tar archive preserving executable
permissions, checksums, a target-specific CycloneDX Cargo SBOM, source identity,
and signature bundles. Attestation requires successful build and test jobs and
is limited to trusted main/tag runs. This workflow has not yet been run on
GitHub for these changes, so no new signed release or attestation is claimed.

## Durable administration and routing — final local verification

The current implementation adds hub-local SQLite state and the OS-authenticated
`admin` command. Registration, credential issuance/rotation/expiry/revocation,
transactional audit, and deterministic least-loaded routing are implemented.
Private credential files are synchronized before commit; tokens are not stored
in plaintext or printed. Every 500 ms the hub refreshes the registrations; read
failure or a two-second read timeout clears authorization and cancels sessions.
All tethers in one hub must expose the same serving contract.

The final source was frozen before the Linux builds and checked byte-for-byte
against all 30 Cargo input files afterward. Verification on 2026-09-05:

| Check | Result |
|---|---|
| Full Rust stable 1.96 suite on macOS ARM64 | 149 passed, 0 failed, 0 ignored |
| Full Rust 1.85 suite on macOS ARM64 | 149 passed, 0 failed, 0 ignored |
| Full Rust 1.85 suite on Linux ARM64 musl | 149 passed, 0 failed, 0 ignored |
| Full Rust 1.85 suite on Linux x86-64 musl under emulation | 149 passed, 0 failed, 0 ignored |
| Formatting, clippy with warnings denied, rustdoc with warnings denied | Passed |
| Python pytest and Ruff | 16 passed; lint passed |
| Lock validation, Python package build, actionlint | Passed |
| Strict documentation build | Passed with the final administration and verification documentation |
| Streaming gate, six events at 0.4-second intervals | Passed: 0.00, 0.40, 0.81, 1.21, 1.61, 2.01 seconds |
| 65-second idle soak | Passed: one authorization, zero resets/refusals/dial failures/tunnel errors, all processes alive |
| Both Linux release binaries | Expected architecture, static linkage, no dynamic loader |
| Packaged checksums and private SQLite administration as UID/GID 65534 | Passed on both architectures with read-only root, no capabilities, and no network |

The new tests cover 16 storage scenarios, real CLI/hub/tether lifecycle across
restarts and store loss, stable routing with a held response, and 128 concurrent
streams across two real tethers followed by 503 when both are full. Persisted
expiry is moved into the past in the process test; lease timing has separate
live transport coverage. Credential crash durability relies on SQLite and
file/parent synchronization ordering plus forced commit-failure tests; no
power-loss or physical-disk crash qualification is claimed.

These **unsigned development artifacts** include uncommitted changes above
`b180fe0` on `codex/durable-hub-control`. Their metadata marks the source dirty.
The target-specific SBOMs include bundled SQLite's Cargo dependency.

| Target | Current binary SHA-256 | Local archive |
|---|---|---|
| `aarch64-unknown-linux-musl` | `035739d89c1257fdd2b5970f08c481b8d8ebf308b8c2a2754d8347890b8dad35` | `dist/linux-arm64/anvil-ring-0.1.0-aarch64-unknown-linux-musl.tar.gz` |
| `x86_64-unknown-linux-musl` | `cccac915ee5295d74fcd27aed60fb1f0f6ff24a6a36dd436787170bfbf3a9b02` | `dist/linux-amd64/anvil-ring-0.1.0-x86_64-unknown-linux-musl.tar.gz` |

Historical transport-only artifacts remain under `dist/transport-baseline/`.
Source review found no material correctness or security defects; its two-tether
capacity coverage gap was closed by the 128-stream test. See
[ADR-0007](docs/adr/0007-durable-hub-administration.md) for the operating contract.

## Work still required before unattended or multi-rental deployment

1. Document and operate TLS termination, certificate issuance, certificate
   rotation, and caller-frontend network restrictions in the target environment.
2. Run the exact-target TLS diagnostic from every intended provider image as the
   same unprivileged user that will run the tether.
3. Run the updated release workflow from a clean, reviewed commit and verify
   the downloadable checksums, SBOM, signature bundles, and build provenance.
   Local builds and packaging pass; trusted GitHub signing and publication
   remain unverified.
4. Build and run the combined vLLM-and-tether container on an authorized Linux
   GPU host with a real model. The local supervisor test uses controlled fake
   children and cannot replace that run.
5. Publish the documentation portal by enabling GitHub Pages with GitHub Actions
   and verifying the deployed URL.
6. Define measured capacity, latency, availability, recovery-time, and incident
   response objectives before increasing traffic or rental count.

The canonical commands and environment-specific checks are in
[`docs/testing.md`](docs/testing.md) and
[`docs/production-rollout.md`](docs/production-rollout.md).

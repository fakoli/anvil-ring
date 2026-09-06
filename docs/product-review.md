# Product and production review

**Review date:** 2026-08-31
**Scope:** runtime behavior, CLI contract, HTTP responses, deployment artifacts,
tests, release workflows, and public documentation

## Executive assessment

Anvil Ring has a clear product boundary and a tested request-transport path:
make a loopback model-serving endpoint on short-lived infrastructure reachable
without granting the rental inbound network access or routing authority. The
implemented Rust path preserves HTTP status, headers, streaming cadence, bounded
flow control, and explicit failure.

The repository is suitable for a controlled single-rental pilot after the
documented launch checks pass. It is not yet suitable for an unattended or
fleet-wide rollout. The remaining gap is primarily verification and operational
evidence rather than packet transport: durable administration and deterministic
multi-tether routing are locally verified, while TLS termination is external,
hosted signing and publication remain open, and the local release workflow now
produces checksum, SBOM, and provenance evidence. The combined vLLM-and-tether container
manifest or build-provenance record, and the combined vLLM-and-tether container
image has not been built and exercised with a real GPU in this workspace.

## Product intent

The primary job is:

> When a model server runs on short-lived hardware outside the trusted network,
> make it callable from trusted infrastructure without opening the rental to
> inbound traffic or turning it into a general-purpose network peer.

Three principles define the product:

1. The rental initiates the connection and listens on nothing.
2. The trusted hub owns registration, authorization, routing, revocation, and
   observable state.
3. A model call remains an HTTP streaming call: upstream status, headers, body
   order, delivery cadence, and failure are preserved rather than disguised.

Anvil Ring owns reachability. [Anvil Serving](https://fakoli.github.io/anvil-serving/)
owns model lifecycle, capability aliases, qualification, and serving policy.
Ring may carry traffic to an Anvil Serving gateway, but it must not silently
absorb those responsibilities.

## Review method and limits

The review used:

- direct inspection of the Rust hub, tether, proxy, frame codec, frontend, and
  header/stream handling;
- comparison of shipped CLI help with runtime parsing and contract tests;
- inspection of the Python diagnostic, package metadata, Docker supervisor,
  release workflow, schemas, ADRs, and operator documentation; and
- unit, integration, cancellation, streaming, queue-bound, packaging, and
  deployment-supervisor tests.

The product has no graphical runtime interface, so there is no application UI
to audit. The documentation portal publishes guides; it cannot register, route,
or revoke a running tether. Real provider egress, Linux musl linking, TLS
termination, GPU execution, and production-load behavior require checks in
their target environments.

## Users and success criteria

| User | Job | Success signal |
|---|---|---|
| Rental operator | Bring one rental online without changing provider inbound rules | Hub records `Up`; `/healthz` reports `tether-up` |
| Model-serving operator | Keep vLLM, SGLang, or the Anvil Serving gateway loopback-only | Authenticated requests arrive and stream incrementally |
| API caller | Call an OpenAI-compatible route and receive the upstream's real outcome | Upstream 2xx/4xx/5xx, headers, and body reach the caller without fabrication or truncation |
| Release engineer | Produce an identifiable, portable rental-side artifact | The reviewed musl binary has the expected architecture, no loader, and a recorded checksum |
| Provider evaluator | Determine whether the intended hub is reachable from the final rental image | Exact-target TLS probe produces a redacted evidence record and exit status 0 |
| Incident operator | Distinguish hub, tether, and model failure and roll back safely | Health, state transitions, status codes, and artifact identities support a bounded response |

## Journey assessment

### Discover and understand

The README, documentation portal, CLI help, and package metadata now describe
the same three runtime modes, the separate diagnostics command, the Anvil
Serving relationship, and the single-tether release boundary. Historical
investigation notes and superseded decisions are separated from current
runbooks.

**Assessment:** clear and internally consistent.

### Qualify a provider

The operator runs `anvil-ring-probe-egress` as the final image's unprivileged
user. With `--no-defaults`, the exit status qualifies only the explicitly named
TLS hub target. The tool accepts no secret, redacts the local hostname by
default, and reports WSS-specific guidance rather than reopening the superseded
Chisel/SSH decision.

**Assessment:** suitable as a pre-deployment reachability check; a saved passing
result is still required for every intended provider image.

### Bring up the hub and tether

Separate caller and tether credentials are supplied outside process arguments. The hub
can use durable registrations managed by `anvil-ring admin`; the explicit demo
compatibility mode still uses a process-local registration, and the tether rejects a routable plaintext
WebSocket URL or non-loopback upstream. The supported pilot places the hub
listener behind external TLS and admits one rental.

**Assessment:** supportable for a controlled pilot; durable multi-rental use
awaits target-dependent rollout evidence.

### Call the serving endpoint

The caller authenticates before the frontend reveals tether state. The hub
chooses the tether; the caller cannot supply a target. Request and response
queues are bounded. The tether connects only to loopback. Engine status and
end-to-end headers are preserved, hop-by-hop headers are removed, chunked coding
is handled at the owning hop, and body chunks stream incrementally.

When Anvil Serving is upstream, it retains capability and model authority. Ring
forwards the caller's authorization header unchanged and does not perform token
exchange.

**Assessment:** the request path preserves the required behavior and has an
explicit upstream-authentication integration constraint.

### Lose a tether and recover

The tether session owns its WebSocket writer and engine tasks. Session loss ends
in-flight response bodies so callers can retry. Hub liveness is refreshed only
by a tether PONG answering a hub PING, and the tether redials with bounded
backoff.

**Assessment:** cancellation and session cleanup are covered. A fixture that
crashes the tether process and produces a TCP reset remains unimplemented.

### Build and release

CI runs all Rust targets, Python tests, and Ruff, and builds static musl binaries
for aarch64 and x86-64. The combined container starts loopback-only vLLM beside the
outbound tether under a non-root identity. The documentation workflow performs
a documentation build in which warnings fail the job.

**Assessment:** reproducible build contracts exist. General production still
needs published checksum/provenance material, a real target artifact check, and
a real GPU image run.

## Findings and disposition

| Severity | Finding | Disposition |
|---|---|---|
| P0, resolved | Cancelling a tether could detach its WebSocket writer and leave callers waiting | Writer ownership and common session teardown are regression-tested |
| P0, resolved | Public CLI and README described an obsolete scaffold | Rust is the sole `anvil-ring` runtime; Python installs only the named diagnostic |
| P0, resolved | CI treated the fixed tether-disconnection behavior as an expected failure | All Rust targets are required to pass |
| P0, resolved | The combined container did not start the advertised engine-plus-tether path | Non-root two-process supervisor and deployment contract tests are present |
| P1, resolved | High-volume command and body paths were unbounded | Explicit queue capacities, flow control, and saturation behavior are tested |
| P1, resolved | Completed/abandoned streams could retain capacity or upstream tasks | Completion cleanup and abort-on-drop ownership are regression-tested |
| P1, resolved | Traffic unrelated to the control loop could refresh liveness | Only the expected PONG proves bidirectional liveness |
| P1, resolved | Schema and ADR language described a non-live protocol | The schema is historical; ADR-0004 is the selected transport |
| P2, resolved | Modes silently ignored additional process arguments | Unexpected arguments fail with status 2 and point to environment configuration |
| P1, resolved | Durable registration, lifecycle, audit, and deterministic multi-tether routing were locally verified on supported Rust/Python toolchains | Target-dependent rollout evidence remains open |
| P1, open | TLS termination and certificate operations are external | Requires a supported ingress runbook and live qualification |
| P1, open | Hosted signing and publication have not run | Local workflow produces checksum, SBOM, and provenance evidence; hosted release metadata remains a gate |
| P1, open | No intended-provider evidence or real combined-container GPU run is recorded | Blocks environment qualification |
| P2, open | A crashed-process TCP-reset fixture is absent | Cancellation and terminal socket errors are covered, but process-crash evidence should be added |
| P2, resolved | The streaming timing assertion had no reliable negative control | The real proxy passes and a complete deliberately buffered response fails the same predicate |
| P2, open | The runtime has no browser administration interface | The local command is the administration surface; the documentation portal does not administer runtime state |

## Experience quality

| Dimension | Assessment |
|---|---|
| Clarity | Commands, package roles, portal navigation, product boundary, and deployment status are explicit |
| Feedback | `Up`, `Down`, and `Revoked`, `/healthz`, and distinct caller statuses expose actionable state |
| Consistency | One Rust runtime command, one diagnostic command, and one environment/file configuration model |
| Error prevention | Non-loopback upstreams, routable plaintext hubs, missing authentication, unexpected process arguments, and queue saturation fail closed |
| Recovery | Tether redial is automatic; dead sessions end callers; pilot rollback has an explicit order |
| Accessibility | CLI output is plain text, non-interactive, and not color-dependent; documentation has searchable static navigation |
| Trust | Upstream failures are not rewritten as success, and open production blockers are stated directly |

## Recommended next implementation phase

Complete target-dependent evidence for the locally verified tether records,
credential lifecycle, audit events, and deterministic routing. In parallel,
complete the release-engineering and environment checks in [Production rollout](production-rollout.md).

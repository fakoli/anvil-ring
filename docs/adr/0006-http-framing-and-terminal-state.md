# ADR-0006: HTTP framing and terminal stream state

- **Status:** Accepted
- **Date:** 2026-09-05
- **Refines:** ADR-0004

## Evidence

New full-path tests reproduced corrupted fixed-length bodies, leaked chunk
framing from fragmented headers, delayed completion on keep-alive connections,
successful-looking truncation, and stalled uploads that hid upstream rejection
or tether failure. Queue-saturation tests showed that socket writes could prevent
lease expiry. These failures contradicted the existing architectural guarantees.

## Decision

Use the existing Hyper HTTP/1 client for each tether-to-engine exchange. Preserve
status and header octets, regenerate per-hop request framing, and stream body
frames through the existing bounded queues. Reject unsupported transfer codings
before exposing an upstream response. Uploads and responses progress independently.

Keep terminal stream state separate from the data queue. Clean completion drains
all queued data; failure produces an HTTP body error. Acquire completion before
polling the queue so a concurrent final enqueue cannot be lost. Close admission
and drain sessions atomically. Cancel uploads at terminal state and on caller
abandonment.

Both session writers are owned tasks and are cancelled on teardown. Control
processing never waits for queue capacity. The hub enforces its lease independently
of tether behavior. Rotation invalidates old credentials, and attachment rechecks
authority under the same locks used by registration.

## Verification and boundary

`cargo test --locked --all-targets -- --test-threads=1` includes full-path HTTP,
real process crash/TCP reset, saturated writer, lease, revocation, admission, and
completion-race regressions. These tests use loopback fake engines to control wire
behavior. They do not replace provider egress, Linux artifact, or real GPU/model
qualification.

The in-memory registry remains a separate control-plane limitation. This decision
does not claim durable administration or fleet readiness.

Tokio may return `Pending` before inspecting a nonempty response queue when its
cooperative scheduling budget is exhausted. The completed-body path therefore
closes the receiver and drains it to `Ready(None)`; it never treats `Pending` as
an empty queue. This was reproduced in the Linux ARM64 stress test and has an
additional deterministic budget-exhaustion regression.

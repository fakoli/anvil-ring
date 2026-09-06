# Architecture decision record

Architecture decision records explain why Anvil Ring has its current boundaries.
An accepted ADR describes the active direction unless a later record explicitly
supersedes it. Historical evidence remains available but must not be read as the
current operator contract.

| ADR | Status | Decision or evidence |
|---|---|---|
| [0001](0001-outbound-tether-not-mesh.md) | Accepted | Use an outbound tether rather than a mesh network |
| [0002](0002-transport-open.md) | Superseded by 0004 | Historical comparison of Chisel, SSH reverse forwarding, and an owned tunnel |
| [0003](0003-egress-evidence-run1.md) | Historical evidence | Workstation control run for the provider egress diagnostic |
| [0004](0004-rust-request-transport-container.md) | Accepted | Implement the request-transport runtime in Rust and support an engine-and-tether container image |
| [0005](0005-streaming-negative-control.md) | Accepted; automated comparison restored | Require the streaming timing check to accept the real proxy and reject a deliberately buffering canary |
| [0007](0007-durable-hub-administration.md) | Implementation in progress | Durable local administration, credential lifecycle, audit, and deterministic multi-tether routing; under verification |

Current operational guidance lives in the [operator guide](../operator-guide.md),
[CLI reference](../cli-reference.md), and
[production rollout](../production-rollout.md).

- [ADR-0006: HTTP framing and terminal stream state](0006-http-framing-and-terminal-state.md)
- [ADR-0007: Durable hub administration and routing](0007-durable-hub-administration.md)

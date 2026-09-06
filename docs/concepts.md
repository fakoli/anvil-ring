# Concepts and terminology

This page defines the product terms used throughout the Anvil Ring
documentation.

| Term | Meaning in Anvil Ring |
|---|---|
| Caller | An application or operator that sends an HTTP request to the hub's caller frontend. A caller presents the bearer token configured in `ANVIL_RING_CALLER_TOKEN`. |
| Hub | The trusted, always-on Anvil Ring process. It accepts tether WebSocket connections, authenticates tethers, tracks whether a tether is available, and optionally exposes the caller frontend. |
| Caller frontend | The HTTP listener created by `anvil-ring hub` when `ANVIL_RING_FRONTEND_LISTEN` and `ANVIL_RING_CALLER_TOKEN` are configured. It authenticates callers and sends their requests through an available tether. |
| Tether | The `anvil-ring tether` process on the rental. It creates an outbound WebSocket connection to the hub and forwards hub requests to one loopback serving upstream. It opens no listening port. |
| Rental | The short-lived host that runs the model server and tether. The supported example is a disposable GPU instance from an infrastructure provider. |
| Serving upstream | The HTTP server that receives the forwarded request from the tether. It may be vLLM, SGLang, or an Anvil Serving gateway and must listen on a loopback address. |
| Loopback | A network address reachable only from the same host, such as `127.0.0.1`. Binding the serving upstream to loopback prevents direct network access to the model server. |
| Registration | A durable hub-side record that associates a stable tether identifier with a credential, label, active state, and expiry. Explicit demo mode still creates the compatibility-only in-memory `demo-1` record. |
| Tether credential | The secret shared between the trusted hub configuration and one tether. The hub uses its SHA-256 digest to authorize the tether. This secret is separate from the caller token. |
| Caller token | The bearer token the caller frontend requires in the HTTP `Authorization` header. It does not authorize a tether. |
| Lease | The bounded period for which a tether authorization remains valid. The tether reauthorizes before the lease expires; revocation prevents a new lease. |
| Tunnel stream | One caller request and its response carried as identified binary frames over the shared tether WebSocket. A single tether can carry multiple streams concurrently. |
| Server-sent events (SSE) | A streaming HTTP response format commonly used for model tokens. Anvil Ring forwards each response chunk when it arrives instead of waiting for the entire body. |
| Backpressure | Flow control that pauses a producer when a bounded queue is full. Anvil Ring uses bounded queues so a slow caller, hub, tether, or engine cannot create unlimited in-memory buffering. |
| Hop-by-hop header | An HTTP header that applies to one network connection, such as `Connection` or `Transfer-Encoding`. Each Anvil Ring HTTP hop removes or regenerates these headers. |
| End-to-end header | An HTTP header that describes the request or response across intermediaries, such as `Content-Type`. Anvil Ring preserves these headers, subject to its security filtering. |
| Request-transport runtime | The Rust hub, tether, proxy, framing, streaming, authentication, and lifecycle code that carries requests. |
| Administration service | The local OS-authenticated `anvil-ring admin` command and SQLite state used to create, list, rotate, revoke, and audit tether registrations. Durable administration and deterministic routing are locally verified; target-dependent rollout gates remain open. |
| Documentation portal | The published guide at `https://fakoli.github.io/anvil-ring/`. It contains usage and design information but cannot administer a running service. |
| Main project portal | The GitHub repository at `https://github.com/fakoli/anvil-ring`. It contains source code, releases, issues, and contribution history. |

## Anvil Serving relationship

Anvil Serving manages model lifecycle and capability routing. A *capability* is
a stable API-facing name that Anvil Serving maps to a qualified model or serving
configuration. Anvil Ring does not create that mapping. It carries the caller's
HTTP request to the loopback endpoint selected by the operator.

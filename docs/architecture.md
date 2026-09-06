# Architecture

## Product responsibility

Anvil Ring owns an authenticated reachability path, not the serving lifecycle.
It transports HTTP from a trusted hub to one loopback upstream on a disposable
host.

[Anvil Serving](https://fakoli.github.io/anvil-serving/) is the adjacent product
that stores model artifacts, starts and stops serving processes, records
qualification, and maps stable capability names to model configurations. A
deployment may point Ring directly at vLLM or SGLang, or at a
loopback Anvil Serving gateway. That choice does not change Ring's trust model.

## System shape

Anvil Ring is an asymmetric HTTP tunnel. The disposable rental always dials the
hub; the hub never dials or discovers the rental. Callers reach an authenticated
HTTP frontend on the hub. Multiple identified request streams share the tether's
single WebSocket, and the tether turns each stream back into a separate loopback
HTTP connection to the model engine.

| Component | Runs where | Network behavior | Owner |
|---|---|---|---|
| Caller frontend | Trusted hub | Listens for authenticated HTTP calls | `cargo/src/frontend.rs` |
| Registry/session hub | Trusted hub | Accepts outbound tether WebSockets | `cargo/src/hub.rs` |
| Durable administration | Trusted hub host | Local OS-authenticated registration and audit commands | `cargo/src/admin.rs` |
| Tether | Disposable rental | Dials the hub; opens no listener | `cargo/src/tunnel.rs` |
| Model engine | Disposable rental | Listens on loopback only | vLLM/SGLang |
| Local proxy mode | Trusted/local environment | Authenticated single-upstream reverse proxy | `cargo/src/proxy.rs` |
| Egress probe | Provider candidate | Makes anonymous outbound test connections only | `anvil_ring/probe_egress.py` |

## Deployment shapes

### Direct engine

The tether's loopback upstream is a vLLM or SGLang HTTP endpoint. The Ring hub
is the caller authentication boundary, and the model engine remains private to
the rental.

### Anvil Serving integration

The tether's loopback upstream is the Anvil Serving capability gateway. Ring
provides reachability; Anvil Serving continues to map stable capability names
to models and own serving policy, model readiness, and qualification.

The caller's `Authorization` header is authenticated by the Ring frontend and
forwarded end to end because it is not a hop-by-hop header. If the Anvil Serving
gateway also requires authentication, its bearer-token contract must therefore
be intentionally aligned with the caller credential used at the Ring frontend,
or another trusted boundary must terminate and replace authentication before
the request reaches Ring. Ring does not rewrite one caller token into a separate
upstream token.

### Local proxy

`anvil-ring proxy` omits the tether and hub. It exists to isolate
authentication, header handling, status preservation, and streaming behavior.
It is not the rental-side production topology.

## Trust boundaries

### Caller to hub

The frontend requires a caller bearer token before it reveals whether a tether
exists. An unauthorized caller receives 401. An authorized caller with no live
tether receives 502. The caller cannot name a tether; routing policy stays on the
hub.

The local `anvil-ring admin` command manages SQLite registration state. The hub
loads a complete validated snapshot before accepting sessions and refreshes it
periodically. Registration changes, revocation, rotation, and expiry are applied
with session checks; state read failure clears authorization and tears down
affected sessions.

For each request, routing considers only active, unexpired, responsive sessions
with available stream and command capacity. It selects the session with the fewest
active streams, breaking ties by registration ID in lexical order. Admission and
selection reserve capacity together; saturation returns 503 and a disconnected
eligible pool returns 502.

`GET /healthz` is the only unauthenticated route. It reports hub availability and
whether any tether is currently `Up`; it does not test model readiness.

### Tether to hub

The first binary WebSocket message must be `HELLO` with a credential. The hub
maps only that credential to a pre-registered tether and returns a bounded lease.
The tether cannot request a host, port, permission, lease, or route. A tether
sending `OPEN`, `RESP_HEAD` in the wrong direction, or another authorization
frame mid-session is disconnected as a protocol violation.

### Tether to engine

The configured upstream is parsed and checked at startup and again for every
stream. Only loopback hosts are accepted. Each request gets its own TCP connection
to the engine. Hyper owns HTTP framing on this hop, including fragmented
headers, chunked transfer coding, content lengths, and premature EOF. Request
uploads and responses progress independently; an engine can reject an unfinished
upload, and a lost tether can end it immediately.

## Request lifecycle

1. The caller sends authenticated HTTP to the hub frontend.
2. The frontend selects an `Up` tether without reading a caller-supplied target.
3. The registry allocates a monotonic 16-bit stream id and registers response
   state.
4. The hub admits an `OPEN` frame only if the bounded command queue has room.
5. A cancellable upload task sends request `DATA` through the bounded hub writer
   queue; `HALF_END` finishes the HTTP request body without cancelling its response.
6. The tether opens a loopback engine connection and pumps request and response
   directions concurrently.
7. The tether sends a distinct `RESP_HEAD`, then body-only `DATA`, then `END`.
8. The hub republishes the engine status and end-to-end headers, filters
   hop-by-hop headers, and streams body chunks to the caller.
9. Normal `END`, caller abandonment, engine failure, tether death, or revocation
   removes the stream and releases its tasks/channels.

The frontend waits for a real upstream response head. No head is a 502, never an
invented 200. Engine 4xx and 5xx statuses pass through unchanged.

## Framing

The live wire contract is the binary `Frame` codec in
`cargo/src/frames.rs`, covered by round-trip, partial-frame, unknown-type,
oversize, and control-frame tests.

| Frame | Direction | Purpose |
|---|---|---|
| `Hello` | Tether → hub | Present registration credential |
| `Welcome` | Hub → tether | Confirm authorization and lease duration |
| `Open` | Hub → tether | Start a request with its HTTP head |
| `Data` | Both | Carry request or response body bytes for a stream |
| `HalfEnd` | Hub → tether | Finish the request body while leaving the response open |
| `RespHead` | Tether → hub | Carry the engine's status line and headers separately |
| `End` | Both | Complete or abort one stream |
| `Ping` / `Pong` | Both | Prove application-loop liveness |
| `GoAway` | Both | End a session for revocation, lease refresh, or shutdown |

`schemas/tether-v1.json` is retained as a historical registration-manifest
design. It is not the live frame protocol and is not shipped in the Python tools
package.

## Streaming and backpressure

Every potentially high-volume hop has an explicit bound:

| Queue | Capacity | Saturation behavior |
|---|---:|---|
| Hub command frames → tether WebSocket | 256 frames | New `OPEN` is refused with 503; accepted request data waits for capacity |
| Tether request frames → one engine pump | 64 frames per stream | That stream is aborted and receives `END`; other streams continue |
| Engine pumps → tether WebSocket writer | 256 frames | Engine reads pause until the writer drains |
| Hub response chunks → one caller | 64 chunks per stream | A full queue fails that stream; control messages and other callers keep progressing |

The hub also admits at most 64 concurrent streams per tether; excess requests
receive 503 before an `OPEN` is sent. Both WebSocket writers are owned tasks,
so a blocked socket cannot postpone revocation, lease expiry, or session teardown.
Control frames use nonblocking queue admission; failure to enqueue a required
control frame closes the session.

The system bounds frame count, not total encoded bytes. Frame decoding rejects
oversized payload lengths before allocating, and HTTP response heads are capped
at 64 KiB while incomplete.

## Lifecycle and liveness

- `AbortOnDrop` owns the tether WebSocket writer so cancelling the parent session
  cannot detach a socket-holding child task.
- `StreamHandle` owns each engine pump's abort handle. Removing a stream cancels
  its upstream work immediately.
- A completion channel removes normally completed stream handles, so the
  64-stream limit is concurrent capacity rather than a 64-request lifetime cap.
- The hub sends application `PING` frames and refreshes `last_seen` only on the
  tether's `PONG`. Engine traffic alone is not liveness proof.
- Clean `END` drains queued chunks before completing the caller response. An
  error `END` or session loss produces an HTTP body error, never a successful
  terminator for partial output. An independent terminal signal works even when
  the chunk queue is full.
- Session teardown closes stream admission and drains existing streams under one
  lock, so a concurrent request cannot register after cleanup.
- Terminal responses cancel unfinished uploads even if callers retain the response
  body. Upload installation is synchronized with response completion.
- A generation stamp prevents an old reconnecting session from detaching its
  newer replacement.
- The tether reauthorizes before lease expiry and redials with bounded backoff.
  The hub independently enforces the lease deadline, including for a peer that
  keeps sending `PONG` or stops reading the WebSocket.
- Registration replacement invalidates the old digest and cancels its session.
  Attachment rechecks the credential atomically with registration and revocation.

## Authentication and secret handling

- Secrets are environment or file inputs, never CLI options or URL parameters.
- The tether prefers `ANVIL_RING_CRED_FILE`; `ANVIL_RING_CREDENTIAL` is the
  fallback.
- The hub stores a SHA-256 digest lookup, not plaintext. This is safe only for
  high-entropy machine credentials; human passwords would require a password KDF.
- Logs may include tether ids, peer addresses, state transitions, and a short
  digest prefix, but never the credential or caller token.
- The combined-container supervisor removes every `ANVIL_RING_*` variable from the
  vLLM child environment; only the tether receives hub and credential inputs.
- Plain `ws://` requires a literal loopback IPv4 or IPv6 address. Production
  uses `wss://`. User information, query parameters, and fragments are rejected
  before dialing; URL credentials cannot appear in error messages.

## Runtime modes

### `anvil-ring admin`

Uses `ANVIL_RING_STATE_DIR` to initialize and administer durable registrations.
Credential issuance and rotation require `ANVIL_RING_CREDENTIAL_OUT`; output is
created privately and credentials are never printed.

### `anvil-ring hub`

In durable mode, reads validated registrations from `ANVIL_RING_STATE_DIR`,
accepts tether connections, and optionally starts the caller frontend. In
explicit demo mode it registers one `demo-1` tether from
`ANVIL_RING_DEMO_CREDENTIAL`.

### `anvil-ring tether`

Reads the hub URL, credential, and loopback upstream from environment/file,
dials out, authenticates, serves multiplexed streams, and redials on loss.

### `anvil-ring proxy`

Runs a local authenticated reverse proxy without the tunnel. It is useful for
isolating HTTP/header/streaming behavior and as a local fallback, but it is not
the rental-side deployment mode.

## Current limits

- Durable administration and deterministic routing are locally verified.
  Required target-dependent rollout evidence remains tracked in
  [ADR-0007](adr/0007-durable-hub-administration.md).
- TLS termination is an external deployment responsibility.
- HTTP/1.1 only; no WebSocket upgrade passthrough or arbitrary TCP forwarding.
- Transfer codings other than `chunked` are rejected with 502 before exposing a
  response head. End-to-end `Content-Encoding` is preserved.
- The combined container image currently starts vLLM and the tether. An SGLang
  image needs its own pinned base image and startup command, even though the
  tether can proxy either loopback HTTP server.
- No browser-based runtime administration interface. The documentation portal
  publishes guides, and the main project portal contains source and releases;
  neither can register, list, route, or revoke a running tether.

These limits permit a controlled single-rental pilot when the
[production checks](production-rollout.md) pass. Target-specific TLS, provider,
GPU/model, artifact, monitoring, and objective gates remain open for unattended
or fleet-wide rollout.

Configured upstream URLs must use HTTP loopback and contain no user-info, query,
or fragment. Startup logs only the validated socket address. The local proxy
and tether both connect to that literal address, without a later DNS lookup.
Caller request paths and queries continue to pass through unchanged.

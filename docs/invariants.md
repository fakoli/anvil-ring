# Architectural guarantees

These requirements define Anvil Ring's security and runtime behavior. A change
that breaks one changes the product's design and requires an architecture
decision record that explains the reason, tradeoffs, and migration plan.

## Rental connections are outbound only

The remote (disposable) side MUST NOT listen on any port or accept any inbound
connection. Every connection is initiated by the remote side toward the hub.
This lets an operator reach a rental without enabling provider port forwarding.

## The rental process requires no elevated privileges

The client MUST run as an unprivileged user with no `CAP_NET_ADMIN`, no TUN
device, no kernel module, and no system service. If a feature only works with
root on the remote host, it violates the product boundary.

## Credential revocation ends access

A revoked token MUST stop working within one reconnect interval, and MUST NOT
be able to keep an already-established tunnel alive indefinitely. Idle tunnels
have a bounded lifetime.

## Rental identity is registered at runtime

Ephemeral hosts get a short-lived credential bound to a registration, not a
long-lived key committed to an image. Durable administration MUST support
registration and rotation without baking identity into the rental image. The
local `anvil-ring admin` command and SQLite state implement this control; its
persistence, lifecycle, and multi-tether behavior are locally verified; target
dependent rollout evidence remains open.

## The hub controls authorization and routing

Authorization and routing decisions are made on the always-on side only. The
remote side is never trusted to select a registration, caller population,
routable target, lease, or permission.

## A disconnected tether ends affected requests

A lost tether MUST be distinguishable from a slow one within a stated timeout,
and MUST be reported as an explicit state transition rather than an endpoint that
hangs. Every in-flight caller response must end when its tether disconnects.

## The rental uses one self-contained executable

The remote-side binary MUST be one statically linked, self-contained artifact
that requires **no separately installed runtime or package** on the host.
Vendored crate dependencies compiled into that artifact are permitted, and
`Cargo.lock` is committed so the exact tree is reviewable.

The client runs in third-party environments. Installing packages at deployment
time would add a separate supply-chain operation on the rental. A static binary
and committed lockfile keep the deployed dependency set reviewable.

## Secrets stay out of URLs, command arguments, and logs

Registration credentials and caller tokens MUST NOT appear in URLs, logs, or
command-line arguments. Process arguments are visible in tools such as `ps` and
may remain in shell history on a shared rental. Runtime secrets come from
environment variables or files. Caller authentication uses the HTTP
`Authorization` header and must not move into a query parameter or another
cache-prone location.

## Streaming responses are delivered incrementally

Every chunk received from the inference engine MUST be flushed to the caller
immediately. Buffered SSE presents as elevated time-to-first-token and is
indistinguishable from a slow model. A regression test MUST measure incremental
arrival rather than only checking the completed response body.

## The serving upstream listens on loopback only

The direct vLLM/SGLang engine or Anvil Serving gateway MUST bind loopback and
MUST NOT be published from the rental. Ring enforces caller authentication at
its frontend. Additional upstream authentication is permitted only when its
token contract is intentionally compatible with the end-to-end
`Authorization` header; Ring does not perform credential exchange.

## The caller receives the upstream's actual result

The caller MUST receive the engine's real status and end-to-end headers. No
response head is a tunnel failure, not an empty `200 OK`; an incomplete body is
an explicit end/failure, not a successful-looking truncation. Each HTTP hop may
remove or regenerate hop-by-hop framing, but it MUST NOT invent model output or
rewrite an engine error into success.

# Invariants

These are the properties anvil-ring must never violate. A change that breaks one
is a design change, not a bug fix, and needs an ADR.

## I-1 — Outbound only
The remote (disposable) side MUST NOT listen on any port or accept any inbound
connection. Every connection is initiated by the remote side toward the hub.
This is what makes "no port-forwarding on a cloud we don't control" true rather
than aspirational.

## I-2 — No privileges on the remote side
The client MUST run as an unprivileged user with no `CAP_NET_ADMIN`, no TUN
device, no kernel module, and no system service. If a feature only works with
root on the remote host, the feature is wrong.

## I-3 — Revocation is effective
A revoked token MUST stop working within one reconnect interval, and MUST NOT
be able to keep an already-established tunnel alive indefinitely. Idle tunnels
have a bounded lifetime.

## I-4 — Identity is re-registrable, not baked
Ephemeral hosts get a short-lived credential bound to a registration, not a
long-lived key committed to an image. Restarting the container re-registers; it
does not reuse a stale identity silently.

## I-5 — The hub is the only authority
Authorization decisions (which tether may expose which port, to whom) are made
on the always-on side only. The remote side is never trusted to self-describe
its own permissions.

## I-6 — A dead tether is observable
A lost tether MUST be distinguishable from a slow one within a stated timeout,
and MUST surface as an explicit state transition rather than an endpoint that
hangs. Silent half-open tunnels are the failure mode this project exists to
avoid, since a hung model endpoint looks identical to a slow model.

## I-7 — Stdlib-only client
The client uses the Python standard library only, matching `anvil-events`.
Rationale: the client ships into third-party environments where a transitive
dependency is supply-chain surface we are importing onto someone else's GPU box.

## I-8 — No secret in a URL, header-for-cache, or argv
Tokens MUST NOT appear in URLs, must not be echoed in logs, and MUST NOT be
passed as command-line arguments (argv is visible in `ps` and in shell history on
the shared rental host). Read from env or a file descriptor.

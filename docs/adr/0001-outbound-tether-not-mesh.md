# ADR-0001: Outbound-initiated tether, not a mesh network

- **Status:** Accepted
- **Date:** 2026-08-27
- **Deciders:** operator

## Context

We want to deploy a container running vLLM or SGLang to any GPU rental provider
and reach its serving endpoint from our own network, which is a private
WireGuard-based overlay with MagicDNS naming.

The default answer is to join the rental to that overlay. Constraints that make
that a poor fit:

1. Rentals are **publicly reachable** by default; the hard-NAT case a mesh exists
   to solve mostly does not apply to the disposable side.
2. We cannot open inbound ports on a provider we do not administer.
3. We often cannot install a system service or obtain a TUN interface.
4. The node is **ephemeral** — identity must be re-registrable, not baked.
5. The workload is a single long-lived HTTP endpoint, not a many-to-many mesh.

## Decision

Do not build or embed a mesh. Build a **tether**: the remote side maintains one
outbound connection to an always-on hub we operate, and mapped ports are reached
by multiplexing over that connection. Directionality is enforced by invariant
(I-1), not by convention.

The overlay network remains where it already is — on the hub side, which we
already operate and audit. anvil-ring terminates *into* that network and thereby
inherits good names without asking any rental to host a network daemon.

## Consequences

**Positive**
- No inbound firewall rules, no provider networking permissions, no root.
- Blast radius of a compromised rental is one mapped port with a revocable
  credential (I-3), not membership in a network.
- The hub is a single enforcement point for authZ (I-5).

**Negative, accepted**
- Hub is a single point of failure for new connections. Existing tunnels survive
  a hub restart only if the transport supports resumption; this is an open risk.
- Throughput is bounded by one TCP-ish connection through a relay-grade host.
  Model *responses* are streamed JSON, so this is acceptable; bulk artifact
  transfer is explicitly out of scope.
- We own availability of the hub, which we did not own when a third party ran the
  relay.

## Alternatives rejected

- **Embed a VPN node per rental.** Buys hole punching and relay anycast that this
  workload does not need, and requires a privileged daemon on an unowned host.
- **Rebuild the mesh from primitives (libp2p DCUtR / Circuit Relay v2).** Has the
  right pieces; would require reimplementing the relay placement, control plane,
  and ACL model. Wrong substrate for one HTTP server with long-lived streams.
- **Provider-side public exposure with a token.** Rejected outright: puts a
  permanent public attack surface on a machine holding model weights and no
  durable patch story.

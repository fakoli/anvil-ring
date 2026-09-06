# Origin story

Anvil Ring began with a narrow operational problem: a model server was ready to
run on rented GPU capacity, but the rental could not safely join the trusted
network or accept inbound traffic.

This page explains why the product is an outbound tether, how it works with
Anvil Serving, and why it is named Anvil Ring.

## The problem

A model-serving host that an operator owns has a stable identity, a known
network boundary, and a lifecycle the operator controls. A GPU rental has the
opposite properties:

- the host is short-lived and its address may change with every allocation;
- provider policy may prevent inbound firewall changes or port publication;
- the workload may run without root privileges, system services, or a TUN
  device; and
- persistent machine identity and long-lived private keys are poor fits for a
  disposable container.

The requirement was not general network membership. It was one authenticated,
streaming HTTP path to a loopback model endpoint.

## The design observation

The rental is still able to initiate outbound connections. That changes the
topology.

Instead of asking the trusted side to discover or dial the rental, the rental
dials an always-on hub and keeps an authenticated session open. Caller requests
travel from the hub over that existing session. The rental never opens a
listener, and it never asks for a target, route, or permission.

This is intentionally smaller than a VPN:

- no overlay address space;
- no virtual network interface;
- no many-to-many routing;
- no remote network daemon with broad reach; and
- no authority on the disposable side to expand access.

The smaller contract makes the security boundary testable. The hub owns
registration, authorization, routing, leases, and revocation. The tether owns
only an outbound connection and access to one loopback upstream.

## Why “Ring”

The name refers to the audible ring of an anvil: a signal that carries beyond
the forge while the anvil itself remains fixed in place. In the same way, Anvil
Ring does not move the serving system or make the rental a general network peer.
It provides a narrow signal path back to infrastructure the operator already
trusts.

The name also states the product's narrow question: can an authorized caller
reach a serving endpoint that accepts no inbound connection?

## How Anvil Ring and Anvil Serving work together

Anvil Serving and Anvil Ring operate at different layers.

[Anvil Serving](https://fakoli.github.io/anvil-serving/) stores model artifacts,
starts and stops serving processes, records qualification evidence, and maps
stable capability names to model configurations through its gateway. Its
[main project portal](https://github.com/fakoli/anvil-serving) contains the
source and release history.

Anvil Ring owns the reachability path for a serving endpoint that must remain
loopback-only on an outbound-only host. The configured Ring upstream may be:

1. a direct vLLM or SGLang endpoint; or
2. the loopback Anvil Serving gateway, when capability routing and serving
   policy should remain in Anvil Serving.

In the second topology, Ring carries the HTTP request and preserves streaming.
Anvil Serving still maps the requested capability name to a model configuration.
Ring does not change that mapping, select a fallback model, start or stop a
model, or allocate GPU capacity.

The Anvil Ring [documentation portal](https://fakoli.github.io/anvil-ring/) and
[main project portal](https://github.com/fakoli/anvil-ring) cover the transport
and its operational boundary.

## The governing constraint

The remote side assumes no privilege and no persistence. It runs as an
unprivileged process, opens no listening port, accepts a re-registrable
credential through a file or environment input, and loses access when the hub
revokes or expires the session.

That constraint is not an implementation detail. It is the reason Anvil Ring is
an outbound tether rather than a remotely managed network endpoint, and it is
the standard against which every future registration, routing, or
administration feature must be reviewed.

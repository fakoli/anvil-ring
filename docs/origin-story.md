# Origin story: anvil-ring

*De-identified for public release. No hostnames, tokens, or account details appear
in this document or anywhere in this repository.*

## The problem this started from

The serving tier was built to run on hardware we own: a workstation, a mini, a
machine with a name we chose and a network we control. Reaching those boxes is a
solved problem — they are on our network, they have names, and the names resolve.

Renting a GPU is the opposite situation. The machine has no name we picked, it
sits behind a cloud provider's firewall, we cannot open inbound ports, we often
cannot install a system service, and on some providers we cannot get a TUN
interface at all. And it is temporary: the rental ends, the container goes away,
and the next one has a different address.

The obvious answer is "put it on the same private network as everything else."
That answer has a hidden cost worth naming out loud: to use it you must run a
network daemon on a machine you do not own and will not keep, and the thing you
actually wanted — one HTTPS endpoint serving one model — is now accompanied by a
whole mesh network, a relay anycast system, and a hole-punching stack, none of
which your workload asked for.

## The observation that changed the design

A rented GPU box is usually **already publicly reachable**. The hard problem that
a mesh network exists to solve — two machines that neither can accept a
connection from the other — mostly does not apply here. The awkward party is the
one we control.

So the topology is not a mesh. It is a **tether to a place we already trust**.
The container does not wait to be found. It calls home, over a connection it
initiated, and holds the line open. Everything we want to reach it with rides
back down that line, in the direction that never required anyone to open a port.

That is the entire trick, and it is old: it is the reason a client can reach a
server behind NAT, and the reason your browser works behind a router nobody
configured. We just pointed it at our own infrastructure instead of a website.

## Why the name is a piece of a smithy

The family is named for the forge, so the fourth member had to be too.

A **ring** is the rope on a farrier's anvil stand. The anvil is heavy, immovable,
and sits at the center of the shop. The ring is on the *far side* of it. You stand
anywhere in the shop, pull the rope, and the anvil answers with a sound — a
signal you send outward that produces a response from a fixed, known point.

That is exactly the shape of this thing: we are not moving the anvil, and we are
not building a second one somewhere else. We are running a line from wherever the
work happens back to the place that answers.

It also fits the family's division of labor without straining:

| Repo | Asks |
|---|---|
| `anvil` | who is working |
| `anvil-serving` | what is running |
| `anvil-events` | what happened |
| `anvil-ring` | can we reach it |

## What it deliberately is not

**Not a VPN, and not trying to be one.** There is no overlay network, no virtual
interface, no address space to plan, and no daemon to install on the remote host.
If you need arbitrary bidirectional routing between many machines, you want a
real VPN and should use one; this is a smaller tool with a smaller blast radius.

**Not peer-to-peer.** There is one always-on side (our own network) and one
disposable side (the rental). The always-on side is where authentication, ACLs,
and bookkeeping live, because that is the side we can audit.

**Not a general tunnel service.** It exists to expose a model server's port. The
transport underneath is swappable and boring on purpose — the value is in the
lifecycle around it: registration, identity across restarts, health, and knowing
when a tether has gone dead.

## The constraint that keeps it honest

The remote side has **no privileges and no persistence assumptions**. It runs as
an unprivileged user in a container, holds a re-registrable token rather than a
baked key, opens no listening ports, and loses all access the moment the token is
revoked. Nothing about joining succeeds only if the operator happened to grant
root.

That constraint is the reason the design is a tether and not a tunnel we manage
from the remote end. It is also the reason the name is about a rope you pull
rather than a door you open.

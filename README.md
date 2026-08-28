# anvil-ring

> [!IMPORTANT] Early scaffold. This repository contains the design contract and a
> CLI skeleton only. There is no working tunnel yet — see [STATE.md](STATE.md).

**anvil-ring** is an outbound-initiated tether for the Anvil family. You deploy a
container to a GPU rental you've never seen, it calls *home*, and the model
server's port becomes reachable from your own network — with no port-forwarding,
no inbound firewall rules, no TUN device, and no root on the remote host.

It is deliberately **not** a VPN. There is no overlay network and no mesh: there
is one always-on side you control and one disposable side that dials out.

`anvil` asks *who*, `anvil-serving` asks *what*, `anvil-events` asks *what
happened*, and `anvil-ring` asks **can we reach it**.

## Naming

The executable is `anvil-ring`, always with the `anvil-` prefix. There is no bare
`ring` command and no short alias — the prefix is the namespace, and the prefix is
the collision avoidance.

```bash
# On the rental host -- dials OUT, listens on nothing (I-1).
anvil-ring up --serve http://127.0.0.1:8000

# On the hub side -- run over SSH or an existing private-network session, so the
# hub itself never opens an inbound port to the internet either.
ssh <hub> anvil-ring list
ssh <hub> anvil-ring call my-rental -- curl -s /v1/models
```

## Why not just join the tailnet

Because the rented box is usually the *publicly reachable* party, and the
machines you actually control are the hard-to-reach ones. A mesh's hole-punching
and relay machinery solves a problem you don't have here, and to buy it you'd run
a network daemon on a machine you don't own and won't keep. anvil-ring keeps the
one property that matters — an outbound connection to infrastructure you already
trust — and drops the rest.

Read [docs/origin-story.md](docs/origin-story.md) for the reasoning, and
[docs/invariants.md](docs/invariants.md) for the rules that bound the design.

## Status

| Piece | State |
|---|---|
| Design contract, invariants, ADR-0001 | ✅ written |
| Egress probe tool (`anvil-ring probe-egress`) | ✅ built, run, evidence recorded (ADR-0003) |
| Registration + token lifecycle | ⬜ not started |
| Transport (chisel vs. `ssh -R`) | ⚠️ **undecided** — ADR-0002, needs one probe *from a rental* |
| Hub (macOS/launchd + container) | ⬜ not started |
| Working end-to-end test | ⬜ not started |

## License

MIT

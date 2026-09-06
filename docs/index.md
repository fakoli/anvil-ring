# Anvil Ring

Anvil Ring is an outbound-only, authenticated HTTP tunnel for model servers on
short-lived infrastructure. A rental-side tether dials an always-on hub, and
authenticated callers reach the loopback serving endpoint through the hub. The
rental opens no inbound port and cannot choose its own route or permission.

## Access points

| Portal | Use it for |
|---|---|
| [Documentation portal](https://fakoli.github.io/anvil-ring/) | Published guides, architecture, operations, and decision history after the GitHub Pages workflow is enabled |
| [Main project portal](https://github.com/fakoli/anvil-ring) | Source, releases, issues, and contribution history |
| [Anvil Serving documentation](https://fakoli.github.io/anvil-serving/) | Model lifecycle, capability gateway, evaluation, and serving operations |
| [Anvil Serving project](https://github.com/fakoli/anvil-serving) | Source and release history for the serving layer |

These are project information portals, not runtime administration interfaces.
Runtime administration uses the local OS-authenticated `anvil-ring admin`
command and `ANVIL_RING_STATE_DIR`; there is no web console or network
administration listener.

As verified on 2026-08-31, the documentation URL returns HTTP 404. The repository
owner must set GitHub Pages to use **GitHub Actions**, then run the documentation
workflow from `main`, before the portal becomes available. Until then, these
source pages remain available through the main project portal.

## Start with your task

| Goal | Start here |
|---|---|
| Understand a product or protocol term | [Concepts and terminology](concepts.md) |
| Evaluate the complete local path | [Operator guide](operator-guide.md#local-evaluation) |
| Review every command and setting | [CLI reference](cli-reference.md) |
| Decide whether to deploy | [Production rollout](production-rollout.md) |
| Understand the system and trust model | [Architecture](architecture.md) |
| Integrate Anvil Serving | [Anvil Serving integration](architecture.md#anvil-serving-integration) |
| Verify a change or release | [Testing and verification](testing.md) |
| Understand why the product exists | [Origin story](origin-story.md) |
| Review architecture decisions | [Decision record](adr/README.md) |
| Read historical incident evidence | [Investigation archive](history.md) |

## Deployment status

The Rust caller-to-engine request path is implemented and exercised end to end.
A controlled single-rental pilot is supportable when the documented TLS, credential,
monitoring, provider, and rollback checks are satisfied.

A general fleet-wide rollout is not yet approved. Durable credential lifecycle,
local administration, and deterministic multi-tether routing are locally
verified. The release process still needs a
published, checksummed artifact and a real combined-container GPU run before
the deployment can be treated as an unattended production service.

See [Production rollout](production-rollout.md) for the decision matrix and
required evidence.

## Product relationship

[Anvil Serving](https://fakoli.github.io/anvil-serving/) manages and qualifies
the capability being served. Anvil Ring supplies reachability when that
capability runs on a host that may initiate outbound connections but must not
accept inbound traffic. A capability alias is a stable API name that Anvil
Serving maps to a model or serving configuration. Ring neither creates that
mapping nor changes which model it selects. Ring carries the caller's HTTP
request to the loopback upstream selected by the operator.

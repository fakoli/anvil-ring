# anvil-ring Rust runtime

The Rust crate contains the production proxy, hub, tether, binary frame codec,
streaming body, lifecycle ownership, and network harnesses.

[Documentation portal (publication pending)](https://fakoli.github.io/anvil-ring/) ·
[Main project portal](https://github.com/fakoli/anvil-ring)

Until GitHub Pages is enabled, read the same documentation from
[`../docs/index.md`](../docs/index.md) in the main project portal.

“Production” here identifies the Rust request-transport implementation, not
approval for an unattended or multi-rental deployment. Durable registration,
local administration, and deterministic routing are locally verified;
target-dependent rollout gates remain open.
See the [production rollout guide](../docs/production-rollout.md) before using it
outside a controlled single-rental pilot.

## Modes

| Command | Role |
|---|---|
| `anvil-ring proxy` or `anvil-ring` | Local authenticated reverse proxy |
| `anvil-ring hub` | Tether listener plus optional authenticated caller frontend |
| `anvil-ring tether` | Outbound-only rental client and loopback engine proxy |
| `anvil-ring admin` | Local durable registration, credential, and audit administration |

Run `anvil-ring --help` for the complete environment contract. Modes accept no
extra command-line arguments; secrets and configuration stay out of process
arguments.
The rendered reference is in [`../docs/cli-reference.md`](../docs/cli-reference.md).

## Source ownership

| File | Responsibility |
|---|---|
| `src/main.rs` | Mode dispatch and environment configuration |
| `src/hub.rs` | Registry, authorization, tether sessions, stream routing, liveness |
| `src/tunnel.rs` | Outbound client, reconnect and authorization-renewal loop, engine request tasks, WebSocket writer |
| `src/frontend.rs` | Caller HTTP listener and streaming response body |
| `src/proxy.rs` | Local single-upstream proxy and loopback/auth guards |
| `src/frames.rs` | Binary tunnel protocol |
| `src/chunked.rs` | Incremental transfer-coding decoder |
| `src/headers.rs` | Hop-by-hop header filtering |

See [`../docs/architecture.md`](../docs/architecture.md) for the full request and
lifecycle contracts.

## Build

```bash
cargo build --locked --bin anvil-ring
cargo build --release --locked --bin anvil-ring
```

Static Linux artifacts are built with musl in CI. See
[`../docs/cross-build.md`](../docs/cross-build.md).

## Test

```bash
cargo fmt --check
cargo test --locked --all-targets -- --test-threads=1
cargo clippy --locked --all-targets -- -D warnings
```

Serial integration execution is load-bearing: harnesses bind loopback ports and
parallel collisions can resemble tunnel failures.

Focused lifecycle verification:

```bash
cargo test --test forward_e2e -- --test-threads=1
```

`forward_e2e` covers the complete caller-to-engine path, streaming cadence,
authentication ordering, no-tether status, tether cancellation, and more than
one full concurrency window of sequential requests. Tether cancellation is a
required passing regression, not an expected failure.

The buffering canary is a test-only executable. `proxy_e2e` requires the real
proxy to pass the incremental-arrival predicate and the canary to fail the same
predicate while returning the complete body. ADR-0005 records the comparison.

## Live local harnesses

```bash
cargo build --bin anvil-ring
python3 scripts/live_stream_gate.py --events 6 --gap 0.8
python3 scripts/soak_tunnel.py 100 "$PWD/target/debug/anvil-ring"
```

The streaming harness requires the caller to receive the engine's status, every
event, and incremental delivery. The soak measures whether one idle authorized
connection reconnects or resets during the observation window.

## Test-environment cautions

- Pass an explicit binary path to scripts; a stale `anvil-ring` on `PATH` can
  produce convincing results from old code.
- Do not loosen harness listeners with `SO_REUSEADDR`; a successful reused bind
  does not prove the test owns the port.
- Prefer ephemeral loopback ports for new tests.
- A macOS build does not verify Linux musl linking or a GPU container.

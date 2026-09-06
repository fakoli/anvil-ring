# anvil-ring

An outbound-only, authenticated HTTP tunnel for model servers running on
short-lived infrastructure.

[Documentation portal (publication pending)](https://fakoli.github.io/anvil-ring/) ·
[Main project portal](https://github.com/fakoli/anvil-ring) ·
[Anvil Serving documentation](https://fakoli.github.io/anvil-serving/)

Anvil Ring makes a loopback model endpoint on a disposable GPU rental reachable
through infrastructure you control. The rental initiates the connection, opens
no listening socket, requires neither root access nor a TUN device, and cannot
choose its own route or permissions. The hub authenticates tethers and callers,
preserves upstream HTTP status and headers, and streams response bodies
incrementally.

The main project portal is available now. The documentation portal is configured
but returns HTTP 404 until GitHub Pages is set to **GitHub Actions** and the
documentation workflow runs from `main`; use the [documentation
source](docs/index.md) in the meantime.

> [!IMPORTANT]
> The complete caller-to-engine request path is implemented and end-to-end
> tested, and durable hub administration plus deterministic routing are locally
> verified. The repository is not yet approved for an
> unattended fleet-wide production rollout: external TLS, provider, GPU/model,
> release, monitoring, and operating gates remain open. Use the
> [production rollout guide](docs/production-rollout.md) for the pilot boundary,
> required checks, and remaining blockers.

## Product boundary

Anvil Ring solves reachability. It does not select models, manage GPU capacity,
or replace the software that deploys models and assigns API capabilities to
them.

- [Anvil Serving](https://github.com/fakoli/anvil-serving) owns model lifecycle,
  capability aliases, qualification, and the serving gateway.
- Anvil Ring carries authenticated HTTP between an always-on hub and a
  loopback-only serving endpoint on an outbound-only host.
- The upstream may be a direct vLLM/SGLang endpoint or an Anvil Serving gateway.
  When Anvil Serving is upstream, it retains model and routing authority.

The longer design rationale is in the [origin story](docs/origin-story.md).

## What is implemented

| Component | Purpose | Current status |
|---|---|---|
| `anvil-ring hub` | Accept outbound tethers and authenticated caller traffic | Durable registration and deterministic routing locally verified |
| `anvil-ring admin` | Initialize and administer durable hub registrations | Implemented and locally verified |
| `anvil-ring tether` | Dial the hub and proxy requests to a loopback upstream | Implemented and end-to-end tested |
| `anvil-ring proxy` | Run the authenticated reverse proxy without a tunnel | Implemented and tested |
| `anvil-ring-probe-egress` | Qualify outbound TLS access from a provider image | Implemented Python diagnostic |
| Combined vLLM-and-tether container | Run `vllm serve` on loopback beside the tether | Supervisor contract tested; real Linux/GPU build still required |

The Rust binary under [`cargo/`](cargo/) is the request-transport runtime: it
authenticates and carries caller HTTP requests, streaming responses, and tunnel
lifecycle messages. The Python
distribution is diagnostics-only and deliberately does not install an
`anvil-ring` command, so it cannot shadow the Rust executable.

## Local evaluation

Build the runtime:

```bash
cd cargo
cargo build --release --locked --bin anvil-ring
```

Start the hub and caller frontend:

```bash
export ANVIL_RING_DEMO_CREDENTIAL='replace-with-a-random-tether-secret'
export ANVIL_RING_HUB_LISTEN='127.0.0.1:8443'
export ANVIL_RING_FRONTEND_LISTEN='127.0.0.1:8080'
export ANVIL_RING_CALLER_TOKEN='replace-with-a-random-caller-secret'
./target/release/anvil-ring hub
```

Start a tether beside a model server listening on loopback:

```bash
export ANVIL_RING_HUB_URL='ws://127.0.0.1:8443/ring'
export ANVIL_RING_CREDENTIAL='the-same-random-tether-secret-used-by-the-hub'
export ANVIL_RING_UPSTREAM='http://127.0.0.1:8000'
./target/release/anvil-ring tether
```

Plain `ws://` is restricted to literal loopback testing. A routable deployment
must terminate TLS in front of the hub and use `wss://`.

Call the model through the frontend:

```bash
curl -N \
  -H "Authorization: Bearer $ANVIL_RING_CALLER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"model":"your-model","messages":[{"role":"user","content":"Hello"}],"stream":true}' \
  http://127.0.0.1:8080/v1/chat/completions
```

Replace `your-model` with the model identifier accepted by the configured vLLM,
SGLang, or Anvil Serving upstream.

Check hub and tether liveness:

```bash
curl http://127.0.0.1:8080/healthz
# ok tether-up
```

The unauthenticated health endpoint proves the hub is serving and reports
whether a tether is `Up`; it does not prove that a model has finished loading.

## Provider qualification

Run the diagnostic inside the same rental image and under the same unprivileged
identity intended for deployment:

```bash
uv sync --extra dev
uv run anvil-ring-probe-egress \
  --no-defaults \
  --target ring.example.internal:443:tls \
  --out egress-provider.json
```

The probe accepts no credentials, redacts the local hostname by default, and
returns success only when it observes a usable TLS target. `--no-defaults`
ensures the exit status qualifies the intended hub rather than a public control
target.

## Documentation

- [Operator guide](docs/operator-guide.md) — build, configuration, deployment,
  health, status codes, and troubleshooting
- [Concepts and terminology](docs/concepts.md) — definitions for hub, tether,
  caller frontend, serving upstream, registration, lease, and related terms
- [CLI reference](docs/cli-reference.md) — shipped modes, environment contract,
  diagnostics, and exit behavior
- [Production rollout](docs/production-rollout.md) — launch decision, pilot
  boundary, required checks, rollback, and open blockers
- [Architecture](docs/architecture.md) — components, trust boundaries, request
  flow, lifecycle, bounded flow control, and Anvil Serving integration
- [Testing](docs/testing.md) — test matrix, local and CI checks, and known evidence
  limits
- [Invariants](docs/invariants.md) — non-negotiable security and behavior rules
- [Decision record](docs/adr/README.md) — current and superseded architecture
  decisions
- [Current state](STATE.md) — dated implementation and verification snapshot

## Verification

```bash
cd cargo
cargo fmt --check
cargo test --locked --all-targets -- --test-threads=1
cargo clippy --locked --all-targets -- -D warnings

cd ..
uv sync --extra dev
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .
```

Integration tests run serially because several harnesses bind loopback ports.
CI also builds static musl binaries for aarch64 and x86-64. The complete
verification procedure,
including documentation, packaging, and environment-specific checks, is in the
[testing guide](docs/testing.md).

## License

MIT

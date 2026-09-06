# Operator guide

This guide covers local evaluation, durable hub administration, and a controlled
single-rental pilot. Durable registration, credential lifecycle, and
deterministic multi-tether routing are locally verified. Target-dependent TLS,
provider, GPU, hosted release, and operating gates remain open.
Before exposing any network listener beyond loopback, complete the
[production rollout checks](production-rollout.md).

Project access:

- [Documentation portal](https://fakoli.github.io/anvil-ring/)
- [Main project portal](https://github.com/fakoli/anvil-ring)
- [Anvil Serving documentation](https://fakoli.github.io/anvil-serving/)

## Prerequisites

- Rust 1.85+ to build locally, or a release-built static `anvil-ring` binary.
- A model server listening on a loopback address on the rental.
- A trusted hub host with TLS termination for the tether WebSocket in production.
- A durable hub state directory owned by the hub OS user, or explicit demo mode
  for local compatibility.
- A tether registration credential and one independent caller bearer token.

Use durable mode for persistent registration and administration. Explicit demo
mode remains available for local or pilot compatibility and must not be combined
with durable state.

## Durable hub administration

Initialize the private SQLite state directory as the same OS user that will run
the hub:

```bash
export ANVIL_RING_STATE_DIR='/var/lib/anvil-ring'
./anvil-ring admin init
```

Issue a credential into a new private file:

```bash
export ANVIL_RING_CREDENTIAL_OUT='/run/secrets/rental-a-credential'
./anvil-ring admin register rental-a 'Rental A' 3600
export ANVIL_RING_CRED_FILE='/run/secrets/rental-a-credential'
```

Use `admin list` for effective state; `admin rotate ID TTL_SECONDS` writes a
replacement credential to a new output path; `admin revoke ID` disables a
registration; and `admin audit [AFTER_SEQUENCE]` reads ordered audit events.
The command never prints token material or overwrites credential output. See
[ADR-0007](adr/0007-durable-hub-administration.md) for refresh and failure
semantics.

## Local evaluation

All listeners and URLs in this section are loopback-only. They establish the
functional path but do not represent the routable production topology.

### Build

```bash
cd cargo
cargo build --release --locked --bin anvil-ring
./target/release/anvil-ring --help
```

The runtime accepts modes only. Configuration and secrets are not CLI arguments.

### Run the hub

```bash
export ANVIL_RING_DEMO_CREDENTIAL='a-long-random-tether-secret'
export ANVIL_RING_HUB_LISTEN='127.0.0.1:8443'
export ANVIL_RING_FRONTEND_LISTEN='127.0.0.1:8080'
export ANVIL_RING_CALLER_TOKEN='a-different-long-random-caller-secret'
./target/release/anvil-ring hub
```

For durable mode, set `ANVIL_RING_STATE_DIR` and omit
`ANVIL_RING_DEMO_CREDENTIAL`. The hub loads a validated registration snapshot
before accepting connections and refreshes it every 500 ms; failed or slow
reads fail closed and clear authorization. Changes take effect at the next
refresh.

| Variable | Required | Meaning |
|---|---|---|
| `ANVIL_RING_DEMO_CREDENTIAL` | Yes | Credential registered as tether `demo-1` |
| `ANVIL_RING_HUB_LISTEN` | No | Tether listener; defaults to `127.0.0.1:8443` |
| `ANVIL_RING_FRONTEND_LISTEN` | For caller traffic | Enables the caller-facing HTTP frontend |
| `ANVIL_RING_CALLER_TOKEN` | With frontend | Bearer token accepted from callers |

For a routable pilot, keep the Rust listener private and place a TLS-aware
reverse proxy or ingress in front of it. Tethers must use the resulting `wss://`
URL. The Rust hub listener itself does not terminate TLS. See
[Production rollout](production-rollout.md#network-and-tls) before exposing it.

Expected state messages include:

```text
anvil-ring: event demo-1 Up
anvil-ring: demo-1 (demo rental) Up(...)
```

The credential is never printed; a short digest prefix may appear for
correlation.

### Run the tether

Prefer a credential file on a multi-user or orchestrated host:

```bash
export ANVIL_RING_HUB_URL='wss://ring.example.internal/ring'
export ANVIL_RING_CRED_FILE='/run/secrets/anvil-ring-credential'
export ANVIL_RING_UPSTREAM='http://127.0.0.1:8000'
./anvil-ring tether
```

For a local-only test, `ANVIL_RING_CREDENTIAL` may replace the file and
`ws://127.0.0.1:8443/ring` may replace the WSS URL.

| Variable | Required | Meaning |
|---|---|---|
| `ANVIL_RING_HUB_URL` | Yes | `wss://` hub URL; `ws://` accepted only for loopback |
| `ANVIL_RING_CRED_FILE` | Preferred | File containing the tether credential |
| `ANVIL_RING_CREDENTIAL` | Fallback | Tether credential in the environment |
| `ANVIL_RING_UPSTREAM` | No | Loopback engine URL; defaults to `http://127.0.0.1:8000` |

The tether opens no listener. It logs authorization, reauthorization,
disconnects, and dial failures. After a failed connection it waits 0.5 seconds,
doubles the delay after each failure up to 30 seconds, and resets the delay after
a successful session.

### Call the frontend

```bash
curl -N \
  -H 'Authorization: Bearer a-different-long-random-caller-secret' \
  -H 'Content-Type: application/json' \
  -d '{"model":"your-model","messages":[{"role":"user","content":"Hello"}],"stream":true}' \
  http://127.0.0.1:8080/v1/chat/completions
```

Replace `your-model` with the model identifier accepted by the configured vLLM,
SGLang, or Anvil Serving upstream.

The route and request body are forwarded to the model engine. The engine's
status is preserved. Hop-by-hop headers are removed at each HTTP boundary.

### Caller status guide

| Status | Meaning | Operator action |
|---:|---|---|
| 401 | Caller token missing or invalid | Correct the caller bearer token |
| 400 | Caller request body failed while being read | Retry from a healthy client; inspect caller transport |
| 429 | One tether used all 65,535 request identifiers without reconnecting | Reconnect the tether and investigate unexpectedly long-lived request volume |
| 502 | No up tether, tunnel died, or no upstream response head arrived | Check hub `Up/Down`, tether logs, then engine readiness |
| 503 | Tether revoked or its bounded command queue is saturated | Stop new load, inspect tether/engine health, allow redial |
| Other 4xx/5xx | The engine's own response | Handle as an upstream model-server result |

### Health

```bash
curl http://127.0.0.1:8080/healthz
```

Responses:

- `ok tether-up` — hub and at least one tether are currently responsive.
- `ok tether-down` — hub is serving, but it has no `Up` tether.

This endpoint is unauthenticated so an orchestrator can distinguish hub failure
from rental failure. Do not use it as a full engine-readiness check; it proves the
tether control loop, not that a model has finished loading.

## Anvil Serving as the upstream

To expose an Anvil Serving capability gateway instead of a direct engine, bind
the gateway to loopback and set `ANVIL_RING_UPSTREAM` to that URL:

```bash
export ANVIL_RING_UPSTREAM='http://127.0.0.1:8000'
./anvil-ring tether
```

Anvil Serving retains the mapping from stable capability names to models, model
startup and shutdown, readiness, and qualification. Ring preserves the caller's
`Authorization` header.
If the Anvil Serving gateway also enforces bearer authentication, configure an
intentionally compatible token contract; Ring does not exchange the caller
token for a different upstream credential.

Use the [Anvil Serving documentation portal](https://fakoli.github.io/anvil-serving/)
for serving and gateway operations.

## Local proxy mode

Use proxy mode to test HTTP behavior without the tunnel:

```bash
export ANVIL_RING_LISTEN='127.0.0.1:8080'
export ANVIL_RING_UPSTREAM='http://127.0.0.1:8000'
export ANVIL_RING_TOKEN='local-caller-secret'
./anvil-ring proxy
```

`ANVIL_RING_ALLOW_NO_AUTH=1` is available only for an explicit trusted local
test. Without a token or that override, proxy mode refuses to start.

## Combined vLLM-and-tether container image

The image starts `vllm serve` on `127.0.0.1:8000` and supervises it beside
`anvil-ring tether`. Pin the base image, preferably by digest:

```bash
docker build \
  -f deploy/Dockerfile \
  --build-arg 'VLLM_IMAGE=vllm/vllm-openai:<tag>@sha256:<digest>' \
  -t anvil-ring-vllm .
```

Run without publishing the engine port:

```bash
docker run --rm --gpus all --ipc=host \
  -e ANVIL_RING_HUB_URL='wss://ring.example.internal/ring' \
  -e ANVIL_RING_CRED_FILE='/run/secrets/anvil-ring-credential' \
  -v "$PWD/anvil-ring-credential:/run/secrets/anvil-ring-credential:ro" \
  anvil-ring-vllm your-model-id
```

Replace `your-model-id` with a model identifier supported by `vllm serve`.

Do not add `-p 8000:8000`: the model server is intentionally loopback-only. The
entrypoint rejects user-supplied `--host` and `--port` options so runtime
arguments cannot weaken that boundary.

The supervisor exits when either vLLM or the tether exits and terminates the
other child. The supervisor contract is tested without a GPU; build and model
startup still require an authorized Linux/GPU environment.

## Provider egress probe

```bash
uv sync --extra dev
uv run anvil-ring-probe-egress \
  --no-defaults \
  --target ring.example.internal:443:tls \
  --out egress-provider.json
```

Run this inside the final rental image as its runtime user. `--no-defaults`
makes the exit status depend on the intended hub target rather than the default
public connectivity checks for GitHub, npm, Cloudflare, and SSH. Review
`transport_hint` and the target's `usable` field. The
local hostname is redacted from console and JSON output unless
`--include-hostname` is deliberately passed.

## Troubleshooting

### Tether is repeatedly unauthorized

Use exactly the same high-entropy value for `ANVIL_RING_DEMO_CREDENTIAL` on the
hub and the tether credential input. The hub intentionally does not disclose
whether a value was unknown or revoked.

### Tether refuses the hub URL

Use `wss://` for a routable host. Plain `ws://` is restricted to literal
loopback/localhost testing to prevent credentials crossing the network in clear
text.

### Tether refuses the upstream

Bind the engine to `127.0.0.1` or `::1` and use that loopback URL. A routable
engine address violates the single-auth-boundary design and is rejected.

### Caller gets 502 after model startup

Check, in order:

1. `/healthz` says `tether-up`.
2. The tether logged authorization rather than repeated dial/refusal messages.
3. The model process is listening on loopback port 8000 and has completed loading.
4. The hub frontend and caller token use the expected environment values.

### Caller stream ends when the tether is cancelled

That is intentional. The hub ends in-flight response bodies when the tether
session disappears so callers can retry instead of waiting indefinitely.

## Production handoff

Before admitting pilot traffic, record:

- the source commit, binary checksum, and image digest;
- the exact-target provider probe artifact;
- the TLS ingress and certificate owner;
- the credential source and rotation/rollback procedure;
- the full-path streaming and failure-test results; and
- the operator responsible for monitoring and rollback.

The complete checklist and blockers for unattended or multi-rental deployment are in
[Production rollout](production-rollout.md).

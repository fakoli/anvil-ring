# CLI reference

Anvil Ring has two command-line interfaces:

- `anvil-ring`, the Rust request-transport runtime; and
- `anvil-ring-probe-egress`, the Python deployment diagnostic.

The runtime service modes accept no mode-specific command-line options. Runtime
configuration comes from environment variables or a credential file so secrets
do not appear in process listings or shell history. The local `admin` mode uses
the subcommands documented below.

The `admin` mode authenticates through the operating system account and private
state directory, without a network administration listener.

## Runtime commands

| Command | Behavior |
|---|---|
| `anvil-ring` | Start local proxy mode; equivalent to `anvil-ring proxy` |
| `anvil-ring proxy` | Start the authenticated local reverse proxy |
| `anvil-ring hub` | Accept tether sessions and, when configured, caller HTTP traffic |
| `anvil-ring tether` | Dial the hub and forward requests to one loopback upstream |
| `anvil-ring admin ...` | Initialize and administer durable hub registrations |
| `anvil-ring --help`, `-h` | Print the runtime and environment contract |
| `anvil-ring --version`, `-V` | Print the package version |

Additional arguments to service modes, help, or version are rejected with exit
status 2. Invalid admin commands fail with a nonzero status. Token, credential,
URL, and listen-address flags are deliberately unsupported.

## Proxy environment

| Variable | Required | Meaning |
|---|---|---|
| `ANVIL_RING_LISTEN` | No | Bind address; defaults to `127.0.0.1:8080` |
| `ANVIL_RING_UPSTREAM` | No | Loopback HTTP upstream; defaults to `http://127.0.0.1:8000` |
| `ANVIL_RING_TOKEN` | Normally | Bearer token accepted from callers |
| `ANVIL_RING_ALLOW_NO_AUTH` | Local testing only | The exact value `1` explicitly disables proxy authentication |

Proxy mode refuses to start when neither a token nor the explicit no-auth
override is present. Do not use the override on a routable listener.

## Hub environment

| Variable | Required | Meaning |
|---|---|---|
| `ANVIL_RING_HUB_LISTEN` | No | Tether WebSocket bind address; defaults to `127.0.0.1:8443` |
| `ANVIL_RING_STATE_DIR` | Durable hub mode | Private directory containing the hub SQLite state |
| `ANVIL_RING_DEMO_CREDENTIAL` | Demo mode only | Credential for the single in-memory `demo-1` registration |
| `ANVIL_RING_FRONTEND_LISTEN` | For caller traffic | Enables the caller-facing HTTP listener |
| `ANVIL_RING_CALLER_TOKEN` | With the frontend | Bearer token accepted from callers |

The Rust hub listener does not terminate TLS. Keep it private behind a TLS-aware
ingress for any routable deployment. Set `ANVIL_RING_STATE_DIR` for durable
registration and routing; it is mutually exclusive with demo mode. Durable hub
administration and deterministic routing are locally verified. Target-dependent
TLS, provider, GPU, hosted release, and operating gates remain open.

## Durable administration

```text
anvil-ring admin init
anvil-ring admin register ID LABEL TTL_SECONDS
anvil-ring admin rotate ID TTL_SECONDS
anvil-ring admin revoke ID
anvil-ring admin list
anvil-ring admin audit [AFTER_SEQUENCE]
```

`ANVIL_RING_STATE_DIR` is required for every admin command. `register` and
`rotate` also require `ANVIL_RING_CREDENTIAL_OUT`; the command creates that
new 0600 file and never prints or overwrites a credential. See
[ADR-0007](adr/0007-durable-hub-administration.md) for validation and failure
semantics.

## Tether environment

| Variable | Required | Meaning |
|---|---|---|
| `ANVIL_RING_HUB_URL` | Yes | `wss://` hub URL; `ws://` is accepted only for literal loopback/localhost |
| `ANVIL_RING_CRED_FILE` | Preferred | File containing the registration credential |
| `ANVIL_RING_CREDENTIAL` | Fallback | Registration credential in the environment |
| `ANVIL_RING_UPSTREAM` | No | Loopback HTTP upstream; defaults to `http://127.0.0.1:8000` |

When both credential inputs are present, the file is used. The upstream is
validated at startup and again for every stream. A routable upstream is rejected.

## Egress diagnostic

```text
anvil-ring-probe-egress [--out PATH] [--target HOST:PORT[:MODE]]
                        [--no-defaults] [--include-hostname]
```

| Option | Meaning |
|---|---|
| `--out PATH` | Write the JSON evidence record; defaults to `egress-probe.json` |
| `--target HOST:PORT[:MODE]` | Add a target; repeatable. Mode is `tls`, `tcp`, or `ssh-banner` |
| `--no-defaults` | Probe only the explicitly supplied targets |
| `--include-hostname` | Include the local hostname in console and JSON output |

Use `--no-defaults` with the intended hub target for a deployment check whose
exit status applies only to that target:

```bash
anvil-ring-probe-egress \
  --no-defaults \
  --target ring.example.internal:443:tls \
  --out egress-provider.json
```

The diagnostic authenticates to nothing and accepts no token or credential
option. Its exit statuses are:

| Status | Meaning |
|---:|---|
| 0 | At least one probed TLS target completed a verified handshake |
| 1 | No probed TLS target was usable |
| 2 | The command line or target specification was invalid |

Raw TCP and SSH results remain useful evidence but do not qualify the shipped
WSS transport.

## Runtime exit behavior

| Status | Meaning |
|---:|---|
| 0 | Help/version completed, or a service stopped normally |
| 2 | Invalid command line or a fail-closed configuration refusal |
| Other nonzero | Startup, bind, transport, or runtime error reported by the process |

Services also report state transitions and operational errors on standard
error. Logs may include tether ids, peer addresses, state, and a short credential
digest prefix, but must never include a full credential or caller token.

## Further access

- [Documentation portal](https://fakoli.github.io/anvil-ring/)
- [Main project portal](https://github.com/fakoli/anvil-ring)
- [Operator guide](operator-guide.md)
- [Production rollout](production-rollout.md)

# anvil-ring (Rust)

Outbound-initiated reverse proxy: the **tether** runs beside a local inference port on a
machine you don't control (a GPU rental) and dials OUT to the **hub** on your own network.
Nothing listens on the rental, so there are no inbound firewall rules, no port-forwards,
and no root. Invariants and their enforcement sites: `../docs/invariants.md`.

## Layout

```
src/        hub.rs (hub + caller frontend)  tunnel.rs (tether + framing)
            proxy.rs (plain single-target proxy)  wire.rs (frame codec)  main.rs
tests/      forward_e2e.rs  end_to_end.rs  proxy_e2e.rs  bare_lf_head_panic_probe.rs
            wire_probe.rs   integration.rs
scripts/    live_stream_gate.py   soak_tunnel.py   (see "Verification harnesses")
```

Three run modes, all from one binary (always the `anvil-` prefix; there is no bare
`ring` command):

| Mode | Listens | Purpose |
|---|---|---|
| `anvil-ring hub` | hub ports only | Registry + caller frontend; authorizes tethers, routes calls |
| `anvil-ring tether` | **nothing** | Outbound only (I-1); proxies to a loopback upstream (I-10) |
| `anvil-ring` | loopback | Plain single-target proxy, no tunnel (dev/fallback) |

## Build

```bash
cargo build            # debug binaries: target/debug/anvil-ring
cargo build --release
```

## Test

```bash
cargo test --lib                                  # unit tests
cargo test --test forward_e2e -- --test-threads=1 # end-to-end (see caveat)
cargo test --test proxy_e2e
```

Verified from a clean copy of `Cargo.toml`/`Cargo.lock`/`src`/`tests` with no other
repo state: `cargo test --lib` -> 54 passed.

**Run integration tests with `--test-threads=1`.** They bind fixed loopback ports, so
two tests running concurrently collide and fail in ways that look like product bugs.

### Test-environment traps (each cost real time)
- **`/tmp` is not writable here, and `spawn_blocking` + `std::fs` ignores an
  overridden `TMPDIR`.** Use `tokio::fs` for any temp file in a test, or the failure is
  a red herring.
- **Never add `SO_REUSEADDR` to a test bind.** A reuse-bind can succeed while a
  different process answers on that port, so the test silently measures something else.
  A clean bind proves the port was ours. If a port is held, change the port pair.
- **Always pass the binary by path when a harness takes one.** A stale binary from
  `PATH` produces confident, wrong results.

## Verification harnesses

Both are in-repo (the scratch copies in `/tmp` did not survive a reboot, which lost a
gate once) and both use inert local credentials plus `127.0.0.1` only.

```bash
# Streaming contract: no truncation, response head arrives first and comes from the
# engine (not fabricated), and delivery is paced rather than buffered. Exit 0 = pass.
python3 scripts/live_stream_gate.py --events 6 --gap 0.8

# Link stability: hold one tunnel open, touch nothing, count re-authorizations and
# resets. Verdict is STEADY STATE unless the link actually flaps.
python3 scripts/soak_tunnel.py 100 "$PWD/target/debug/anvil-ring"
```

The soak measures an **idle** tunnel. An idle tunnel has no reason to reconnect, so its
verdict says nothing about reconnect frequency — do not read it as "reconnects are rare".

## Open defect: I-6

`tether_death_midstream_ends_the_caller` in `tests/forward_e2e.rs` **fails by design**:
it is the standing red light for a caller that is never terminated when its tether dies.
Treat 3/4 on `forward_e2e` as the expected state, not a regression. Plan, measurements,
and the ordering rationale are in `../STATE.md`.

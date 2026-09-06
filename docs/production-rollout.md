# Production rollout

## Release decision

| Deployment scope | Recommendation | Reason |
|---|---|---|
| Local evaluation | Approved | Loopback path and failure behavior are covered by the local suite |
| Controlled single-rental pilot | Conditional approval | Supportable only after every required pilot check below passes |
| Unattended or fleet-wide production | Not approved | Durable administration and deterministic routing are locally verified; target dependent release and operating gates remain open |

Passing unit tests does not authorize a deployment.
The Rust caller-to-engine request path is tested. An unattended or multi-rental
rollout also requires evidence for persistent registrations, credential administration,
reproducible release artifacts, evidence from each provider image, monitoring,
and a rehearsed rollback procedure.

## Supported pilot topology

The supported pilot has one trusted hub process and one registered rental-side
tether:

1. A TLS-aware ingress exposes the public `wss://` endpoint.
2. The Rust hub listener remains on a private or loopback address behind that
   ingress.
3. The caller frontend is exposed only through the organization's authenticated
   network or a separate TLS-aware ingress.
4. One rental-side tether dials the hub and forwards only to a loopback HTTP
   upstream.
5. The upstream is either a direct model engine or the loopback Anvil Serving
   gateway.

Durable mode uses `ANVIL_RING_STATE_DIR` and the local `anvil-ring admin`
commands for registration, rotation, revocation, listing, and audit. The
explicit `ANVIL_RING_DEMO_CREDENTIAL` mode remains a one-registration local or
pilot compatibility mode. Durable administration and deterministic routing are
locally verified; this guide keeps the target dependent gates open.

## Required pilot checks

Every item is required for a controlled pilot.

### Artifact and supply chain

- [ ] The exact commit is identified and the working tree is clean for the
  release build.
- [ ] The complete Rust, Python, lint, documentation, and packaging commands in
  [Testing and verification](testing.md) pass.
- [ ] The aarch64 or x86-64 musl binary is built in the Linux release workflow,
  and `file`, `readelf`, and `ldd` checks confirm the expected architecture and
  absence of a dynamic loader.
- [ ] The distributed binary has a published SHA-256 checksum. A signed release
  or provenance attestation is strongly recommended before use outside a
  controlled pilot.
- [ ] The combined container image, which runs vLLM and the tether together,
  pins the vLLM base by digest and is built on an authorized Linux builder.
- [ ] A vulnerability and license review covers the final binary and base image.

The updated workflow prepares checksums, a target-specific Cargo SBOM, and
source identity, and signs provenance/SBOM attestations after successful build
and test jobs on trusted main/tag runs. Local packaging has been exercised, but
the updated signing job has not yet run on GitHub. A verified download from that
job and a distribution decision remain release gates. See
[Release verification](release-verification.md).

### Network and TLS

- [ ] The intended rental image passes the exact-target network check below.
  Exit status 0 means the named host accepted TCP and completed TLS negotiation:

  ```bash
  anvil-ring-probe-egress \
    --no-defaults \
    --target ring.example.internal:443:tls \
    --out egress-provider.json
  ```

- [ ] The public tether route terminates trusted TLS and the tether uses a
  `wss://` URL.
- [ ] The raw hub listener is not publicly reachable.
- [ ] The model engine and Anvil Serving gateway, when present, bind to loopback.
- [ ] The rental publishes no engine or tether port. In the combined container, do not
  add `-p 8000:8000`.
- [ ] Firewall and ingress rules restrict the caller frontend to its intended
  caller population.

### Identity and secrets

- [ ] Tether and caller secrets are independent, randomly generated, and stored
  in the organization's secret manager.
- [ ] The tether uses `ANVIL_RING_CRED_FILE` rather than a literal environment
  value when the platform supports secret files.
- [ ] No secret appears in process arguments, URLs, image layers, Compose files, logs, or the
  checked-in probe artifact.
- [ ] Restart, rotation, and revocation procedures use the durable state and
  credential output files, with rollback steps recorded.
- [ ] When Anvil Serving auth is enabled upstream, its bearer-token contract is
  intentionally aligned with the forwarded `Authorization` header. Ring does
  not rewrite caller credentials for a second upstream token.

### Functional and failure evidence

- [ ] A real model completes a non-streaming request through the complete path.
- [ ] A real streaming request preserves status, headers, all events, and
  incremental delivery.
- [ ] Engine 4xx and 5xx responses reach the caller unchanged.
- [ ] Invalid caller and tether credentials fail without revealing registration
  state.
- [ ] Cancelling or killing the tether ends in-flight callers within the exact
  number of seconds recorded in the deployment runbook.
- [ ] Queue saturation returns an explicit 503 for new work without unbounded
  memory growth.
- [ ] The deployment survives the expected idle interval and reconnects after a
  controlled hub or network interruption.
- [ ] The Rust suite includes both streaming timing tests: the real proxy passes
  incremental delivery and the deliberately buffering canary fails the same
  timing predicate. See [ADR-0005](adr/0005-streaming-negative-control.md).

### Operations and monitoring

- [ ] `/healthz` is monitored for both hub availability and `tether-up` state.
- [ ] Model readiness is monitored separately; Ring health does not prove that a
  model is loaded or qualified.
- [ ] Alerts cover repeated tether authorization failure, repeated redial,
  `Down`/`Revoked` transitions, frontend 502/503 rates, and process exit.
- [ ] Logs have retention, redaction, and access controls appropriate for peer
  addresses and tether identifiers.
- [ ] Resource limits and restart policy are explicit for hub, ingress, tether,
  and model processes.
- [ ] An operator owns the launch window, stop decision, and rollback.

## Rollout sequence

1. **Qualify the provider.** Run the exact-target egress probe from the final
   rental image and retain the redacted JSON record.
2. **Build and identify artifacts.** Produce the static binary and optional
   combined container image from the reviewed commit; record checksums and image
   digests.
3. **Deploy the trusted side.** Start the private hub behind TLS ingress, then
   verify `ok tether-down` before any rental connects.
4. **Deploy one tether.** Start the loopback model or Anvil Serving gateway, then
   start the tether with a secret file and confirm the `Up` transition.
5. **Run acceptance checks.** Exercise authentication, real streaming, upstream
   failure pass-through, cancellation, and reconnect.
6. **Admit limited traffic.** Before launch, record the allowed caller
   applications and request/concurrency limits. Watch health, errors, latency,
   reconnects, and memory while that traffic runs.
7. **Close or extend the pilot.** Roll back when any required check fails. Do not
   expand beyond one registration until the locally verified controls are paired
   with all target dependent gates.

## Rollback

The safest rollback removes the rental's authority first:

1. stop admitting new caller traffic at the frontend or ingress;
2. stop the tether or replace the registered credential and restart the hub;
3. allow in-flight callers to end or time out according to the incident policy;
4. stop the rental/model workload;
5. preserve logs, probe evidence, artifact identities, and the exact failure
   timeline; and
6. return callers to the previous serving path.

In durable mode, credential replacement is performed with `admin rotate` and a
new credential output path; the hub refreshes the state snapshot and cancels the
old session. Demo mode still requires hub restart for credential replacement.

## Blockers for unattended or multi-rental deployment

The following work must be verified before an unattended or multi-rental rollout:

1. persistence, credential lifecycle, local administration, audit, and deterministic
   routing evidence required by [ADR-0007](adr/0007-durable-hub-administration.md);
2. a supported TLS/ingress deployment with certificate rotation;
3. published, checksummed release artifacts, a software bill of materials, and
  build provenance that identifies the source and build process;
4. real-provider and real-GPU image qualification evidence;
5. capacity, latency, availability, recovery, and incident-response objectives.

Until those conditions are satisfied, documentation and release notes should
say “controlled single-rental pilot,” not “fleet-ready” or “fully production
ready.”

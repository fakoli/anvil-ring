# Static Linux builds from macOS

The shipped rental binary must be a self-contained, statically linked executable
with no rental-side package installation. A direct macOS
`cargo build --target *-unknown-linux-musl` compiles Rust but normally fails at
link time because Apple's linker does not understand GNU/musl linker flags.

## Canonical path: CI

`.github/workflows/release-artifact.yml` builds both supported targets in a
native-target Alpine container:

- `aarch64-unknown-linux-musl`
- `x86_64-unknown-linux-musl`

The workflow requires the expected architecture, accepts static or static-PIE
wording from `file`, rejects a `PT_INTERP` loader segment with `readelf`, and
checks that `ldd` resolves no dynamic dependencies.

Each architecture's download contains a tar archive, SHA-256 manifests, a
CycloneDX Cargo dependency inventory, `Cargo.lock`, `LICENSE`, and source
identity. The tar archive preserves executable permissions. Unpack that archive
before installing the binary; GitHub's artifact download itself may reset file
permissions.

After the build and test jobs pass, trusted `main` and version-tag runs sign
provenance and SBOM attestations and upload an `anvil-ring-<arch>-attested`
artifact with the signature bundles. Pull requests produce unsigned build
artifacts. The workflow does not create a GitHub Release or deploy a service.
See [Release verification](release-verification.md) for verification commands
and the remaining hosted-workflow evidence. Do not improvise a release builder
on a fleet-service host.

## Combined vLLM-and-tether container image

The repository Dockerfile builds the Rust binary under the target platform and
copies it into an explicitly selected vLLM base:

```bash
docker buildx build \
  --platform linux/amd64 \
  -f deploy/Dockerfile \
  --build-arg 'VLLM_IMAGE=vllm/vllm-openai:<tag>@sha256:<digest>' \
  -t anvil-ring-vllm:amd64 .
```

Use `linux/arm64` for an aarch64 rental. The base image is intentionally a
required build argument: silently following `latest` would make the multi-GB
runtime and its launcher behavior non-reproducible.

Building under `TARGETPLATFORM` is slower when Docker needs emulation, but it
avoids the previous error where `FROM --platform=$BUILDPLATFORM` plus `uname -m`
selected the builder's architecture and mislabeled the final image.

## Binary-only local container build

On a machine capable of running the target architecture (natively or through
configured emulation):

```bash
docker run --rm --platform linux/arm64 \
  -v "$PWD/cargo:/src" -w /src \
  rust:1.85-alpine sh -c '
    apk add --no-cache musl-dev
    rustup target add aarch64-unknown-linux-musl
    cargo build --release --locked \
      --target aarch64-unknown-linux-musl --bin anvil-ring
  '
```

Change both platform and target to x86-64/`x86_64-unknown-linux-musl` for amd64.

## Verify an artifact

```bash
file cargo/target/aarch64-unknown-linux-musl/release/anvil-ring
readelf -l cargo/target/aarch64-unknown-linux-musl/release/anvil-ring \
  | grep INTERP && echo 'unexpected dynamic loader'
```

The second command should print no `INTERP` row. A local macOS unit and
integration suite cannot verify the architecture or static-link properties of a
Linux release artifact.

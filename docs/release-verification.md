# Release verification

## What a download contains

The release workflow builds both Linux musl targets using the pinned Rust 1.85
Alpine image. It checks ELF architecture and static linking, packages the result,
and runs the packaged executable as UID/GID 65534 with no network, no extra
capabilities, and a read-only filesystem.

Each architecture's download contains:

| File | Purpose |
|---|---|
| `anvil-ring-<version>-<target>.tar.gz` | Installable archive preserving executable permissions |
| `anvil-ring` | Same executable, exposed separately for attestation verification |
| `sbom.cdx.json` | CycloneDX 1.5 Cargo dependency inventory for this binary and target |
| `Cargo.lock` | Locked Cargo resolution used by the build |
| `build-info.json` | Version, target, source revision, dirty-tree flag, binary and lockfile digests |
| `LICENSE` | Project license |
| `SHA256SUMS` | Digests of the unpacked content |
| `ARCHIVE-SHA256SUMS` | Digest of the installable archive |
| `provenance.sigstore.json` | Signed provenance for the binary and archive, on trusted runs |
| `sbom.sigstore.json` | Signed association of the binary with its Cargo SBOM, on trusted runs |

The pinned `cargo-cyclonedx` 0.5.9 tool uses Cargo metadata for the specified
target and default features. Its inventory includes transitive and build-time
Cargo dependencies, excludes development-only dependencies, and records crate
licenses. It does not inventory the vLLM container, system packages, model
weights, or individual C/assembly files bundled inside a crate. Those require
separate image and license review. See the
[generator's documentation](https://github.com/CycloneDX/cyclonedx-rust-cargo/tree/cargo-cyclonedx-0.5.9/cargo-cyclonedx).

The packaging script checks that the ELF architecture and the SBOM's version and
target match. It rejects an ELF dynamic-loader segment. It also removes the
local checkout path from the SBOM's package reference. A checksum detects changed
bytes; source identity and signatures establish which workflow vouched for them.

## Signing boundary

Build and test jobs have read-only repository permissions. The signing job runs
only after both succeed, for `main` or version-tag runs, and never for a pull
request. It has narrowly scoped OIDC and attestation permissions and does not
check out or execute the downloaded program. It checks the transferred file
digests before signing. All third-party actions are pinned to commit SHAs.

The job uses GitHub's short-lived signing identity and publishes its signature
bundles in `anvil-ring-<arch>-attested` run artifacts. There is no long-lived
signing key in the repository. This follows
[GitHub's artifact-attestation workflow](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations).

The workflow does not create a GitHub Release, publish a container, or deploy
services. A local development bundle has no GitHub signature. The source revision
in a dirty bundle identifies its base commit only, and
`source_tree_dirty: true` disqualifies it as a release of that commit.

## Verify before installation

Download the attested artifact for the required architecture from the intended
successful workflow run. Obtain the expected full commit SHA and ref from the
reviewed source/run, independently of the downloaded `build-info.json`.

The following example verifies an ARM64 archive from `main`. Replace the version
and commit value with the intended release. For a tag, use its exact
`refs/tags/v...` ref instead of `refs/heads/main`.

```bash
EXPECTED_COMMIT='<reviewed-full-commit-sha>'
ARCHIVE='anvil-ring-0.1.0-aarch64-unknown-linux-musl.tar.gz'

sha256sum -c ARCHIVE-SHA256SUMS
gh attestation verify "$ARCHIVE" \
  --bundle provenance.sigstore.json \
  --repo fakoli/anvil-ring \
  --signer-workflow fakoli/anvil-ring/.github/workflows/release-artifact.yml \
  --source-ref refs/heads/main --source-digest "$EXPECTED_COMMIT" \
  --deny-self-hosted-runners

tar -xzf "$ARCHIVE"
sha256sum -c SHA256SUMS
gh attestation verify anvil-ring \
  --bundle sbom.sigstore.json \
  --repo fakoli/anvil-ring \
  --signer-workflow fakoli/anvil-ring/.github/workflows/release-artifact.yml \
  --source-ref refs/heads/main --source-digest "$EXPECTED_COMMIT" \
  --predicate-type https://cyclonedx.org/bom \
  --deny-self-hosted-runners
./anvil-ring --version
```

On macOS, use `shasum -a 256 -c` for checksum verification; the Linux binary
itself still needs a Linux host. The verification flags are documented in the
[GitHub CLI manual](https://cli.github.com/manual/gh_attestation_verify).

## Local development packaging

After building and checking the ELF as described in [Cross-builds](cross-build.md):

```bash
cargo install cargo-cyclonedx --version 0.5.9 --locked
export SOURCE_DATE_EPOCH="$(git log -1 --format=%ct)"
cargo cyclonedx --manifest-path cargo/Cargo.toml --format json \
  --describe binaries --target aarch64-unknown-linux-musl \
  --target-in-filename --spec-version 1.5
python3 scripts/package_release.py \
  --binary cargo/target/aarch64-unknown-linux-musl/release/anvil-ring \
  --sbom cargo/anvil-ring_bin_aarch64-unknown-linux-musl.cdx.json \
  --target aarch64-unknown-linux-musl --output dist/aarch64
```

The script refuses a dirty checkout by default. `--allow-dirty` permits a labelled
development bundle for local testing. Neither this flag nor a local checksum
can establish hosted build provenance.

## Evidence as of 2026-09-05

Both Linux release builds, 124-test suites on each Linux architecture, local
SBOM generation, archive/checksum checks, and unprivileged executable smoke tests
passed. Five Python packaging regressions cover download integrity, executable
permissions, source-path normalization, architecture mismatch, dynamic loaders,
truncated ELF headers, and an SBOM for the wrong target.

The updated signing workflow has not run on GitHub for this working tree.
Signature verification against that hosted run remains required. No real vLLM
GPU container or provider egress qualification is implied by these local results.

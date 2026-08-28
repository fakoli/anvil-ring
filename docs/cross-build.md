# Cross-compiling the static Linux binary (I-7) from macOS
#
# Problem: `cargo build --target aarch64-unknown-linux-musl` on macOS fails at
# LINK time, not compile time:
#
#   ld: unknown options: --as-needed -Bstatic -Bdynamic --eh-frame-hdr -z --gc-sections ...
#   clang: error: linker command failed
#
# Cause: cargo uses `cc` as the linker for the musl target, and on macOS `cc` is
# clang driving Apple's linker, which does not understand GNU ld flags. The Rust
# code compiles fine; there is no source problem here.
#
# This is exactly why the Dockerfile cross-builds in a Linux builder stage rather
# than on the operator's laptop.

## Option A — Docker (recommended, matches deploy/Dockerfile)

```bash
cd <repo root>
docker build -f deploy/Dockerfile --build-arg VLLM_IMAGE=vllm/vllm-openai .
# or build just the binary, no engine image pulled:
docker run --rm -v "$PWD/cargo":/src -w /src rust:1.85-alpine sh -c \
  'apk add --no-cache musl-dev \
   && rustup target add aarch64-unknown-linux-musl \
   && cargo build --release --locked --target aarch64-unknown-linux-musl --bin anvil-ring'
```

The artifact lands in `cargo/target/aarch64-unknown-linux-musl/release/anvil-ring`.
Verify it is really static:

```bash
file cargo/target/aarch64-unknown-linux-musl/release/anvil-ring
# expect: statically linked (no dynamic dependencies)
```

## Option B — a cross toolchain on the host

Install `messense/rust-musl-cross` tap, then point cargo's musl linkers at it:

```bash
brew tap messense/musl-cross && brew install messense/musl-cross/musl-cross
cat >> .cargo/config.toml <<'EOF'
[target.aarch64-unknown-linux-musl]
linker = "aarch64-linux-musl-gcc"
rustflags = ["-C", "target-feature=+crt-static"]
[target.x86_64-unknown-linux-musl]
linker = "x86_64-linux-musl-gcc"
rustflags = ["-C", "target-feature=+crt-static"]
EOF
cargo build --release --target aarch64-unknown-linux-musl --bin anvil-ring
```

Not installed on this machine as of 2026-08-28, so Option A is the verified path.

## CI note

CI runs on Linux, so this failure mode does not exist there — add
`cargo build --release --target <musl triple>` to the release job and the
`file` check above as the gate that enforces I-7.


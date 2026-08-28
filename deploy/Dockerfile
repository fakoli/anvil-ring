# anvil-ring — FUSED serving image (ADR-0004, decision 2).
#
# One image = one `docker run` = vLLM/SGLang behind anvil-ring, reachable from
# our network with no port-forwarding, no inbound rules, and no TUN/root on the
# rental (I-1, I-2).
#
# ADR-0004 records the costs of fusing knowingly: release cadence is coupled to
# the engine's, a proxy-only fix republishes a multi-GB artifact, and the image
# already ships uvicorn/starlette so this adds a second HTTP stack. The
# mitigation is a hard constraint, not a preference: the binary is statically
# linked and reads config only from its own environment, so converting to a
# sidecar later is a one-line COPY change rather than a refactor.
#
# Build the binary in the builder stage below, for a musl target so it runs
# regardless of the base image's glibc version:
#   aarch64-unknown-linux-musl / x86_64-unknown-linux-musl

ARG BUILDPLATFORM=linux/amd64
FROM --platform=$BUILDPLATFORM rust:1.85-alpine AS build
RUN apk add --no-cache musl-dev

WORKDIR /src
COPY cargo/Cargo.toml cargo/Cargo.lock ./
COPY cargo/src ./src
# Pick the musl target matching the build platform; rustup knows the triple.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    set -eux; \
    ARCH="$(uname -m)"; \
    case "$ARCH" in \
      aarch64) TARGET="aarch64-unknown-linux-musl" ;; \
      x86_64)  TARGET="x86_64-unknown-linux-musl" ;; \
      *) echo "unsupported build arch: $ARCH" >&2; exit 1 ;; \
    esac; \
    rustup target add "$TARGET"; \
    cargo build --release --locked --target "$TARGET" --bin anvil-ring

ARG VLLM_IMAGE=vllm/vllm-openai
FROM ${VLLM_IMAGE}

# I-10: the engine MUST bind loopback. anvil-ring is the only thing allowed to
# reach it, so authz lives in exactly one place and there is one thing to audit.
ENV VLLM_HOST=127.0.0.1 \
    ANVIL_RING_LISTEN=0.0.0.0:8080 \
    ANVIL_RING_UPSTREAM=http://127.0.0.1:8000

# Static binary. `scratch`-style COPY of a musl build has no runtime deps to
# satisfy, which is the point of I-7.
COPY --from=build target/*/release/anvil-ring /usr/local/bin/anvil-ring

# Non-root (I-2). The proxy needs no privileges: no bind below 1024, no TUN, no
# CAP_NET_ADMIN.
USER nobody

# NOTE for anyone tempted to add EXPOSE 8000: that would advertise the engine
# port, which is the opposite of the design. Only anvil-ring's listener exists,
# and even that is reached over an OUTBOUND tunnel (I-1), not a published port.
ENTRYPOINT ["/usr/local/bin/anvil-ring", "proxy"]

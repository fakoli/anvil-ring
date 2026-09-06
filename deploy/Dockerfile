# Fused rental-side image: a loopback-only vLLM engine plus the outbound tether.
# The base image must be pinned explicitly, preferably by digest:
#   docker build --build-arg VLLM_IMAGE=vllm/vllm-openai:<tag>@sha256:<digest> ...

ARG TARGETPLATFORM
ARG VLLM_IMAGE

# Build under the target platform (emulated when necessary). Building under
# BUILDPLATFORM and selecting the target with `uname -m` produced the builder's
# architecture, not the image architecture.
FROM --platform=$TARGETPLATFORM rust:1.85-alpine AS build
RUN apk add --no-cache musl-dev

WORKDIR /src
COPY cargo/Cargo.toml cargo/Cargo.lock ./
COPY cargo/src ./src
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

FROM ${VLLM_IMAGE}

USER root

# vLLM's supported container command is `vllm serve`. The supervisor adds the
# mandatory loopback host/port and starts `anvil-ring tether` beside it.
COPY --from=build /src/target/*/release/anvil-ring /usr/local/bin/anvil-ring
COPY deploy/entrypoint.py /usr/local/bin/anvil-ring-entrypoint
RUN chmod 0755 /usr/local/bin/anvil-ring /usr/local/bin/anvil-ring-entrypoint \
    && mkdir -p /home/vllm/.cache /home/vllm/.config \
    && chown -R 2000:0 /home/vllm \
    && (grep -Eq '^[^:]*:[^:]*:2000:' /etc/passwd \
        || echo 'anvil-ring:x:2000:0:Anvil Ring:/home/vllm:/bin/sh' >> /etc/passwd)

ENV HOME=/home/vllm \
    ANVIL_RING_UPSTREAM=http://127.0.0.1:8000

# Numeric non-root identity works even when a pinned older base image lacks the
# newer named `vllm` account. No TUN device, privileged port, or capability is
# required.
USER 2000:0

# No EXPOSE: the engine binds loopback and the rental accepts no inbound traffic.
ENTRYPOINT ["/usr/local/bin/anvil-ring-entrypoint"]

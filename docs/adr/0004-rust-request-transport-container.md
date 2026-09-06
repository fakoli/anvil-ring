# ADR-0004: Rust request transport in a combined serving container

- **Status:** Accepted
- **Date:** 2026-08-28
- **Supersedes:** the earlier "standard-library-only Python client" position and
  ADR-0002's framing (see Consequences)

## Context

Two decisions arrived together and are coupled: the implementation language, and
the fact that the component is a **proxy that terminates in front of the inference
engine** rather than an unstructured TCP relay.

The workload is OpenAI-compatible token streaming. vLLM and SGLang emit
server-sent events: many small chunks per second, each of which must be
**flushed immediately**. Failure is
silent and adversarial — buffered output presents as *latency*, not as an error,
so it is attributed to the model rather than the proxy.

## Decision 1 — Rust for the request-transport runtime

The Python scaffold was written before the proxy requirement existed. Its
standard-library-only rule was copied from `anvil-events` without validation
against this project's deployment constraints.
That reasoning does not transfer: an event journal is bursty small JSON over a
socket; a token stream is sustained per-chunk latency sensitivity.

Why the project selected Rust instead of extending the Python scaffold:

- Per-chunk flush is per-chunk syscall plus per-chunk allocation — exactly where
  GIL contention and GC pauses land.
- Stdlib `http.server` is single-threaded by default; concurrency means threads
  (GIL) or asyncio, and asyncio has **no stdlib HTTP client**, so streaming
  through becomes hand-rolled `http.client` in a thread pool.
- A ~100 MB interpreter base layer inside an image already multiple GB, pulled per
  rental.

Rust over Go, per operator preference: a small static binary, and `hyper` exposes
each HTTP body frame so the proxy can forward chunks as they arrive. **Accepted cost:**
Rust has no `net/http/httputil.ReverseProxy`. Hop-by-hop
header stripping, `Connection: keep-alive` semantics, and `Expect: 100-continue`
are ours to implement correctly, and are the most likely source of early bugs.
Go remains the cheaper option if that cost later proves real — that is a
reversible judgment, not a locked-in one.

## Decision 2 — one combined container based on the serving engine

Anvil Ring is built as a standalone static binary but can be **shipped inside a
`FROM vllm/vllm:...` image**. Starting that image runs vLLM on loopback and the
Anvil Ring tether as supervised child processes. SGLang remains a supported
loopback upstream, but this repository does not yet provide an SGLang container
image or startup command.

**Costs, accepted knowingly:**
- Release cadence is coupled to the serving engine's. A proxy-only fix requires a
  full image republish of a multi-GB artifact.
- The image already carries `uvicorn`/`starlette`; we add a second HTTP stack.
- Image size is dominated by CUDA/PyTorch, so the pull cost is paid regardless —
  but it is paid per rental.

**Portability constraint:** the binary MUST be
statically linked with a Linux musl target, self-contained, and MUST NOT read
configuration from any path outside its environment variables. This keeps it
possible to copy the binary into the serving image or run it in a separate
container without changing the Rust implementation.

## Consequences

**ADR-0002 (transport) is superseded in practice.** Because the component is now an HTTP
proxy speaking over its own tunnel, the "forward arbitrary TCP" requirement that
made chisel vs. `ssh -R` interesting mostly disappears: we proxy HTTP through a
tunnel we implement, and TLS/WebSocket come from the Rust standard ecosystem. The
open empirical question from ADR-0003—whether each provider permits outbound TLS
to the selected hub—still requires a deployment check, but it no longer blocks
the transport implementation.

**The self-contained-executable requirement replaces the original language.**
The original requirement said the client could use only its language's standard
library. It now says:

> The remote-side binary adds **no third-party language-runtime dependency that
> must be separately installed** on the host. Locked, reviewable crate
> dependencies compiled into one static artifact are permitted.

The original objective — avoid unnecessary installation-time supply chain on a
rented GPU host — is preserved and is arguably served better: one static binary
with an audited `Cargo.lock` has a smaller and more inspectable dependency set
than installing a language runtime and packages on each rental.

## Language split, accepted

A future persistent administration service could use Python because it would not
process token-stream chunks. The shipped hub and tether are currently both Rust
and share the binary frame codec in `cargo/src/frames.rs`; its round-trip and
rejection tests define the implemented wire format. `schemas/tether-v1.json` is
an unimplemented registration-manifest design artifact, not the live protocol
and not a packaged runtime dependency.

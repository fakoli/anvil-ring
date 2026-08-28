# ADR-0004: Data plane in Rust; proxy fused into the serving image

- **Status:** Accepted
- **Date:** 2026-08-28
- **Supersedes:** the "stdlib-only Python client" position in I-7, and implicitly
  ADR-0002's framing (see Consequences)

## Context

Two decisions arrived together and are coupled: the implementation language, and
the fact that the component is a **proxy that terminates in front of the inference
engine** rather than a dumb byte-pipe.

The workload is OpenAI-compatible token streaming. vLLM/SGLang emit SSE: many
small chunks per second, each of which must be **flushed immediately**. Failure is
silent and adversarial — buffered output presents as *latency*, not as an error,
so it is attributed to the model rather than the proxy.

## Decision 1 — Rust for the data plane

The Python scaffold was written before the proxy requirement existed, and its
invariant I-7 ("stdlib-only") was imported from `anvil-events` by pattern-match.
That reasoning does not transfer: an event journal is bursty small JSON over a
socket; a token stream is sustained per-chunk latency sensitivity.

Why Python specifically is the wrong tool for *this* shape:

- Per-chunk flush is per-chunk syscall plus per-chunk allocation — exactly where
  GIL contention and GC pauses land.
- Stdlib `http.server` is single-threaded by default; concurrency means threads
  (GIL) or asyncio, and asyncio has **no stdlib HTTP client**, so streaming
  through becomes hand-rolled `http.client` in a thread pool.
- A ~100 MB interpreter base layer inside an image already multiple GB, pulled per
  rental.

Rust over Go, per operator preference: smaller static binary, and `hyper` gives
explicit `body.data()` + `body.reset()` streaming control. **Accepted cost,
recorded honestly:** Rust has no `net/http/httputil.ReverseProxy`. Hop-by-hop
header stripping, `Connection: keep-alive` semantics, and `Expect: 100-continue`
are ours to implement correctly, and are the most likely source of early bugs.
Go remains the cheaper option if that cost later proves real — that is a
reversible judgment, not a locked-in one.

## Decision 2 — one fused image based on the serving engine

anvil-ring is built as a standalone static binary but **shipped inside a
`FROM vllm/vllm:...` image**, so `docker run` of one image yields "SGLang or vLLM,
behind anvil-ring, reachable from our network."

**Costs, accepted knowingly:**
- Release cadence is coupled to the serving engine's. A proxy-only fix requires a
  full image republish of a multi-GB artifact.
- The image already carries `uvicorn`/`starlette`; we add a second HTTP stack.
- Image size is dominated by CUDA/PyTorch, so the pull cost is paid regardless —
  but it is paid per rental.

**Mitigation built in as a constraint (not a preference):** the binary MUST be
statically linked (`musl` target, `CGO`-free), self-contained, and MUST NOT read
configuration from any path outside its own env. This makes fused-vs-sidecar a
one-line `Dockerfile` difference in either direction. If the coupling costs turn
out to hurt, extraction is a copy-statement change, not a refactor.

## Consequences

**ADR-0002 (transport) is largely mooted.** Because the component is now an HTTP
proxy speaking over its own tunnel, the "forward arbitrary TCP" requirement that
made chisel vs. `ssh -R` interesting mostly disappears: we proxy HTTP through a
tunnel we implement, and TLS/WebSocket come from the Rust standard ecosystem. The
open empirical question from ADR-0003 — per-provider egress policy — remains open
and still gates deployment, but it no longer gates the implementation.

**I-7 is redefined, and this is the important one.** It previously said
"stdlib-only." It now says:

> The remote-side binary adds **no third-party language-runtime dependency that
> must be separately installed** on the host. Vendored, source-audited crate
> dependencies compiled into one static artifact are permitted.

The *why* of the original — do not import supply chain onto someone else's GPU box
as an install step — is preserved and is arguably served better: one static binary
with an audited `Cargo.lock` has a smaller and more inspectable surface than
`pip install`-ing a chisel shim.

## Language split, accepted

Hub/control plane may remain Python. It has no latency sensitivity, and a
control-plane/data-plane language split is standard rather than accidental
inconsistency. What must stay shared is the **wire contract** (`schemas/`), which
is language-neutral by construction.

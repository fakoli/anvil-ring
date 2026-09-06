# ADR-0005: Verify the streaming timing check with a buffering canary

- **Status:** Accepted; automated comparison restored
- **Original date:** 2026-08-28
- **Updated:** 2026-08-31

## Context

A proxy can return the correct final response body while still delaying every
server-sent event until the engine finishes. Body equality alone cannot detect
that failure. The proxy integration test therefore records when response bytes
arrive and requires multiple reads spread across the engine's emission window.

A useful timing test must also prove that it rejects known buffering behavior.
The repository includes `anvil-ring-buffering-canary`, a test executable that
authenticates and forwards the same HTTP request as the local proxy but reads the
complete upstream response before sending any of it to the caller.

## Earlier harness failure

The first automated comparison used fixed ports and unreliable child-process
startup handling. On the original macOS test host, the spawned canary exited or
failed to return a response before the assertion could measure it. The project
retained the canary and removed that unreliable test.

The canary also contained an independent request-forwarding defect: it read the
caller's request body but forwarded only the rewritten headers. A fake engine
that honored `Content-Length` waited for the missing body, so the canary returned
no response. That behavior made it an invalid negative control because buffering
was not its only difference from the real proxy.

## Decision

The `proxy_e2e` test harness now:

1. asks the operating system for unused loopback ports;
2. waits for the child process's own listening log before sending a request;
3. forwards a complete request body through the canary;
4. sends the same four-event streaming fixture through the real proxy and the
   buffering canary; and
5. applies the same `arrived_incrementally` timing predicate to both responses.

`streaming_arrives_incrementally_not_all_at_once` requires the real proxy to
pass. `streaming_assertion_rejects_the_buffering_canary` requires the canary to
return the complete body and fail the timing predicate. Both tests run as part
of `cargo test --locked --all-targets`.

## Consequences

The streaming regression now has an automated negative control. A change that
makes the timing predicate too weak will fail because the deliberately buffered
response will appear incremental. A change that breaks the canary's ordinary
HTTP forwarding will also fail before the timing result is accepted.

The canary remains test infrastructure and must not be deployed as an Anvil Ring
runtime. Its source is intentionally buffered; replacing that behavior with
incremental writes would invalidate the comparison.

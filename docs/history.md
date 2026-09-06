# Investigation archive

On 2026-08-28, testing found that cancelling a tether during a streaming
response could leave the caller's response body open. The hub observed the
disconnect, but one detached WebSocket writer retained ownership of the
connection and prevented complete session cleanup.

The repair made the tether session own its writer task, routed post-attach
errors through one teardown path, and ended every in-flight response when the
session closed. The current `forward_e2e` test cancels a tether during an active
stream and requires both an ended caller response and a recorded hub state
transition.

Earlier scratch plans, raw measurements, and session notes were removed from the
published portal during the production documentation review. Git history retains
those records when forensic detail is required. Use [Testing and
verification](testing.md) for the current test procedure and the
[STATE snapshot](https://github.com/fakoli/anvil-ring/blob/main/STATE.md) for
dated command results.

Superseded transport analysis and dated provider-probe evidence remain in the
[architecture decision records](adr/README.md), where each page states its
current decision status.

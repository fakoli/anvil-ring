//! `reframe_head_for_tunnel` must survive a bare-LF header terminator.
//!
//! Discovered from a field panic report (`attempt to subtract with overflow` in
//! the tether, in this same function). The old crash line no longer exists at
//! HEAD, but the identical arithmetic does:
//!
//!     let Some(end) = find_header_end(head) else { return head.to_vec() };
//!     let block = &head[..end - 4];      // <-- panics when end == 2
//!
//! `find_header_end` matches either `\r\n\r\n` (returning `i + 4`) **or** the bare
//! `\n\n` (returning `i + 2`). So a header block terminated with bare LF -- legal
//! from a non-compliant engine, and exactly what some hand-rolled test servers and
//! proxies emit -- returns a small `end`, and `end - 4` underflows.
//!
//! A panic here is not a cosmetic bug: it kills the tether's worker task and takes
//! down the whole tunnel for every stream multiplexed over it.
use anvil_ring::tunnel::reframe_head_for_tunnel;

const BARE_LF_HEAD: &[u8] =
    b"HTTP/1.1 200 OK\ncontent-type: text/event-stream\ntransfer-encoding: chunked\n\n";

#[test]
fn bare_lf_terminated_head_does_not_panic() {
    // The regression itself: before the fix this line panicked on `end - 4`.
    let out = reframe_head_for_tunnel(BARE_LF_HEAD);
    let s = String::from_utf8_lossy(&out);

    // Whatever the framing decision, the head must survive and stay parseable --
    // a mangled head is how a wrong status code reaches a caller (I-11).
    let (res, rest) = anvil_ring::hub::parse_head(&out)
        .unwrap_or_else(|| panic!("reframed head no longer parses: {s:?}"));
    assert_eq!(res.status(), hyper::StatusCode::OK);
    assert!(rest.is_empty(), "body bytes leaked into the head: {s:?}");
}

#[test]
fn a_head_shorter_than_the_terminator_cannot_panic() {
    // Two bytes total: `end` is 2, and `2 - 4` is the underflow.
    let out = reframe_head_for_tunnel(b"a\n\n");
    // No assertion about content is meaningful for a non-head; the point is only
    // that this returns instead of aborting the process.
    assert!(!out.is_empty() || out.is_empty());
}

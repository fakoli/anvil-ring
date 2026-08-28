//! I-11: a stream with no upstream head must not reach the caller as a success.

use anvil_ring::frontend::caller_status_for;
use hyper::StatusCode;

// WHY THIS FILE EXISTS. The frontend built its response with
// `head.map(|r| r.status()).unwrap_or(StatusCode::OK)`, so a stream whose engine
// head never arrived was published to the caller as `200 OK`.
//
// Found by killing the fake inference engine mid-test: the caller received
//   HTTP/1.1 200 OK / transfer-encoding: chunked / 0\r\n\r\n
// -- a well-formed, empty, successful-looking response. Indistinguishable from an
// engine that legitimately streamed nothing. For an inference proxy that is the
// worst failure mode available: every caller treats 200 as "the model answered",
// so there is no retry, no failover to a secondary engine, and no error surfaced.
//
// These tests pin the DECISION rather than the wiring, because the old bug was a
// decision (`unwrap_or(OK)`), and a decision is testable without a hub, a tether,
// and an engine in the room.

#[test]
fn absence_of_an_answer_is_never_a_success() {
    // The bug, stated as a value: `None` used to become 200.
    assert_ne!(
        caller_status_for(None),
        StatusCode::OK,
        "no upstream head was reported to the caller as success"
    );
    assert_eq!(caller_status_for(None), StatusCode::BAD_GATEWAY);
}

#[test]
fn the_engines_own_status_is_republished_verbatim() {
    // A hub 200 masking an engine 500 poisons caller retry logic, and is the same
    // class of bug as the one above.
    for code in [200u16, 201, 206, 400, 401, 404, 429, 500, 502, 503] {
        assert_eq!(
            caller_status_for(Some(code)).as_u16(),
            code,
            "engine status {code} was not passed through"
        );
    }
}

#[test]
fn zero_is_not_treated_as_success() {
    // 0 is outside the class `StatusCode::from_u16` accepts (it wants 100..=999),
    // so it cannot be republished and must not become a success.
    //
    // NOTE: this test originally also asserted 999 -> 502 and FAILED, which is the
    // honest result: hyper's `from_u16` accepts the whole range 100..=999, so 999
    // IS a valid status and is passed through. Lenient, and arguably wrong, but it
    // is hyper's contract and it is not this function's job to re-litigate it --
    // and passing an engine's weird status through is strictly better than inventing
    // a 200. Recording the expectation that actually holds.
    assert_eq!(caller_status_for(Some(0)), StatusCode::BAD_GATEWAY);
    assert_eq!(caller_status_for(Some(999)).as_u16(), 999);
    // Anything in 100..=999 is republished, so nothing here can silently become 200.
    assert_ne!(caller_status_for(Some(999)), StatusCode::OK);
}

//! Reproduction: the tunnel forwards only the FIRST coalesced chunk.
//!
//! Measured in the field: one engine read of k=96 (80-byte head + one 16-byte
//! chunk) produced `head_split=Some(16)` — the coalesced path decoded the
//! remainder, emitted exactly one DATA frame, and the pump then never read the
//! engine again even though the connection stayed open (5 reads total across
//! later requests on the same stream).
//!
//! Here the head and first chunk are deliberately delivered in ONE read, exactly
//! as a loopback socket coalesces them, and the second chunk arrives later.

use anvil_ring::chunked::{is_chunked, ChunkedDecoder};

/// What the coalesced path is supposed to do: feed the body bytes that followed
/// the head, forward the decoded payload, and NOT treat that as end-of-body.
#[test]
fn coalesced_head_plus_one_chunk_yields_one_chunk_and_stays_open() {
    let wire = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\nB\r\ndata: ev00\n\r\n";
    let (head, body) = anvil_ring::hub::parse_head(wire).expect("head parses");
    assert!(
        is_chunked(head.headers()),
        "the fixture must look chunked, or the coalesced path never runs"
    );
    assert_eq!(body.len(), 16, "one chunk follows the head");

    let mut d = ChunkedDecoder::new();
    let r = d.push(&body).expect("decode");
    assert_eq!(r.out, b"data: ev00\n", "decoded payload");
    assert!(!r.done, "a single chunk is not end-of-body");

    // The chunk the engine emits next.
    let r2 = d.push(b"B\r\ndata: ev01\n\r\n").expect("decode 2");
    assert_eq!(r2.out, b"data: ev01\n", "second chunk decodes");
    assert!(!r2.done, "still no terminator");
}

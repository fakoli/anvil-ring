#[path = "common/chunked_decoder.rs"]
mod chunked_decoder;

use chunked_decoder::decode_chunked;

#[test]
fn decode_chunked_agrees_with_the_engine() {
    // The decoder is test-only code, so it gets a test too: a decoder that drops
    // the last chunk would silently make every e2e assertion vacuously weak.
    assert_eq!(
        decode_chunked("5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"),
        "hello world"
    );
    // Empty-body case: a zero-length first chunk means no payload, not a parse bug.
    assert_eq!(decode_chunked("0\r\n\r\n"), "");
}

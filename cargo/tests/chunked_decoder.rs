/// Minimal chunked-transfer decoder: hex length line, that many bytes, CRLF,
/// repeat, stop at a zero-length chunk.
///
/// Why the test needs it: the tunnel forwards the engine's bytes *verbatim*, so
/// the wire carries HTTP/1.1 chunked framing. A real caller has that decoded by
/// its HTTP client; this test hand-rolls a socket, so it must decode too.
/// Asserting on raw frames would assert the wrong thing. Note this is the third
/// time chunked framing has bitten a *test* rather than the product in this
/// project -- see the `body_of` pitfall in STATE.md.
fn decode_chunked(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let nl = match bytes[i..].windows(2).position(|w| w == b"\r\n") {
            Some(p) => i + p,
            None => break,
        };
        let line = std::str::from_utf8(&bytes[i..nl])
            .unwrap_or("")
            .trim()
            .to_string();
        let len = match usize::from_str_radix(&line, 16) {
            Ok(n) => n,
            Err(_) => break,
        };
        if len == 0 {
            break;
        }
        let start = nl + 2;
        let end = (start + len).min(bytes.len());
        out.push_str(std::str::from_utf8(&bytes[start..end]).unwrap_or(""));
        i = end + 2; // skip the chunk trailer CRLF
    }
    out
}

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

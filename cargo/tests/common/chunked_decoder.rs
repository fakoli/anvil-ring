/// Decode the HTTP/1.1 chunk framing used by the raw-socket integration tests.
///
/// Production HTTP clients remove this framing automatically. These tests use
/// sockets directly, so their assertions must decode the body before comparing
/// application payloads.
pub fn decode_chunked(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::new();
    let mut cursor = 0usize;

    while cursor < bytes.len() {
        let Some(line_end) = bytes[cursor..].windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let line_end = cursor + line_end;
        let size = std::str::from_utf8(&bytes[cursor..line_end])
            .ok()
            .and_then(|line| usize::from_str_radix(line.trim(), 16).ok());
        let Some(size) = size else { break };
        if size == 0 {
            break;
        }

        let start = line_end + 2;
        let end = (start + size).min(bytes.len());
        output.push_str(std::str::from_utf8(&bytes[start..end]).unwrap_or(""));
        cursor = end + 2;
    }

    output
}

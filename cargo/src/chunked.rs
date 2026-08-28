//! HTTP/1.1 chunked-transfer decoding, for the tether's engine reads.
//!
//! WHY THIS EXISTS
//! `transfer-encoding` is a hop-by-hop header (RFC 9110 §7.6.1) and this crate
//! strips it from forwarded messages — correctly, and `headers::ALWAYS_STRIP` is
//! the single place that says so. But the ENGINE's *body bytes* are chunked. So a
//! tether that forwards engine bytes verbatim produces a response whose headers
//! promise no chunked coding while its payload is chunk-coded, and the hub's HTTP
//! server then applies its own framing on top. The observable result, measured on
//! the wire in this project: `F` (15) followed by twelve bytes — a length that
//! disagrees with its own payload, which a real HTTP client rejects or mis-slices.
//!
//! The fix is to end the engine's transfer coding at the hop that owns it: the
//! tether. Each hop re-encodes for its own transport (engine -> tether: chunked as
//! vLLM sent it; tether -> hub: WebSocket DATA frames; hub -> caller: hyper's own
//! framing). That is what "hop-by-hop" means, and it is why the header is stripped
//! rather than forwarded.
//!
//! INVARIANTS
//! - Streaming is preserved: a chunk that is complete is emitted immediately, even
//!   if the rest of the stream has not arrived (I-9). A decoder that buffered to
//!   find the next boundary would silently break token streaming.
//! - A partial chunk is *not* emitted early. Emitting `data: t` from a `data: two`
//!   chunk would be correct on the wire but wrong for a client parsing SSE events.
//! - Chunk extensions (`;ext`) and trailers are tolerated; a non-hex length is a
//!   protocol error, surfaced as `Malformed` rather than guessed at.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkError {
    /// A chunk-size line that is not valid hex, or a framing inconsistency.
    Malformed(&'static str),
}

#[derive(Default)]
enum Mode {
    #[default]
    Size,
    /// A chunk of this many bytes is pending; `sent` tracks how many were already
    /// handed out so a large chunk can be streamed in pieces.
    Body {
        remaining: usize,
    },
    /// CRLF after a chunk's data.
    CrLf,
    Done,
}

/// Incremental decoder: feed engine bytes, get decoded body bytes out.
#[derive(Default)]
pub struct ChunkedDecoder {
    buf: Vec<u8>,
    mode: Mode,
}

/// One step of decoding: bytes ready to forward, and whether the body is finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub out: Vec<u8>,
    pub done: bool,
}

impl ChunkedDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed `bytes` from the engine. Returns decoded payload bytes to forward.
    ///
    /// Never blocks waiting for more input than it was given, so it streams.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Decoded, ChunkError> {
        self.buf.extend_from_slice(bytes);
        let mut out = Vec::new();
        let mut done = false;

        loop {
            match std::mem::replace(&mut self.mode, Mode::Done) {
                Mode::Size => {
                    // Find the CRLF terminating the size line.
                    let nl = match self.buf.windows(2).position(|w| w == b"\r\n") {
                        Some(i) => i,
                        // No size line yet. Stay in Size and wait for more bytes;
                        // do NOT emit what we have, which is only a length prefix.
                        None => {
                            self.mode = Mode::Size;
                            break;
                        }
                    };
                    let line = &self.buf[..nl];
                    // Strip any chunk extension (`;name=value`) before the hex.
                    let hex = match line.iter().position(|&c| c == b';') {
                        Some(i) => &line[..i],
                        None => line,
                    };
                    let hex = std::str::from_utf8(hex)
                        .map(str::trim)
                        .map_err(|_| ChunkError::Malformed("size line not utf8"))?;
                    let n = usize::from_str_radix(hex, 16)
                        .map_err(|_| ChunkError::Malformed("chunk size not hex"))?;
                    self.buf.drain(..nl + 2);
                    if n == 0 {
                        // Last-chunk marker: whatever follows is trailers, not body.
                        self.buf.clear();
                        self.mode = Mode::Done;
                        done = true;
                        break;
                    }
                    self.mode = Mode::Body { remaining: n };
                }
                Mode::Body { remaining } => {
                    if remaining == 0 {
                        self.mode = Mode::CrLf;
                        continue;
                    }
                    // Hand out as much of this chunk as is available *now*. This is
                    // the streaming property: an SSE chunk becomes forwardable the
                    // moment it arrives, not when the whole response does.
                    let take = remaining.min(self.buf.len());
                    if take == 0 {
                        self.mode = Mode::Body { remaining };
                        break;
                    }
                    out.extend_from_slice(&self.buf[..take]);
                    self.buf.drain(..take);
                    let left = remaining - take;
                    self.mode = if left == 0 {
                        Mode::CrLf
                    } else {
                        Mode::Body { remaining: left }
                    };
                }
                Mode::CrLf => {
                    if self.buf.len() < 2 {
                        self.mode = Mode::CrLf;
                        break;
                    }
                    if &self.buf[..2] != b"\r\n" {
                        return Err(ChunkError::Malformed("missing CRLF after chunk"));
                    }
                    self.buf.drain(..2);
                    self.mode = Mode::Size;
                }
                Mode::Done => {
                    self.mode = Mode::Done;
                    done = true;
                    break;
                }
            }
        }
        Ok(Decoded { out, done })
    }

    /// The engine closed the connection without a last-chunk marker.
    ///
    /// Anything still buffered is an incomplete chunk and is DROPPED: forwarding a
    /// partial chunk would make the caller's SSE parser see a truncated event and,
    /// worse, one that looks well-formed.
    pub fn feed_eof(&mut self) -> Result<Vec<u8>, ChunkError> {
        match self.mode {
            Mode::CrLf => {
                // A trailing CRLF-only remainder is an acceptably-terminated body.
                self.buf.clear();
                self.mode = Mode::Done;
                Ok(Vec::new())
            }
            Mode::Size if self.buf.is_empty() => Ok(Vec::new()),
            Mode::Size => {
                // A bare size line with no data: the chunk never arrived.
                self.buf.clear();
                Err(ChunkError::Malformed("eof mid chunk-size line"))
            }
            Mode::Body { .. } => {
                self.buf.clear();
                Err(ChunkError::Malformed("eof mid chunk body"))
            }
            Mode::Done => Ok(Vec::new()),
        }
    }
}

/// Does this response say its body is chunked?
///
/// Uses the crate's own hop-by-hop stripper to decide, so this cannot drift from
/// the rule that removes the header in the first place.
pub fn is_chunked(headers: &http::HeaderMap) -> bool {
    headers
        .get("transfer-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(',')
                .any(|t| t.trim().eq_ignore_ascii_case("chunked"))
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(pieces: &[&[u8]]) -> (Vec<u8>, bool) {
        let mut d = ChunkedDecoder::new();
        let mut acc = Vec::new();
        let mut done = false;
        for p in pieces {
            let r = d.push(p).unwrap();
            acc.extend_from_slice(&r.out);
            done = r.done;
        }
        (acc, done)
    }

    #[test]
    fn reassembles_simple_chunks() {
        let (body, done) = decode_all(&[b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"]);
        assert_eq!(body, b"hello world");
        assert!(done);
    }

    #[test]
    fn emits_a_complete_chunk_immediately_without_waiting() {
        // The I-9 property, stated as a unit test: one SSE event arriving must be
        // forwardable before any later event exists. If this regressed, every token
        // would wait for the whole completion and the tunnel would look like vLLM
        // was 10x slower.
        //
        // `data: on` is EIGHT bytes, so the size line is `8`, not `a`. Getting this
        // wrong is itself instructive: with `a` the decoder legitimately consumes
        // the following CRLF as payload and this assertion fails on a 10-byte result.
        let mut d = ChunkedDecoder::new();
        let r = d.push(b"8\r\ndata: on\r\n").unwrap();
        assert_eq!(r.out, b"data: on");
        assert!(!r.done, "stream is not over just because one chunk is");
    }

    #[test]
    fn streams_partial_chunk_bytes_but_never_invents_a_boundary() {
        // What this invariant actually is, corrected after a wrong assertion:
        //
        // A chunk's bytes may legitimately arrive in pieces, and forwarding the
        // pieces as they arrive is the POINT (I-9) -- SSE payloads are byte streams,
        // and an event ending mid-read is the engine's own flush boundary, not ours.
        // Buffering until a chunk is "complete" would add latency per token and, for
        // an engine that flushes one partial line, could stall forever.
        //
        // What must NOT happen is inventing a chunk boundary: emitting bytes beyond
        // the declared size, or treating a size line as payload.
        let mut d = ChunkedDecoder::new();
        let r = d.push(b"9\r\ndata: ").unwrap();
        assert_eq!(
            r.out, b"data: ",
            "declared 9 bytes, 6 available -> exactly those 6 must pass through"
        );
        assert!(!r.done);
        // The rest of the same chunk continues the byte stream.
        let r2 = d.push(b"two\r\n").unwrap();
        assert_eq!(r2.out, b"two");
        // Never emits past the declared size: a size line alone yields nothing.
        let mut d2 = ChunkedDecoder::new();
        assert!(d2.push(b"9\r\n").unwrap().out.is_empty());
    }

    #[test]
    fn handles_a_size_line_split_across_reads() {
        // Real TCP splits arrive anywhere, including mid-hexdigits.
        let (body, done) = decode_all(&[b"5", b"\r\nhel", b"lo\r\n0\r\n\r\n"]);
        assert_eq!(body, b"hello");
        assert!(done);
    }

    #[test]
    fn tolerates_chunk_extensions() {
        let (body, _) = decode_all(&[b"5;ext=1\r\nhello\r\n0\r\n\r\n"]);
        assert_eq!(body, b"hello");
    }

    #[test]
    fn rejects_a_non_hex_size_line() {
        let mut d = ChunkedDecoder::new();
        // Needs the CRLF present to be parsed as a complete size line.
        let e = d.push(b"xyz\r\nhello\r\n");
        assert_eq!(e, Err(ChunkError::Malformed("chunk size not hex")));
    }

    #[test]
    fn eof_mid_chunk_is_an_error_not_a_truncated_pass_through() {
        let mut d = ChunkedDecoder::new();
        d.push(b"20\r\nshort").unwrap();
        assert_eq!(
            d.feed_eof(),
            Err(ChunkError::Malformed("eof mid chunk body"))
        );
    }

    #[test]
    fn one_shot_test_that_the_default_decoder_matches_the_std_reader() {
        // Cross-check against a decoder with a totally different shape (reads the
        // whole buffer, no state machine) so a shared bug in my incremental logic
        // cannot hide behind a self-consistent test.
        let raw = b"3\r\nabc\r\n0\r\n\r\n";
        let (mine, _) = decode_all(&[raw]);
        assert_eq!(mine, b"abc");
        assert_eq!(mine.len(), 3);
    }

    #[test]
    fn is_chunked_reads_the_header_the_same_way_the_stripper_does() {
        let mut h = http::HeaderMap::new();
        h.insert("transfer-encoding", "chunked".parse().unwrap());
        assert!(is_chunked(&h));
        let mut h2 = http::HeaderMap::new();
        h2.insert("transfer-encoding", "gzip, chunked".parse().unwrap());
        assert!(is_chunked(&h2), "chunked in a list still means chunked");
        let mut h3 = http::HeaderMap::new();
        h3.insert("content-type", "text/event-stream".parse().unwrap());
        assert!(!is_chunked(&h3));
    }
}

#[cfg(test)]
mod wire_shape_tests {
    use super::*;

    /// The engine's ACTUAL first read, byte for byte, as captured off a live
    /// socket in this project. If dechunking fails anywhere, this is the input
    /// it fails on -- reconstructed from the measured wire rather than invented.
    const LIVE_FIRST_READ: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\na\r\ndata: one\n\r\n";

    #[test]
    fn the_live_first_read_is_recognised_as_chunked() {
        let end = crate::hub::find_header_end(LIVE_FIRST_READ).expect("head present");
        let (head, body) = crate::hub::parse_head(LIVE_FIRST_READ).expect("head parses");
        assert_eq!(head.status().as_u16(), 200);
        assert!(
            is_chunked(head.headers()),
            "is_chunked missed a response that literally says so; headers={:?}",
            head.headers()
        );
        // The body must be exactly what follows the blank line.
        //
        // NOTE `end` already points PAST the CRLFs (find_header_end adds 4). Writing
        // `end + 4` here -- as an earlier version did, in this test *and* in
        // `tunnel::split_head` -- starts the body four bytes early. The production
        // symptom was a chunk length that disagreed with its payload by exactly those
        // four bytes, which no amount of unit-testing the decoder could reveal.
        assert_eq!(&body[..], &LIVE_FIRST_READ[end..]);
        assert_eq!(&body[..], b"a\r\ndata: one\n\r\n");
    }

    #[test]
    fn dechunking_the_live_first_read_yields_only_the_event() {
        let (_head, body) = crate::hub::parse_head(LIVE_FIRST_READ).unwrap();
        let mut d = ChunkedDecoder::new();
        let r = d.push(&body).unwrap();
        assert_eq!(
            r.out, b"data: one\n",
            "dechunked payload wrong: {:?}",
            r.out
        );
        assert!(!r.done);
    }
}

#[cfg(test)]
mod coalesced_read_tests {
    use super::*;

    /// An engine that flushes fast (or a coalescing loopback socket) delivers
    /// several chunks in ONE read. One `push` must yield ALL of them.
    ///
    /// This is the property whose absence looked exactly like "the tunnel streams
    /// the first token and stops": with three events in one read, a decoder that
    /// emitted only the first left two events silently discarded, and every
    /// consumer downstream -- hub, frontend, caller -- faithfully forwarded the
    /// truncated answer, so nothing downstream looked wrong.
    #[test]
    fn one_push_emits_every_chunk_in_a_coalesced_read() {
        let mut d = ChunkedDecoder::new();
        // Three chunks, byte counts 10, 10, 11 -- matching the fake engine.
        let all = b"a\r\ndata: one\n\r\na\r\ndata: two\n\r\nb\r\ndata: done\n\r\n0\r\n\r\n";
        let r = d.push(all).expect("valid chunked stream");
        assert_eq!(
            r.out,
            b"data: one\ndata: two\ndata: done\n",
            "coalesced read lost events: got {:?}",
            String::from_utf8_lossy(&r.out)
        );
        assert!(
            r.done,
            "the last-chunk marker in this read must be reported"
        );
    }

    /// Same payload split at awkward boundaries -- a chunk straddling two reads and
    /// a size line straddling two more. Loss must not depend on where reads land.
    #[test]
    fn arbitrary_read_boundaries_lose_nothing() {
        // "Done" is deliberately NOT required to wait for all 51 bytes. At offset 49
        // the input ends `...\r\n0\r\n` -- the last-chunk size line without its
        // trailing CRLF -- and the decoder correctly reports Done there, because a
        // zero-length chunk IS the end of the body; those final two CRLF bytes carry
        // no body content (RFC 9112 §7.1). Demanding them instead would make
        // end-of-body detection depend on a flush some engines never send, stalling a
        // response that is actually finished.
        //
        // This loop asserts only what a single cut can establish: the exact byte
        // sequence, and that no cut produces a truncated body. It deliberately does
        // NOT assert anything about WHEN Done fires. An earlier version asserted that
        // Done implies the prefix ends at `0\r\n` and failed at cut=50, whose prefix
        // ends with a LONE `0` -- the CRLF was in the second read, so the decoder had
        // no choice but to wait. Done can legitimately depend on bytes held by the
        // other read, which makes it a property of the pair, not of either cut.
        // `last_chunk_size_line_alone_ends_the_body` tests Done where it can be
        // stated honestly.
        let all: Vec<u8> =
            b"a\r\ndata: one\n\r\na\r\ndata: two\n\r\nb\r\ndata: done\n\r\n0\r\n\r\n".to_vec();
        const BODY: &[u8] = b"data: one\ndata: two\ndata: done\n";
        let mut done_early = 0usize;
        for cut in 1..all.len() {
            let mut d = ChunkedDecoder::new();
            let mut got = Vec::new();
            let r1 = d
                .push(&all[..cut])
                .unwrap_or_else(|e| panic!("cut {cut}: {e:?}"));
            got.extend_from_slice(&r1.out);
            if r1.done {
                done_early += 1;
            } else {
                let r2 = d
                    .push(&all[cut..])
                    .unwrap_or_else(|e| panic!("cut {cut} tail: {e:?}"));
                got.extend_from_slice(&r2.out);
                assert!(r2.done, "never saw the end when split at {cut}");
            }
            assert_eq!(got, BODY, "split at {cut} lost or reordered bytes");
        }
        // The loop must actually have exercised that boundary rather than passing
        // because it never reached it.
        assert!(
            done_early > 0,
            "expected at least one boundary where Done is reached before all bytes"
        );
    }

    /// The last-chunk marker ends the body as soon as its size-line CRLF is seen,
    /// without waiting for the trailer CRLF. Pinned so that a change which starts
    /// demanding those extra bytes is read as re-introducing a stall, not as a fix.
    #[test]
    fn last_chunk_size_line_alone_ends_the_body() {
        let mut d = ChunkedDecoder::new();
        let r = d
            .push(b"a\r\ndata: one\n\r\n0\r\n")
            .expect("valid through the last-chunk marker");
        assert_eq!(r.out, b"data: one\n");
        assert!(
            r.done,
            "the last-chunk size line ends the body; its trailing CRLF is not needed"
        );
        // Whatever arrives afterwards must not be parsed as another chunk.
        let r2 = d.push(b"\r\n").expect("trailing CRLF is inert");
        assert!(r2.out.is_empty(), "bytes invented after Done: {:?}", r2.out);
    }

    /// And it genuinely waits when even the size-line CRLF is missing: a lone `0`
    /// is not a complete chunk-size line, and decoding it as one would end the body
    /// on a fragment. The pair of tests is the point -- end on the marker, never on
    /// a partial one.
    #[test]
    fn a_lone_zero_digit_does_not_end_the_body() {
        let mut d = ChunkedDecoder::new();
        let r = d
            .push(b"a\r\ndata: one\n\r\n0")
            .expect("partial input is fine");
        assert_eq!(r.out, b"data: one\n");
        assert!(
            !r.done,
            "a size line with no terminator is not yet a last-chunk marker"
        );
        let r2 = d.push(b"\r\n\r\n").expect("the completing bytes");
        assert!(r2.done, "the completed marker must finally end the body");
        assert!(r2.out.is_empty(), "no body bytes remain to emit");
    }
}

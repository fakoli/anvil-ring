//! Wire frames for the outbound tunnel.
//!
//! Hand-rolled and tiny on purpose: a frame is parsed once per proxied chunk, so
//! its cost is multiplied by token rate (I-9's concern), and a permissive length
//! field on a connection we treat as untrusted-by-default is where parser bugs
//! live. Hence: fixed 5-byte header, u32 length, an explicit hard cap, and
//! rejection of anything unrecognized rather than best-effort tolerance.
//!
//! Frame layout (network byte order):
//!
//!   byte 0     type
//!   bytes 1-2  stream id (u16) — 0 is reserved for control
//!   bytes 3-6  payload length (u32)
//!   bytes 7..  payload
//!
//! Types:
//!   0x01 HELLO      client -> hub, once, after the TLS handshake. Carries the
//!                   registration credential in the payload. Never re-sent.
//!   0x02 WELCOME    hub -> client, once, after it has authorized HELLO. Payload
//!                   is the lease lifetime in seconds as decimal ASCII: the client
//!                   MUST reconnect and re-authorize within it (I-3).
//!   0x03 OPEN       hub -> client: open stream <id>, payload is the request head
//!                   (origin-form request line + headers, CRLF terminated).
//!   0x04 DATA       either direction: a chunk on an open stream.
//!   0x05 END        either direction: end-of-stream. Payload may carry a one-line
//!                   error reason; empty means clean completion.
//!   0x06 PING       either direction.  0x07 PONG is the reply.
//!   0x08 GOAWAY     hub -> client: stop dialing, this lease is ending. Lets a
//!                   revoked credential end an idle tunnel promptly (I-3).
//!
//! There is deliberately NO frame type for "serve this port" or "this is my
//! permission": I-5 puts those decisions on the hub, so a client that could
//! self-describe its own permissions would be a hole in the threat model.

use std::fmt;

pub const HEADER_LEN: usize = 7;
/// Refuse anything larger than this before allocating. Request heads are small;
/// DATA is chunk-sized. A 4 GiB length field from a peer must not be trusted.
pub const MAX_PAYLOAD: u32 = 1 << 20;

pub const T_HELLO: u8 = 0x01;
pub const T_WELCOME: u8 = 0x02;
pub const T_OPEN: u8 = 0x03;
pub const T_DATA: u8 = 0x04;
pub const T_END: u8 = 0x05;
pub const T_PING: u8 = 0x06;
pub const T_PONG: u8 = 0x07;
pub const T_GOAWAY: u8 = 0x08;
/// Request body is complete; the peer must half-close toward the engine and keep
/// streaming the answer. Distinct from END ("I am gone"), because aborting a
/// request and finishing one both happen, and conflating them truncates the answer.
pub const T_HALF_END: u8 = 0x09;
/// An engine's response head, carried separately from the body. The tether cannot
/// know whether a body is chunk-coded until it has SEEN the body, and it must not
/// forward a `transfer-encoding` header for framing it is about to remove -- so
/// the head is withheld until the decision is made. Without this frame type the
/// only options were "send a possibly-lying head early" or "send the head as a
/// body chunk", and the second one puts the raw status line in the caller's body.
pub const T_RESP_HEAD: u8 = 0x0A;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Hello {
        credential: Vec<u8>,
    },
    Welcome {
        lease_secs: u64,
    },
    Open {
        stream: u16,
        head: Vec<u8>,
    },
    Data {
        stream: u16,
        bytes: Vec<u8>,
    },
    /// Engine response head. Sent at most once per stream, before any DATA, and
    /// already reframed: any framing header the tether consumed is gone.
    RespHead {
        stream: u16,
        head: Vec<u8>,
    },
    End {
        stream: u16,
        reason: Vec<u8>,
    },
    /// Request-side half-close: no more request bytes, answer still expected.
    HalfEnd {
        stream: u16,
    },
    Ping,
    Pong,
    GoAway {
        reason: Vec<u8>,
    },
}

impl Frame {
    pub fn type_tag(&self) -> u8 {
        match self {
            Frame::Hello { .. } => T_HELLO,
            Frame::Welcome { .. } => T_WELCOME,
            Frame::Open { .. } => T_OPEN,
            Frame::RespHead { .. } => T_RESP_HEAD,
            Frame::Data { .. } => T_DATA,
            Frame::End { .. } => T_END,
            Frame::Ping => T_PING,
            Frame::Pong => T_PONG,
            Frame::GoAway { .. } => T_GOAWAY,
            Frame::HalfEnd { .. } => T_HALF_END,
        }
    }

    pub fn stream(&self) -> u16 {
        match self {
            Frame::Open { stream, .. }
            | Frame::Data { stream, .. }
            | Frame::RespHead { stream, .. }
            | Frame::End { stream, .. } => *stream,
            _ => 0,
        }
    }

    /// Serialize to bytes, ready to hand to a WebSocket binary message.
    pub fn encode(&self) -> Vec<u8> {
        let (payload, stream): (&[u8], u16) = match self {
            Frame::Hello { credential } => (credential, 0),
            Frame::Welcome { lease_secs } => {
                // ASCII so a human reading a hexdump can see the lease.
                return {
                    let s = lease_secs.to_string();
                    with_header(T_WELCOME, 0, s.as_bytes())
                };
            }
            Frame::Open { stream, head } => (head, *stream),
            Frame::Data { stream, bytes } => (bytes, *stream),
            Frame::RespHead { stream, head } => (head, *stream),
            // No payload: the stream id already rides in the header, so HalfEnd is
            // the generic 7-byte frame with an empty body.
            Frame::HalfEnd { stream } => (&[][..], *stream),
            Frame::End { stream, reason } => (reason, *stream),
            Frame::Ping | Frame::Pong => (&[][..], 0),
            Frame::GoAway { reason } => (reason, 0),
        };
        with_header(self.type_tag(), stream, payload)
    }

    /// Parse one frame from the front of `buf`. Returns the frame and the number
    /// of bytes consumed, or `None` if fewer bytes are available than needed.
    ///
    /// Errors are hard: a short header, an over-long payload, an unknown type, or
    /// a control frame carrying a nonzero stream id all abort the connection.
    /// Silent tolerance here would let a desynchronized peer masquerade frames.
    pub fn decode(buf: &[u8]) -> Result<Option<(Frame, usize)>, FrameError> {
        if buf.len() < HEADER_LEN {
            return Ok(None);
        }
        let kind = buf[0];
        let stream = u16::from_be_bytes([buf[1], buf[2]]);
        let len = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]);
        if len > MAX_PAYLOAD {
            return Err(FrameError::TooLong(len));
        }
        let total = HEADER_LEN + len as usize;
        if buf.len() < total {
            return Ok(None); // need more bytes; do not consume the partial frame
        }
        let payload = &buf[HEADER_LEN..total];
        let frame = match kind {
            T_HELLO => Frame::Hello {
                credential: payload.to_vec(),
            },
            T_WELCOME => {
                let s = std::str::from_utf8(payload)
                    .map_err(|_| FrameError::Malformed("welcome not utf8"))?;
                Frame::Welcome {
                    lease_secs: s.parse().map_err(|_| FrameError::Malformed("bad lease"))?,
                }
            }
            T_OPEN => Frame::Open {
                stream,
                head: payload.to_vec(),
            },
            T_DATA => Frame::Data {
                stream,
                bytes: payload.to_vec(),
            },
            T_RESP_HEAD => Frame::RespHead {
                stream,
                head: payload.to_vec(),
            },
            T_HALF_END => {
                if !payload.is_empty() {
                    // A payload here means a peer invented bytes for a fixed-shape
                    // frame; refusing beats silently ignoring them.
                    return Err(FrameError::Malformed("HALF_END must be empty"));
                }
                Frame::HalfEnd { stream }
            }
            T_END => Frame::End {
                stream,
                reason: payload.to_vec(),
            },
            T_PING => Frame::Ping,
            T_PONG => Frame::Pong,
            T_GOAWAY => Frame::GoAway {
                reason: payload.to_vec(),
            },
            other => return Err(FrameError::UnknownType(other)),
        };
        // Control frames must not claim a stream; that would let a peer smuggle
        // a second meaning onto a data stream.
        if matches!(
            frame,
            Frame::Hello { .. }
                | Frame::Welcome { .. }
                | Frame::Ping
                | Frame::Pong
                | Frame::GoAway { .. }
        ) && stream != 0
        {
            return Err(FrameError::Malformed(
                "control frame with nonzero stream id",
            ));
        }
        Ok(Some((frame, total)))
    }
}

fn with_header(kind: u8, stream: u16, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.push(kind);
    out.extend_from_slice(&stream.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    TooLong(u32),
    UnknownType(u8),
    Malformed(&'static str),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::TooLong(n) => write!(f, "payload length {n} exceeds cap {MAX_PAYLOAD}"),
            FrameError::UnknownType(t) => write!(f, "unknown frame type 0x{t:02x}"),
            FrameError::Malformed(m) => write!(f, "malformed frame: {m}"),
        }
    }
}

impl std::error::Error for FrameError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(f: Frame) {
        let bytes = f.encode();
        let (got, consumed) = Frame::decode(&bytes)
            .expect("decode")
            .expect("complete frame");
        assert_eq!(got, f, "roundtrip changed the frame");
        assert_eq!(consumed, bytes.len(), "did not consume the whole frame");
    }

    #[test]
    fn every_frame_type_roundtrips() {
        roundtrip(Frame::Hello {
            credential: b"reg.abc".to_vec(),
        });
        roundtrip(Frame::Welcome { lease_secs: 900 });
        roundtrip(Frame::Open {
            stream: 7,
            head: b"POST /v1/chat/completions HTTP/1.1\r\nhost: x\r\n\r\n".to_vec(),
        });
        roundtrip(Frame::Data {
            stream: u16::MAX,
            bytes: b"data: hi\n\n".to_vec(),
        });
        roundtrip(Frame::End {
            stream: 3,
            reason: b"upstream reset".to_vec(),
        });
        roundtrip(Frame::Ping);
        roundtrip(Frame::Pong);
        roundtrip(Frame::GoAway {
            reason: b"lease revoked".to_vec(),
        });
    }

    #[test]
    fn welcome_lease_survives_large_and_zero_values() {
        // A lease of 0 must not be silently coerced to "no limit" -- that would
        // turn I-3 inside out.
        roundtrip(Frame::Welcome { lease_secs: 0 });
        roundtrip(Frame::Welcome {
            lease_secs: u64::MAX / 2,
        });
    }

    #[test]
    fn incomplete_frame_needs_more_bytes_and_consumes_nothing() {
        let full = Frame::Data {
            stream: 1,
            bytes: b"0123456789".to_vec(),
        }
        .encode();
        for cut in 1..full.len() {
            let out = Frame::decode(&full[..cut]).expect("no error on partial");
            assert!(out.is_none(), "claimed a frame from {cut} bytes");
        }
    }

    #[test]
    fn oversized_length_field_is_rejected_before_allocating() {
        let mut buf = vec![T_DATA, 0, 1, 0xff, 0xff, 0xff, 0xff];
        buf.extend_from_slice(b"tiny");
        match Frame::decode(&buf) {
            Err(FrameError::TooLong(n)) => assert_eq!(n, 0xffff_ffff),
            other => panic!("expected TooLong, got {other:?}"),
        }
    }

    #[test]
    fn unknown_frame_type_aborts_rather_than_skipping() {
        let mut buf = vec![0x7f, 0, 0, 0, 0, 0, 0];
        buf.extend_from_slice(b"x");
        assert_eq!(
            Frame::decode(&buf),
            Err(FrameError::UnknownType(0x7f)),
            "skipping an unknown frame would desynchronize the stream"
        );
    }

    #[test]
    fn control_frame_cannot_claim_a_stream() {
        // A PING that says it belongs to stream 5 is either a bug or a smuggle.
        let mut buf = vec![T_PING, 0, 5, 0, 0, 0, 0];
        buf.extend_from_slice(b"");
        assert!(matches!(Frame::decode(&buf), Err(FrameError::Malformed(_))));
    }

    #[test]
    fn stream_id_is_big_endian_and_full_width() {
        // Catch an endianness regression explicitly: 0x0100 and 0x0001 differ.
        for id in [1u16, 256, 257, 4096, u16::MAX] {
            let f = Frame::Data {
                stream: id,
                bytes: vec![0xAA],
            };
            let (got, _) = Frame::decode(&f.encode()).unwrap().unwrap();
            assert_eq!(got.stream(), id, "stream id mangled for {id}");
        }
    }

    #[test]
    fn empty_payload_is_distinct_from_no_frame() {
        // DATA with zero bytes is legal and means "empty chunk"; it must decode as
        // a frame, not be mistaken for a partial read.
        let f = Frame::Data {
            stream: 9,
            bytes: vec![],
        };
        let bytes = f.encode();
        let (got, n) = Frame::decode(&bytes).unwrap().expect("should be a frame");
        assert_eq!(got, f);
        assert_eq!(n, HEADER_LEN);
    }

    #[test]
    fn decoding_two_frames_in_one_buffer_advances_by_consumed() {
        // The tunnel receives coalesced WebSocket payloads; the caller must be able
        // to drain a buffer by advancing `consumed`.
        let mut buf = Frame::Data {
            stream: 1,
            bytes: b"aa".to_vec(),
        }
        .encode();
        buf.extend_from_slice(
            &Frame::Data {
                stream: 2,
                bytes: b"bb".to_vec(),
            }
            .encode(),
        );
        let (f1, n1) = Frame::decode(&buf).unwrap().unwrap();
        let (f2, n2) = Frame::decode(&buf[n1..]).unwrap().unwrap();
        assert_eq!(f1.stream(), 1);
        assert_eq!(f2.stream(), 2);
        assert_eq!(n1 + n2, buf.len());
    }
}

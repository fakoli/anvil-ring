//! Hop-by-hop header handling for the anvil-ring proxy.
//!
//! ADR-0004 records the accepted cost of Rust over Go: there is no
//! `net/http/httputil.ReverseProxy`, so header correctness is ours to own.
//!
//! RFC 9110 §7.6.1: hop-by-hop fields describe one transport connection and MUST
//! NOT be forwarded. `Connection` additionally *names* fields that are
//! hop-by-hop for this message, so those must be stripped as well -- that second
//! rule is the part a naive proxy misses, and it produces "works locally, breaks
//! through a proxy" bugs.

use http::{HeaderMap, HeaderName};
use std::collections::HashSet;

/// Fields that are always hop-by-hop and must never be forwarded.
pub const ALWAYS_STRIP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host", // replaced per-hop by the client side
];

/// Strip hop-by-hop headers, honouring the tokens named by `Connection:`.
/// Returns how many headers were removed (for logging/metrics).
pub fn strip_hop_by_hop(headers: &mut HeaderMap) -> usize {
    // Names named by Connection: are hop-by-hop for *this* message only.
    let mut conditional: HashSet<String> = HashSet::new();
    for value in headers.get_all("connection") {
        if let Ok(s) = value.to_str() {
            for token in s.split(',') {
                let t = token.trim().to_ascii_lowercase();
                if !t.is_empty() {
                    conditional.insert(t);
                }
            }
        }
    }

    let mut removed = 0usize;
    for name in ALWAYS_STRIP {
        // Drain returns a mover; count the values actually present.
        removed += headers.remove(*name).into_iter().count();
    }
    for name in conditional {
        if let Ok(n) = HeaderName::from_lowercase(name.as_bytes()) {
            removed += headers.remove(n).into_iter().count();
        }
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn map() -> HeaderMap {
        HeaderMap::new()
    }

    #[test]
    fn strips_always_hop_by_hop_fields() {
        let mut h = map();
        h.insert("connection", HeaderValue::from_static("keep-alive"));
        h.insert("keep-alive", HeaderValue::from_static("timeout=5"));
        h.insert("transfer-encoding", HeaderValue::from_static("chunked"));
        h.insert("x-request-id", HeaderValue::from_static("abc"));

        let removed = strip_hop_by_hop(&mut h);

        assert!(!h.contains_key("connection"));
        assert!(!h.contains_key("keep-alive"));
        assert!(!h.contains_key("transfer-encoding"));
        // End-to-end headers must survive -- dropping these breaks tracing.
        assert!(h.contains_key("x-request-id"));
        assert!(removed >= 3, "removed={removed}");
    }

    #[test]
    fn strips_fields_named_by_connection_header() {
        // The subtle rule: X-Session-Token is hop-by-hop ONLY because Connection
        // names it. Missing this is the classic reverse-proxy leak.
        let mut h = map();
        h.insert(
            "connection",
            HeaderValue::from_static("X-Session-Token, Foo"),
        );
        h.insert("x-session-token", HeaderValue::from_static("secret"));
        h.insert("foo", HeaderValue::from_static("bar"));
        h.insert("authorization", HeaderValue::from_static("Bearer real"));

        strip_hop_by_hop(&mut h);

        assert!(!h.contains_key("x-session-token"));
        assert!(!h.contains_key("foo"));
        // Authorization is end-to-end and MUST pass through (I-10 auth gate).
        assert!(h.contains_key("authorization"));
    }

    #[test]
    fn mixed_case_connection_tokens_do_not_panic() {
        let mut h = map();
        h.insert(
            "connection",
            HeaderValue::from_static("CoNnEcTiOn, KEEP-ALIVE"),
        );
        h.insert("x-custom", HeaderValue::from_static("1"));
        strip_hop_by_hop(&mut h); // must not panic
        assert!(!h.contains_key("connection"));
    }

    #[test]
    fn idempotent_on_empty_map() {
        let mut h = map();
        assert_eq!(strip_hop_by_hop(&mut h), 0);
    }
}

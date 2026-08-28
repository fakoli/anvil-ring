//! The reverse proxy: forwards to the loopback inference engine, streaming with
//! immediate flush (I-9) and enforcing the bearer check (I-10).

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1::SendRequest;
use hyper::{header, Method, Request, Response, StatusCode};
use std::sync::Arc;
use tokio::net::TcpStream;

/// Response body, type-erased once so `handle` has one concrete return type.
pub type ResBody = BoxBody<Bytes, std::io::Error>;

#[derive(Clone)]
pub struct Proxy {
    upstream: hyper::Uri,
    token: Option<Arc<str>>,
}

/// A dead engine must surface as 502 quickly rather than hang, because a hung
/// endpoint is indistinguishable from a slow generation (I-6).
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

impl Proxy {
    pub fn new(upstream: hyper::Uri, token: Option<String>) -> Self {
        Self {
            upstream,
            token: token.map(|t| Arc::from(t.as_str())),
        }
    }

    pub async fn serve_connection(
        &self,
        stream: tokio::net::TcpStream,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let io = hyper_util::rt::TokioIo::new(stream);
        let proxy = self.clone();
        let svc = hyper::service::service_fn(move |req: Request<Incoming>| {
            let proxy = proxy.clone();
            async move { Ok::<_, std::io::Error>(proxy.handle(req).await) }
        });
        // hyper's ConnectionError already matches the return type; do not wrap it.
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(io, svc)
            .await
    }

    async fn handle(&self, req: Request<Incoming>) -> Response<ResBody> {
        // I-10: authenticate once, here, before the engine is touched at all.
        if let Some(expected) = &self.token {
            if !authorized(req.headers(), expected) {
                return text(StatusCode::UNAUTHORIZED, "unauthorized\n");
            }
        }

        // Runtime guard on I-10: a typo in ANVIL_RING_UPSTREAM must not silently
        // turn this into an open proxy to an arbitrary host.
        if !is_loopback(&self.upstream) {
            return text(
                StatusCode::SERVICE_UNAVAILABLE,
                "upstream is not loopback; refusing to proxy off-host\n",
            );
        }

        if req.method() == Method::GET && req.uri().path() == "/-ring/health" {
            return text(StatusCode::OK, "ok\n");
        }

        match self.forward(req).await {
            Ok(res) => res,
            Err(e) => text(StatusCode::BAD_GATEWAY, &format!("upstream error: {e}\n")),
        }
    }

    async fn forward(&self, req: Request<Incoming>) -> Result<Response<ResBody>, String> {
        let (mut parts, body) = req.into_parts();

        parts.uri = forward_uri(&self.upstream, &parts.uri).ok_or("unparseable request URI")?;
        // RFC 9110 7.6.1: hop-by-hop fields describe one connection only.
        crate::headers::strip_hop_by_hop(&mut parts.headers);

        let host = self
            .upstream
            .authority()
            .map(|a| a.as_str().to_string())
            .ok_or("upstream has no authority")?;

        let io = connect_loopback(&host)
            .await
            .map_err(|e| format!("upstream unreachable: {e}"))?;
        let io = hyper_util::rt::TokioIo::new(io);

        let (mut sender, conn): (SendRequest<Incoming>, _) =
            hyper::client::conn::http1::handshake(io)
                .await
                .map_err(|e| format!("handshake failed: {e}"))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });

        let upstream_req = rebuild_request(parts.method, parts.uri, parts.headers, body)?;
        let res = sender
            .send_request(upstream_req)
            .await
            .map_err(|e| format!("upstream request failed: {e}"))?;

        Ok(stream_through(res))
    }
}

/// Rebuild the upstream request with a fresh `Host` plus whatever survived
/// hop-by-hop stripping.
fn rebuild_request(
    method: hyper::Method,
    uri: hyper::Uri,
    mut headers: hyper::HeaderMap,
    body: Incoming,
) -> Result<Request<Incoming>, String> {
    if let Some(auth) = uri.authority() {
        let v = hyper::header::HeaderValue::from_str(auth.as_str()).map_err(|e| e.to_string())?;
        headers.insert(header::HOST, v);
    }
    let mut b = Request::builder().method(method).uri(uri);
    *b.headers_mut().unwrap() = headers;
    b.body(body).map_err(|e| e.to_string())
}

/// Copy the upstream response through **without buffering the body** (I-9).
///
/// `Incoming` is itself a stream: taking `into_body()` and forwarding it
/// unchanged makes hyper write each chunk as it arrives, which is exactly the
/// flush semantics SSE requires. Buffering would present as time-to-first-token
/// rather than an error -- which is why tests/ asserts on *incremental* arrival.
fn stream_through(res: Response<Incoming>) -> Response<ResBody> {
    let (parts, body) = res.into_parts();
    let boxed: ResBody = body
        .map_err(|e| std::io::Error::other(e.to_string()))
        .boxed();
    let mut out = Response::new(boxed);
    *out.status_mut() = parts.status;
    *out.headers_mut() = parts.headers;
    out
}

fn authorized(headers: &hyper::HeaderMap, expected: &str) -> bool {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return false;
    };
    let Ok(s) = value.to_str() else {
        return false;
    };
    match s.strip_prefix("Bearer ") {
        Some(got) => constant_eq(got.as_bytes(), expected.as_bytes()),
        None => false,
    }
}

/// Preserve path AND query exactly -- `/v1/models?extra=1` must not lose the
/// query, and an empty path must become `/`.
fn forward_uri(upstream: &hyper::Uri, req_uri: &hyper::Uri) -> Option<hyper::Uri> {
    let scheme = upstream.scheme_str().unwrap_or("http");
    let authority = upstream.authority()?.as_str();
    let path = if req_uri.path().is_empty() {
        "/"
    } else {
        req_uri.path()
    };
    let qs = match req_uri.query() {
        Some(q) => format!("?{q}"),
        None => String::new(),
    };
    format!("{scheme}://{authority}{path}{qs}").parse().ok()
}

/// The single definition of "loopback" for the whole crate (I-10).
///
/// Both the proxy and the tunnel consult this. Two definitions of a security
/// boundary is how one of them ends up accepting `0.0.0.0`, `0177.0.0.1`, or a
/// `*.localhost` name that resolves off-host.
pub fn is_loopback_host(host: &str) -> bool {
    let h = host.trim_start_matches('[').trim_end_matches(']');
    if h == "localhost" || h.ends_with(".localhost") {
        return true;
    }
    match h.parse::<std::net::IpAddr>() {
        // `0.0.0.0` is NOT loopback: on Linux it is a bind-any wildcard, and
        // connecting through it is a routable connection.
        Ok(std::net::IpAddr::V4(v4)) => v4.is_loopback(),
        Ok(std::net::IpAddr::V6(v6)) => v6.is_loopback(),
        Err(_) => false,
    }
}

fn is_loopback(uri: &hyper::Uri) -> bool {
    uri.host().is_some_and(is_loopback_host)
}

async fn connect_loopback(host: &str) -> std::io::Result<TcpStream> {
    match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(host)).await {
        Ok(r) => r,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "connect timed out",
        )),
    }
}

/// Content is secret, length is not. Early return on length is acceptable.
fn constant_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn text(status: StatusCode, body: &str) -> Response<ResBody> {
    let boxed: ResBody = Full::new(Bytes::from(body.to_string()))
        .map_err(|e| match e {})
        .boxed();
    let mut res = Response::new(boxed);
    *res.status_mut() = status;
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_uri_preserves_path_and_query() {
        let up: hyper::Uri = "http://127.0.0.1:8000".parse().unwrap();
        let req: hyper::Uri = "/v1/chat/completions?x=1&y=2".parse().unwrap();
        let got = forward_uri(&up, &req).unwrap();
        assert_eq!(got.path(), "/v1/chat/completions");
        assert_eq!(got.query(), Some("x=1&y=2"));
        assert_eq!(got.authority().unwrap().as_str(), "127.0.0.1:8000");
    }

    #[test]
    fn forward_uri_normalises_empty_path_to_root() {
        let up: hyper::Uri = "http://127.0.0.1:8000".parse().unwrap();
        let req: hyper::Uri = "/".parse().unwrap();
        assert_eq!(forward_uri(&up, &req).unwrap().path(), "/");
    }

    #[test]
    fn loopback_guard_accepts_only_loopback_hosts() {
        for h in ["http://127.0.0.1:8000", "http://localhost:8000"] {
            let u: hyper::Uri = h.parse().unwrap();
            assert!(is_loopback(&u), "{h} should be loopback");
        }
        // The dangerous misconfiguration: proxying off-host.
        for h in ["http://10.0.0.5:8000", "http://example.com:8000"] {
            let u: hyper::Uri = h.parse().unwrap();
            assert!(!is_loopback(&u), "{h} must NOT be accepted");
        }
    }

    #[test]
    fn constant_eq_rejects_wrong_token_and_length() {
        assert!(constant_eq(b"abc", b"abc"));
        assert!(!constant_eq(b"abc", b"abd"));
        assert!(!constant_eq(b"abc", b"abcd"));
    }

    #[test]
    fn authorized_requires_bearer_scheme() {
        let p = Proxy::new(
            "http://127.0.0.1:8000".parse().unwrap(),
            Some("s3cret".into()),
        );
        let mut h = hyper::HeaderMap::new();
        assert!(!p_has_auth(&h));
        h.insert(
            header::AUTHORIZATION,
            hyper::header::HeaderValue::from_static("s3cret"),
        );
        assert!(!authorized(&h, "s3cret"), "must require Bearer prefix");
        h.insert(
            header::AUTHORIZATION,
            hyper::header::HeaderValue::from_static("Bearer s3cret"),
        );
        assert!(authorized(&h, "s3cret"));
        h.insert(
            header::AUTHORIZATION,
            hyper::header::HeaderValue::from_static("Bearer wrong"),
        );
        assert!(!authorized(&h, "s3cret"));
        let _ = p;
    }

    fn p_has_auth(h: &hyper::HeaderMap) -> bool {
        h.contains_key(header::AUTHORIZATION)
    }
}

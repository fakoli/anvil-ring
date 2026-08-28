//! Streaming body for the caller-facing frontend: engine bytes, in order, flushed.
//!
//! This type is why I-9 survives the extra hop. If the frontend collected the whole
//! engine answer before replying, every property the proxy's flush test proved would
//! be undone one hop later -- and undone *undetected*, since the bytes would still
//! arrive correct. A timing assertion at this layer is what keeps that honest.
//!
//! Design note, because the alternative was tried and rejected: the receiver is
//! stored as `Mutex<Option<Receiver>>` and *taken* for the duration of one poll.
//! Polling a `tokio::sync::Mutex` in place means the guard's `Drop` runs only after
//! the outer borrow ends, which is too late to hand the receiver back before
//! returning -- the compiler refuses it. Taking the value out keeps the guard
//! confined to a statement, and leaves the mutex unlocked for the whole interval
//! between polls, which is exactly what a single-consumer stream wants.

use crate::hub::{ChunkOrEnd, Registry};
use bytes::Bytes;
use futures_util::Stream;
// Imported at module scope (not only inside `handle`) because `caller_status_for`
// is a public, pure decision and returns one.
use hyper::StatusCode;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::mpsc;

/// Either a live tunnel stream, or a fixed body for an error or health answer.
pub enum TunnelBody {
    /// `Arc<Forwarded>` keeps the stream registered, and the stream's channel
    /// sender alive, for as long as this body exists. Without it the frontend's
    /// `Forwarded` dies at the end of the request handler, `StreamGuard::drop`
    /// unregisters the stream and releases the last sender mid-response, and the
    /// body ends early while looking complete. See `Forwarded::into_body`.
    Live(
        Arc<Mutex<Option<mpsc::Receiver<ChunkOrEnd>>>>,
        Arc<dyn Send + Sync>,
    ),
    Fixed(Bytes),
}

impl TunnelBody {
    /// A body that streams the engine's answer through `rx`.
    ///
    /// The `Forwarded` is required, not incidental: it is what keeps the hub's
    /// stream registration and channel sender alive until the body is drained.
    pub fn live(rx: mpsc::Receiver<ChunkOrEnd>, stream: Arc<crate::hub::Forwarded>) -> Self {
        TunnelBody::Live(Arc::new(Mutex::new(Some(rx))), stream)
    }

    /// Body over a bare channel, for tests that exercise the CHANNEL/BODY contract
    /// with no tunnel attached.
    ///
    /// Deliberately separate from `live`: production must go through
    /// `from_forwarded`, because a body whose stream can be dropped underneath it
    /// is exactly the bug that truncated every response -- the last sender died
    /// while the body was still being read, and the body then ended looking
    /// complete. Nothing built here keeps a stream registered, so nothing should
    /// mistake this for the real path.
    pub fn live_for_test(rx: mpsc::Receiver<ChunkOrEnd>) -> Self {
        TunnelBody::Live(Arc::new(Mutex::new(Some(rx))), Arc::new(()))
    }

    /// Convenience: consume the forwarded stream into its own body, so the
    /// stream's lifetime is exactly the response's lifetime.
    pub fn from_forwarded(fwd: crate::hub::Forwarded) -> Self {
        let rx = fwd
            .take_rx()
            .expect("stream already handed over its receiver");
        TunnelBody::live(rx, Arc::new(fwd))
    }

    /// A body with all its bytes known up front (health, errors). Sets
    /// `content-length` honestly, which matters because a wrong length is a
    /// different bug class than a missing one.
    pub fn fixed(text: &str) -> Self {
        TunnelBody::Fixed(Bytes::from(text.to_string()))
    }

    pub fn is_fixed(&self) -> bool {
        matches!(self, TunnelBody::Fixed(_))
    }
}

impl Stream for TunnelBody {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            TunnelBody::Fixed(buf) => {
                if buf.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok(std::mem::take(buf))))
                }
            }
            TunnelBody::Live(slot, _stream) => {
                // Take the receiver out for this poll. The mutex is held only for
                // this statement -- never across the `poll_next` below -- so a
                // competing poll waits on the lock rather than on our work.
                let mut taken = match slot.lock() {
                    Ok(mut guard) => match guard.take() {
                        Some(rx) => rx,
                        // Taken already and not returned. By construction there is
                        // exactly one consumer per stream, so this is a contract
                        // violation, not a race; ending the stream is the safe
                        // answer (a hung caller is worse than a short one).
                        None => return Poll::Ready(None),
                    },
                    Err(_) => return Poll::Ready(None),
                };

                // poll_recv is the receiver's own poll. It returns Ready(None)
                // only once the channel is closed AND drained, which is a real end.
                let out = Pin::new(&mut taken).poll_recv(cx);

                match out {
                    Poll::Ready(Some(ChunkOrEnd::Chunk(b))) => {
                        eprintln!("BODY poll_next -> Chunk {}B (total {}B)", b.len(), 0);
                        if let Ok(mut guard) = slot.lock() {
                            *guard = Some(taken);
                        }
                        Poll::Ready(Some(Ok(b)))
                    }
                    // END or a closed channel means no more bytes. The receiver is
                    // deliberately NOT restored: leaving it gone makes any later
                    // poll end cleanly via the `None` arm above.
                    Poll::Ready(Some(ChunkOrEnd::End)) | Poll::Ready(None) => {
                        eprintln!("BODY poll_next -> END (terminates the caller's body)");
                        Poll::Ready(None)
                    }
                    Poll::Pending => {
                        if let Ok(mut guard) = slot.lock() {
                            *guard = Some(taken);
                        }
                        Poll::Pending
                    }
                }
            }
        }
    }
}

/// Serve the caller-facing HTTP surface on `listen`.
///
/// `caller_token` authenticates callers. In the fleet this is the router's own
/// credential: a caller may not name a tether, present a registration credential,
/// or otherwise influence routing (I-5).
pub async fn serve_frontend(
    listen: SocketAddr,
    registry: Arc<Registry>,
    caller_token: String,
) -> std::io::Result<()> {
    use hyper::server::conn::http1;
    use hyper::service::service_fn;

    let listener = tokio::net::TcpListener::bind(listen).await?;
    tokio::spawn(async move {
        loop {
            let Ok((sock, _peer)) = listener.accept().await else {
                continue;
            };
            let reg = registry.clone();
            let token = caller_token.clone();
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(sock);
                let svc = service_fn(move |req| {
                    let reg = reg.clone();
                    let token = token.clone();
                    async move { handle(req, reg, token).await }
                });
                if let Err(e) = http1::Builder::new()
                    .keep_alive(true)
                    .serve_connection(io, svc)
                    .with_upgrades()
                    .await
                {
                    // Surfaced, not swallowed (I-6).
                    eprintln!("anvil-ring frontend: connection error: {e}");
                }
            });
        }
    });
    Ok(())
}

/// Pick which tether serves a request.
///
/// Deliberately not parameterized by anything the caller sends (I-5). One up
/// tether is picked today; a fleet with several needs a policy here, and that
/// policy must live on the hub regardless of what a caller asks for.
fn pick_tether(registry: &Registry) -> Option<String> {
    registry
        .status()
        .into_iter()
        .find(|(_, _, s)| matches!(s, crate::hub::TetherState::Up(_)))
        .map(|(id, _, _)| id)
}

async fn handle(
    req: hyper::Request<hyper::body::Incoming>,
    registry: Arc<Registry>,
    caller_token: String,
) -> Result<hyper::Response<TunnelBody>, hyper::Error> {
    // (hyper requires the service future's error to be hyper::Error; every failure
    // above is already turned into a Response, so Err is only for transport.)
    use hyper::{Method, Response, StatusCode};

    // Liveness first: works with no token and no tether, so an orchestrator can
    // tell "hub up, tethers down" apart from "hub down" -- the distinction that
    // decides whether to restart the hub or page for a rental.
    if req.method() == Method::GET && req.uri().path() == "/healthz" {
        let up = registry
            .status()
            .iter()
            .any(|(_, _, s)| matches!(s, crate::hub::TetherState::Up(_)));
        return Ok(Response::new(TunnelBody::fixed(if up {
            "ok tether-up\n"
        } else {
            "ok tether-down\n"
        })));
    }

    // Authenticate BEFORE revealing anything about tethers: answering 502 to an
    // unauthenticated caller would confirm that a serving tether exists.
    if !crate::proxy::authorized(req.headers(), &caller_token) {
        return Ok(error_response(StatusCode::UNAUTHORIZED));
    }

    let Some(tether_id) = pick_tether(&registry) else {
        // Distinct from 401 on purpose -- see the module comment in hub.rs.
        return Ok(error_response(StatusCode::BAD_GATEWAY));
    };

    match registry.forward(&tether_id, req).await {
        Ok(forwarded) => {
            // The engine's own status and headers must reach the caller. An engine
            // 500 republished as a hub 200 would poison every caller's retry logic
            // (I-11: never invent an upstream answer).
            let head = forwarded.head().await;
            // No head means no upstream answer, and that is a FAILURE, reported as
            // one. This line was `unwrap_or(StatusCode::OK)`, which fabricated a
            // 200 for a response that never arrived -- verified by killing the
            // engine: the caller received `200 OK` with a valid chunked terminator
            // and no body, indistinguishable from a working engine that happened to
            // stream nothing. For an inference proxy that is the worst possible
            // failure mode, because callers treat 200 as "the model answered".
            //
            // Waiting for the head is also what lets an engine's own 500 through
            // instead of republishing it as a hub 200, which would poison caller
            // retry logic. The cost is one round trip before response headers; it is
            // paid once per request, not per token, and it does NOT buffer the body
            // (I-9 holds -- the body still streams from `rx` unbuffered).
            let Some(head) = head else {
                return Ok(error_response(caller_status_for(None)));
            };
            let mut builder = Response::builder()
                .status(caller_status_for(Some(head.status().as_u16())));
            for (name, value) in head.headers() {
                // Hop-by-hop must not cross this hop; copying blindly would
                // reintroduce e.g. transfer-encoding, which we strip and re-frame.
                if !crate::headers::is_hop_by_hop(name.as_str()) {
                    builder = builder.header(name.clone(), value.clone());
                }
            }
            // An http::Error here means a header from the engine that hyper
            // rejects: an upstream protocol fault. Answer 500, never a fabricated
            // 200 (I-11). hyper::Error has no public constructor, so the service
            // returns a Response rather than an Err.
            Ok(builder
                .body(TunnelBody::from_forwarded(forwarded))
                .unwrap_or_else(|_| error_response(StatusCode::INTERNAL_SERVER_ERROR)))
        }
        Err(e) => Ok(match e {
            crate::hub::ForwardError::NoTether | crate::hub::ForwardError::TetherGone => {
                error_response(StatusCode::BAD_GATEWAY)
            }
            crate::hub::ForwardError::Revoked => {
                // 503, not 403: the caller did nothing wrong, the fleet refused the
                // upstream. Which is which belongs in the hub log, not the body.
                error_response(StatusCode::SERVICE_UNAVAILABLE)
            }
            crate::hub::ForwardError::Idhausted => error_response(StatusCode::TOO_MANY_REQUESTS),
            crate::hub::ForwardError::CallerBody => error_response(StatusCode::BAD_REQUEST),
        }),
    }
}

fn error_response(status: hyper::StatusCode) -> hyper::Response<TunnelBody> {
    // The reason is NOT echoed into the body: a response body is readable by
    // whoever received the status, and "tether revoked" / "no tether" are fleet
    // facts. They are logged at their source instead.
    hyper::Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_LENGTH, "0")
        .body(TunnelBody::fixed(""))
        .unwrap()
}

/// The single decision: given what came back from the tunnel, what status does
/// the caller get?
///
/// Kept as a pure function so the invariant is testable without a hub, a tether,
/// and an engine. `None` -- no upstream head -- is NOT a success. Encoding this
/// as `Option<StatusCode>` with `unwrap_or(OK)` is exactly the mistake this
/// guards: it makes "we got nothing" and "the engine answered 200 with nothing"
/// the same value, and a caller cannot tell a dead engine from an idle one.
pub fn caller_status_for(head_status: Option<u16>) -> StatusCode {
    match head_status {
        // The engine's own status is republished verbatim, including 4xx/5xx, so
        // caller retry logic sees the truth (I-11).
        //
        // `from_u16` accepts 100..=999, so within this function's input type
        // (a u16 that already PARSED as a number -- see `parse_head`, which does
        // `.parse::<u16>().ok()?`) the only unmappable values are 0 and 1000+.
        // Those cannot arrive from a parsed head, so the fallback below is defence
        // in depth rather than a live path. It is written because the alternative
        // -- no fallback -- is what made the original bug fatal rather than odd.
        Some(code) => StatusCode::from_u16(code)
            // Unreachable for parsed heads; kept so a future caller that feeds this
            // something else fails as a bad upstream and not as a success.
            .unwrap_or(StatusCode::BAD_GATEWAY),
        None => StatusCode::BAD_GATEWAY,
    }
}

impl http_body::Body for TunnelBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        // Bridge Stream -> Body: hyper 1 reads bodies as frames, and every chunk
        // here is a DATA frame. No trailers are ever produced, which is correct --
        // an SSE stream has none, and claiming them would be inventing structure.
        match futures_util::Stream::poll_next(self, cx) {
            Poll::Ready(Some(Ok(b))) => Poll::Ready(Some(Ok(http_body::Frame::data(b)))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            TunnelBody::Fixed(b) => b.is_empty(),
            // Conservative: a live stream might have bytes in flight, so never
            // claim EOF. Claiming it wrongly lets hyper answer with no body.
            TunnelBody::Live(..) => false,
        }
    }
}

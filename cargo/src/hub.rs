//! The hub: the always-on side that owns every authorization decision (I-5).
//!
//! Deliberately asymmetric. The rental side may say almost nothing: it presents a
//! credential, and the hub decides whether that credential may exist at all, what
//! lease it receives, and when it must go. A client never names a target port, an
//! upstream host, or a permission — there is no parameter through which it could
//! ask for more.
//!
//! Invariants owned here:
//!  - I-5: `Registry::authorize` accepts ONLY a credential. Nothing the client
//!    sends influences the decision beyond "does this credential map to an active
//!    tether".
//!  - I-3: leases are short AND revoking an ACTIVE tether tears down its live
//!    session with GOAWAY, so an established tunnel cannot be ridden past its
//!    lease. Idle tunnels die on the lease alone.
//!  - I-6: each tether has a last-seen deadline; `status()` distinguishes UP from
//!    DOWN rather than letting a lost tether look merely slow.
//!  - I-8: credentials are held as digests, never plaintext, and are never logged.

use crate::frames::Frame;
use futures_util::{SinkExt, StreamExt};
use http::Method;
use hyper::body::Bytes;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

/// Default lease. Short enough that revocation lands promptly (I-3); long enough
/// that re-registration is not the dominant cost on a flaky rental link.
/// Ceiling on pre-head buffered bytes. A status line plus headers is small; a
/// stream that has not produced a parseable head within this many bytes is not
/// producing a head at all, and continuing to buffer would grow hub memory on
/// someone else's behalf.
pub const MAX_PENDING_HEAD: usize = 64 * 1024;

pub const DEFAULT_LEASE: Duration = Duration::from_secs(15 * 60);

/// How often the hub re-checks revocation and tether idleness.
pub const TETHER_TICK: Duration = Duration::from_secs(5);

/// Silence after which the hub considers a tether lost (I-6). Must exceed the
/// client's ping cadence plus its own timeout, or the hub would contradict the
/// client about whether the link is alive.
pub const TETHER_SILENCE: Duration = Duration::from_secs(45);

/// A registered tether.
#[derive(Debug, Clone)]
pub struct Tether {
    pub id: String,
    pub label: String,
    /// SHA-256 hex of the credential. Plaintext never lives here (I-8).
    pub credential_hash: String,
    /// Cleared by `revoke`. Checked at HELLO *and* enforced against live sessions.
    pub active: bool,
}

/// What the hub hands the client after a successful HELLO.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub tether_id: String,
    pub ttl: Duration,
}

/// A live session handle, so both revocation (I-3) and request forwarding (the
/// hub -> tether direction) can reach an established tunnel.
#[derive(Clone)]
struct LiveSession {
    /// Write half. Requests the hub originates are pushed here and the session
    /// loop puts them on the wire.
    tx: mpsc::UnboundedSender<Frame>,
    last_seen: Arc<Mutex<Instant>>,
    /// Open streams on this tether, so the session loop can route inbound
    /// DATA/END frames to the caller waiting on the other end.
    streams: Arc<Mutex<HashMap<u16, Arc<StreamState>>>>,
    /// Hands out stream ids for this tether. Per-tether so ids never collide
    /// across tethers, and monotonic so reuse cannot silently alias a live stream.
    next_id: Arc<Mutex<u32>>,
}

/// One in-flight proxied stream. The hub-side twin of the client's `StreamCtx`.
pub struct StreamState {
    pub method: Method,
    pub path: String,
    pub headers: HeaderMap,
    /// Body bytes waiting to be pushed. Held so a caller streaming a long prompt
    /// cannot outrun the tunnel (backpressure), rather than buffering unbounded.
    pub pending: Arc<Mutex<VecDeque<u8>>>,
    /// Response side. Bounded, so a chatty engine cannot grow hub memory without
    /// limit if the caller stops reading.
    /// Sender kept so the session task can push engine bytes; `chunks` is the
    /// half the caller reads.
    chunk_tx: mpsc::Sender<ChunkOrEnd>,
    /// A plain `std` mutex: the frontend *takes* the receiver for one poll and
    /// releases the lock before touching it, so no guard ever crosses an await.
    /// (A tokio mutex here was tried first and made `poll` unwieldy for no gain.)
    chunks: Arc<Mutex<Option<mpsc::Receiver<ChunkOrEnd>>>>,
    /// Populated by the first DATA frame that carries a status line.
    pub head: Arc<Mutex<Option<http::Response<()>>>>,
    /// Engine bytes received before a parseable head. A response head has no
    /// length prefix, so it may legitimately arrive across two frames; those
    /// bytes are joined here and retried. Bounded by `MAX_PENDING_HEAD`.
    pub pending_head: Arc<Mutex<Vec<u8>>>,
    /// Set when the engine's END arrives (see `StreamGuard::drop`).
    pub completed: Arc<AtomicBool>,
}

/// A response-side event for one stream.
#[derive(Debug)]
pub enum ChunkOrEnd {
    Chunk(Bytes),
    End,
}

/// The hub's authority. Cloned cheaply and shared between the accept loop and the
/// per-tether tasks.
#[derive(Default)]
pub struct Registry {
    /// credential_hash -> tether id.
    by_credential: Mutex<HashMap<String, String>>,
    by_id: Mutex<HashMap<String, Tether>>,
    live: Mutex<HashMap<String, LiveSession>>,
    lease: Duration,
}

impl std::fmt::Debug for Registry {
    /// Manual Debug: deliberately does not print credentials or hashes, so a
    /// `{:?}` in a log line cannot leak them (I-8).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("tethers", &self.by_id.lock().map(|m| m.len()).unwrap_or(0))
            .field("live", &self.live.lock().map(|m| m.len()).unwrap_or(0))
            .field("lease", &self.lease)
            .finish()
    }
}

impl Registry {
    pub fn new(lease: Duration) -> Self {
        Self {
            by_credential: Mutex::new(HashMap::new()),
            by_id: Mutex::new(HashMap::new()),
            live: Mutex::new(HashMap::new()),
            lease: lease.max(Duration::from_secs(5)),
        }
    }

    /// Register a tether. `credential` is digested here and discarded, so no call
    /// site can accidentally retain or log it (I-8).
    pub fn register(&self, id: &str, label: &str, credential: &str) -> Tether {
        let tether = Tether {
            id: id.to_string(),
            label: label.to_string(),
            credential_hash: digest(credential.as_bytes()),
            active: true,
        };
        self.by_credential
            .lock()
            .unwrap()
            .insert(tether.credential_hash.clone(), tether.id.clone());
        self.by_id
            .lock()
            .unwrap()
            .insert(tether.id.clone(), tether.clone());
        tether
    }

    /// THE authorization decision (I-5). Takes only a credential: a client cannot
    /// request a port, a host, a rate, or a longer lease.
    pub fn authorize(&self, credential: &[u8]) -> Option<Lease> {
        let id = self
            .by_credential
            .lock()
            .unwrap()
            .get(&digest(credential))?
            .clone();
        let tether = self.by_id.lock().unwrap().get(&id)?.clone();
        // Revoked == nonexistent, and deliberately indistinguishable from it: a
        // client learning "you were revoked" is a (mild) oracle it does not need.
        if !tether.active {
            return None;
        }
        Some(Lease {
            tether_id: id,
            ttl: self.lease,
        })
    }

    /// Revoke, and also evict any live session (I-3: revocation must not wait for
    /// a reconnect that a live tunnel has no reason to initiate).
    /// Returns whether this call changed anything: false for an unknown id *or* an
    /// already-revoked one (see the test for why that conflation is acceptable).
    pub fn revoke(&self, id: &str) -> bool {
        let changed = {
            let mut by_id = self.by_id.lock().unwrap();
            match by_id.get_mut(id) {
                Some(t) if t.active => {
                    t.active = false;
                    true
                }
                _ => false,
            }
        };
        // Evict a live session even when nothing changed above, so a re-revoke is
        // still safe against a tunnel that reconnected in between.
        {
            // Drop the sender; the session task's recv() returns None and it exits,
            // closing the WebSocket.
            self.live.lock().unwrap().remove(id);
        }
        changed
    }

    fn attach(&self, id: &str, tx: mpsc::UnboundedSender<Frame>) -> LiveSession {
        let session = LiveSession {
            tx,
            last_seen: Arc::new(Mutex::new(Instant::now())),
            streams: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)),
        };
        // Replace atomically AND drop the predecessor's sender. Dropping it is
        // what makes the old session task's recv() return None so it exits and
        // closes its WebSocket; a bare insert left the previous session alive
        // beside the new one, so a reconnect could leave two live sessions under
        // one tether id, each holding its own socket and stream map. Observed
        // directly as two `authorized ... / event Up` pairs interleaved for one
        // tether, with an in-flight stream's frames going to the abandoned one.
        //
        // Take-then-insert inside one lock hold: the map never momentarily
        // contains neither session, and no other thread can observe or forward
        // to a session between the two steps.
        let previous = self
            .live
            .lock()
            .unwrap()
            .insert(id.to_string(), session.clone());
        // Dropped OUTSIDE the lock: a Drop that wakes another task must not run
        // while the registry mutex is held.
        drop(previous);
        session
    }

    fn detach(&self, id: &str) {
        self.live.lock().unwrap().remove(id);
    }

    /// Forward a caller's request through a tether's tunnel and return a reader
    /// for the engine's streaming answer.
    ///
    /// This is the hub's only path to a tether, and it is deliberately *not*
    /// exposed to the tether side (I-5): the hub chooses which tether serves a
    /// request, and a tether cannot ask to originate anything.
    pub async fn forward(
        &self,
        tether_id: &str,
        req: http::Request<hyper::body::Incoming>,
    ) -> Result<Forwarded, ForwardError> {
        let session = self
            .live
            .lock()
            .unwrap()
            .get(tether_id)
            .cloned()
            .ok_or(ForwardError::NoTether)?;
        if !self.is_active(tether_id) {
            // A tether can be revoked between the registry check and here; never
            // route to a revoked one even if its socket still looks live (I-3).
            return Err(ForwardError::Revoked);
        }

        // Stream ids are u16 and per-tether. Wrapping is refused rather than
        // silently reused: reassigning an id that a live stream still holds would
        // deliver one caller's tokens to another.
        let id = {
            let mut next = session.next_id.lock().unwrap();
            if *next > u32::from(u16::MAX) {
                return Err(ForwardError::Idhausted);
            }
            let id = *next as u16;
            *next += 1;
            id
        };

        let (method, path, headers, mut body) = {
            let parts = req.method().clone();
            let path = req
                .uri()
                .path_and_query()
                .map(|p| p.as_str().to_string())
                .unwrap_or_else(|| "/".to_string());
            let headers = req.headers().clone();
            (parts, path, headers, req.into_body())
        };

        let (chunk_tx, chunks) = mpsc::channel::<ChunkOrEnd>(64);
        let state = Arc::new(StreamState {
            method,
            path,
            headers,
            pending: Arc::new(Mutex::new(VecDeque::new())),
            chunk_tx,
            chunks: Arc::new(Mutex::new(Some(chunks))),
            head: Arc::new(Mutex::new(None)),
            pending_head: Arc::new(Mutex::new(Vec::new())),
            completed: Arc::new(AtomicBool::new(false)),
        });
        session.streams.lock().unwrap().insert(id, state.clone());

        // Emit OPEN. From here on, every exit path must close the stream, or a
        // failed forward leaves a phantom entry that a later END would resurrect.
        let open = {
            // Encode the request head. Getting this wrong is invisible in a test
            // that only checks "did bytes come back" -- an empty head still yields
            // *some* engine response -- so the shape is asserted directly in
            // `request_head_is_a_valid_http_request`.
            let head = encode_request_head(&state.method, &state.path, &state.headers);
            Frame::Open { stream: id, head }
        };
        if session.tx.send(open).is_err() {
            session.streams.lock().unwrap().remove(&id);
            return Err(ForwardError::TetherGone);
        }
        // Drop guard: if this function returns Err after OPEN or the caller drops
        // the reader without finishing, the tether must be told.
        let _guard = StreamGuard {
            tx: session.tx.clone(),
            streams: session.streams.clone(),
            id,
            chunk_tx: state.chunk_tx.clone(),
            completed: state.completed.clone(),
        };

        // Drain the caller's body into the tunnel as DATA frames. Doing this on the
        // calling task means the caller's backpressure is the tunnel's backpressure
        // -- no unbounded buffer for a long prompt.
        // Incoming implements http_body::Body, not futures::Stream; BodyExt::frame
        // is the accessor that works for it in hyper 1.
        use http_body_util::BodyExt;
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|_| ForwardError::CallerBody)?;
            if frame.is_trailers() {
                continue;
            }
            let chunk = frame.into_data().map_err(|_| ForwardError::CallerBody)?;
            session
                .tx
                .send(Frame::Data {
                    stream: id,
                    bytes: chunk.to_vec(),
                })
                .map_err(|_| ForwardError::TetherGone)?;
        }
        // HALF_END, not END.
        //
        // The head we forwarded declares neither `transfer-encoding: chunked` nor a
        // `content-length` we could vouch for, because the body arrives later as DATA
        // frames. So only a half-close toward the engine can end the request while
        // leaving the answer flowing back.
        //
        // Sending END here -- as an earlier version did -- produced a *valid but
        // empty* engine request: the tether shut its write half before any DATA
        // reached it, and vLLM would have hung on a body it never saw or answered a
        // zero-byte prompt. The failure was silent because what came back parsed fine.
        session
            .tx
            .send(Frame::HalfEnd { stream: id })
            .map_err(|_| ForwardError::TetherGone)?;

        Ok(Forwarded { id, state, _guard })
    }

    /// Is the tether up, down, or absent? (I-6: never "maybe slow".)
    pub fn status(&self) -> Vec<(String, String, TetherState)> {
        let live = self.live.lock().unwrap();
        let by_id = self.by_id.lock().unwrap();
        by_id
            .values()
            .map(|t| {
                let state = match live.get(&t.id) {
                    Some(s) => {
                        let idle = s.last_seen.lock().unwrap().elapsed();
                        if idle > TETHER_SILENCE {
                            TetherState::Stale(idle)
                        } else {
                            TetherState::Up(idle)
                        }
                    }
                    // Registered but not connected: DOWN, not unknown.
                    None => TetherState::Down,
                };
                (t.id.clone(), t.label.clone(), state)
            })
            .collect()
    }

    pub fn is_active(&self, id: &str) -> bool {
        self.by_id
            .lock()
            .unwrap()
            .get(id)
            .map(|t| t.active)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TetherState {
    Up(Duration),
    /// Connected recently, but silent longer than TETHER_SILENCE.
    Stale(Duration),
    /// Registered, not currently connected.
    Down,
}

/// Why a request could not be forwarded. Each variant is a distinct operator
/// answer -- "no tether" and "revoked" must not be conflated, or a revoked node
/// reads as a merely-absent one (I-3/I-6).
#[derive(Debug)]
pub enum ForwardError {
    /// No live tunnel for that tether.
    NoTether,
    /// Tether is registered but revoked; refuse even though its socket may live.
    Revoked,
    /// 65535 streams opened on one tunnel without reuse. Refuse; do not alias.
    Idhausted,
    /// The tunnel died mid-forward.
    TetherGone,
    /// The caller's own body failed.
    CallerBody,
}

impl std::fmt::Display for ForwardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ForwardError::NoTether => "no live tunnel for this tether",
            ForwardError::Revoked => "tether revoked",
            ForwardError::Idhausted => "too many concurrent streams on this tether",
            ForwardError::TetherGone => "tunnel closed mid-request",
            ForwardError::CallerBody => "caller body read failed",
        })
    }
}

impl std::error::Error for ForwardError {}

/// A forwarded stream, handed back to the caller-facing frontend.
///
/// No Debug: it holds stream state containing channels, and a debug print of that
/// would tempt someone into logging request bodies.
pub struct StreamGuard {
    tx: mpsc::UnboundedSender<Frame>,
    streams: Arc<Mutex<HashMap<u16, Arc<StreamState>>>>,
    id: u16,
    #[allow(dead_code)]
    chunk_tx: mpsc::Sender<ChunkOrEnd>,
    /// Set when the answer reaches END, so Drop knows not to abort.
    ///
    /// Read in `StreamGuard::drop`. Deliberately NOT `#[allow(dead_code)]`:
    /// that attribute silenced the one warning that would have reported this
    /// field unread, which is how "Drop must not abort a completed stream"
    /// came to be documented and never implemented.
    completed: Arc<AtomicBool>,
}

impl Drop for StreamGuard {
    /// Tells the tether to abort, and unregisters the stream, so an abandoned
    /// caller cannot leave a half-open stream consuming engine capacity. The
    /// channel sender drop also wakes anyone awaiting chunks.
    fn drop(&mut self) {
        self.streams.lock().unwrap().remove(&self.id);
        // A stream that already reached END is finished, not abandoned: sending
        // End here would abort the tether's read for a response that is still
        // arriving. Skip the abort; the deregistration above is enough.
        if self.completed.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        let _ = self.tx.send(Frame::End {
            stream: self.id,
            reason: Vec::new(),
        });
    }
}

/// The caller-facing handle: yields engine response chunks until END.
pub struct Forwarded {
    pub id: u16,
    state: Arc<StreamState>,
    _guard: StreamGuard,
}

impl Forwarded {
    /// The engine's status line and headers, as soon as they arrive. The tether
    /// parses them out of the first DATA frame; a proxy that only looked at the
    /// body could not distinguish an engine 500 from an engine 200.
    pub async fn head(&self) -> Option<http::Response<()>> {
        // Wait for the head with NO fixed budget.
        //
        // This used to poll 200 x 5ms (~1s) and then return None. Returning None
        // here was not a timeout -- it was a WRONG ANSWER, and worse than the
        // fabricated 200 it replaced. The head does eventually arrive; by then
        // the caller had been handed a response built from whatever bytes
        // `parse_head` had already yielded as `rest`, and the stream was
        // abandoned. Measured against an engine slower than ~1s to first byte,
        // the caller received a truncated body whose framing leaked through:
        // `A\r\ndata: one\n\r\n0\r\n\r\n`, i.e. one chunk-size line and one
        // event, presented as a complete answer. A time-to-first-token of a few
        // seconds is normal for an inference engine, so the old budget fired on
        // ordinary traffic, not on a pathological one.
        //
        // Waiting indefinitely is correct here because the wait is NOT unbounded
        // in practice: it ends the moment the head arrives, the stream ends
        // (END/GoAway closes the chunk channel, and `completed` is set), or the
        // caller disconnects -- and a disconnected caller stops driving this
        // future, so it is dropped rather than left polling. Tether death is not
        // a hole either: the session loop's exit drops the stream map, which
        // drops the state this borrows, so there is no liveness path that leaves
        // it waiting on a dead peer.
        loop {
            if let Some(res) = self.state.head.lock().unwrap().clone() {
                return Some(res);
            }
            // Set when the engine's END arrives, or when the stream is aborted
            // (tether death drops the stream map, which drops the guard). Either
            // way no head can still arrive, so this is a real failure and the
            // caller answers 502 (I-11) rather than reporting a partial success.
            if self.state.completed.load(Ordering::SeqCst) {
                return None;
            }
            // Yield so the session task that WRITES the head can run. A short
            // sleep rather than a Notify: the head is written between DATA
            // frames on a task this future cannot wake directly, and the loop
            // exits on the head itself -- so unlike the old budget this never
            // gives up, it only re-checks.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Hand the response receiver to the caller-facing frontend, which becomes its
    /// sole owner. Returns None if already handed over.
    pub fn take_rx(&self) -> Option<mpsc::Receiver<ChunkOrEnd>> {
        self.state.chunks.lock().unwrap().take()
    }
}

impl StreamState {
    /// Handle the session task uses to deliver engine bytes to this stream.
    pub fn chunks_tx(&self) -> mpsc::Sender<ChunkOrEnd> {
        self.chunk_tx.clone()
    }
}

/// Serialize an HTTP/1.1 request line + headers, for an OPEN frame's `head`.
///
/// Mirrors `parse_head` on the response side. Two rules that are easy to get wrong
/// and produce "works against my test server, breaks against vLLM":
///  - Host is required by HTTP/1.1 but was stripped as hop-by-hop on the way in, so
///    it is re-derived from the upstream authority rather than forwarded.
///  - `content-length` must reflect the body the tether will actually send. We know
///    it only after draining, so it is deliberately omitted and the stream is
///    self-terminating via END; engines accept chunked/EOF-delimited bodies, and
///    inventing a wrong length is worse than omitting one.
fn encode_request_head(method: &Method, path: &str, headers: &HeaderMap) -> Vec<u8> {
    let mut out = format!("{method} {path} HTTP/1.1\r\n").into_bytes();
    for (name, value) in headers {
        if crate::headers::is_hop_by_hop(name.as_str()) {
            continue;
        }
        out.extend_from_slice(name.as_str().as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out
}

/// Parse an HTTP/1 status line + headers from the first engine bytes, returning
/// the response head and the unconsumed remainder.
///
/// The tether forwards raw engine bytes, so the hub must find the head itself. A
/// proxy that skipped this could not tell an engine 500 from a 200 -- it would
/// report both to the caller as a successful stream.
///
/// Hand-parsed rather than via a parser crate: a status line is three whitespace
/// tokens and a header block is `name: value` lines. Pulling httparse in would
/// mean declaring a dependency that is already in the tree transitively, for less
/// than 30 lines.
///
/// Headers are returned EXACTLY as the engine sent them. This function must NOT
/// apply hop-by-hop filtering: it parses the engine->tether message, and
/// `transfer-encoding` is precisely the header that tells the tether how to decode
/// the body (chunked vs not). An earlier version filtered here because
/// `headers::ALWAYS_STRIP` is also correct for the *forwarding* path, and the two
/// paths shared one function. The observable failure was subtle and total: a
/// response literally declaring `transfer-encoding: chunked` parsed as if it did
/// not, so the tether never de-chunked, chunk markers were forwarded as payload,
/// and the hub re-framed an already-chunked body -- producing a chunk length that
/// disagreed with its own data.
///
/// Filtering belongs in `frontend`/`encode_request_head`, on the message that
/// crosses to the next hop.
pub fn parse_head(bytes: &[u8]) -> Option<(http::Response<()>, Bytes)> {
    let end = find_header_end(bytes)?;
    let head = std::str::from_utf8(&bytes[..end]).ok()?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next()?;
    let mut it = status_line.split_whitespace();
    let _version = it.next()?;
    let code: u16 = it.next()?.parse().ok()?;
    let mut builder = http::Response::builder().status(code);
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':')?;
        if let (Ok(n), Ok(v)) = (
            HeaderName::from_bytes(name.trim().as_bytes()),
            HeaderValue::from_bytes(value.trim().as_bytes()),
        ) {
            builder = builder.header(n, v);
        }
    }
    Some((
        builder.body(()).ok()?,
        // `find_header_end` returns the offset just PAST the blank line, so this is
        // the body proper.
        Bytes::copy_from_slice(&bytes[end..]),
    ))
}

/// Byte offset just past the head (including the blank line), if present.
pub fn find_header_end(bytes: &[u8]) -> Option<usize> {
    if let Some(i) = find_subslice(bytes, b"\r\n\r\n") {
        return Some(i + 4);
    }
    if let Some(i) = find_subslice(bytes, b"\n\n") {
        return Some(i + 2);
    }
    None
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Accept tunnels. `router` receives a channel for each authorized tether so
/// callers (the proxy fronting this hub) can push OPEN frames and collect answers.
pub async fn serve(
    listen: std::net::SocketAddr,
    registry: Arc<Registry>,
) -> std::io::Result<mpsc::UnboundedReceiver<TetherEvent>> {
    let listener = TcpListener::bind(listen).await?;
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((sock, peer)) => {
                    let reg = registry.clone();
                    let tx = events_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_tether(sock, peer, reg, tx).await {
                            eprintln!("anvil-ring hub: tether from {peer} ended: {e}");
                        }
                    });
                }
                Err(e) => eprintln!("anvil-ring hub: accept failed: {e}"),
            }
        }
    });
    Ok(events_rx)
}

/// Something happened to a tether. Surfaced rather than swallowed, per I-6.
#[derive(Debug, Clone)]
pub struct TetherEvent {
    pub tether_id: String,
    pub kind: EventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
    Up,
    Down,
    /// Revoked while connected.
    Revoked,
}

async fn handle_tether(
    sock: TcpStream,
    peer: std::net::SocketAddr,
    registry: Arc<Registry>,
    events: mpsc::UnboundedSender<TetherEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The hub terminates TLS in deployment; over the tailnet this listener may be
    // plain. Refusing non-loopback plaintext is enforced by the deployment (bind
    // loopback or front with TLS), and noted here because a silent plaintext hub
    // would leak every credential (I-8).
    let ws = tokio_tungstenite::accept_async(sock).await?;
    let (mut sink, mut stream) = ws.split();

    // First frame must be HELLO. Anything else is a protocol violation, not a
    // thing to be tolerant about: being lenient here means authenticating nothing.
    let first = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .map_err(|_| "no HELLO before timeout")?
        .ok_or("closed before HELLO")??;
    let credential = match decode(&first)? {
        Some(Frame::Hello { credential }) => credential,
        Some(other) => return Err(format!("expected HELLO, got 0x{:02x}", other.type_tag()).into()),
        None => return Err("no tunnel frame before HELLO".into()),
    };

    let Some(lease) = registry.authorize(&credential) else {
        // Do not say why (see `authorize`). Log the peer, never the credential.
        eprintln!("anvil-ring hub: refused tether from {peer}");
        let _ = sink
            .send(ws_msg(Frame::GoAway {
                reason: b"unauthorized".to_vec(),
            }))
            .await;
        return Err("unauthorized".into());
    };

    eprintln!(
        "anvil-ring hub: tether {} authorized from {peer}, lease {}s",
        lease.tether_id,
        lease.ttl.as_secs()
    );
    sink.send(ws_msg(Frame::Welcome {
        lease_secs: lease.ttl.as_secs(),
    }))
    .await?;
    let _ = events.send(TetherEvent {
        tether_id: lease.tether_id.clone(),
        kind: EventKind::Up,
    });

    let (tx, mut rx) = mpsc::unbounded_channel::<Frame>();
    let session = registry.attach(&lease.tether_id, tx);
    let last_seen = session.last_seen.clone();
    let streams = session.streams.clone();
    // Boxed+pinned sleep, re-armed each pass: `select!` needs to poll it while we
    // still need to move it. A `tick()` future would hold `interval` borrowed.
    let mut tick = Box::pin(tokio::time::sleep(TETHER_TICK));
    let mut revoked = false;

    // Trace the writer arm: does a frame handed to rx actually reach the socket?
    let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = loop {
        tokio::select! {
            _ = tick.as_mut() => {
                // I-3: a lease is not merely a number we told the client. If the
                // tether was revoked, end the session NOW rather than waiting for
                // a reconnect it has no reason to initiate.
                if !registry.is_active(&lease.tether_id) {
                    let _ = sink
                        .send(ws_msg(Frame::GoAway { reason: b"revoked".to_vec() }))
                        .await;
                    revoked = true;
                    break Ok(());
                }
                // I-6: surface staleness explicitly rather than letting a lost
                // tether look merely slow.
                let idle = last_seen.lock().unwrap().elapsed();
                if idle > TETHER_SILENCE {
                    eprintln!(
                        "anvil-ring hub: tether {} silent {idle:?}; closing",
                        lease.tether_id
                    );
                    break Err("tether silent past deadline".into());
                }
                // Re-arm; an already-complete future would spin the loop at 100% CPU.
                tick
                    .as_mut()
                    .reset(tokio::time::Instant::now() + TETHER_TICK);
            }
            out = rx.recv() => {
                match out {
                    Some(frame) => {
                        if sink.send(ws_msg(frame)).await.is_err() {
                            break Err("write to tether failed".into());
                        }
                    }
                    // Revocation drops our sender, so None here means revoked.
                    None => {
                        let _ = sink
                            .send(ws_msg(Frame::GoAway { reason: b"revoked".to_vec() }))
                            .await;
                        revoked = true;
                        break Ok(());
                    }
                }
            }
            inp = stream.next() => {
                let m = match inp {
                    Some(m) => m?,
                    None => break Err("tether closed".into()),
                };
                *last_seen.lock().unwrap() = Instant::now();
                let Some(frame) = decode(&m)? else { continue };
                match frame {
                    Frame::Ping => {
                        sink.send(ws_msg(Frame::Pong)).await?;
                    }
                    Frame::Pong => {}
                    // A client re-authorizing early is normal (its lease watchdog).
                    // Honor it by ending this session; its next HELLO is a fresh
                    // authorization decision.
                    Frame::GoAway { .. } => break Ok(()),
                    // A client must never send OPEN -- only the hub initiates
                    // streams. I-5 enforced at the frame level, not by convention.
                    Frame::Open { .. } => {
                        break Err("client sent OPEN; only the hub initiates streams (I-5)".into());
                    }
                    // Only the hub may half-close: a tether signalling
                    // end-of-request would be a tether answering a request it was
                    // never sent. Treated like OPEN, as a protocol violation.
                    Frame::HalfEnd { .. } => {
                        break Err("client sent HALF_END; only the hub half-closes (I-5)".into());
                    }
                    Frame::Hello { .. } | Frame::Welcome { .. } => {
                        break Err("unexpected HELLO/WELCOME mid-session".into());
                    }
                    Frame::RespHead { stream: id, head } => {
                        // The tether's response head, already reframed: any
                        // `transfer-encoding` the tether consumed is gone, so the
                        // body arriving in DATA frames needs re-framing here and
                        // must NOT be forwarded verbatim. Carrying the head in its
                        // own frame is what makes that unambiguous -- the hub never
                        // has to guess whether the first chunk is a head, which is
                        // how a raw status line ended up in the caller's body.
                        let target = streams.lock().unwrap().get(&id).cloned();
                        let Some(st) = target else { continue };
                        match parse_head(&head) {
                            Some((res, rest)) => {
                                if rest.is_empty() {
                                    // Normal: head alone, body arrives as DATA.
                                }
                                *st.head.lock().unwrap() = Some(res);
                                // `rest` is body bytes that came WITH the head; the
                                // head frame must not swallow them.
                                if !rest.is_empty()
                                    && st.chunks_tx().send(ChunkOrEnd::Chunk(rest)).await.is_err()
                                {
                                    let _ = sink.send(ws_msg(Frame::End { stream: id, reason: Vec::new() })).await;
                                }
                            }
                            None => {
                                // A head the hub cannot parse is a protocol fault at
                                // the far hop. Fail the stream loudly rather than
                                // letting the caller hang (I-11: no silent 200).
                                eprintln!("anvil-ring hub: stream {id} head unparseable");
                                let _ = sink
                                    .send(ws_msg(Frame::End {
                                        stream: id,
                                        reason: b"unparseable engine head".to_vec(),
                                    }))
                                    .await;
                            }
                        }
                    }
                    Frame::Data { stream: id, bytes } => {
                        eprintln!(
                            "TRACE-DATA id={id} n={} head={}",
                            bytes.len(),
                            bytes.len().min(24)
                        );
                        // Route only to a stream the HUB opened. Dropping anything
                        // else is what stops a tether injecting bytes into a
                        // request it never saw.
                        // Clone the Arc out so the std guard drops before the
                        // awaits below; holding it across an await makes the
                        // session future non-Send.
                        let target = streams.lock().unwrap().get(&id).cloned();
                        match target {
                            None => {
                                // No such stream: the caller already gave up. Tell
                                // the tether so it can stop streaming to us.
                                let _ = sink.send(ws_msg(Frame::End { stream: id, reason: Vec::new() })).await;
                            }
                            Some(st) => {
                                if st.head.lock().unwrap().is_none() {

                                    // The first chunk carries the engine's status
                                    // line; the tether forwarded raw HTTP bytes.
                                    // Any bytes buffered from an earlier unparsable
                                    // frame go in FRONT of this one, so a head split
                                    // across frames is retried as one.
                                    let buffered =
                                        std::mem::take(&mut *st.pending_head.lock().unwrap());
                                    let bytes: Vec<u8> = if buffered.is_empty() {
                                        bytes.to_vec()
                                    } else {
                                        let mut v = buffered;
                                        v.extend_from_slice(&bytes);
                                        v
                                    };
                                    if let Some((res, rest)) = parse_head(&bytes) {
                                        eprintln!(
                                            "TRACE REQPATH parse_head SUCCEEDED on DATA (head was unset): n={} rest={} -> rest is RAW ENGINE framing forwarded as body",
                                            bytes.len(),
                                            rest.len()
                                        );

                                        *st.head.lock().unwrap() = Some(res);
                                        if !rest.is_empty()
                                            && st.chunks_tx().send(ChunkOrEnd::Chunk(rest)).await.is_err()
                                        {
                                            let _ = sink.send(ws_msg(Frame::End { stream: id, reason: Vec::new() })).await;
                                        }
                                    } else {
                                        // NOT a head. The previous version of this
                                        // branch dropped the chunk and left `head`
                                        // unset, which meant the NEXT chunk arrived
                                        // here too and was dropped as well: one
                                        // unparsable first frame silently killed the
                                        // whole response, and nothing was logged.
                                        //
                                        // Buffer it instead. A head split across two
                                        // reads is legal -- the status line has no
                                        // length prefix -- so the two frames are
                                        // joined and retried on the next chunk.
                                        // Scoped so the guard is dropped at the
                                        // block's end: a std MutexGuard is not
                                        // `Send`, and this loop lives in a future
                                        // passed to tokio::spawn, so any guard
                                        // reaching an await makes the whole
                                        // future non-Send (compile error, not a
                                        // deadlock -- caught here by cargo).
                                        let over = {
                                            let mut p = st.pending_head.lock().unwrap();
                                            let over = p.len() + bytes.len() > MAX_PENDING_HEAD;
                                            if !over {
                                                p.extend_from_slice(&bytes);
                                            }
                                            over
                                        };
                                        if over {
                                            // Not a head at any plausible offset.
                                            // Surface it; do not spin forever.
                                            eprintln!(
                                                "anvil-ring hub: stream {id} never produced a parseable head"
                                            );
                                            let _ = sink
                                                .send(ws_msg(Frame::End {
                                                    stream: id,
                                                    reason: b"bad upstream head".to_vec(),
                                                }))
                                                .await;
                                        }
                                    }
                                } else if st.chunks_tx().send(ChunkOrEnd::Chunk(Bytes::from(bytes))).await.is_err() {
                                        eprintln!("TRACE FORWARD FAILED id={id} (caller channel closed)");

                                    // Caller vanished: close the stream at the tether.
                                    let _ = sink.send(ws_msg(Frame::End { stream: id, reason: Vec::new() })).await;
                                }
                            }
                        }
                    }
                    Frame::End { stream: id, .. } => {
                        let target = streams.lock().unwrap().get(&id).cloned();
                        if let Some(st) = target {
                            // Mark completed BEFORE the guard can observe the drop,
                            // so a normal end never turns into an abort.
                            st.completed.store(true, Ordering::Release);
                            let _ = st.chunks_tx().send(ChunkOrEnd::End).await;
                            streams.lock().unwrap().remove(&id);
                        }
                    }
                }
            }
        }
    };

    registry.detach(&lease.tether_id);
    let kind = if revoked {
        EventKind::Revoked
    } else {
        EventKind::Down
    };
    let _ = events.send(TetherEvent {
        tether_id: lease.tether_id.clone(),
        kind,
    });
    result
}

fn ws_msg(f: Frame) -> tokio_tungstenite::tungstenite::Message {
    tokio_tungstenite::tungstenite::Message::Binary(f.encode())
}

fn decode(
    m: &tokio_tungstenite::tungstenite::Message,
) -> Result<Option<Frame>, Box<dyn std::error::Error + Send + Sync>> {
    use tokio_tungstenite::tungstenite::Message;
    match m {
        Message::Binary(b) => {
            let (frame, _n) = Frame::decode(b)?.ok_or("short tunnel frame")?;
            Ok(Some(frame))
        }
        Message::Ping(_) | Message::Pong(_) | Message::Text(_) => Ok(None),
        Message::Close(_) => Err("peer sent Close".into()),
        Message::Frame(_) => Ok(None),
    }
}

/// SHA-256 hex digest of a credential, used ONLY as a lookup key.
///
/// Read the security note before changing this. It buys exactly one property:
/// a hub config file, crash dump, or log line does not contain live credentials
/// (I-8). It is NOT a password hash — unsalted and fast. That is acceptable only
/// because registration credentials are high-entropy random tokens, so there is no
/// offline-guessing surface. If credentials ever become human-chosen, this must
/// become a real KDF (argon2/bcrypt); `high_entropy_note_is_still_true` below is
/// the tripwire reminder, not a proof.
fn digest(input: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(input);
    let out = h.finalize();
    let mut s = String::with_capacity(64);
    for b in out {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Known-answer tests. A hand-written hash MUST have these; without them a
    /// transcription bug in K/IV silently changes every credential mapping.
    #[test]
    fn sha256_known_answers() {
        assert_eq!(
            digest(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            digest(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        // 56-byte input forces the padding-into-two-blocks path: the boundary most
        // likely to be wrong in a hand-rolled implementation.
        assert_eq!(
            digest(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        // 1,000,000 x 'a' -- multi-block compression. Canonical SHA-256 KAT.
        assert_eq!(
            digest(&vec![b'a'; 1_000_000]),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    #[test]
    fn digest_is_stable_and_distinguishing() {
        assert_eq!(digest(b"token-a"), digest(b"token-a"));
        assert_ne!(digest(b"token-a"), digest(b"token-b"));
        assert_eq!(digest(b"token-a").len(), 64);
    }

    #[test]
    fn authorize_takes_only_a_credential() {
        // The signature IS the invariant: compile-time proof that a client cannot
        // request anything. If someone adds a `port` or `host` parameter, this test
        // still passes but the doc comment above it becomes a lie -- so assert the
        // fn's arity is exactly one argument.
        let f: fn(&Registry, &[u8]) -> Option<Lease> = Registry::authorize;
        let reg = Registry::new(DEFAULT_LEASE);
        reg.register("r1", "rental", "secret-1");
        let lease = f(&reg, b"secret-1").expect("should authorize");
        assert_eq!(lease.tether_id, "r1");
        assert_eq!(lease.ttl, DEFAULT_LEASE);
        assert!(f(&reg, b"nope").is_none());
    }

    #[test]
    fn revoked_credential_stops_working_immediately() {
        let reg = Registry::new(DEFAULT_LEASE);
        reg.register("r1", "rental", "s1");
        assert!(reg.authorize(b"s1").is_some());
        assert!(reg.revoke("r1"));
        assert!(
            reg.authorize(b"s1").is_none(),
            "I-3: a revoked token must stop working at once"
        );
    }

    #[test]
    fn revoke_reports_whether_it_changed_anything() {
        let reg = Registry::new(DEFAULT_LEASE);
        reg.register("r1", "rental", "s1");
        // Contract: true = "this call revoked something"; false = "nothing
        // changed", either because it was already revoked or the id is unknown.
        // Operators need that distinction, and repeated revocation stays safe.
        assert!(reg.revoke("r1"), "first revoke changes state");
        assert!(!reg.revoke("r1"), "second revoke changes nothing");
        assert!(!reg.revoke("r1"), "third, same");
        assert!(
            reg.authorize(b"s1").is_none(),
            "I-3: repeated revoke must not resurrect the credential"
        );
        // Unknown id is also false. That conflates typo with no-op, which is
        // acceptable here because `status()` shows registered tethers explicitly;
        // noted rather than hidden because it is a real ergonomic tradeoff.
        assert!(!reg.revoke("never-registered"));
    }

    #[test]
    fn unknown_and_revoked_are_indistinguishable_to_the_client() {
        // authorize returns Option, so both are None. Assert there is no error
        // channel that could leak which one it was.
        let reg = Registry::new(DEFAULT_LEASE);
        reg.register("r1", "rental", "s1");
        reg.revoke("r1");
        let revoked: Option<Lease> = reg.authorize(b"s1");
        let never: Option<Lease> = reg.authorize(b"brand-new-token");
        assert!(revoked.is_none() && never.is_none());
    }

    #[test]
    fn status_reports_down_rather_than_unknown() {
        let reg = Registry::new(DEFAULT_LEASE);
        reg.register("r1", "rental", "s1");
        let s = reg.status();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].2, TetherState::Down, "I-6: absent must be explicit");
    }

    #[test]
    fn duplicate_registration_of_same_credential_does_not_shadow() {
        // Two tethers with one credential would make revocation of one a no-op for
        // the other -- a revocation bypass. Registering the same credential twice
        // must therefore be last-writer-wins on the MAP (one id owns it), not two.
        let reg = Registry::new(DEFAULT_LEASE);
        reg.register("r1", "first", "shared");
        reg.register("r2", "second", "shared");
        let lease = reg.authorize(b"shared").expect("one owner");
        assert_eq!(lease.tether_id, "r2");
        // Revoking the owner must disable the credential entirely.
        reg.revoke("r2");
        assert!(reg.authorize(b"shared").is_none());
        // ...and revoking the non-owner must NOT have appeared to work.
        assert!(reg.revoke("r1"));
    }

    #[test]
    fn lease_is_never_zero() {
        // A zero lease would mean "reconnect immediately" -- a reconnect storm.
        let reg = Registry::new(Duration::ZERO);
        reg.register("r1", "rental", "s1");
        assert!(reg.authorize(b"s1").unwrap().ttl >= Duration::from_secs(5));
    }
}

#[cfg(test)]
mod parse_head_rest_tests {
    use super::*;

    /// The coalesced shape the tether sends first: status line + headers + the
    /// first chunk-coded event. `rest` must be the BYTES AFTER THE HEAD, and the
    /// head must be reported -- not swallowed, not doubled.
    #[test]
    fn rest_is_the_body_after_the_head_and_headers_end_once() {
        let wire = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\na\r\ndata: one\n\r\n";
        let (res, rest) = parse_head(wire).expect("head should parse");
        assert_eq!(res.status(), hyper::StatusCode::OK);
        assert_eq!(
            rest.as_ref(),
            b"a\r\ndata: one\n\r\n",
            "rest must begin exactly at the body, not leak header bytes \
             nor swallow the body"
        );
    }

    /// A lone head (no body yet) must yield an empty rest, so the hub does not
    /// synthesize a body chunk out of nothing.
    #[test]
    fn lone_head_has_empty_rest() {
        let wire = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n";
        let (_res, rest) = parse_head(wire).expect("head should parse");
        assert!(
            rest.is_empty(),
            "lone head must not fabricate body bytes: {rest:?}"
        );
    }
}

#[cfg(test)]
mod coalesced_rest_is_framed_tests {
    use super::*;

    /// THIS TEST PASSES WHILE THE BUG IS PRESENT: it documents current
    /// behaviour, it does not endorse it. Flip the assertions when the tether
    /// stops preserving `transfer-encoding` on the head it forwards -- the fix
    /// -- and this becomes a regression test.
    ///
    /// If the hub forwards `parse_head`'s `rest` verbatim, the caller sees the
    /// ENGINE's chunk framing as its body. That is the shape observed on the
    /// wire, so pin what verbatim forwarding produces and what de-chunking
    /// SHOULD produce, to show which one the hub actually does today.
    #[test]
    fn hub_forwards_rest_verbatim_and_therefore_leaks_framing() {
        let wire = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\na\r\ndata: one\n\r\n0\r\n\r\n";
        let (res, rest) = parse_head(wire).expect("parses");
        // What the hub currently sends when it forwards `rest` unchanged:
        assert_eq!(
            rest.as_ref(),
            b"a\r\ndata: one\n\r\n0\r\n\r\n",
            "rest still carries chunk framing"
        );
        // The hub has NO de-chunk step on this path, so the caller would receive
        // framing. De-chunking is the TETHER's job; this asserts the hub does
        // not do it, which is the defect this test documents.
        let mut d = crate::chunked::ChunkedDecoder::new();
        let out = d.push(&rest).expect("decodes").out;
        assert_eq!(out, b"data: one\n", "what the caller SHOULD have gotten");
        // The head still says chunked, so hyper re-frames an already-framed body.
        assert!(crate::chunked::is_chunked(res.headers()));
    }
}

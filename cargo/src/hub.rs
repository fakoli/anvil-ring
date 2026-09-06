//! The hub is the always-on side that owns every authorization and routing
//! decision.
//!
//! Deliberately asymmetric. The rental side may say almost nothing: it presents a
//! credential, and the hub decides whether that credential may exist at all, what
//! lease it receives, and when it must go. A client never names a target port, an
//! upstream host, or a permission — there is no parameter through which it could
//! ask for more.
//!
//! Guarantees implemented here:
//!  - `Registry::authorize` accepts only a credential. Nothing the client
//!    sends influences the decision beyond "does this credential map to an active
//!    tether".
//!  - Authorization lifetimes are bounded. Revoking an active tether ends its
//!    live session with `GOAWAY`, so an established tunnel cannot continue past
//!    its authorization period. The same period also bounds idle tunnels.
//!  - Each tether has a last-seen deadline; `status()` distinguishes `Up` from
//!    `Down` rather than letting a lost tether look merely slow.
//!  - Credentials are held as digests, never plaintext, and are never logged.

use crate::frames::Frame;
use crate::tasks::AbortOnDrop;
use futures_util::{SinkExt, StreamExt};
use http::Method;
use hyper::body::Bytes;
use hyper::header::HeaderMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};

/// Ceiling on pre-head buffered bytes. A status line plus headers is small; a
/// stream that has not produced a parseable head within this many bytes is not
/// producing a head at all, and continuing to buffer would grow hub memory on
/// someone else's behalf.
pub const MAX_PENDING_HEAD: usize = 64 * 1024;

/// Default authorization lifetime. Short enough for prompt revocation; long
/// enough that reauthorization is not the dominant cost on a flaky rental link.
pub const DEFAULT_LEASE: Duration = Duration::from_secs(15 * 60);

/// How often the hub re-checks revocation and tether idleness.
pub const TETHER_TICK: Duration = Duration::from_secs(5);

/// How often the hub asks the tether's session loop to prove it is responsive.
pub const TETHER_PING_INTERVAL: Duration = Duration::from_secs(10);

/// Silence after which the hub considers a tether lost. This measures
/// acknowledged hub pings, not arbitrary inbound bytes: an engine pump may keep
/// streaming even while the code that accepts new requests is wedged.
pub const TETHER_SILENCE: Duration = Duration::from_secs(45);

/// Maximum hub-to-tether frames waiting to reach the WebSocket writer.
///
/// Request bodies can be arbitrarily large. A bounded queue makes the caller's
/// upload wait for the tunnel instead of turning a slow or wedged tether into
/// unbounded hub memory growth.
pub const COMMAND_CHANNEL_CAPACITY: usize = 256;

/// A registered tether.
#[derive(Debug, Clone)]
pub struct Tether {
    pub id: String,
    pub label: String,
    /// SHA-256 hex of the credential. Plaintext never lives here.
    pub credential_hash: String,
    /// Cleared by `revoke`. Checked at HELLO *and* enforced against live sessions.
    pub active: bool,
    /// Absolute credential expiry. None is reserved for explicit demo registrations.
    pub expires_at: Option<u64>,
}

impl Tether {
    fn remaining(&self) -> Option<Duration> {
        if !self.active {
            return None;
        }
        match self.expires_at {
            None => Some(Duration::MAX),
            Some(seconds) => std::time::UNIX_EPOCH
                .checked_add(Duration::from_secs(seconds))?
                .duration_since(std::time::SystemTime::now())
                .ok()
                .filter(|remaining| !remaining.is_zero()),
        }
    }
}

/// What the hub hands the client after a successful HELLO.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub tether_id: String,
    pub ttl: Duration,
}

/// A live session handle, so both revocation and request forwarding (the
/// hub -> tether direction) can reach an established tunnel.
#[derive(Clone)]
struct LiveSession {
    /// Distinct per authorized session, so an exiting session can tell whether the
    /// registry still points at IT or at a successor that re-authorized under the
    /// same tether id. Cloning a LiveSession shares this value; only a newly
    /// authorized session gets a new one.
    generation: u64,
    /// Write half. Requests the hub originates are pushed here and the session
    /// loop puts them on the wire.
    tx: mpsc::Sender<Frame>,
    /// Emergency session stop used when a dropped caller cannot enqueue END
    /// because the bounded command queue is full. Closing the whole unhealthy
    /// session is safer than leaving an engine stream orphaned.
    abort_tx: watch::Sender<bool>,
    last_seen: Arc<Mutex<Instant>>,
    /// Open streams on this tether, so the session loop can route inbound
    /// DATA/END frames to the caller waiting on the other end.
    streams: Arc<Mutex<HashMap<u16, Arc<StreamState>>>>,
    closed: Arc<AtomicBool>,
    /// Hands out stream ids for this tether. Per-tether so ids never collide
    /// across tethers, and monotonic so reuse cannot silently alias a live stream.
    next_id: Arc<Mutex<u32>>,
}

impl LiveSession {
    fn admit(&self, id: u16, state: Arc<StreamState>) -> Result<(), ForwardError> {
        let mut streams = self.streams.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return Err(ForwardError::TetherGone);
        }
        if streams.len() >= crate::tunnel::MAX_CONCURRENT_STREAMS {
            return Err(ForwardError::TetherBusy);
        }
        streams.insert(id, state);
        Ok(())
    }

    fn close(&self) {
        let abandoned: Vec<_> = {
            let mut streams = self.streams.lock().unwrap();
            self.closed.store(true, Ordering::Release);
            streams.drain().map(|(_, state)| state).collect()
        };
        for state in abandoned {
            state.fail();
        }
    }
}

/// One in-flight caller request and its response state.
pub struct StreamState {
    pub method: Method,
    pub path: String,
    pub headers: HeaderMap,
    /// Response side. Bounded, so a chatty engine cannot grow hub memory without
    /// limit if the caller stops reading.
    /// Sender kept so the session task can push engine bytes; `chunks` is the
    /// half the caller reads.
    chunk_tx: mpsc::Sender<ChunkOrEnd>,
    /// A plain `std` mutex: the frontend *takes* the receiver for one poll and
    /// releases the lock before touching it, so no guard ever crosses an await.
    /// (A tokio mutex here was tried first and made `poll` unwieldy for no gain.)
    chunks: Arc<Mutex<Option<mpsc::Receiver<ChunkOrEnd>>>>,
    /// Populated by the tether's RESP_HEAD frame.
    pub head: Arc<Mutex<Option<http::Response<()>>>>,
    /// Set when the engine's END arrives (see `StreamGuard::drop`).
    pub completed: Arc<AtomicBool>,
    failure: Arc<StreamFailure>,
    upload: Mutex<Option<tokio::task::AbortHandle>>,
}

/// Failure notification independent of response queue capacity.
/// A disconnected tether must wake even a caller whose chunk queue is full.
#[derive(Default)]
pub struct StreamFailure {
    outcome: AtomicU8,
    waker: futures_util::task::AtomicWaker,
}

impl StreamFailure {
    fn fail(&self) {
        self.outcome.store(2, Ordering::Release);
        self.waker.wake();
    }

    fn complete(&self) {
        self.outcome.store(1, Ordering::Release);
        self.waker.wake();
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.outcome.load(Ordering::Acquire) == 1
    }

    pub(crate) fn poll_failed(&self, cx: &std::task::Context<'_>) -> bool {
        self.waker.register(cx.waker());
        self.outcome.load(Ordering::Acquire) == 2
    }
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
    /// Serializes route selection through stream/command admission.
    routing: Mutex<()>,
    /// credential_hash -> tether id.
    by_credential: Mutex<HashMap<String, String>>,
    by_id: Mutex<HashMap<String, Tether>>,
    live: Mutex<HashMap<String, LiveSession>>,
    /// Monotonic per authorized session; see `LiveSession::generation`.
    next_generation: std::sync::atomic::AtomicU64,
    lease: Duration,
}

impl std::fmt::Debug for Registry {
    /// Manual Debug: deliberately does not print credentials or hashes, so a
    /// `{:?}` in a log line cannot leak them.
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
            routing: Mutex::new(()),
            by_credential: Mutex::new(HashMap::new()),
            by_id: Mutex::new(HashMap::new()),
            live: Mutex::new(HashMap::new()),
            next_generation: std::sync::atomic::AtomicU64::new(1),
            lease: lease.max(Duration::from_secs(5)),
        }
    }

    /// Register a tether. `credential` is digested here and discarded, so no call
    /// site can accidentally retain or log it.
    pub fn register(&self, id: &str, label: &str, credential: &str) -> Tether {
        let _routing = self.routing.lock().unwrap();
        let tether = Tether {
            id: id.to_string(),
            label: label.to_string(),
            credential_hash: digest(credential.as_bytes()),
            active: true,
            expires_at: None,
        };
        let mut credentials = self.by_credential.lock().unwrap();
        let mut tethers = self.by_id.lock().unwrap();
        if let Some(previous) = tethers.insert(tether.id.clone(), tether.clone()) {
            if credentials.get(&previous.credential_hash) == Some(&previous.id) {
                credentials.remove(&previous.credential_hash);
            }
        }
        credentials.insert(tether.credential_hash.clone(), tether.id.clone());
        if let Some(previous) = self.live.lock().unwrap().remove(id) {
            previous.close();
            let _ = previous.abort_tx.send(true);
        }
        tether
    }

    /// Atomically apply a trusted, validated persistent registration snapshot.
    pub fn replace_records(&self, records: Vec<crate::admin::Record>) {
        let _routing = self.routing.lock().unwrap();
        let mut credentials = self.by_credential.lock().unwrap();
        let mut tethers = self.by_id.lock().unwrap();
        let mut live = self.live.lock().unwrap();
        let replacement: HashMap<String, Tether> = records
            .into_iter()
            .map(|record| {
                let tether = Tether {
                    id: record.id,
                    label: record.label,
                    credential_hash: record.credential_hash,
                    active: record.active,
                    expires_at: Some(record.expires_at),
                };
                (tether.id.clone(), tether)
            })
            .collect();
        live.retain(|id, session| {
            let unchanged = replacement
                .get(id)
                .zip(tethers.get(id))
                .is_some_and(|(next, old)| {
                    next.remaining().is_some()
                        && next.credential_hash == old.credential_hash
                        && next.expires_at == old.expires_at
                });
            if !unchanged {
                session.close();
                let _ = session.abort_tx.send(true);
            }
            unchanged
        });
        *credentials = replacement
            .values()
            .map(|t| (t.credential_hash.clone(), t.id.clone()))
            .collect();
        *tethers = replacement;
    }

    /// Fail closed after an unavailable or invalid control-plane snapshot.
    pub fn clear_records(&self) {
        self.replace_records(Vec::new());
    }

    /// The authorization decision. Takes only a credential: a client cannot
    /// request a port, a host, a rate, or a longer lease.
    pub fn authorize(&self, credential: &[u8]) -> Option<Lease> {
        let credentials = self.by_credential.lock().unwrap();
        let tethers = self.by_id.lock().unwrap();
        let id = credentials.get(&digest(credential))?;
        let tether = tethers.get(id)?;
        // Revoked == nonexistent, and deliberately indistinguishable from it: a
        // client learning "you were revoked" is a (mild) oracle it does not need.
        let remaining = tether.remaining()?;
        if remaining < Duration::from_secs(1) {
            return None;
        }
        Some(Lease {
            tether_id: id.clone(),
            ttl: self.lease.min(remaining),
        })
    }

    /// Revoke and also evict any live session; revocation must not wait for
    /// a reconnect that a live tunnel has no reason to initiate).
    /// Returns whether this call changed anything: false for an unknown id *or* an
    /// already-revoked one (see the test for why that conflation is acceptable).
    pub fn revoke(&self, id: &str) -> bool {
        let _routing = self.routing.lock().unwrap();
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
            // Cancellation is explicit; live requests can retain sender clones.
            if let Some(session) = self.live.lock().unwrap().remove(id) {
                session.close();
                let _ = session.abort_tx.send(true);
            }
        }
        changed
    }

    fn attach(
        &self,
        id: &str,
        credential: &[u8],
        tx: mpsc::Sender<Frame>,
        abort_tx: watch::Sender<bool>,
    ) -> Option<LiveSession> {
        let _routing = self.routing.lock().unwrap();
        // Keep authorization and installation atomic with rotation/revocation.
        let credentials = self.by_credential.lock().unwrap();
        let tethers = self.by_id.lock().unwrap();
        let tether = tethers.get(id)?;
        let hash = digest(credential);
        if tether.remaining().is_none()
            || tether.credential_hash != hash
            || credentials.get(&hash).map(String::as_str) != Some(id)
        {
            return None;
        }
        let session = LiveSession {
            generation: self
                .next_generation
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            tx,
            abort_tx,
            last_seen: Arc::new(Mutex::new(Instant::now())),
            streams: Arc::new(Mutex::new(HashMap::new())),
            closed: Arc::new(AtomicBool::new(false)),
            next_id: Arc::new(Mutex::new(1)),
        };
        // Install under one lock, then explicitly cancel the predecessor. Its
        // task and in-flight callers may still hold sender clones.
        let previous = self
            .live
            .lock()
            .unwrap()
            .insert(id.to_string(), session.clone());
        // Dropped OUTSIDE the lock: a Drop that wakes another task must not run
        // while the registry mutex is held.
        if let Some(previous) = previous {
            previous.close();
            let _ = previous.abort_tx.send(true);
        }
        Some(session)
    }

    /// Unregister `id` ONLY if the registered session is still this same session.
    ///
    /// A plain `remove(id)` is wrong whenever a tether re-authorizes: the NEW
    /// session is installed under the same id, and when the OLD session's loop
    /// finally exits it deletes the NEW, live one. That pulls a healthy tunnel
    /// out of the routing table (callers get 502 NoTether) while the new session
    /// -- nothing tells it -- stays connected but unroutable.
    fn detach(&self, id: &str, session: &LiveSession) {
        let mut live = self.live.lock().unwrap();
        let owns_it = live
            .get(id)
            .map(|current| current.generation == session.generation)
            .unwrap_or(false);
        if owns_it {
            live.remove(id);
        }
        // Otherwise a newer session owns this id and MUST keep it.
    }

    /// Forward a caller's request through a tether's tunnel and return a reader
    /// for the engine's streaming answer.
    ///
    /// This is the hub's only path to a tether, and it is deliberately *not*
    /// exposed to the tether side: the hub chooses which tether serves a request,
    /// and a tether cannot ask to originate anything.
    pub async fn forward(
        &self,
        tether_id: &str,
        req: http::Request<hyper::body::Incoming>,
    ) -> Result<Forwarded, ForwardError> {
        let _routing = self.routing.lock().unwrap();
        let session = self
            .live
            .lock()
            .unwrap()
            .get(tether_id)
            .cloned()
            .ok_or(ForwardError::NoTether)?;
        if !self.is_active(tether_id) {
            // A tether can be revoked between the registry check and here; never
            // route to a revoked one even if its socket still looks live.
            return Err(ForwardError::Revoked);
        }
        let permit = reserve_stream_start(&session.tx)?;
        Self::start_forward(session, permit, req)
    }

    /// Choose the least-loaded eligible session, with stable lexical ties.
    /// Selection and OPEN admission are atomic with control-plane changes.
    pub fn forward_any(
        &self,
        req: http::Request<hyper::body::Incoming>,
    ) -> Result<Forwarded, ForwardError> {
        let _routing = self.routing.lock().unwrap();
        let (_, session, permit) = self.reserve_route()?;
        Self::start_forward(session, permit, req)
    }

    // Caller holds `routing` through stream admission. Session failures may
    // still race admission; they fail that request without retrying model work.
    fn reserve_route(
        &self,
    ) -> Result<(String, LiveSession, mpsc::OwnedPermit<Frame>), ForwardError> {
        let tethers = self.by_id.lock().unwrap();
        let live = self.live.lock().unwrap();
        let mut candidates: Vec<_> = live
            .iter()
            .filter_map(|(id, session)| {
                if tethers.get(id)?.remaining().is_none()
                    || session.closed.load(Ordering::Acquire)
                    || session.tx.is_closed()
                    || session.last_seen.lock().unwrap().elapsed() > TETHER_SILENCE
                {
                    return None;
                }
                let count = session.streams.lock().unwrap().len();
                Some((count, id, session))
            })
            .collect();
        candidates.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        let mut busy = false;
        for (count, id, session) in candidates {
            if count >= crate::tunnel::MAX_CONCURRENT_STREAMS
                || *session.next_id.lock().unwrap() > u32::from(u16::MAX)
            {
                busy = true;
                continue;
            }
            match reserve_stream_start(&session.tx) {
                Ok(permit) => return Ok((id.clone(), session.clone(), permit)),
                Err(ForwardError::TetherBusy) => busy = true,
                Err(_) => {}
            }
        }
        Err(if busy {
            ForwardError::TetherBusy
        } else {
            ForwardError::NoTether
        })
    }

    fn start_forward(
        session: LiveSession,
        permit: mpsc::OwnedPermit<Frame>,
        req: http::Request<hyper::body::Incoming>,
    ) -> Result<Forwarded, ForwardError> {
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

        let state = Arc::new(StreamState::new(method, path, headers));
        session.admit(id, state.clone())?;

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
        permit.send(open);
        // Drop guard: if this function returns Err after OPEN or the caller drops
        // the reader without finishing, the tether must be told.
        let mut guard = StreamGuard {
            tx: session.tx.clone(),
            abort_tx: session.abort_tx.clone(),
            streams: session.streams.clone(),
            id,
            chunk_tx: state.chunk_tx.clone(),
            completed: state.completed.clone(),
            upload: None,
        };

        // Upload and response progress independently: an engine may reject a
        // request before its body is complete, and tether failure must not wait
        // for a stalled caller. The response guard owns this task's cancellation.
        let upload_state = state.clone();
        let upload_tx = session.tx.clone();
        let abort_tx = session.abort_tx.clone();
        let upload = tokio::spawn(async move {
            use http_body_util::BodyExt;
            let result: Result<(), ForwardError> = async {
                while let Some(frame) = body.frame().await {
                    let frame = frame.map_err(|_| ForwardError::CallerBody)?;
                    if let Ok(chunk) = frame.into_data() {
                        // DATA must never collide with the empty HALF_END sentinel.
                        if !chunk.is_empty() {
                            upload_tx
                                .send(Frame::Data {
                                    stream: id,
                                    bytes: chunk.to_vec(),
                                })
                                .await
                                .map_err(|_| ForwardError::TetherGone)?;
                        }
                    }
                }
                upload_tx
                    .send(Frame::HalfEnd { stream: id })
                    .await
                    .map_err(|_| ForwardError::TetherGone)
            }
            .await;
            if result.is_err() && !upload_state.completed.load(Ordering::Acquire) {
                upload_state.fail();
                if matches!(
                    upload_tx.try_send(Frame::End {
                        stream: id,
                        reason: b"request upload failed".to_vec(),
                    }),
                    Err(mpsc::error::TrySendError::Full(_))
                ) {
                    let _ = abort_tx.send(true);
                }
            }
        });
        guard.upload = Some(upload.abort_handle());
        state.install_upload(upload.abort_handle());
        drop(upload);
        Ok(Forwarded {
            id,
            state,
            _guard: guard,
        })
    }

    /// Report each registered tether as up, stale, or down.
    pub fn status(&self) -> Vec<(String, String, TetherState)> {
        let by_id = self.by_id.lock().unwrap();
        let live = self.live.lock().unwrap();
        by_id
            .values()
            .map(|t| {
                let state = match live.get(&t.id).filter(|_| t.remaining().is_some()) {
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
            .map(|t| t.remaining().is_some())
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
/// answer. "No tether" and "revoked" remain distinct so an authenticated
/// operator can distinguish absence from a deliberate access decision.
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
    /// The bounded writer queue is full; this tether is not accepting new work.
    TetherBusy,
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
            ForwardError::TetherBusy => "tunnel command queue is full",
            ForwardError::CallerBody => "caller body read failed",
        })
    }
}

impl std::error::Error for ForwardError {}

/// Admit a new stream without waiting behind an already-saturated tether.
///
/// Once OPEN is accepted, request DATA applies ordinary bounded backpressure.
/// Before OPEN, however, waiting would make overload look like a slow model and
/// consume a stream id for work the tether cannot yet see. A full queue is a
/// caller-visible 503; a closed queue is a dead tunnel (502).
fn reserve_stream_start(
    tx: &mpsc::Sender<Frame>,
) -> Result<mpsc::OwnedPermit<Frame>, ForwardError> {
    match tx.clone().try_reserve_owned() {
        Ok(permit) => Ok(permit),
        Err(mpsc::error::TrySendError::Full(_)) => Err(ForwardError::TetherBusy),
        Err(mpsc::error::TrySendError::Closed(_)) => Err(ForwardError::TetherGone),
    }
}

/// A forwarded stream, handed back to the caller-facing frontend.
///
/// No Debug: it holds stream state containing channels, and a debug print of that
/// would tempt someone into logging request bodies.
pub struct StreamGuard {
    upload: Option<tokio::task::AbortHandle>,
    tx: mpsc::Sender<Frame>,
    abort_tx: watch::Sender<bool>,
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
        if let Some(upload) = &self.upload {
            upload.abort();
        }
        self.streams.lock().unwrap().remove(&self.id);
        // A stream that already reached END is finished, not abandoned: sending
        // End here would abort the tether's read for a response that is still
        // arriving. Skip the abort; the deregistration above is enough.
        if self.completed.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        let abort = Frame::End {
            stream: self.id,
            reason: Vec::new(),
        };
        if matches!(
            self.tx.try_send(abort),
            Err(mpsc::error::TrySendError::Full(_))
        ) {
            // Drop cannot await capacity. A full writer queue means the tether is
            // already unable to keep up; end the session so its teardown closes
            // every stream rather than silently leaking this one upstream.
            let _ = self.abort_tx.send(true);
        }
    }
}

/// The caller-facing handle: yields engine response chunks until END.
pub struct Forwarded {
    pub id: u16,
    state: Arc<StreamState>,
    _guard: StreamGuard,
}

impl Forwarded {
    pub(crate) fn failure(&self) -> Arc<StreamFailure> {
        self.state.failure.clone()
    }

    /// Await a real response head or terminal failure. Slow model startup has
    /// no arbitrary one-second cutoff; session loss and caller abandonment still
    /// end the wait. Head polling yields to the session task that writes it.
    pub async fn head(&self) -> Option<http::Response<()>> {
        loop {
            if let Some(res) = self.state.head.lock().unwrap().clone() {
                return Some(res);
            }
            if self.state.completed.load(Ordering::SeqCst) {
                return None;
            }
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
    fn new(method: Method, path: String, headers: HeaderMap) -> Self {
        let (chunk_tx, chunks) = mpsc::channel(64);
        Self {
            method,
            path,
            headers,
            chunk_tx,
            chunks: Arc::new(Mutex::new(Some(chunks))),
            head: Arc::new(Mutex::new(None)),
            completed: Arc::new(AtomicBool::new(false)),
            failure: Arc::new(StreamFailure::default()),
            upload: Mutex::new(None),
        }
    }

    fn install_upload(&self, task: tokio::task::AbortHandle) {
        let mut upload = self.upload.lock().unwrap();
        if self.completed.load(Ordering::Acquire) {
            task.abort();
        } else {
            *upload = Some(task);
        }
    }

    fn finish_upload(&self) {
        self.completed.store(true, Ordering::Release);
        if let Some(task) = self.upload.lock().unwrap().take() {
            task.abort();
        }
    }

    fn complete(&self) {
        self.finish_upload();
        self.failure.complete();
    }

    fn fail(&self) {
        self.finish_upload();
        self.failure.fail();
    }

    /// Handle the session task uses to deliver engine bytes to this stream.
    pub fn chunks_tx(&self) -> mpsc::Sender<ChunkOrEnd> {
        self.chunk_tx.clone()
    }
}

/// Serialize an origin-form request head for OPEN. Hop-by-hop fields are
/// removed here; the tether regenerates Host and HTTP body framing.
fn encode_request_head(method: &Method, path: &str, headers: &HeaderMap) -> Vec<u8> {
    let mut headers = headers.clone();
    crate::headers::strip_hop_by_hop(&mut headers);
    let mut out = format!("{method} {path} HTTP/1.1\r\n").into_bytes();
    for (name, value) in &headers {
        out.extend_from_slice(name.as_str().as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out
}

/// Parse a complete HTTP response head without changing header octets or
/// repeated fields. Returns any remainder separately. Filtering belongs at each
/// forwarding boundary; this parser also serves historical framing fixtures.
pub fn parse_head(bytes: &[u8]) -> Option<(http::Response<()>, Bytes)> {
    let mut headers = [httparse::EMPTY_HEADER; 128];
    let mut parsed = httparse::Response::new(&mut headers);
    let end = match parsed.parse(bytes).ok()? {
        httparse::Status::Complete(end) if end <= MAX_PENDING_HEAD => end,
        _ => return None,
    };
    let mut builder = http::Response::builder().status(parsed.code?);
    for header in parsed.headers.iter() {
        builder = builder.header(header.name, header.value);
    }
    Some((
        builder.body(()).ok()?,
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

/// A tether state change reported to the operator event channel.
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
    // would leak every credential.
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

    let authorization_started = tokio::time::Instant::now();
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
    // Sending WELCOME may block. Its duration must not extend authorization.
    let lease_deadline_at = authorization_started + lease.ttl;

    eprintln!(
        "anvil-ring hub: tether {} authorized from {peer}, lease {}s",
        lease.tether_id,
        lease.ttl.as_secs()
    );
    sink.send(ws_msg(Frame::Welcome {
        lease_secs: lease.ttl.as_secs(),
    }))
    .await?;
    let (tx, mut rx) = mpsc::channel::<Frame>(COMMAND_CHANNEL_CAPACITY);
    let (abort_tx, mut abort_rx) = watch::channel(false);
    let session = registry
        .attach(&lease.tether_id, &credential, tx, abort_tx)
        .ok_or("authorization changed during handshake")?;
    let _ = events.send(TetherEvent {
        tether_id: lease.tether_id.clone(),
        kind: EventKind::Up,
    });
    let mut writer = AbortOnDrop::new(tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            sink.send(ws_msg(frame)).await?;
        }
        Ok::<(), tokio_tungstenite::tungstenite::Error>(())
    }));
    let last_seen = session.last_seen.clone();
    let streams = session.streams.clone();
    // Boxed+pinned sleep, re-armed each pass: `select!` needs to poll it while we
    // still need to move it. A `tick()` future would hold `interval` borrowed.
    let mut tick = Box::pin(tokio::time::sleep(TETHER_TICK));
    let lease_deadline = tokio::time::sleep_until(lease_deadline_at);
    tokio::pin!(lease_deadline);
    let mut last_ping = Instant::now() - TETHER_PING_INTERVAL;
    let mut revoked = false;

    let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = loop {
        tokio::select! {
            _ = &mut lease_deadline => {
                break Err("authorization lease expired".into());
            }
            changed = abort_rx.changed() => {
                if changed.is_ok() && *abort_rx.borrow() {
                    revoked = !registry.is_active(&lease.tether_id);
                    break Err("session cancelled".into());
                }
            }
            _ = tick.as_mut() => {
                // The authorization lifetime is enforced, not merely reported. If the
                // tether was revoked, end the session NOW rather than waiting for
                // a reconnect it has no reason to initiate.
                if !registry.is_active(&lease.tether_id) {
                    revoked = true;
                    break Ok(());
                }
                // Report staleness explicitly rather than letting a lost
                // tether look merely slow.
                let idle = last_seen.lock().unwrap().elapsed();
                if idle > TETHER_SILENCE {
                    eprintln!(
                        "anvil-ring hub: tether {} silent {idle:?}; closing",
                        lease.tether_id
                    );
                    break Err("tether silent past deadline".into());
                }
                if last_ping.elapsed() >= TETHER_PING_INTERVAL {
                    if session.tx.try_send(Frame::Ping).is_err() {
                        break Err("write liveness PING to tether failed".into());
                    }
                    last_ping = Instant::now();
                }
                // Re-arm; an already-complete future would spin the loop at 100% CPU.
                tick
                    .as_mut()
                    .reset(tokio::time::Instant::now() + TETHER_TICK);
            }
            _ = writer.task() => {
                break Err("tether writer closed".into());
            }
            inp = stream.next() => {
                let m = match inp {
                    // Do not use `?` after the session is attached. Returning
                    // directly from this function would skip the teardown below,
                    // leaving every in-flight caller parked on an open channel.
                    Some(Ok(m)) => m,
                    Some(Err(e)) => break Err(e.into()),
                    None => break Err("tether closed".into()),
                };
                let frame = match decode(&m) {
                    Ok(Some(frame)) => frame,
                    Ok(None) => continue,
                    Err(e) => break Err(e),
                };
                if frame_proves_liveness(&frame) {
                    *last_seen.lock().unwrap() = Instant::now();
                }
                match frame {
                    Frame::Ping => {
                        if session.tx.try_send(Frame::Pong).is_err() {
                            break Err("write PONG to tether failed".into());
                        }
                    }
                    Frame::Pong => {}
                    // A client re-authorizing early is normal (its lease watchdog).
                    // Honor it by ending this session; its next HELLO is a fresh
                    // authorization decision.
                    Frame::GoAway { .. } => break Ok(()),
                    // A client must never send OPEN -- only the hub initiates
                    // streams. This authority is enforced at the frame level.
                    Frame::Open { .. } => {
                        break Err("client sent OPEN; only the hub may initiate a request stream".into());
                    }
                    // Only the hub may half-close: a tether signalling
                    // end-of-request would be a tether answering a request it was
                    // never sent. Treated like OPEN, as a protocol violation.
                    Frame::HalfEnd { .. } => {
                        break Err("client sent HALF_END; only the hub may finish a request body".into());
                    }
                    Frame::Hello { .. } | Frame::Welcome { .. } => {
                        break Err("unexpected HELLO/WELCOME mid-session".into());
                    }
                    Frame::RespHead { stream: id, head } => {
                        let target = streams.lock().unwrap().get(&id).cloned();
                        let Some(st) = target else { continue };
                        let parsed = parse_head(&head).filter(|(res, rest)| {
                            res.status().as_u16() >= 200 && rest.is_empty()
                                && st.head.lock().unwrap().is_none()
                        });
                        if let Some((res, _)) = parsed {
                            *st.head.lock().unwrap() = Some(res);
                        } else {
                            st.fail();
                            streams.lock().unwrap().remove(&id);
                            if session.tx.try_send(Frame::End {
                                stream: id, reason: b"invalid response head".to_vec(),
                            }).is_err() { break Err("tether command queue unavailable".into()); }
                        }
                    }
                    Frame::Data { stream: id, bytes } => {
                        let target = streams.lock().unwrap().get(&id).cloned();
                        let accepted = target.as_ref().is_some_and(|st| {
                            st.head.lock().unwrap().is_some()
                                && st.chunk_tx.try_send(ChunkOrEnd::Chunk(Bytes::from(bytes))).is_ok()
                        });
                        if !accepted {
                            if let Some(st) = target { st.fail(); }
                            streams.lock().unwrap().remove(&id);
                            if session.tx.try_send(Frame::End {
                                stream: id, reason: b"caller unavailable or response queue full".to_vec(),
                            }).is_err() { break Err("tether command queue unavailable".into()); }
                        }
                    }
                    Frame::End { stream: id, reason } => {
                        let target = streams.lock().unwrap().get(&id).cloned();
                        if let Some(st) = target {
                            // Mark completed BEFORE the guard can observe the drop,
                            // so a normal end never turns into an abort.
                            if reason.is_empty() {
                                st.complete();
                            } else {
                                st.fail();
                            }
                            streams.lock().unwrap().remove(&id);
                        }
                    }
                }
            }
        }
    };

    drop(writer);
    // The tether is gone. Every stream it was serving must be ended for its
    // caller, or that caller waits forever on a channel whose senders are all
    // still alive -- the session loop is this task, so nothing else can wake it.
    //
    // Measured before this existed: after `tether.abort()`, the caller read 797
    // bytes and then hung for the full 15s timeout. `StreamGuard::drop` cannot
    // cover this, because it only runs when the CALLER goes away; this is the
    // upstream dying, which requires session-level cleanup.
    //
    // Failure is out-of-band: a full queue cannot hide it or stall teardown.
    // A successful HTTP terminator would disguise incomplete model output.
    session.close();

    registry.detach(&lease.tether_id, &session);
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

/// A PONG is proof that the tether's central session loop received a hub PING
/// and scheduled a response. DATA alone may come from an already-spawned engine
/// pump, so treating it as liveness can hide a control loop that is wedged and no
/// longer able to accept or cancel streams.
fn frame_proves_liveness(frame: &Frame) -> bool {
    matches!(frame, Frame::Pong)
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
        // Only Ping/Pong are ignorable: they are keepalives with no bearing on a
        // stream's fate.
        //
        // `Text` and `Frame` must NOT be ignored. A peer that dies without a close
        // handshake, including a TCP reset, surfaces here. This arm used to return
        // Ok(None) for
        // it -- the loop then `continue`d and called `next()` on a dead socket,
        // spinning without ever ending its streams. Measured: 75 iterations, then
        // the caller hung for the full 15s timeout with the tether already gone.
        // Treating anything unexpected as terminal prevents a silently wedged
        // session.
        Message::Ping(_) | Message::Pong(_) => Ok(None),
        Message::Text(_) => Err("peer sent Text; tunnel frames are binary".into()),
        Message::Close(_) => Err("peer sent Close".into()),
        Message::Frame(_) => Err("peer sent a raw frame we cannot classify".into()),
    }
}

/// SHA-256 hex digest of a credential, used ONLY as a lookup key.
///
/// Read the security note before changing this. It buys exactly one property:
/// a hub config file, crash dump, or log line does not contain live credentials.
/// It is NOT a password hash—unsalted and fast. That is acceptable only
/// because registration credentials are high-entropy random tokens, so there is no
/// offline-guessing surface. If credentials ever become human-chosen, this must
/// become a password key-derivation function such as Argon2 or bcrypt;
/// `high_entropy_note_is_still_true` below is a reminder, not proof.
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

    fn record(id: &str, credential: &str, expires_at: u64) -> crate::admin::Record {
        crate::admin::Record {
            id: id.into(),
            label: "test".into(),
            credential_hash: digest(credential.as_bytes()),
            active: true,
            expires_at,
        }
    }

    #[test]
    fn durable_snapshot_changes_cancel_only_affected_sessions() {
        let registry = Registry::new(DEFAULT_LEASE);
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        registry.replace_records(vec![
            record("a", "first", future),
            record("b", "other", future),
        ]);
        let (tx, _rx) = mpsc::channel(2);
        let (abort, a_cancelled) = watch::channel(false);
        registry.attach("a", b"first", tx.clone(), abort).unwrap();
        let (abort, b_cancelled) = watch::channel(false);
        registry.attach("b", b"other", tx, abort).unwrap();
        registry.replace_records(vec![
            record("a", "first", future),
            record("b", "other", future),
        ]);
        assert!(!*a_cancelled.borrow());
        assert!(!*b_cancelled.borrow());
        registry.replace_records(vec![
            record("a", "rotated", future),
            record("b", "other", future),
        ]);
        assert!(*a_cancelled.borrow());
        assert!(!*b_cancelled.borrow());
        assert!(registry.authorize(b"first").is_none());
        assert!(registry.authorize(b"rotated").is_some());
        registry.clear_records();
        assert!(*b_cancelled.borrow());
        assert!(registry.authorize(b"other").is_none());
    }

    #[test]
    fn credential_expiry_bounds_authorization_and_prevents_attachment() {
        let registry = Registry::new(DEFAULT_LEASE);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        registry.replace_records(vec![record("a", "first", now + 10)]);
        let lease = registry.authorize(b"first").unwrap();
        assert!(lease.ttl <= Duration::from_secs(10));
        assert!(lease.ttl > Duration::from_secs(8));
        registry.replace_records(vec![record("a", "first", now)]);
        assert!(registry.authorize(b"first").is_none());
        assert!(!registry.is_active("a"));
        let (tx, _rx) = mpsc::channel(2);
        let (abort, _cancelled) = watch::channel(false);
        assert!(registry.attach("a", b"first", tx, abort).is_none());
    }

    #[test]
    fn routing_is_stable_and_skips_full_or_less_available_sessions() {
        let registry = Registry::new(DEFAULT_LEASE);
        registry.register("b", "second", "b-token");
        registry.register("a", "first", "a-token");
        let (a_tx, _a_rx) = mpsc::channel(1);
        let (b_tx, _b_rx) = mpsc::channel(1);
        let (abort, _cancelled) = watch::channel(false);
        let a = registry
            .attach("a", b"a-token", a_tx, abort.clone())
            .unwrap();
        let b = registry.attach("b", b"b-token", b_tx, abort).unwrap();
        let (id, _, reserved) = registry.reserve_route().unwrap();
        assert_eq!(id, "a", "lexical order breaks equal-load ties");
        assert_eq!(
            registry.reserve_route().unwrap().0,
            "b",
            "reserved OPEN capacity cannot be double-spent"
        );
        drop(reserved);
        a.admit(
            1,
            Arc::new(StreamState::new(Method::GET, "/".into(), HeaderMap::new())),
        )
        .unwrap();
        assert_eq!(
            registry.reserve_route().unwrap().0,
            "b",
            "prefer fewer in-flight responses"
        );
        b.tx.try_send(Frame::Ping).unwrap();
        assert_eq!(
            registry.reserve_route().unwrap().0,
            "a",
            "skip a saturated command queue"
        );
        a.tx.try_send(Frame::Ping).unwrap();
        assert!(matches!(
            registry.reserve_route(),
            Err(ForwardError::TetherBusy)
        ));
        registry.revoke("a");
        registry.revoke("b");
        assert!(matches!(
            registry.reserve_route(),
            Err(ForwardError::NoTether)
        ));
    }

    #[test]
    fn routing_excludes_expired_stale_and_stream_saturated_sessions() {
        let registry = Registry::new(DEFAULT_LEASE);
        registry.register("a", "first", "token");
        let (tx, _rx) = mpsc::channel(2);
        let (abort, _cancelled) = watch::channel(false);
        let a = registry.attach("a", b"token", tx, abort).unwrap();
        for id in 1..=crate::tunnel::MAX_CONCURRENT_STREAMS {
            a.admit(
                id as u16,
                Arc::new(StreamState::new(Method::GET, "/".into(), HeaderMap::new())),
            )
            .unwrap();
        }
        assert!(matches!(
            registry.reserve_route(),
            Err(ForwardError::TetherBusy)
        ));
        a.streams.lock().unwrap().clear();
        *a.last_seen.lock().unwrap() = Instant::now() - TETHER_SILENCE - Duration::from_secs(1);
        assert!(matches!(
            registry.reserve_route(),
            Err(ForwardError::NoTether)
        ));
        *a.last_seen.lock().unwrap() = Instant::now();
        registry
            .by_id
            .lock()
            .unwrap()
            .get_mut("a")
            .unwrap()
            .expires_at = Some(1);
        assert!(matches!(
            registry.reserve_route(),
            Err(ForwardError::NoTether)
        ));
    }

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
            "a revoked token must stop working at once"
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
            "repeated revoke must not resurrect the credential"
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
        assert_eq!(s[0].2, TetherState::Down, "absence must be explicit");
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

    #[test]
    fn replacing_registration_invalidates_the_previous_credential() {
        let reg = Registry::new(DEFAULT_LEASE);
        reg.register("r1", "rental", "old-secret");
        reg.register("r1", "replacement", "new-secret");
        assert!(reg.authorize(b"old-secret").is_none());
        assert_eq!(reg.authorize(b"new-secret").unwrap().tether_id, "r1");
    }

    #[tokio::test]
    async fn a_revoked_registration_cannot_attach_after_authorization() {
        let reg = Registry::new(DEFAULT_LEASE);
        reg.register("r1", "rental", "secret");
        assert!(reg.authorize(b"secret").is_some());
        reg.revoke("r1");
        let (tx, _rx) = mpsc::channel(1);
        let (abort_tx, _abort_rx) = watch::channel(false);
        assert!(reg.attach("r1", b"secret", tx, abort_tx).is_none());
        assert!(matches!(reg.status()[0].2, TetherState::Down));
    }

    #[tokio::test]
    async fn replacing_registration_cancels_the_previous_session() {
        let reg = Registry::new(DEFAULT_LEASE);
        reg.register("r1", "rental", "old-secret");
        let (tx, _rx) = mpsc::channel(1);
        let (abort_tx, mut abort_rx) = watch::channel(false);
        reg.attach("r1", b"old-secret", tx, abort_tx).unwrap();
        reg.register("r1", "replacement", "new-secret");
        tokio::time::timeout(Duration::from_millis(100), abort_rx.changed())
            .await
            .expect("rotation must cancel the old session")
            .unwrap();
        assert!(*abort_rx.borrow());
        assert!(matches!(reg.status()[0].2, TetherState::Down));
    }

    #[test]
    fn forwarded_request_removes_connection_named_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("connection", "x-local".parse().unwrap());
        headers.insert("x-local", "private".parse().unwrap());
        headers.insert("x-request-id", "trace-123".parse().unwrap());
        let head = String::from_utf8(encode_request_head(&Method::GET, "/", &headers)).unwrap();
        assert!(!head.contains("x-local"));
        assert!(head.contains("x-request-id: trace-123\r\n"));
    }

    #[tokio::test]
    async fn command_queue_applies_backpressure() {
        let reg = Registry::new(DEFAULT_LEASE);
        let (tx, _rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        let (abort_tx, _abort_rx) = watch::channel(false);
        reg.register("r1", "queue test", "secret");
        let session = reg.attach("r1", b"secret", tx, abort_tx).unwrap();

        for _ in 0..COMMAND_CHANNEL_CAPACITY {
            session
                .tx
                .try_send(Frame::Ping)
                .expect("queue should accept frames up to its bound");
        }
        assert!(matches!(
            session.tx.try_send(Frame::Ping),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        assert!(matches!(
            reserve_stream_start(&session.tx),
            Err(ForwardError::TetherBusy)
        ));
    }

    #[tokio::test]
    async fn a_full_response_queue_does_not_block_session_control() {
        let reg = Arc::new(Registry::new(DEFAULT_LEASE));
        reg.register("r1", "test", "secret");
        let address = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let _events = serve(address, reg.clone()).await.unwrap();
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{address}/ring"))
            .await
            .unwrap();
        ws.send(ws_msg(Frame::Hello {
            credential: b"secret".to_vec(),
        }))
        .await
        .unwrap();
        ws.next().await.unwrap().unwrap();
        while reg.live.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        let (chunk_tx, chunks) = mpsc::channel(2);
        let state = Arc::new(StreamState {
            method: Method::GET,
            path: "/".into(),
            headers: HeaderMap::new(),
            chunk_tx,
            chunks: Arc::new(Mutex::new(Some(chunks))),
            head: Arc::new(Mutex::new(None)),
            completed: Arc::new(AtomicBool::new(false)),
            failure: Arc::new(StreamFailure::default()),
            upload: Mutex::new(None),
        });
        reg.live
            .lock()
            .unwrap()
            .get("r1")
            .unwrap()
            .streams
            .lock()
            .unwrap()
            .insert(1, state.clone());
        ws.send(ws_msg(Frame::RespHead {
            stream: 1,
            head: b"HTTP/1.1 200 OK\r\n\r\n".to_vec(),
        }))
        .await
        .unwrap();
        for _ in 0..3 {
            ws.send(ws_msg(Frame::Data {
                stream: 1,
                bytes: b"chunk".to_vec(),
            }))
            .await
            .unwrap();
        }
        ws.send(ws_msg(Frame::Ping)).await.unwrap();
        tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                if matches!(
                    decode(&ws.next().await.unwrap().unwrap()).unwrap(),
                    Some(Frame::Pong)
                ) {
                    break;
                }
            }
        })
        .await
        .expect("a stalled caller must not block control messages");
        assert!(state.completed.load(Ordering::Acquire));
        assert!(reg.is_active("r1"));
    }

    #[tokio::test]
    async fn closed_sessions_reject_admission_and_fail_existing_streams() {
        let reg = Registry::new(DEFAULT_LEASE);
        reg.register("r1", "test", "secret");
        let (tx, _rx) = mpsc::channel(2);
        let (abort_tx, _abort_rx) = watch::channel(false);
        let session = reg.attach("r1", b"secret", tx, abort_tx).unwrap();
        let state = Arc::new(StreamState::new(Method::GET, "/".into(), HeaderMap::new()));
        session.admit(1, state.clone()).unwrap();
        session.close();
        assert!(state.completed.load(Ordering::Acquire));
        assert!(matches!(
            session.admit(2, state),
            Err(ForwardError::TetherGone)
        ));
        assert!(session.streams.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_terminal_response_cancels_upload_even_if_the_body_is_retained() {
        let state = StreamState::new(Method::POST, "/".into(), HeaderMap::new());
        let task = tokio::spawn(std::future::pending::<()>());
        state.install_upload(task.abort_handle());
        state.complete();
        assert!(task.await.unwrap_err().is_cancelled());
        // A response can finish before its upload task is installed.
        let late = tokio::spawn(std::future::pending::<()>());
        state.install_upload(late.abort_handle());
        assert!(late.await.unwrap_err().is_cancelled());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn final_chunk_is_drained_when_completion_races_the_consumer() {
        for _ in 0..10_000 {
            let state = Arc::new(StreamState::new(Method::GET, "/".into(), HeaderMap::new()));
            let rx = state.chunks.lock().unwrap().take().unwrap();
            let mut body = crate::frontend::TunnelBody::Live(
                Arc::new(Mutex::new(Some(rx))),
                Arc::new(()),
                state.failure.clone(),
            );
            let producer = tokio::spawn(async move {
                state
                    .chunk_tx
                    .send(ChunkOrEnd::Chunk(Bytes::from_static(b"last")))
                    .await
                    .unwrap();
                state.complete();
            });
            assert_eq!(
                body.next()
                    .await
                    .expect("last chunk must not disappear")
                    .unwrap(),
                "last"
            );
            assert!(body.next().await.is_none());
            producer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn completed_body_drains_after_cooperative_budget_is_exhausted() {
        tokio::spawn(async {
            let state = StreamState::new(Method::GET, "/".into(), HeaderMap::new());
            state
                .chunk_tx
                .try_send(ChunkOrEnd::Chunk(Bytes::from_static(b"last")))
                .unwrap();
            state.complete();
            let rx = state.chunks.lock().unwrap().take().unwrap();
            let mut body = crate::frontend::TunnelBody::Live(
                Arc::new(Mutex::new(Some(rx))),
                Arc::new(()),
                state.failure.clone(),
            );
            // Ready receives consume Tokio's per-poll cooperative budget. The
            // next receive can be Pending despite the queued response chunk.
            let (tx, mut budget) = mpsc::channel(128);
            for _ in 0..128 {
                tx.try_send(()).unwrap();
            }
            for _ in 0..128 {
                budget.recv().await.unwrap();
            }
            assert_eq!(body.next().await.unwrap().unwrap(), "last");
            assert!(body.next().await.is_none());
        })
        .await
        .unwrap();
    }

    #[test]
    fn only_a_pong_proves_bidirectional_hub_liveness() {
        assert!(frame_proves_liveness(&Frame::Pong));
        assert!(!frame_proves_liveness(&Frame::Ping));
        assert!(!frame_proves_liveness(&Frame::Data {
            stream: 7,
            bytes: b"still streaming".to_vec(),
        }));
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
mod raw_parse_head_contract_tests {
    use super::*;

    /// `parse_head` is deliberately framing-agnostic: it returns the bytes after
    /// the header block exactly. The live tether consumes chunked coding before
    /// it sends `RespHead` + `Data`, so this raw shape never crosses the runtime
    /// hub-to-caller boundary. Pinning the primitive keeps that ownership clear.
    #[test]
    fn raw_parser_preserves_the_body_for_the_tether_decoder() {
        let wire = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\na\r\ndata: one\n\r\n0\r\n\r\n";
        let (res, rest) = parse_head(wire).expect("parses");
        assert_eq!(
            rest.as_ref(),
            b"a\r\ndata: one\n\r\n0\r\n\r\n",
            "the raw parser must not mutate body bytes"
        );
        let mut d = crate::chunked::ChunkedDecoder::new();
        let out = d.push(&rest).expect("decodes").out;
        assert_eq!(out, b"data: one\n");
        assert!(crate::chunked::is_chunked(res.headers()));
    }
}

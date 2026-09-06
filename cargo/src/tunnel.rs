//! The outbound tunnel: an authenticated, self-healing WSS connection from the
//! disposable host to the hub, carrying multiplexed proxied streams.
//!
//! Security and lifecycle responsibilities:
//!
//!  - This module only dials out. It has no listener or inbound accept path.
//!  - The hub returns an authorization lifetime in `WELCOME`; the tether
//!    reconnects and reauthorizes at 75% of that period, so revocation applies to
//!    an idle tunnel as well as an active one.
//!  - `PING` and `PONG` messages distinguish a disconnected peer from a slow
//!    model and end a half-open session after the stated timeout.
//!  - Credentials come from a file or environment variable and never appear in
//!    log output or process arguments.
//!  - `forward_engine` checks the loopback-only upstream for every stream, not
//!    only at process startup.
//!
//! Transport (settling ADR-0002): WSS, normally over TCP 443, with HTTP carried
//! through the owned tunnel. Provider egress qualification still matters, but
//! Chisel and `ssh -R` are no longer live transport choices.

use crate::frames::Frame;
use crate::tasks::AbortOnDrop;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;

/// Streams one tether serves at once. Bounded so a hub bug or a flood cannot
/// exhaust the rental's file descriptors.
pub const MAX_CONCURRENT_STREAMS: usize = 64;

/// Request-body frames waiting for one engine connection. A slow engine may not
/// turn an arbitrarily large caller upload into rental-host memory growth.
pub const STREAM_INPUT_CAPACITY: usize = 64;

/// Frames waiting for the single WebSocket sink. This completes backpressure in
/// the response direction: a slow hub eventually pauses engine reads instead of
/// letting per-stream pumps grow an unbounded tether-side queue.
pub const WS_WRITER_CAPACITY: usize = 256;

/// Heartbeat cadence and the point at which a peer is declared disconnected.
/// Deliberately wider than one interval: a single late pong on a congested link
/// must not tear down a serving endpoint.
pub const PING_INTERVAL: Duration = Duration::from_secs(10);
pub const PING_TIMEOUT: Duration = Duration::from_secs(25);

const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

type Wss = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsMsg = tokio_tungstenite::tungstenite::Message;

/// Owns one engine pump. Removing the stream from the session map cancels the
/// task immediately; dropping a Tokio `JoinHandle` alone would detach it and let
/// a cancelled caller keep consuming engine and socket resources.
struct StreamHandle {
    tx: mpsc::Sender<Vec<u8>>,
    abort: tokio::task::AbortHandle,
}

struct PumpCompletions {
    tx: mpsc::UnboundedSender<u16>,
    rx: mpsc::UnboundedReceiver<u16>,
}

impl PumpCompletions {
    fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self { tx, rx }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamInputResult {
    Queued,
    Missing,
    Overloaded,
}

fn enqueue_stream_input(
    streams: &mut HashMap<u16, StreamHandle>,
    id: u16,
    bytes: Vec<u8>,
) -> StreamInputResult {
    let result = match streams.get(&id) {
        Some(stream) => stream.tx.try_send(bytes),
        None => return StreamInputResult::Missing,
    };
    match result {
        Ok(()) => StreamInputResult::Queued,
        Err(mpsc::error::TrySendError::Full(_)) => {
            // Removing the handle aborts the engine pump. Keep the multiplexed
            // session alive; only this stream exceeded its bounded input budget.
            streams.remove(&id);
            StreamInputResult::Overloaded
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            // Hyper may have finished the request while still reading the
            // response. Only completion notification or END cancels that pump.
            StreamInputResult::Queued
        }
    }
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// State the proxy consults before accepting a caller.
#[derive(Debug, Default)]
pub struct TunnelState {
    /// True only between WELCOME and teardown. Requests in a reconnect window are
    /// REFUSED, not queued — queueing hides the outage and turns a dead tether
    /// into a slow one, which would hide an outage as model latency.
    pub up: AtomicBool,
    /// Tunnels established, for status output.
    pub generations: AtomicU64,
    /// Lease seconds from the last WELCOME. 0 means never authorized.
    pub lease_secs: AtomicU64,
}

impl TunnelState {
    /// Is the tunnel currently authorized and usable?
    ///
    /// Named rather than reading `up` directly at call sites: "can I route through
    /// this tether right now" is the question callers actually have, and it is
    /// false across every reconnect window.
    pub fn is_up(&self) -> bool {
        self.up.load(Ordering::Acquire)
    }
}

pub struct ClientConfig {
    pub hub_url: String,
    pub credential: Vec<u8>,
    pub state: Arc<TunnelState>,
}

impl ClientConfig {
    /// Resolve the credential from a file (preferred) or an env var.
    ///
    /// Not a command-line option: process arguments are visible in `ps` on a
    /// shared rental host and may persist in shell history.
    pub fn credential_from_env() -> io::Result<Vec<u8>> {
        if let Ok(path) = std::env::var("ANVIL_RING_CRED_FILE") {
            return Ok(trim_cred(std::fs::read(&path)?));
        }
        if let Ok(v) = std::env::var("ANVIL_RING_CREDENTIAL") {
            return Ok(trim_cred(v.into_bytes()));
        }
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "no credential: set ANVIL_RING_CRED_FILE (preferred) or ANVIL_RING_CREDENTIAL. \
             Refusing to start unauthenticated or read a secret from process arguments.",
        ))
    }
}

fn trim_cred(mut v: Vec<u8>) -> Vec<u8> {
    while matches!(v.last(), Some(b'\n' | b'\r' | b' ' | b'\t')) {
        v.pop();
    }
    v
}

fn log(msg: &str) {
    // Never interpolate a credential into log output.
    eprintln!("anvil-ring: {msg}");
}

/// Dial out, serve, and reconnect forever. Only returns on a fatal config error.
pub async fn run_client(cfg: ClientConfig, upstream: String) -> io::Result<()> {
    let mut backoff = BACKOFF_MIN;
    loop {
        match dial(&cfg.hub_url).await {
            Ok(ws) => match serve_over(ws, &cfg, &upstream).await {
                Ok(()) => log("tunnel closed by hub"),
                Err(e) => log(&format!("tunnel error: {e}")),
            },
            Err(e) => log(&format!("dial failed: {e}")),
        }
        cfg.state.up.store(false, Ordering::SeqCst);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

async fn dial(hub_url: &str) -> Result<Wss, Box<dyn std::error::Error + Send + Sync>> {
    let uri: http::Uri = hub_url.parse().map_err(|_| "invalid hub URL")?;
    if !matches!(uri.scheme_str(), Some("ws" | "wss")) {
        return Err("hub URL must use wss (ws only for literal loopback)".into());
    }
    if uri.authority().is_none_or(|a| a.as_str().contains('@'))
        || uri.query().is_some()
        || hub_url.contains('#')
    {
        return Err("hub URL must not contain credentials, query parameters, or fragments".into());
    }
    if uri.scheme_str() == Some("ws") && !url_is_loopback(hub_url) {
        return Err("plaintext ws requires a literal loopback address; use wss".into());
    }
    let (ws, _resp) =
        tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(hub_url))
            .await
            .map_err(|_| "dial timed out")??;
    Ok(ws)
}

fn url_is_loopback(url: &str) -> bool {
    url.parse::<http::Uri>()
        .ok()
        .and_then(|uri| {
            uri.host()?
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .ok()
        })
        .is_some_and(|ip| ip.is_loopback())
}

/// Run one tunnel session over an established WebSocket.
pub async fn serve_over(
    ws: Wss,
    cfg: &ClientConfig,
    upstream: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // All writes go through one channel so per-stream pump tasks can emit DATA
    // frames without contending for the socket. A single writer preserves
    // WebSocket message ordering.
    let (mut sink, mut stream) = ws.split();
    let (tx, mut rx) = mpsc::channel::<WsMsg>(WS_WRITER_CAPACITY);
    let writer = AbortOnDrop::new(tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            if sink.send(m).await.is_err() {
                break;
            }
        }
    }));

    // Authenticate before serving anything. The hub owns this decision, so the
    // tether does not act on a self-description of its own permissions.
    tx.send(msg(Frame::Hello {
        credential: cfg.credential.clone(),
    }))
    .await?;

    let lease = loop {
        let m = tokio::time::timeout(PING_TIMEOUT, stream.next())
            .await
            .map_err(|_| "hub silent before WELCOME")?
            .ok_or("closed before WELCOME")??;
        match decode(&m)? {
            Some(Frame::Welcome { lease_secs }) => break lease_secs,
            Some(other) => {
                return Err(format!("expected WELCOME, got 0x{:02x}", other.type_tag()).into())
            }
            None => continue,
        }
    };
    cfg.state.lease_secs.store(lease, Ordering::SeqCst);
    cfg.state.up.store(true, Ordering::SeqCst);
    let gen = cfg.state.generations.fetch_add(1, Ordering::SeqCst) + 1;
    log(&format!("tunnel #{gen} authorized; lease {lease}s"));

    let lease_secs = if lease == 0 { 60 } else { lease };
    let mut lease_tick = Box::pin(tokio::time::sleep(
        Duration::from_secs(lease_secs * 3 / 4).max(Duration::from_secs(5)),
    ));
    let mut hb = Heartbeat::new();
    // Per-stream inbound queues plus an abort handle. Removing a StreamHandle
    // cancels its engine pump, which is how caller END and session teardown free
    // the upstream connection promptly.
    let mut streams: HashMap<u16, StreamHandle> = HashMap::new();
    // A completed pump must release its MAX_CONCURRENT_STREAMS slot even when the
    // hub has no reason to send another frame for that stream. At most 64 pumps
    // can exist, so this completion queue is intrinsically bounded by the map.
    let mut completions = PumpCompletions::new();

    let result = run_session(
        &mut stream,
        &tx,
        &mut lease_tick,
        &mut hb,
        &mut streams,
        &mut completions,
        upstream,
    )
    .await;

    streams.clear();
    drop(tx);
    drop(writer);
    result
}

async fn run_session(
    stream: &mut futures_util::stream::SplitStream<Wss>,
    tx: &mpsc::Sender<WsMsg>,
    lease_tick: &mut Pin<Box<tokio::time::Sleep>>,
    hb: &mut Heartbeat,
    streams: &mut HashMap<u16, StreamHandle>,
    completions: &mut PumpCompletions,
    upstream: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        tokio::select! {
            completed = completions.rx.recv() => {
                if let Some(id) = completed {
                    streams.remove(&id);
                }
            }
            _ = lease_tick.as_mut() => {
                                        log("lease window elapsed; reconnecting to re-authorize");
                let _ = tx
                    .try_send(msg(Frame::GoAway { reason: b"lease refresh".to_vec() }));
                return Ok(());
            }
            _ = hb.ping_tick.as_mut() => {
                tx.try_send(msg(Frame::Ping))?;
                // RE-ARM. Without this the interval fires exactly once and the
                // tunnel goes silent for the rest of its life, because the ONLY
                // other place that rearms these timers is `hb.reset()` in the
                // inbound arm -- which requires an INBOUND frame, and an idle
                // tunnel receives none.
                //
                // Measured, before this line existed: the hub tore the session
                // down on a timer three times running --
                //     Up(6.226s)  Up(6.213s)  Up(6.203s)
                // consistent to 20ms, which is a watchdog, not data loss -- and
                // the tether reconnected each time. Every frame still reached
                // `sink.send` with Ok, because writes to a socket whose peer just
                // stopped READING still buffer normally; the reset only surfaces
                // once the buffers fill. A streaming response longer than the
                // silence window therefore loses everything after its first event.
                hb.ping_tick
                    .as_mut()
                    .reset(tokio::time::Instant::now() + PING_INTERVAL);
            }
            _ = hb.dead.as_mut() => {
                                        return Err("heartbeat timeout: the hub did not answer before the liveness deadline".into());
            }
            m = stream.next() => {
                let m = match m {
                    Some(m) => m?,
                    None => return Err("hub closed connection".into()),
                };
                let Some(frame) = decode(&m)? else { continue };
                match frame {
                    Frame::Ping => { tx.try_send(msg(Frame::Pong))?; }
                    Frame::Pong => {
                        hb.mark_alive();
                        hb.pong_deadline = None;
                    }
                    // The hub never sends a response head; only the tether
                    // produces one. A hub doing so is a protocol violation,
                    // mirroring the hub-side rule that a client may not send
                    // an OPEN request frame.
                    Frame::RespHead { .. } => {
                        break Err(
                            "hub sent RESP_HEAD; only the tether answers with a head".into(),
                        );
                    }
                    Frame::GoAway { reason } => {
                        log(&format!("hub GOAWAY: {}", String::from_utf8_lossy(&reason)));
                        return Ok(());
                    }
                    Frame::Open { stream: id, head } => {
                        if streams.len() >= MAX_CONCURRENT_STREAMS {
                            tx.try_send(msg(Frame::End { stream: id, reason: b"overloaded".to_vec() }))?;
                            continue;
                        }
                        let (to_stream, from_hub) =
                            mpsc::channel::<Vec<u8>>(STREAM_INPUT_CAPACITY);
                        let reply = tx.clone();
                        let completed = completions.tx.clone();
                        let upstream = upstream.to_owned();
                        let pump = tokio::spawn(async move {
                            let result = forward_engine(head, &upstream, from_hub, &reply, id).await;
                            let reason = if result.is_ok() {
                                Vec::new()
                            } else {
                                // Do not expose upstream data or credentials in an error.
                                b"engine request or response failed".to_vec()
                            };
                            let _ = reply.send(msg(Frame::End { stream: id, reason })).await;
                            let _ = completed.send(id);
                        });
                        let handle = StreamHandle {
                            tx: to_stream,
                            abort: pump.abort_handle(),
                        };
                        streams.insert(id, handle);
                        // StreamHandle owns the cancellation capability;
                        // dropping this JoinHandle only detaches the task.
                        drop(pump);
                    }
                    Frame::Data { stream: id, bytes } => {
                        if enqueue_stream_input(streams, id, bytes)
                            == StreamInputResult::Overloaded
                        {
                            tx.try_send(msg(Frame::End {
                                    stream: id,
                                    reason: b"request body exceeded tether backpressure"
                                        .to_vec(),
                                }))?;
                        }
                        // Unknown stream: dropped, not materialized. Either a late
                        // chunk after our END or a peer bug; neither warrants
                        // inventing state.
                    }
                    Frame::HalfEnd { stream: id } => {
                        // Empty input finishes the HTTP request body; its response
                        // pump remains alive until completion or explicit abort.
                        if enqueue_stream_input(streams, id, Vec::new())
                            == StreamInputResult::Overloaded
                        {
                            tx.try_send(msg(Frame::End {
                                    stream: id,
                                    reason: b"request body exceeded tether backpressure"
                                        .to_vec(),
                                }))?;
                        }
                    }
                    Frame::End { stream: id, .. } => {
                        streams.remove(&id);
                    }
                    Frame::Hello { .. } | Frame::Welcome { .. } => {
                        return Err("unexpected HELLO/WELCOME mid-session".into());
                    }
                }
                // Also re-arm on ANY inbound frame, not just PONG: traffic is
                // proof of life, and insisting on a pong during a busy stream
                // would tear down a healthy tunnel.
                //
                // NOTE the `continue` above (control frames decode to None) skips
                // this line, so a tunnel whose peer answers only with WS-level
                // pings would still be rearmed by ping_tick's own arm -- but not
                // the reverse. Kept here so real traffic always counts.
                hb.reset();
            }
        }
    }
}

/// One engine HTTP exchange. Hyper owns HTTP framing on this hop, including
/// fragmented heads, informational responses, content lengths, and premature EOF.
/// Request and response bodies remain streamed through bounded channels.
async fn forward_engine(
    head: Vec<u8>,
    upstream: &str,
    from_hub: mpsc::Receiver<Vec<u8>>,
    reply: &mpsc::Sender<WsMsg>,
    id: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use bytes::Bytes;
    use http_body_util::{BodyExt, StreamBody};

    let addr = crate::proxy::loopback_authority(upstream, 80)?;
    let mut raw_headers = [httparse::EMPTY_HEADER; 128];
    let mut parsed = httparse::Request::new(&mut raw_headers);
    if !parsed.parse(&head)?.is_complete() {
        return Err("incomplete request head".into());
    }
    let mut request = http::Request::builder()
        .method(parsed.method.ok_or("missing method")?)
        .uri(parsed.path.ok_or("missing path")?);
    for header in parsed.headers.iter() {
        request = request.header(header.name, header.value);
    }
    let headers = request.headers_mut().ok_or("invalid request headers")?;
    crate::headers::strip_hop_by_hop(headers);
    headers.insert(http::header::HOST, addr.to_string().parse()?);
    // Continue is negotiated on the caller hop. The hub has already accepted
    // the upload, so asking the engine to gate it a second time is unnecessary.
    headers.remove(http::header::EXPECT);
    if !headers.contains_key(http::header::CONTENT_LENGTH) {
        // Regenerate framing explicitly, including GET requests with bodies.
        headers.insert(
            http::header::TRANSFER_ENCODING,
            http::HeaderValue::from_static("chunked"),
        );
    }
    let body = futures_util::stream::unfold(Some(from_hub), |state| async move {
        let mut rx = state?;
        match rx.recv().await {
            Some(bytes) if bytes.is_empty() => None, // HALF_END
            Some(bytes) => Some((
                Ok::<_, io::Error>(http_body::Frame::data(Bytes::from(bytes))),
                Some(rx),
            )),
            None => Some((
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "request ended without HALF_END",
                )),
                None,
            )),
        }
    });
    let request = request.body(StreamBody::new(Box::pin(body)))?;
    let tcp = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await??;
    tcp.set_nodelay(true)?;
    let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
        .max_buf_size(crate::hub::MAX_PENDING_HEAD)
        .handshake(hyper_util::rt::TokioIo::new(tcp))
        .await?;
    let _connection = AbortOnDrop::new(tokio::spawn(connection));
    let response = sender.send_request(request).await?;
    let (mut parts, mut body) = response.into_parts();
    for value in parts.headers.get_all(http::header::TRANSFER_ENCODING) {
        if value
            .to_str()?
            .split(',')
            .any(|coding| !coding.trim().eq_ignore_ascii_case("chunked"))
        {
            return Err("unsupported upstream transfer coding".into());
        }
    }
    crate::headers::strip_hop_by_hop(&mut parts.headers);
    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        parts.status.as_u16(),
        parts.status.canonical_reason().unwrap_or("")
    )
    .into_bytes();
    for (name, value) in &parts.headers {
        head.extend_from_slice(name.as_str().as_bytes());
        head.extend_from_slice(b": ");
        head.extend_from_slice(value.as_bytes());
        head.extend_from_slice(b"\r\n");
    }
    head.extend_from_slice(b"\r\n");
    reply
        .send(msg(Frame::RespHead { stream: id, head }))
        .await?;
    while let Some(frame) = body.frame().await {
        if let Ok(bytes) = frame?.into_data() {
            if !bytes.is_empty() {
                reply
                    .send(msg(Frame::Data {
                        stream: id,
                        bytes: bytes.to_vec(),
                    }))
                    .await?;
            }
        }
    }
    Ok(())
}

/// Historical framing helper, retained for characterization tests. The runtime
/// engine path uses Hyper instead.
/// Rewrite an engine response head for forwarding through the tunnel, removing
/// `transfer-encoding`.
///
/// The tether DE-CHUNKS the body before it crosses the tunnel, so that header
/// must not survive: it would describe framing that no longer exists downstream,
/// and the hub's caller-facing hyper would apply chunk framing a SECOND time to
/// already-bare bytes. That double framing is what produced a caller-visible body
/// with chunk-size markers and a terminator in it.
///
/// Hop-by-hop per RFC 9110 is not "is this header hop-by-hop" but "does THIS hop
/// change the body's framing": the hop that changes it must not forward the
/// header, and must not honour one it received.
///
/// Only a value that is exactly `chunked` (optionally repeated) is removed. A
/// list like `gzip, chunked` is left ALONE: we consumed the chunking but not the
/// gzip, and deleting the header would then claim an unencoded body we did not
/// produce. Passing it through un-understood is the conservative answer.
pub fn reframe_head_for_tunnel(head: &[u8]) -> Vec<u8> {
    let Some(end) = crate::hub::find_header_end(head) else {
        return head.to_vec();
    };
    // A bare-LF terminator (`\n\n`) is a different shape than CRLF-CRLF and this
    // function re-emits fields as CRLF lines; rewriting LF-delimited fields to
    // CRLF could split a line whose value legitimately contains a bare LF. So a
    // bare-LF head is passed through UNCHANGED -- the conservative answer -- and
    // the caller's own hop-by-hop handling applies.
    //
    // This is also a correctness fix, not just a style choice: the block slice
    // below is `end - 4`, and `find_header_end` returns `end = i + 2` for `\n\n`.
    // A short bare-LF head (end < 4) underflowed and panicked the tether worker,
    // taking down the whole tunnel. Reproduced by
    // tests/bare_lf_head_panic_probe.rs.
    if head.get(end.saturating_sub(4)..end) != Some(b"\r\n\r\n".as_slice()) {
        return head.to_vec();
    }
    // `find_header_end` returns the offset just PAST the blank line, so the
    // header block (status line + field lines) is everything before its final
    // CRLFCRLF. Getting this bound wrong by two bytes duplicated a CRLF and the
    // head no longer parsed -- pinned by reframe_head_tests.
    let block = &head[..end - 4];
    let mut out: Vec<u8> = Vec::with_capacity(head.len());
    for (i, line) in block.split(|b| *b == b'\r').enumerate() {
        // Split on CR; every separator is re-emitted below, so the LF that
        // follows each CR is stripped from every field but the first.
        let line = if i == 0 {
            line
        } else {
            line.strip_prefix(b"\n").unwrap_or(line)
        };
        let keep = match line.iter().position(|b| *b == b':') {
            Some(colon) => {
                let name = &line[..colon];
                let value = &line[colon + 1..];
                let is_te = std::str::from_utf8(name)
                    .map(|n| n.trim().eq_ignore_ascii_case("transfer-encoding"))
                    .unwrap_or(false);
                let only_chunked = std::str::from_utf8(value)
                    .map(|v| {
                        !v.is_empty()
                            && v.split(',')
                                .all(|t| t.trim().eq_ignore_ascii_case("chunked"))
                    })
                    .unwrap_or(false);
                !(is_te && only_chunked)
            }
            None => true,
        };
        if !keep {
            continue;
        }
        if i > 0 {
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(line);
    }
    out.extend_from_slice(b"\r\n\r\n");
    out
}

/// Heartbeat state, kept in one struct so the ping cadence, the dead deadline, and
/// the overdue count cannot drift out of step with each other.
struct Heartbeat {
    /// Both timers are boxed+pinned: `select!` needs to poll them simultaneously
    /// (two mutable borrows of `hb`, which disjoint fields allow) and `reset()`
    /// needs to move them afterwards.
    ping_tick: Pin<Box<tokio::time::Sleep>>,
    dead: Pin<Box<tokio::time::Sleep>>,
    overdue: u32,
    /// Deadline by which a PONG must arrive, derived from the last ping we sent.
    /// This is what actually detects a dead peer: `dead` only fires when that
    /// deadline passes without an inbound frame.
    pong_deadline: Option<tokio::time::Instant>,
}

impl Heartbeat {
    fn new() -> Self {
        Self {
            ping_tick: Box::pin(tokio::time::sleep(PING_INTERVAL)),
            dead: Box::pin(tokio::time::sleep(PING_TIMEOUT)),
            overdue: 0,
            pong_deadline: None,
        }
    }
    fn mark_alive(&mut self) {
        self.overdue = 0;
    }
    /// Called on ANY inbound frame, not just PONG: traffic is proof of life, and
    /// insisting on pongs during a busy stream would cause false teardowns.
    fn reset(&mut self) {
        self.ping_tick
            .as_mut()
            .reset(tokio::time::Instant::now() + PING_INTERVAL);
        if let Some(dl) = self.pong_deadline {
            if tokio::time::Instant::now() >= dl {
                self.overdue += 1;
            }
        }
        let budget = if self.overdue >= 3 {
            // Overdue enough that the deadline should fire now rather than wait
            // out another full timeout.
            tokio::time::Instant::now()
        } else {
            tokio::time::Instant::now() + PING_TIMEOUT
        };
        self.dead.as_mut().reset(budget);
    }
}

fn msg(f: Frame) -> WsMsg {
    tokio_tungstenite::tungstenite::Message::Binary(f.encode())
}

fn decode(m: &WsMsg) -> Result<Option<Frame>, Box<dyn std::error::Error + Send + Sync>> {
    use tokio_tungstenite::tungstenite::Message;
    match m {
        Message::Binary(b) => {
            let (frame, _n) = Frame::decode(b)?.ok_or("short tunnel frame")?;
            Ok(Some(frame))
        }
        // WebSocket-level Ping/Pong/Text: tungstenite answers pings itself, and
        // inbound traffic of any kind is liveness.
        Message::Ping(_) | Message::Pong(_) | Message::Text(_) => Ok(None),
        Message::Close(_) => Err("peer sent Close".into()),
        Message::Frame(_) => Ok(None),
    }
}

#[cfg(test)]
mod reframe_head_tests {
    use super::*;

    #[test]
    fn plaintext_hub_urls_require_literal_loopback() {
        assert!(url_is_loopback("ws://[::1]:8443/ring"));
        assert!(url_is_loopback("ws://127.0.0.1:8443/ring"));
        assert!(!url_is_loopback("ws://rental.localhost:8443/ring"));
    }

    #[tokio::test]
    async fn credentials_in_hub_urls_never_appear_in_errors() {
        let error = dial("ws://name:topsecret@127.0.0.1:1/ring")
            .await
            .unwrap_err();
        assert!(!error.to_string().contains("topsecret"));
    }

    const ENGINE_HEAD: &[u8] =
        b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n";

    #[test]
    fn drops_the_framing_header_we_consumed() {
        let out = reframe_head_for_tunnel(ENGINE_HEAD);
        let s = String::from_utf8_lossy(&out);
        assert!(!s.to_lowercase().contains("transfer-encoding"), "kept: {s}");
        assert!(s.contains("content-type: text/event-stream"), "lost: {s}");
        assert!(s.ends_with("\r\n\r\n"), "no terminator: {s}");
        let (res, rest) = crate::hub::parse_head(&out).expect("must still parse");
        assert_eq!(res.status(), hyper::StatusCode::OK);
        assert!(rest.is_empty());
        assert!(
            !crate::chunked::is_chunked(res.headers()),
            "still claims chunked"
        );
    }

    /// `gzip, chunked` is not ours to rewrite: we did not decode the gzip, and
    /// removing the header would claim an unencoded body.
    #[test]
    fn leaves_a_head_it_does_not_understand_alone() {
        let head = b"HTTP/1.1 200 OK\r\ntransfer-encoding: gzip, chunked\r\n\r\n";
        assert_eq!(reframe_head_for_tunnel(head).as_slice(), &head[..]);
    }

    #[test]
    fn a_head_without_the_header_is_byte_identical() {
        let head = b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\n";
        assert_eq!(reframe_head_for_tunnel(head).as_slice(), &head[..]);
    }

    #[test]
    fn an_incomplete_head_is_passed_through_unchanged() {
        let head = b"HTTP/1.1 200 OK\r\ncontent-type: text";
        assert_eq!(reframe_head_for_tunnel(head).as_slice(), &head[..]);
    }
}

#[cfg(test)]
mod stream_handle_tests {
    use super::*;

    #[tokio::test]
    async fn a_closed_request_body_does_not_cancel_the_response_pump() {
        let task = tokio::spawn(std::future::pending::<()>());
        let (tx, rx) = mpsc::channel(1);
        drop(rx); // Hyper has consumed Content-Length bytes, response still pending.
        let mut streams = HashMap::new();
        streams.insert(
            1,
            StreamHandle {
                tx,
                abort: task.abort_handle(),
            },
        );
        enqueue_stream_input(&mut streams, 1, Vec::new()); // delayed HALF_END
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "finishing a request must not abort its response"
        );
        assert!(streams.contains_key(&1));
        streams.clear();
        assert!(task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn lease_expiry_cannot_wait_for_a_full_writer_queue() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            tokio_tungstenite::accept_async(socket).await.unwrap()
        });
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        let _peer = server.await.unwrap();
        let (_, mut stream) = ws.split();
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(msg(Frame::Ping)).unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            run_session(
                &mut stream,
                &tx,
                &mut Box::pin(tokio::time::sleep(Duration::ZERO)),
                &mut Heartbeat::new(),
                &mut HashMap::new(),
                &mut PumpCompletions::new(),
                "http://127.0.0.1:8000",
            ),
        )
        .await;
        assert!(
            result.is_ok(),
            "lease expiry must not wait behind queued body frames"
        );
    }

    #[tokio::test]
    async fn dropping_a_stream_handle_cancels_its_engine_pump() {
        let task = tokio::spawn(std::future::pending::<()>());
        let abort = task.abort_handle();
        let (tx, _rx) = mpsc::channel(1);
        let handle = StreamHandle { tx, abort };

        drop(handle);

        let err = task.await.expect_err("pump should have been cancelled");
        assert!(err.is_cancelled());
    }

    #[tokio::test]
    async fn stream_input_queue_is_bounded() {
        let task = tokio::spawn(std::future::pending::<()>());
        let (tx, _rx) = mpsc::channel(STREAM_INPUT_CAPACITY);
        let handle = StreamHandle {
            tx,
            abort: task.abort_handle(),
        };

        for _ in 0..STREAM_INPUT_CAPACITY {
            handle.tx.try_send(vec![1]).expect("within capacity");
        }
        assert!(matches!(
            handle.tx.try_send(vec![2]),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        drop(handle);
        assert!(task.await.expect_err("cancelled").is_cancelled());
    }

    #[test]
    fn websocket_writer_queue_is_bounded() {
        let (tx, _rx) = mpsc::channel(WS_WRITER_CAPACITY);
        for _ in 0..WS_WRITER_CAPACITY {
            tx.try_send(msg(Frame::Ping)).expect("within capacity");
        }
        assert!(matches!(
            tx.try_send(msg(Frame::Ping)),
            Err(mpsc::error::TrySendError::Full(_))
        ));
    }
}

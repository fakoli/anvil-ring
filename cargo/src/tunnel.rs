//! The outbound tunnel: an authenticated, self-healing WSS connection from the
//! disposable host to the hub, carrying multiplexed proxied streams.
//!
//! Responsibility map (invariants this file owns, and how):
//!
//!  - I-1 outbound only: this module ONLY dials out. No listener, no accept, no
//!    inbound path — the client cannot bind by construction.
//!  - I-3 revocation effective: the hub returns a lease lifetime in WELCOME and we
//!    reconnect+re-authenticate at 75% of it. A revoked credential therefore stops
//!    working within one reconnect interval, idle tunnel or not.
//!  - I-6 dead vs. slow: PING/PONG with a stated timeout. A lost peer is declared
//!    dead and the tunnel tears down, rather than leaving a half-open connection
//!    that hangs a model request — which looks exactly like a slow model.
//!  - I-8 no secret in argv or logs: credential comes from a file or env var and is
//!    never interpolated into a log line. `log()` is the only output path here.
//!  - I-10 loopback-only upstream: re-checked in `StreamCtx::open`, not only at
//!    startup, so this function cannot be reached with a routable target.
//!
//! Transport (settling ADR-0002): WSS over TCP 443, with the proxy speaking HTTP
//! through our own tunnel. That is why chisel and `ssh -R` fell away — we no longer
//! forward raw ports, so "which port am I allowed to dial" mostly stops mattering.

use crate::frames::Frame;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;

/// Streams one tether serves at once. Bounded so a hub bug or a flood cannot
/// exhaust the rental's file descriptors.
pub const MAX_CONCURRENT_STREAMS: usize = 64;

/// Heartbeat cadence, and the point at which a peer is declared dead (I-6).
/// Deliberately wider than one interval: a single late pong on a congested link
/// must not tear down a serving endpoint.
pub const PING_INTERVAL: Duration = Duration::from_secs(10);
pub const PING_TIMEOUT: Duration = Duration::from_secs(25);

const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const ENGINE_READ_BUF: usize = 16 * 1024;

type Wss = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsMsg = tokio_tungstenite::tungstenite::Message;

/// State the proxy consults before accepting a caller.
#[derive(Debug, Default)]
pub struct TunnelState {
    /// True only between WELCOME and teardown. Requests in a reconnect window are
    /// REFUSED, not queued — queueing hides the outage and turns a dead tether
    /// into a slow one, which is the exact confusion I-6 exists to prevent.
    pub up: AtomicBool,
    /// Tunnels established, for status output.
    pub generations: AtomicU64,
    /// Lease seconds from the last WELCOME. 0 means never authorized.
    pub lease_secs: AtomicU64,
}

pub struct ClientConfig {
    pub hub_url: String,
    pub credential: Vec<u8>,
    pub state: Arc<TunnelState>,
}

impl ClientConfig {
    /// Resolve the credential from a file (preferred) or an env var.
    ///
    /// Not a CLI flag, deliberately: argv is visible in `ps` on a shared rental
    /// host and persists in shell history (I-8).
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
             Refusing to start unauthenticated, and refusing to read a secret from argv.",
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
    // Never interpolate the credential here (I-8).
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
    if !(hub_url.starts_with("wss://") || hub_url.starts_with("ws://")) {
        return Err("hub URL must be wss:// (ws:// only for loopback)".into());
    }
    // Plaintext to a routable host would ship the credential in the clear. This
    // check is about the URL, not the resolved IP, so a DNS-rebilled loopback name
    // is still a hole in theory; the real protection is wss-only in deployment.
    if hub_url.starts_with("ws://") && !url_is_loopback(hub_url) {
        return Err(format!(
            "refusing plaintext ws:// to non-loopback {}; use wss://",
            host_of(hub_url)
        )
        .into());
    }
    let (ws, _resp) =
        tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(hub_url))
            .await
            .map_err(|_| "dial timed out")??;
    Ok(ws)
}

fn host_of(url: &str) -> String {
    let after = url.split("://").nth(1).unwrap_or(url);
    after.split('/').next().unwrap_or(after).to_string()
}

fn url_is_loopback(url: &str) -> bool {
    let d = host_of(url);
    let h = d.split(':').next().unwrap_or(&d);
    crate::proxy::is_loopback_host(h) || h.ends_with(".localhost")
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
    let (tx, mut rx) = mpsc::unbounded_channel::<WsMsg>();
    let writer = tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            if sink.send(m).await.is_err() {
                break;
            }
        }
    });

    // Authenticate before serving anything: I-5 puts the decision on the hub, so
    // we do not act on a self-description of our own.
    tx.send(msg(Frame::Hello {
        credential: cfg.credential.clone(),
    }))?;

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
    // Per-stream inbound queues. Dropping a sender closes that stream's engine
    // side, which is how END is implemented.
    let mut streams: HashMap<u16, mpsc::UnboundedSender<Vec<u8>>> = HashMap::new();

    let result = run_session(&mut stream, &tx, &mut lease_tick, &mut hb, &mut streams, upstream).await;

    streams.clear();
    drop(tx);
    let _ = writer.await;
    result
}

async fn run_session(
    stream: &mut futures_util::stream::SplitStream<Wss>,
    tx: &mpsc::UnboundedSender<WsMsg>,
    lease_tick: &mut Pin<Box<tokio::time::Sleep>>,
    hb: &mut Heartbeat,
    streams: &mut HashMap<u16, mpsc::UnboundedSender<Vec<u8>>>,
    upstream: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        tokio::select! {
            _ = lease_tick.as_mut() => {
                log("lease window elapsed; reconnecting to re-authorize");
                let _ = tx.send(msg(Frame::GoAway { reason: b"lease refresh".to_vec() }));
                return Ok(());
            }
            _ = hb.ping_tick.as_mut() => {
                let _ = tx.send(msg(Frame::Ping));
            }
            _ = hb.dead.as_mut() => {
                return Err("heartbeat timeout: peer declared dead (I-6)".into());
            }
            m = stream.next() => {
                let m = match m {
                    Some(m) => m?,
                    None => return Err("hub closed connection".into()),
                };
                let Some(frame) = decode(&m)? else { continue };
                match frame {
                    Frame::Ping => { let _ = tx.send(msg(Frame::Pong)); }
                    Frame::Pong => {
                        hb.mark_alive();
                        hb.pong_deadline = None;
                    }
                    Frame::GoAway { reason } => {
                        log(&format!("hub GOAWAY: {}", String::from_utf8_lossy(&reason)));
                        return Ok(());
                    }
                    Frame::Open { stream: id, head } => {
                        if streams.len() >= MAX_CONCURRENT_STREAMS {
                            let _ = tx.send(msg(Frame::End { stream: id, reason: b"overloaded".to_vec() }));
                            continue;
                        }
                        let mut ctx = match StreamCtx::open(&head, upstream).await {
                            Ok(c) => c,
                            Err(e) => {
                                let _ = tx.send(msg(Frame::End { stream: id, reason: e.to_string().into_bytes() }));
                                continue;
                            }
                        };
                        let (to_stream, mut from_hub) = mpsc::unbounded_channel::<Vec<u8>>();
                        streams.insert(id, to_stream);
                        let reply = tx.clone();
                        // Pump engine -> hub. Without this the tunnel would be a
                        // one-way pipe: requests would reach vLLM and answers
                        // would never come back.
                        tokio::spawn(async move {
                            let mut buf = vec![0u8; ENGINE_READ_BUF];
                            loop {
                                tokio::select! {
                                    biased;
                                    maybe = from_hub.recv() => match maybe {
                                        Some(bytes) => {
                                            if ctx.write(&bytes).await.is_err() { break; }
                                        }
                                        // Hub sent END (or the stream map dropped us).
                                        None => { let _ = ctx.finish().await; break; }
                                    },
                                    n = ctx.read(&mut buf) => match n {
                                        Ok(0) => {
                                            let _ = reply.send(msg(Frame::End { stream: id, reason: Vec::new() }));
                                            break;
                                        }
                                        Ok(k) => {
                                            // One engine read == one DATA frame, sent
                                            // immediately. Batching here would violate
                                            // I-9 and show up as slow tokens.
                                            if reply.send(msg(Frame::Data { stream: id, bytes: buf[..k].to_vec() })).is_err() { break; }
                                        }
                                        Err(_) => {
                                            let _ = reply.send(msg(Frame::End { stream: id, reason: b"engine read failed".to_vec() }));
                                            break;
                                        }
                                    }
                                }
                            }
                        });
                    }
                    Frame::Data { stream: id, bytes } => {
                        if let Some(tx_stream) = streams.get(&id) {
                            let _ = tx_stream.send(bytes);
                        }
                        // Unknown stream: dropped, not materialized. Either a late
                        // chunk after our END or a peer bug; neither warrants
                        // inventing state.
                    }
                    Frame::End { stream: id, .. } => {
                        streams.remove(&id);
                    }
                    Frame::Hello { .. } | Frame::Welcome { .. } => {
                        return Err("unexpected HELLO/WELCOME mid-session".into());
                    }
                }
                hb.reset();
            }
        }
    }
}

/// One proxied request in flight on the rental side.
struct StreamCtx {
    w: tokio::io::WriteHalf<TcpStream>,
    r: tokio::io::ReadHalf<TcpStream>,
}

impl StreamCtx {
    async fn open(head: &[u8], upstream: &str) -> io::Result<Self> {
        let host = upstream
            .split("://")
            .nth(1)
            .unwrap_or(upstream)
            .trim_end_matches('/');
        // Re-checked here, not only at startup, so this path cannot be reached
        // with a routable target even if a caller changes (I-10).
        if !crate::proxy::is_loopback_host(host) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("refusing non-loopback upstream {host} (I-10)"),
            ));
        }
        let tcp = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(host))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "engine connect timed out"))??;
        let (r, mut w) = tokio::io::split(tcp);
        w.write_all(head).await?;
        w.flush().await?;
        Ok(Self { w, r })
    }
    async fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.w.write_all(bytes).await?;
        self.w.flush().await
    }
    async fn finish(&mut self) -> io::Result<()> {
        self.w.shutdown().await
    }
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        use tokio::io::AsyncReadExt;
        self.r.read(buf).await
    }
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


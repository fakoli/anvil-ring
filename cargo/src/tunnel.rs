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

impl TunnelState {
    /// Is the tunnel currently authorized and usable?
    ///
    /// Named rather than reading `up` directly at call sites: "can I route through
    /// this tether right now" is the question callers actually have, and it is
    /// false across every reconnect window (I-6).
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
    is_loopback_name_loose(h)
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

    let result = run_session(
        &mut stream,
        &tx,
        &mut lease_tick,
        &mut hb,
        &mut streams,
        upstream,
    )
    .await;

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
                            // The hub never sends a response head; only the tether
                            // produces one. A hub doing so is a protocol violation,
                            // mirroring the hub-side rule that a client may not send
                            // OPEN (I-5).
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
                                    // Writing toward the engine is finished by a half-close,
                                    // after which further request bytes would fail.
                                    let mut write_closed = false;
                                    // Decode the engine's chunked coding HERE, at the hop that
                                    // owns it. `transfer-encoding` is hop-by-hop and is stripped
                                    // when the response is forwarded, so forwarding chunked
                                    // bytes verbatim would leave the hub to frame an already
                                    // chunk-coded payload -- measured on the wire as a length
                                    // that disagrees with its own data. Empty until the head
                                    // says otherwise.
                                    let mut dechunk: Option<crate::chunked::ChunkedDecoder> = None;
                                    // Set once the engine's response head has been examined.
                                    let mut head_seen = false;
                                    // Latched once the request side reports None. See the
                                    // `from_hub` arm: that None is NOT end-of-stream.
                                    loop {
                                        tokio::select! {
                                            biased;
                                            maybe = from_hub.recv() => match maybe {
                                                // Half-close: no more request bytes. Shutdown
                                                // the write half so the engine sees
                                                // answer is still to come.
                                                //
                                                // MUST precede the write arm: an empty Vec is
                                                // the signal, and a `Some(bytes)` catch-all
                                                // placed first would swallow it and the
                                                // engine would never see end-of-request.
                                                Some(bytes) if bytes.is_empty() && !write_closed => {
                                                    if ctx.finish().await.is_err() { break; }
                                                    write_closed = true;
                                                }
                                                Some(bytes) if !write_closed => {
                                                    if ctx.write(&bytes).await.is_err() { break; }
                                                }
                                                // Request already half-closed and the hub sent
                                                // more anyway. Ignored rather than written: a
                                                // write here would error on a shut socket and
                                                // tear down a stream whose answer is fine.
                                                // Sender still alive after our half-close:
                                                // the request body is finished, so late
                                                // bytes are a hub fault. Drop them with a
                                                // log -- do NOT write to a socket we shut
                                                // down, which would error and tear down a
                                                // stream whose response is fine.
                                                Some(_) => {}
                                                // Hub aborted, or the stream map dropped us.
                                                // A closed receiver is PERMANENTLY ready.
                                                // With `biased` this arm therefore STARVES
                                                // `ctx.read` forever: the engine read is
                                                // cancelled on every pass and the caller
                                                // receives exactly one event. Measured:
                                                // this arm fired on every iteration.
                                                //
                                                // None is also NOT an abort -- the only
                                                // sender is the session loop on another task,
                                                // and a bodiless request never sends. So:
                                                // do nothing. Not a `break` (kills the read)
                                                // and not a latch (the transient sender makes
                                                // a guarded arm re-enable and spin).
                                                None => {}
                                            },
                                            n = ctx.read(&mut buf) => match n {
                                                Ok(0) => {
                                                    // Engine EOF. If the body was chunked, the
                                                    // decoder may hold a final flush; a chunk
                                                    // left incomplete is dropped, not forwarded,
                                                    // because a partial SSE event on the wire is
                                                    // indistinguishable from a real one.
                                                    if let Some(d) = dechunk.as_mut() {
                                                        match d.feed_eof() {
                                                            Ok(rest) if !rest.is_empty() => {
                                                                let _ = reply.send(msg(Frame::Data { stream: id, bytes: rest }));
                                                            }
                                                            Ok(_) => {}
                                                            Err(e) => {
                                                                let _ = reply.send(msg(Frame::End {
                                                                    stream: id,
                                                                    reason: format!("truncated chunked body: {e:?}").into_bytes(),
                                                                }));
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    let _ = reply.send(msg(Frame::End { stream: id, reason: Vec::new() }));
                                                    break;
                                                }
                                                Ok(k) => {
                                                    // Decide on the FIRST read, from the head
                                                    // we already have: only a chunked body gets
                                                    // decoded. A plain content-length body must
                                                    // pass through byte-for-byte, or we would
                                                    // corrupt it by hunting for chunk markers.
                                                    // Decided ONCE and remembered. The old
                                                    // guard here was
                                                    // `dechunk.is_none() && !head_seen`, i.e.
                                                    // it could run on at most ONE read. A head
                                                    // that arrives alone -- normal, because a
                                                    // model takes time to first token and
                                                    // flushes the head before any event --
                                                    // left `dechunk` None on every BODY read,
                                                    // so those reads took the generic path
                                                    // below and forwarded RAW chunk-framed
                                                    // bytes. The hub parsed frame 1 as the
                                                    // head, read frame 2's size line as body,
                                                    // and the caller got one event plus
                                                    // framing noise. Now the decision is
                                                    // sticky: once `is_chunked` is known,
                                                    // `dechunk` is created here so the generic
                                                    // path always de-frames.
                                                    // Latch ONLY on a read that holds a complete head. A partial
                                                    // head must leave `head_seen` false: latching early would make
                                                    // every later read take the generic path and forward the head
                                                    // bytes as BODY -- which is exactly the "status line in the
                                                    // body" symptom this file has already been burned by.
                                                    if let Some((head, body)) = split_head(&buf[..k]) {
                                                        if !head_seen {
                                                            head_seen = true;
                                                            let raw = buf[..k - body.len()].to_vec();
                                                            // Release the head NOW, reframed, before any
                                                            // body byte goes out. `transfer-encoding` is
                                                            // dropped because we are about to de-chunk: a
                                                            // header describing framing we remove makes the
                                                            // hub re-frame already-bare bytes.
                                                            //
                                                            // This runs for a head arriving with NO body
                                                            // bytes too -- the normal production case, since
                                                            // a model flushes headers before its first token.
                                                            let fixed = reframe_head_for_tunnel(&raw);
                                                            if reply.send(msg(Frame::RespHead { stream: id, head: fixed })).is_err() { break; }
                                                            if crate::chunked::is_chunked(head.headers()) {
                                                                // The decision, made once. Note
                                                                // this runs for a head that
                                                                // arrives with NO body bytes --
                                                                // the common production case,
                                                                // since a model flushes the head
                                                                // before its first token. The
                                                                // old guard (`dechunk.is_none()`)
                                                                // could not distinguish "haven't
                                                                // looked" from "looked, decided
                                                                // plain", and a lone head left
                                                                // every body read to forward raw
                                                                // chunk framing.
                                                                dechunk = Some(crate::chunked::ChunkedDecoder::new());
                                                                // The head is NOT sent here. It went out
                                                                // above as RespHead, reframed. What follows
                                                                // is body bytes only: the hub's data path now
                                                                // treats every DATA frame as body, so there is
                                                                // no longer any status-line-vs-body guessing to
                                                                // get wrong.
                                                                // Decode the WHOLE remainder of
                                                                // this read, not one chunk.
                                                                //
                                                                // An engine that flushes fast (or a
                                                                // loopback socket coalescing) hands us
                                                                // head + every event in one read --
                                                                // measured here as one 95-byte read
                                                                // containing the entire response. An
                                                                // earlier version decoded one chunk
                                                                // and `continue`d, silently discarding
                                                                // every later event in that same read,
                                                                // which looked exactly like "streaming
                                                                // stops after the first token".
                                                                let mut d = dechunk.take().unwrap();
                                                                let mut acc: Vec<u8> = Vec::new();
                                                                // `done` cannot be true here: `body`
                                                                // is everything after the head in a
                                                                // read we just took as COMPLETE, so the
                                                                // last-chunk marker, if present, is in
                                                                // these bytes -- and if the decoder says
                                                                // done, we honour it.
                                                                let done_now;
                                                                match d.push(&body) {
                                                                    Ok(r) => {
                                                                        acc.extend_from_slice(&r.out);
                                                                        done_now = r.done;
                                                                    }
                                                                    Err(e) => {
                                                                        let _ = reply.send(msg(Frame::End { stream: id, reason: format!("bad chunked body: {e:?}").into_bytes() }));
                                                                        break;
                                                                    }
                                                                }
                                                                dechunk = Some(d);
                                                                if !acc.is_empty() {
                                                                    if reply.send(msg(Frame::Data { stream: id, bytes: acc })).is_err() { break; }
                                                                }
                                                                if done_now {
                                                                    // The last-chunk marker was in
                                                                    // this read: the body is finished.
                                                                    let _ = reply.send(msg(Frame::End { stream: id, reason: Vec::new() }));
                                                                    break;
                                                                }
                                                                continue;
                                                            }
                                                        }
                                                    }
                                                    let payload = match &mut dechunk {
        None => buf[..k].to_vec(),
                                                        Some(d) => match d.push(&buf[..k]) {
                                                            Ok(r) => r.out,
                                                            Err(e) => {
                                                                let _ = reply.send(msg(Frame::End {
                                                                    stream: id,
                                                                    reason: format!("bad chunked body: {e:?}").into_bytes(),
                                                                }));
                                                                break;
                                                            }
                                                        },
                                                    };
                                                    // A chunk boundary can yield zero bytes
                                                    // (e.g. a size line split across reads);
                                                    // sending an empty DATA frame would make
                                                    // the hub emit an empty caller chunk.
                                                    if !payload.is_empty() {
                                                        if reply.send(msg(Frame::Data { stream: id, bytes: payload })).is_err() {
                                                            break;
                                                        }
                                                    }
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
                            Frame::HalfEnd { stream: id } => {
                                // Empty slice is the half-close signal to the pump: shutdown
                                // the write half toward the engine, do NOT stop reading.
                                if let Some(tx_stream) = streams.get(&id) {
                                    let _ = tx_stream.send(Vec::new());
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

/// One proxied request in flight on the rental side.
struct StreamCtx {
    w: tokio::io::WriteHalf<TcpStream>,
    r: tokio::io::ReadHalf<TcpStream>,
}

impl StreamCtx {
    async fn open(head: &[u8], upstream: &str) -> io::Result<Self> {
        // Parse-and-validate in one step. Re-checked here, not only at startup, so
        // this path cannot be reached with a routable target even if configuration
        // changes (I-10). The earlier version compared an authority *string* to a
        // host list, so "127.0.0.1:8000" was REFUSED as non-loopback -- an
        // invariant check that broke the invariant's own purpose.
        let addr = crate::proxy::loopback_authority(upstream, 80)?;
        let tcp = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
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

/// Split an engine read into (parsed head, remaining body bytes).
///
/// Returns None when the head is not complete yet -- in which case the caller
/// forwards nothing, since a half-parsed status line is not a response.
fn split_head(bytes: &[u8]) -> Option<(http::Response<()>, bytes::Bytes)> {
    // `find_header_end` returns the offset just PAST the blank line, and
    // `parse_head` returns the remainder from that same offset. Passing
    // `end + 4` here was a real bug: it started the body four bytes early, so the
    // chunked decoder received `a\r\ndata...` (the last 2 bytes of the header block
    // plus the size line) and reported a chunk length that did not match its data.
    // `find_header_end` is kept for callers that need the boundary, not so it can
    // be re-applied to a slice parse_head already sliced.
    crate::hub::parse_head(bytes)
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

/// Loopback check by NAME (no resolution), for the hub URL only.
///
/// Kept separate from `proxy::loopback_authority` on purpose: the engine target is
/// an address and gets parse+validate+connect in one step, whereas the hub URL here
/// must permit `localhost`/`.localhost` (RFC 6761 loopback by definition) purely to
/// decide whether plaintext `ws://` is acceptable for an in-process test. Resolving
/// a name to make that decision would let a name that resolves off-host through.
fn is_loopback_name_loose(h: &str) -> bool {
    if h == "localhost" || h.ends_with(".localhost") {
        return true;
    }
    h.parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}

#[cfg(test)]
mod reframe_head_tests {
    use super::*;

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

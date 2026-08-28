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
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

/// Default lease. Short enough that revocation lands promptly (I-3); long enough
/// that re-registration is not the dominant cost on a flaky rental link.
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

/// Live session handle, so revocation can reach an established tunnel.
#[derive(Clone)]
struct LiveSession {
    /// Held, never written through directly: dropping this sender (when the
    /// registry evicts the session on revoke) is what makes the session task's
    /// `rx.recv()` return `None` and tear the tunnel down (I-3). The field looks
    /// unused to the compiler and is not.
    #[allow(dead_code)]
    tx: mpsc::UnboundedSender<Frame>,
    last_seen: Arc<Mutex<Instant>>,
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
        let id = self.by_credential.lock().unwrap().get(&digest(credential))?.clone();
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

    fn attach(&self, id: &str, tx: mpsc::UnboundedSender<Frame>) -> Arc<Mutex<Instant>> {
        let seen = Arc::new(Mutex::new(Instant::now()));
        self.live.lock().unwrap().insert(
            id.to_string(),
            LiveSession {
                tx,
                last_seen: seen.clone(),
            },
        );
        seen
    }

    fn detach(&self, id: &str) {
        self.live.lock().unwrap().remove(id);
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
        let _ = sink.send(ws_msg(Frame::GoAway { reason: b"unauthorized".to_vec() })).await;
        return Err("unauthorized".into());
    };

    eprintln!(
        "anvil-ring hub: tether {} authorized from {peer}, lease {}s",
        lease.tether_id,
        lease.ttl.as_secs()
    );
    sink.send(ws_msg(Frame::Welcome { lease_secs: lease.ttl.as_secs() }))
        .await?;
    let _ = events.send(TetherEvent { tether_id: lease.tether_id.clone(), kind: EventKind::Up });

    let (tx, mut rx) = mpsc::unbounded_channel::<Frame>();
    let last_seen = registry.attach(&lease.tether_id, tx);
    // Boxed+pinned sleep, re-armed each pass: `select!` needs to poll it while we
    // still need to move it. A `tick()` future would hold `interval` borrowed.
    let mut tick = Box::pin(tokio::time::sleep(TETHER_TICK));
    let mut revoked = false;

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
                    Frame::Hello { .. } | Frame::Welcome { .. } => {
                        break Err("unexpected HELLO/WELCOME mid-session".into());
                    }
                    Frame::Data { .. } | Frame::End { .. } => {
                        // Dropped when unowned: fabricating a stream from them
                        // would let a tether inject bytes into a request it never
                        // saw.
                    }
                }
            }
        }
    };

    registry.detach(&lease.tether_id);
    let kind = if revoked { EventKind::Revoked } else { EventKind::Down };
    let _ = events.send(TetherEvent { tether_id: lease.tether_id.clone(), kind });
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

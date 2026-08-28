//! anvil-ring: an authenticated, flushing reverse proxy in front of an inference
//! engine, plus the outbound tunnel that makes it reachable with no inbound port.
//!
//! Contract lives in ../docs (invariants I-1..I-10, ADR-0001, ADR-0004).
//!   I-9   upstream chunks are flushed immediately (see tests for the assertion)
//!   I-10  the engine binds loopback; auth is enforced here, in one place
//!   I-8   tokens come from the environment, never argv

mod headers;
mod proxy;

use std::net::SocketAddr;

/// The executable is `anvil-ring`, always anvil-prefixed. There is deliberately
/// no bare `ring` and no short alias: the prefix is the namespace (operator
/// directive; see docs/origin-story.md).
pub const PROG: &str = "anvil-ring";
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(String::as_str) {
        None | Some("proxy") => run_proxy().await,
        Some("hub") => run_hub().await,
        Some("tether") => run_tether().await,
        Some("--version") | Some("-V") => {
            println!("{PROG} {VERSION}");
            Ok(())
        }
        Some("--help") | Some("-h") => {
            print_help();
            Ok(())
        }
        Some(other) => {
            eprintln!("{PROG}: unknown subcommand {other:?} (try --help)");
            std::process::exit(2);
        }
    }
}

fn print_help() {
    println!(
        "{PROG} {VERSION} -- authenticated flushing proxy in front of an inference engine

USAGE
    anvil-ring proxy                 start the proxy (default subcommand)
    anvil-ring tether                dial out to the hub (rental side; no listeners)
    anvil-ring hub                   accept tunnels (always-on side)
    anvil-ring --version             print version
    anvil-ring --help                this text

ENVIRONMENT (secrets are never argv -- I-8)
    ANVIL_RING_LISTEN          bind address            (default 127.0.0.1:8080)
    ANVIL_RING_UPSTREAM        engine URI, must be loopback (default http://127.0.0.1:8000)
    ANVIL_RING_TOKEN           bearer token; REQUIRED unless the next var is set
    ANVIL_RING_ALLOW_NO_AUTH   set to 1 to run unauthenticated (local testing only)

    ANVIL_RING_HUB_URL         wss:// hub address (tether)   [tether]
    ANVIL_RING_HUB_LISTEN      hub bind address              [hub]
    ANVIL_RING_CRED_FILE       credential file, preferred    [tether]
    ANVIL_RING_CREDENTIAL      credential, if no file        [tether]

The hub initiates streams and owns every authorization decision; the tether side
cannot name a port, host, or permission. See docs/invariants.md (I-1, I-5).
and ADR-0002/ADR-0004. Only the proxy half exists today."
    );
}

/// Config is read only from the environment (I-8: no secrets in argv).
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

async fn run_proxy() -> Result<(), Box<dyn std::error::Error>> {
    let listen = env_or("ANVIL_RING_LISTEN", "127.0.0.1:8080");
    let upstream = env_or("ANVIL_RING_UPSTREAM", "http://127.0.0.1:8000");

    // No default token: an unauthenticated proxy sitting between the network and
    // the inference port would violate I-10 silently, so absence is fatal unless
    // explicitly overridden for a local test.
    let token = match std::env::var("ANVIL_RING_TOKEN") {
        Ok(t) if !t.is_empty() => Some(t),
        _ => {
            if std::env::var("ANVIL_RING_ALLOW_NO_AUTH").is_ok() {
                eprintln!("{PROG}: WARNING -- authentication DISABLED (ANVIL_RING_ALLOW_NO_AUTH)");
                None
            } else {
                eprintln!(
                    "{PROG}: ANVIL_RING_TOKEN is unset or empty.\n\
                     {PROG}: refusing to start an unauthenticated proxy: with no token\n\
                     {PROG}: there is nothing between the network and the inference port\n\
                     {PROG}: (invariant I-10). For a local trusted test set\n\
                     {PROG}: ANVIL_RING_ALLOW_NO_AUTH=1 . Tokens are never accepted as\n\
                     {PROG}: command-line arguments, since argv leaks via `ps`."
                );
                std::process::exit(2);
            }
        }
    };

    let addr: SocketAddr = listen.parse()?;
    let up: hyper::Uri = upstream.parse()?;

    let proxy = proxy::Proxy::new(up, token.clone());
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!(
        "{PROG}: listening on {addr} -> {upstream} (auth: {})",
        if token.is_some() {
            "bearer"
        } else {
            "DISABLED"
        }
    );

    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, peer) = accept?;
                let proxy = proxy.clone();
                tokio::spawn(async move {
                    if let Err(e) = proxy.serve_connection(stream).await {
                        eprintln!("{PROG}: connection from {peer} ended: {e}");
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("{PROG}: shutting down");
                break;
            }
        }
    }
    Ok(())
}

/// Hub side: accepts outbound tunnels. `anvil-ring hub`.
async fn run_hub() -> Result<(), Box<dyn std::error::Error>> {
    use anvil_ring::hub::{self, Registry};
    use std::sync::Arc;

    let listen = std::env::var("ANVIL_RING_HUB_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:8443".to_string());
    let addr: std::net::SocketAddr = listen.parse()?;
    let cred = std::env::var("ANVIL_RING_DEMO_CREDENTIAL")
        .map_err(|_| "ANVIL_RING_DEMO_CREDENTIAL must name the one tether to register")?;

    let reg = Arc::new(Registry::new(hub::DEFAULT_LEASE));
    let tether = reg.register("demo-1", "demo rental", &cred);
    // Never print the credential; the id and hash prefix are safe (I-8).
    eprintln!(
        "{PROG}: hub on {addr}; tether {} registered (cred {}...)",
        tether.id,
        &tether.credential_hash[..12]
    );

    let mut events = hub::serve(addr, reg.clone()).await?;
    tokio::spawn(async move {
        // Events exist so a lost/revoked tether is a logged state transition (I-6)
        // rather than an absence of evidence. recv() is inherent to the receiver,
        // so no StreamExt import is needed.
        while let Some(ev) = events.recv().await {
            eprintln!("{PROG}: event {} {:?}", ev.tether_id, ev.kind);
        }
    });

    // Status ticker so `status()` (I-6: UP vs DOWN, never "maybe slow") is visible.
    let reg2 = reg.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(20)).await;
            for (id, label, st) in reg2.status() {
                eprintln!("{PROG}: {id} ({label}) {st:?}");
            }
        }
    });

    tokio::signal::ctrl_c().await?;
    Ok(())
}

/// Tether (rental) side: dials out only. `anvil-ring tether`.
async fn run_tether() -> Result<(), Box<dyn std::error::Error>> {
    use anvil_ring::tunnel::{self, ClientConfig, TunnelState};
    use std::sync::Arc;

    let hub_url = std::env::var("ANVIL_RING_HUB_URL")
        .map_err(|_| "ANVIL_RING_HUB_URL must be a wss:// (or loopback ws://) URL")?;
    let upstream = std::env::var("ANVIL_RING_UPSTREAM")
        .unwrap_or_else(|_| "http://127.0.0.1:8000".to_string());
    // Credential from env/file only -- never argv (I-8).
    let credential = ClientConfig::credential_from_env()?;
    if !anvil_ring::proxy::is_loopback_host(
        upstream
            .split("://")
            .nth(1)
            .unwrap_or(&upstream)
            .split(':')
            .next()
            .unwrap_or(""),
    ) {
        return Err("upstream must be loopback; the tether only ever proxies locally (I-10)".into());
    }

    let state = Arc::new(TunnelState::default());
    let cfg = ClientConfig {
        hub_url,
        credential,
        state: state.clone(),
    };
    eprintln!("{PROG}: tether starting (no listening sockets; outbound only)");
    let _ = state;
    tunnel::run_client(cfg, upstream).await.map_err(Into::into)
}

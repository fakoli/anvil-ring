//! anvil-ring: an authenticated, flushing reverse proxy in front of an inference
//! engine, plus the outbound tunnel that makes it reachable with no inbound port.
//!
//! The architectural guarantees and accepted decisions live in `../docs`.
//! Upstream chunks are forwarded incrementally, the serving engine remains on
//! loopback, callers authenticate before proxy use, and secrets never appear in
//! command-line arguments.

use anvil_ring::proxy;
use std::net::SocketAddr;

/// The executable is `anvil-ring`, always anvil-prefixed. There is deliberately
/// no bare `ring` and no short alias: the prefix is the namespace (operator
/// directive; see docs/origin-story.md).
pub const PROG: &str = "anvil-ring";
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.as_slice() {
        [] => run_proxy().await,
        [arg] if arg == "proxy" => run_proxy().await,
        [arg] if arg == "hub" => run_hub().await,
        [arg] if arg == "tether" => run_tether().await,
        [mode, rest @ ..] if mode == "admin" => {
            anvil_ring::admin::run(rest).map_err(|error| -> Box<dyn std::error::Error> { error })
        }
        [arg] if arg == "--version" || arg == "-V" => {
            println!("{PROG} {VERSION}");
            Ok(())
        }
        [arg] if arg == "--help" || arg == "-h" => {
            print_help();
            Ok(())
        }
        [mode, ..]
            if matches!(
                mode.as_str(),
                "proxy" | "hub" | "tether" | "--version" | "-V" | "--help" | "-h"
            ) =>
        {
            eprintln!(
                "{PROG}: {mode} does not accept command-line arguments; configure it with the documented environment variables"
            );
            std::process::exit(2);
        }
        [other, ..] => {
            eprintln!("{PROG}: unknown subcommand {other:?} (try --help)");
            std::process::exit(2);
        }
    }
}

fn print_help() {
    println!(
        "{PROG} {VERSION} -- outbound-only model-serving tunnel and authenticated proxy

USAGE
    anvil-ring proxy                 start the local proxy (also the default mode)
    anvil-ring tether                dial the hub (rental side; no listener)
    anvil-ring hub                   accept tethers and optional caller traffic
    anvil-ring admin init            initialize private hub state
    anvil-ring admin register ID LABEL TTL_SECONDS
    anvil-ring admin rotate ID TTL_SECONDS
    anvil-ring admin revoke ID
    anvil-ring admin list
    anvil-ring admin audit [AFTER_SEQUENCE]
    anvil-ring --version             print the version
    anvil-ring --help                print this help

PROXY ENVIRONMENT
    ANVIL_RING_LISTEN          bind address            (default 127.0.0.1:8080)
    ANVIL_RING_UPSTREAM        engine URI, must be loopback (default http://127.0.0.1:8000)
    ANVIL_RING_TOKEN           bearer token; REQUIRED unless the next var is set
    ANVIL_RING_ALLOW_NO_AUTH   set to 1 to run unauthenticated (local testing only)

TETHER ENVIRONMENT
    ANVIL_RING_HUB_URL         wss:// hub URL (loopback ws:// allowed); REQUIRED
    ANVIL_RING_UPSTREAM        loopback engine URI      (default http://127.0.0.1:8000)
    ANVIL_RING_CRED_FILE       credential file; preferred
    ANVIL_RING_CREDENTIAL      credential when no file is used

HUB ENVIRONMENT
    ANVIL_RING_HUB_LISTEN      tether bind address      (default 127.0.0.1:8443)
    ANVIL_RING_STATE_DIR       private durable hub state directory
    ANVIL_RING_DEMO_CREDENTIAL explicit demo mode; mutually exclusive with state
    ANVIL_RING_FRONTEND_LISTEN caller bind address; enables the HTTP frontend
    ANVIL_RING_CALLER_TOKEN    caller bearer token; REQUIRED with the frontend

ADMIN ENVIRONMENT
    ANVIL_RING_STATE_DIR       hub state directory; REQUIRED (owner-only 0700)
    ANVIL_RING_CREDENTIAL_OUT  new credential file for register/rotate (0600)
    Credential TTL is 60..86400 seconds. Admin uses the authenticated OS account.

Secrets come from environment variables or files, never command-line arguments.
The hub owns authorization and stream routing. The rental initiates the
connection, and the tether can forward only to its configured loopback upstream.

PRODUCTION BOUNDARY
    Use durable state for persistent registrations and audit. Control changes
    reload every 500 ms; read failure cancels authorization and live sessions.
    All tethers share one serving contract; routing picks the least-loaded one.
    Terminate TLS externally and use wss:// for every routable deployment.

DOCUMENTATION
    https://fakoli.github.io/anvil-ring/
DOCUMENTATION SOURCE (use if the portal is not yet published)
    https://github.com/fakoli/anvil-ring/tree/main/docs
PROJECT
    https://github.com/fakoli/anvil-ring"
    );
}

/// Read non-secret configuration from the environment.
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

async fn run_proxy() -> Result<(), Box<dyn std::error::Error>> {
    let listen = env_or("ANVIL_RING_LISTEN", "127.0.0.1:8080");
    let upstream = env_or("ANVIL_RING_UPSTREAM", "http://127.0.0.1:8000");

    // No default token: an unauthenticated proxy sitting between the network and
    // the inference port would be exposed without authentication, so absence is
    // fatal unless explicitly overridden for a trusted loopback test.
    let token = match std::env::var("ANVIL_RING_TOKEN") {
        Ok(t) if !t.is_empty() => Some(t),
        _ => {
            if std::env::var("ANVIL_RING_ALLOW_NO_AUTH").as_deref() == Ok("1") {
                eprintln!("{PROG}: WARNING -- authentication DISABLED (ANVIL_RING_ALLOW_NO_AUTH)");
                None
            } else {
                eprintln!(
                    "{PROG}: ANVIL_RING_TOKEN is unset or empty.\n\
                     {PROG}: refusing to start because no bearer token would protect\n\
                     {PROG}: the inference port. For a trusted loopback test only, set\n\
                     {PROG}: ANVIL_RING_ALLOW_NO_AUTH=1. Tokens are never accepted as\n\
                     {PROG}: command-line arguments because process listings and shell\n\
                     {PROG}: history can expose them."
                );
                std::process::exit(2);
            }
        }
    };

    let addr: SocketAddr = listen.parse()?;
    let up: hyper::Uri = upstream.parse()?;
    let engine = proxy::loopback_authority(&upstream, 80)?;

    let proxy = proxy::Proxy::new(up, token.clone());
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!(
        "{PROG}: listening on {addr} -> {engine} (auth: {})",
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

    let listen =
        std::env::var("ANVIL_RING_HUB_LISTEN").unwrap_or_else(|_| "127.0.0.1:8443".to_string());
    let addr: std::net::SocketAddr = listen.parse()?;
    let reg = Arc::new(Registry::new(hub::DEFAULT_LEASE));
    let state_dir = std::env::var_os("ANVIL_RING_STATE_DIR").map(std::path::PathBuf::from);
    let demo = std::env::var("ANVIL_RING_DEMO_CREDENTIAL").ok();
    match (&state_dir, demo) {
        (Some(_), Some(_)) => {
            return Err(
                "ANVIL_RING_STATE_DIR and ANVIL_RING_DEMO_CREDENTIAL are mutually exclusive".into(),
            )
        }
        (Some(directory), None) => {
            let records = anvil_ring::admin::Store::open(directory)
                .and_then(|store| store.snapshot())
                .map_err(|error| -> Box<dyn std::error::Error> { error })?;
            reg.replace_records(records);
        }
        (None, Some(credential)) if !credential.is_empty() => {
            reg.register("demo-1", "demo rental", &credential);
        }
        _ => {
            return Err(
                "ANVIL_RING_STATE_DIR or a nonempty ANVIL_RING_DEMO_CREDENTIAL is required".into(),
            )
        }
    }

    // Validate caller configuration before starting either network listener.
    let frontend = match std::env::var("ANVIL_RING_FRONTEND_LISTEN") {
        Ok(front) => {
            let faddr: SocketAddr = front.parse()?;
            let token = std::env::var("ANVIL_RING_CALLER_TOKEN")
                .ok()
                .filter(|token| !token.is_empty())
                .ok_or(
                    "ANVIL_RING_CALLER_TOKEN is required when ANVIL_RING_FRONTEND_LISTEN is set",
                )?;
            Some((faddr, token))
        }
        Err(_) => None,
    };
    let mut events = hub::serve(addr, reg.clone()).await?;
    if let Some((faddr, token)) = frontend {
        anvil_ring::frontend::serve_frontend(faddr, reg.clone(), token).await?;
        eprintln!("{PROG}: frontend on {faddr} (callers authenticate with a bearer token)");
    }
    eprintln!("{PROG}: hub on {addr}");
    let monitor =
        state_dir.map(|directory| tokio::spawn(refresh_registrations(reg.clone(), directory)));

    tokio::spawn(async move {
        // Events make a lost or revoked tether a logged state transition rather
        // than an absence of evidence. recv() is inherent to the receiver,
        // so no StreamExt import is needed.
        while let Some(ev) = events.recv().await {
            eprintln!("{PROG}: event {} {:?}", ev.tether_id, ev.kind);
        }
    });

    // The status ticker reports Up, Stale, or Down instead of leaving an
    // operator to infer tether state from request latency.
    let reg2 = reg.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(20)).await;
            for (id, label, st) in reg2.status() {
                eprintln!("{PROG}: {id} ({label}) {st:?}");
            }
        }
    });

    let stopped = tokio::signal::ctrl_c().await;
    if let Some(monitor) = monitor {
        monitor.abort();
    }
    stopped?;
    Ok(())
}

/// One blocking reader at a time. A timed-out result is never applied later.
async fn refresh_registrations(
    registry: std::sync::Arc<anvil_ring::hub::Registry>,
    directory: std::path::PathBuf,
) {
    use std::time::Duration;
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let path = directory.clone();
        let mut read = tokio::task::spawn_blocking(move || {
            anvil_ring::admin::Store::open(&path).and_then(|store| store.snapshot())
        });
        match tokio::time::timeout(Duration::from_secs(2), &mut read).await {
            Ok(Ok(Ok(records))) => registry.replace_records(records),
            Ok(_) => {
                registry.clear_records();
                eprintln!("{PROG}: registration store unavailable; authorization cleared");
            }
            Err(_) => {
                registry.clear_records();
                eprintln!("{PROG}: registration refresh timed out; authorization cleared");
                // spawn_blocking cannot be aborted once running. Wait for this
                // same worker rather than accumulating stuck reads every tick.
                let _ = read.await;
            }
        }
    }
}

/// Tether (rental) side: dials out only. `anvil-ring tether`.
async fn run_tether() -> Result<(), Box<dyn std::error::Error>> {
    use anvil_ring::tunnel::{self, ClientConfig, TunnelState};
    use std::sync::Arc;

    let hub_url = std::env::var("ANVIL_RING_HUB_URL")
        .map_err(|_| "ANVIL_RING_HUB_URL must be a wss:// (or loopback ws://) URL")?;
    let upstream = std::env::var("ANVIL_RING_UPSTREAM")
        .unwrap_or_else(|_| "http://127.0.0.1:8000".to_string());
    // Credentials come from environment or file input, never process arguments.
    let credential = ClientConfig::credential_from_env()?;
    // Fail fast at startup with a clear message; the per-stream path re-validates.
    // Parsing (rather than string-comparing) is what makes `127.0.0.1:8000` pass --
    // the earlier string check refused exactly that legitimate loopback engine.
    anvil_ring::proxy::loopback_authority(&upstream, 80).map_err(
        |e| -> Box<dyn std::error::Error> {
            format!("upstream must be loopback; the tether only proxies to the local host: {e}")
                .into()
        },
    )?;

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

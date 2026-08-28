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
    anvil-ring --version             print version
    anvil-ring --help                this text

ENVIRONMENT (secrets are never argv -- I-8)
    ANVIL_RING_LISTEN          bind address            (default 127.0.0.1:8080)
    ANVIL_RING_UPSTREAM        engine URI, must be loopback (default http://127.0.0.1:8000)
    ANVIL_RING_TOKEN           bearer token; REQUIRED unless the next var is set
    ANVIL_RING_ALLOW_NO_AUTH   set to 1 to run unauthenticated (local testing only)

The tunnel client that dials out to the hub is not implemented yet -- see STATE.md
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

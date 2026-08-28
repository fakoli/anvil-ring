//! NEGATIVE CONTROL for invariant I-9. This binary deliberately BUFFERSS the
//! entire upstream response before replying.
//!
//! It exists so the streaming regression test can be shown to have teeth: if the
//! real proxy ever started buffering and the test could not tell the difference,
//! the test would be decorative. `tests/negative_control.rs` runs the streaming
//! assertion against this binary and REQUIRES it to fail.
//!
//! DO NOT "FIX" THIS FILE. A correct implementation here defeats its purpose.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

fn main() {
    let listen = std::env::var("ANVIL_RING_LISTEN").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let upstream =
        std::env::var("ANVIL_RING_UPSTREAM").unwrap_or_else(|_| "http://127.0.0.1:8000".into());
    let token = std::env::var("ANVIL_RING_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());

    let upstream_host = upstream
        .split("://")
        .nth(1)
        .and_then(|s| s.split('/').next())
        .unwrap_or("127.0.0.1:8000")
        .to_string();

    let listener = TcpListener::bind(&listen).expect("bind");
    eprintln!(
        "anvil-ring-buffering-canary: listening on {listen} -> {upstream_host} (BUFFERING by design)"
    );

    for conn in listener.incoming() {
        let Ok(client) = conn else { continue };
        let up = upstream_host.clone();
        let token = token.clone();
        std::thread::spawn(move || {
            let _ = handle(client, up, token);
        });
    }
}

fn handle(
    mut client: TcpStream,
    upstream_host: String,
    token: Option<String>,
) -> std::io::Result<()> {
    client.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
    let mut req = Vec::new();
    let mut byte = [0u8; 1];
    while !req.ends_with(b"\r\n\r\n") {
        if client.read(&mut byte)? == 0 {
            return Ok(());
        }
        req.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&req).to_ascii_lowercase();

    // Auth matches the real proxy, so the canary differs in exactly ONE variable:
    // buffering. Anything else would make the negative control uninformative.
    if let Some(expected) = &token {
        let have = head
            .lines()
            .find(|l| l.starts_with("authorization:"))
            .map(|l| l.trim().to_string())
            .unwrap_or_default();
        if have != format!("authorization: bearer {expected}") {
            client.write_all(
                b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            )?;
            return Ok(());
        }
    }

    // Drain the request body so the upstream is not left blocked.
    let mut remaining = head
        .lines()
        .find_map(|l| l.strip_prefix("content-length:"))
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let mut sink = [0u8; 1024];
    while remaining > 0 {
        let n = client.read(&mut sink)?;
        if n == 0 {
            break;
        }
        remaining = remaining.saturating_sub(n);
    }

    let mut up = TcpStream::connect(&upstream_host)?;
    up.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
    // Rewrite the Host header so the plain forward reaches the engine; keep the
    // request-line in origin-form (hyper rejects absolute-form).
    let rewritten = rewrite_host(&req, &upstream_host)?;
    up.write_all(&rewritten)?;
    up.flush()?;

    // ==== THE DELIBERATE DEFECT: read the ENTIRE response, then write it once. ====
    let mut resp = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match up.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                resp.extend_from_slice(&buf[..n]);
                // Stop at the chunked terminator so we do not wait out keep-alive.
                if resp.windows(5).any(|w| w == b"0\r\n\r\n") {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    client.write_all(&resp)?;
    client.flush()?;
    Ok(())
}

/// Keep the request-line in ORIGIN-FORM and rewrite only `Host:`.
///
/// hyper's HTTP/1 client REJECTS absolute-form (`POST http://host/path HTTP/1.1`)
/// on the server side, so forwarding absolute-form made this canary fail to
/// respond at all and the negative-control test could not reach it. Origin form
/// plus a correct Host header is what a real proxy does.
fn rewrite_host(req: &[u8], host: &str) -> std::io::Result<Vec<u8>> {
    let text = String::from_utf8_lossy(req).to_string();
    let (line, rest) = match text.split_once("\r\n") {
        Some(v) => v,
        None => return Ok(req.to_vec()),
    };
    // Drop any existing Host, then set ours.
    let kept: Vec<&str> = rest
        .lines()
        .filter(|l| !l.to_ascii_lowercase().starts_with("host:"))
        .collect();
    let mut out = format!("{line}\r\nHost: {host}\r\n");
    for l in kept {
        if l.is_empty() {
            break;
        }
        out.push_str(l);
        out.push_str("\r\n");
    }
    out.push_str("\r\n");
    Ok(out.into_bytes())
}

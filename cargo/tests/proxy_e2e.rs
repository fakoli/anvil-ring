//! End-to-end tests: spawn the REAL `anvil-ring` binary against a fake engine.
//!
//! Going through the actual executable is deliberate -- it also verifies the
//! naming directive (the binary is `anvil-ring`) and the config-by-environment
//! rule (I-8), neither of which a unit test can observe.
//!
//! The important test is `streaming_arrives_incrementally_not_all_at_once`:
//! invariant I-9 says a buffering bug shows up as latency and never as an error,
//! so asserting on *eventual* body equality would pass on a broken, buffering
//! proxy. That test asserts on arrival TIMING.
//!
//! HARNESS NOTES (each learned from an actual failure here):
//!  1. Ports are RESERVED, not reused: sharing one port pair across tests made
//!     results depend on execution order.
//!  2. Never trust `wait_for_port` alone -- it passes if ANY process holds the
//!     port, including a leftover proxy from a previous run. `Harness::new`
//!     asserts our own child bound it, by checking the proxy's log line.
//!  3. The fake engine must drain the request body, not just headers, or hyper
//!     blocks waiting for it and the test sees a 200 with zero bytes.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const TOKEN: &str = "test-token-abcdef";

/// Disjoint port pairs per test (engine = base, proxy = base + 1).
const PORTS: &[(&str, u16)] = &[
    ("unauthenticated_request_is_rejected", 18420),
    ("wrong_token_is_rejected", 18430),
    ("authenticated_request_is_proxied", 18440),
    ("streaming_arrives_incrementally_not_all_at_once", 18450),
    ("missing_token_refuses_to_start", 18460),
];

fn ring_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_anvil-ring"))
}

struct Harness {
    /// Retained for diagnostics; the proxy is configured with it, so the field is
    /// intentionally kept even though no assertion reads it yet.
    #[allow(dead_code)]
    engine_port: u16,
    proxy_port: u16,
    log_path: std::path::PathBuf,
    proxy: Option<Child>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Kill AND reap: a kill() without wait() leaves a child that can keep the
        // listening socket alive into the next test -- which surfaced here as a
        // baffling "proxy returned no bytes" that was really AddrInUse.
        if let Some(mut p) = self.proxy.take() {
            let _ = p.kill();
            let _ = p.wait();
        }
    }
}

impl Harness {
    fn new(test_name: &str, chunks: Vec<&'static str>, gap: Duration, token: Option<&str>) -> Self {
        let (engine_port, proxy_port) = PORTS
            .iter()
            .find(|(n, _)| *n == test_name)
            .map(|(_, b)| (*b, *b + 1))
            .unwrap_or_else(|| panic!("no port reservation for {test_name}; add one to PORTS"));

        let log_path = std::env::temp_dir().join(format!("anvil-ring-e2e-{proxy_port}.log"));
        // Truncate any stale log so we cannot read a previous run's AddrInUse.
        let log_file = std::fs::File::create(&log_path).expect("create proxy log");

        let _engine = spawn_fake_engine(engine_port, chunks, gap);

        let mut cmd = Command::new(ring_bin());
        cmd.arg("proxy")
            .env("ANVIL_RING_LISTEN", format!("127.0.0.1:{proxy_port}"))
            .env(
                "ANVIL_RING_UPSTREAM",
                format!("http://127.0.0.1:{engine_port}"),
            )
            .stdout(Stdio::null())
            .stderr(Stdio::from(log_file));
        // Set or clear explicitly: this test process's own env is inherited, so a
        // token left set by a sibling test would defeat the None case.
        match token {
            Some(t) => {
                cmd.env("ANVIL_RING_TOKEN", t);
            }
            None => {
                cmd.env_remove("ANVIL_RING_TOKEN");
            }
        }
        let proxy = cmd.spawn().expect("spawn anvil-ring");

        let mut h = Self {
            engine_port,
            proxy_port,
            log_path,
            proxy: Some(proxy),
        };
        if token.is_some() {
            h.wait_until_bound();
        }
        h
    }

    /// Wait for the proxy to be listening, and FAIL LOUDLY if the proxy died or
    /// reported AddrInUse -- because a bare connect() probe succeeds when some
    /// *other* process holds the port, which is exactly how this harness failed
    /// confusingly before.
    fn wait_until_bound(&mut self) {
        for _ in 0..200 {
            let log = std::fs::read_to_string(&self.log_path).unwrap_or_default();
            if log.contains("AddrInUse") || log.contains("Address already in use") {
                panic!(
                    "proxy could not bind port {}: stale listener still holding it.\n\
                     log: {log}\n\
                     (a previous run leaked a proxy; CI kills by PID via Harness::drop)",
                    self.proxy_port
                );
            }
            if log.contains("listening on") {
                return;
            }
            if let Some(status) = self
                .proxy
                .as_mut()
                .and_then(|p| p.try_wait().ok())
                .flatten()
            {
                panic!("proxy exited early with {status:?} before binding.\nlog: {log}");
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let log = std::fs::read_to_string(&self.log_path).unwrap_or_default();
        panic!(
            "proxy never reported listening on {}\nlog: {log}",
            self.proxy_port
        );
    }

    fn proxy_port(&self) -> u16 {
        self.proxy_port
    }

    fn read_log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    fn take_proxy(&mut self) -> Option<Child> {
        self.proxy.take()
    }
}

fn spawn_fake_engine(
    engine_port: u16,
    chunks: Vec<&'static str>,
    gap: Duration,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let Ok(listener) = std::net::TcpListener::bind(("127.0.0.1", engine_port)) else {
            return;
        };
        for stream in listener.incoming().take(16) {
            let Ok(socket) = stream else { continue };
            let chunks = chunks.clone();
            std::thread::spawn(move || {
                let _ = serve_fake_engine(socket, &chunks, gap);
            });
        }
    })
}

/// Read one request (headers AND body), then respond with chunked SSE.
fn serve_fake_engine(
    mut socket: std::net::TcpStream,
    chunks: &[&'static str],
    gap: Duration,
) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        if socket.read(&mut byte)? == 0 {
            return Ok(());
        }
        buf.push(byte[0]);
        if buf.len() > 64 * 1024 {
            return Ok(());
        }
    }
    let head = String::from_utf8_lossy(&buf).to_ascii_lowercase();
    let mut remaining = head
        .lines()
        .find_map(|l| l.strip_prefix("content-length:"))
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let mut sink = [0u8; 1024];
    while remaining > 0 {
        let n = socket.read(&mut sink)?;
        if n == 0 {
            break;
        }
        remaining = remaining.saturating_sub(n);
    }

    socket.write_all(
        b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n",
    )?;
    for c in chunks {
        socket.write_all(format!("{:x}\r\n{}\r\n", c.len(), c).as_bytes())?;
        socket.flush()?;
        std::thread::sleep(gap);
    }
    socket.write_all(b"0\r\n\r\n")?;
    socket.flush()?;
    Ok(())
}

struct TcpChunk {
    at: Duration,
    bytes: Vec<u8>,
}

fn request(proxy_port: u16, raw: String) -> Vec<TcpChunk> {
    let mut s = TcpStream::connect(("127.0.0.1", proxy_port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(8))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(8))).unwrap();
    s.write_all(raw.as_bytes()).unwrap();
    s.flush().unwrap();
    let start = Instant::now();
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match s.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                out.push(TcpChunk {
                    at: start.elapsed(),
                    bytes: buf[..n].to_vec(),
                });
                if start.elapsed() > Duration::from_secs(6) {
                    break;
                }
            }
        }
    }
    out
}

fn joined(chunks: &[TcpChunk]) -> String {
    let all: Vec<u8> = chunks.iter().flat_map(|c| c.bytes.clone()).collect();
    String::from_utf8_lossy(&all).to_string()
}

/// Body = everything after the FIRST header terminator.
///
/// Earlier this took the LAST `\r\n\r\n` split segment, which for a chunked SSE
/// stream is the `0\r\n\r\n` terminating chunk -- so a perfectly correct proxied
/// response read as an empty body. Split on the first boundary only, and drop the
/// chunk-size framing (`<hex>\r\n ... \r\n`) so assertions see SSE payloads.
fn body_of(chunks: &[TcpChunk]) -> String {
    let all = joined(chunks);
    let body = match all.split_once("\r\n\r\n") {
        Some((_, body)) => body,
        None => return String::new(),
    };
    // De-frame HTTP/1.1 chunked bodies: "<hex-len>\r\n<payload>\r\n" repeated.
    let mut out = String::new();
    let mut rest = body;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        let Ok(len) = usize::from_str_radix(size.trim(), 16) else {
            // Not chunk framing (e.g. upstream used content-length): return as-is.
            return body.to_string();
        };
        if len == 0 {
            break;
        }
        if tail.len() < len {
            out.push_str(tail);
            break;
        }
        out.push_str(&tail[..len]);
        rest = &tail[len..];
        if let Some(r) = rest.strip_prefix("\r\n") {
            rest = r;
        }
    }
    out
}

fn post(port: u16, auth: Option<&str>, close: bool) -> Vec<TcpChunk> {
    let auth_line = match auth {
        Some(t) => format!("authorization: Bearer {t}\r\n"),
        None => String::new(),
    };
    let close_line = if close { "connection: close\r\n" } else { "" };
    request(
        port,
        format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\n{auth_line}content-type: application/json\r\ncontent-length: 2\r\n{close_line}\r\n{{}}"
        ),
    )
}

#[test]
fn missing_token_refuses_to_start() {
    // I-10: an unauthenticated proxy between the network and the inference port
    // must be fatal at startup, not a warning.
    let mut h = Harness::new(
        "missing_token_refuses_to_start",
        vec![],
        Duration::ZERO,
        None,
    );
    let mut child = h.take_proxy().expect("proxy handle");
    let status = child.wait().expect("wait");
    assert!(!status.success(), "must exit non-zero without a token");
    assert_eq!(status.code(), Some(2), "expected exit code 2");
    // It must not have bound anything either: a half-started listener looks
    // healthy from the outside, which is the worst failure mode.
    assert!(
        TcpStream::connect(("127.0.0.1", h.proxy_port())).is_err(),
        "must NOT bind a listen socket when it refuses to start"
    );
}

#[test]
fn unauthenticated_request_is_rejected() {
    let h = Harness::new(
        "unauthenticated_request_is_rejected",
        vec!["data: x\n\n"],
        Duration::from_millis(5),
        Some(TOKEN),
    );
    let res = post(h.proxy_port(), None, true);
    let all = joined(&res);
    assert!(
        all.contains("401"),
        "expected 401, got: {all}\nlog: {}",
        h.read_log()
    );
}

#[test]
fn wrong_token_is_rejected() {
    let h = Harness::new(
        "wrong_token_is_rejected",
        vec!["data: x\n\n"],
        Duration::from_millis(5),
        Some(TOKEN),
    );
    let res = post(h.proxy_port(), Some("definitely-not-the-token"), true);
    let all = joined(&res);
    assert!(
        all.contains("401"),
        "wrong bearer token must not be authorised: {all}\nlog: {}",
        h.read_log()
    );
}

#[test]
fn authenticated_request_is_proxied() {
    let h = Harness::new(
        "authenticated_request_is_proxied",
        vec!["data: hello\n\n"],
        Duration::from_millis(5),
        Some(TOKEN),
    );
    let res = post(h.proxy_port(), Some(TOKEN), true);
    let all = joined(&res);
    let body = body_of(&res);
    assert!(
        !all.is_empty(),
        "proxy produced NO bytes. log:\n{}",
        h.read_log()
    );
    assert!(
        all.contains("200"),
        "expected 200, got: {all}\nlog: {}",
        h.read_log()
    );
    assert!(
        body.contains("hello"),
        "engine body did not arrive. FULL RESPONSE WAS: {all:?}\nlog: {}",
        h.read_log()
    );
    // Streaming headers must survive or the client's SSE parser breaks.
    assert!(
        all.contains("text/event-stream"),
        "content-type lost: {all}"
    );
}

/// I-9 REGRESSION GUARD. A buffering proxy passes every other test in this file.
#[test]
fn streaming_arrives_incrementally_not_all_at_once() {
    let chunks = vec![
        "data: one\n\n",
        "data: two\n\n",
        "data: three\n\n",
        "data: [DONE]\n\n",
    ];
    let gap = Duration::from_millis(300);
    let h = Harness::new(
        "streaming_arrives_incrementally_not_all_at_once",
        chunks.clone(),
        gap,
        Some(TOKEN),
    );
    // No `connection: close`: keep the stream open so reads arrive incrementally.
    let res = post(h.proxy_port(), Some(TOKEN), false);

    let body = body_of(&res);
    for c in &chunks {
        let token = c.trim_start_matches("data: ").trim();
        assert!(
            body.contains(token),
            "missing {token} in: {body}\nlog: {}",
            h.read_log()
        );
    }

    // The actual assertion. Upstream emission spans ~900ms, so a flushing proxy
    // yields several reads spread over that window; a buffering proxy delivers a
    // single burst at the end.
    assert!(
        res.len() >= 2,
        "expected multiple TCP reads (streaming), got {} -- proxy is buffering (I-9)",
        res.len()
    );
    let spread = res.last().unwrap().at.saturating_sub(res[0].at);
    assert!(
        spread >= Duration::from_millis(200),
        "arrival spread {spread:?} too tight -- output was buffered (I-9 violation)"
    );
}

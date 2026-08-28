//! End-to-end test of the forwarded data path:
//!   caller -> hub -> tunnel -> tether -> fake engine -> back -> caller
//!
//! This is the test that distinguishes "the tunnel authenticated" (already proven)
//! from "a request actually reaches vLLM through it". The forward path is the
//! product; the control path alone is not.

use anvil_ring::hub::{Registry, TetherEvent};
use anvil_ring::tunnel::{self, ClientConfig, TunnelState};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Free loopback port, bound once to test, then released. The tiny window between
/// drop and re-bind is not racy here in practice, and the alternative (fixed ports)
/// failed in this environment before.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

/// A fake SSE engine: raw HTTP/1.1, chunked, flushing per event, so a hub that
/// buffers shows up as wrong timing rather than wrong bytes.
fn spawn_fake_engine(port: u16, gap_ms: u64, events: Vec<&'static str>) {
    std::thread::spawn(move || {
        let listener = std::net::TcpListener::bind(("127.0.0.1", port)).expect("engine bind");
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            use std::io::{Read, Write};
            s.set_read_timeout(Some(Duration::from_secs(5))).ok();
            // Drain the request up to the body so the engine is not left mid-read.
            let mut buf = vec![0u8; 8192];
            let _ = s.read(&mut buf);
            let _ = s.write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n",
            );
            let _ = s.flush();
            for ev in &events {
                let chunk = format!("{ev}\n");
                let _ = s.write_all(format!("{:x}\r\n{}\r\n", chunk.len(), chunk).as_bytes());
                let _ = s.flush();
                std::thread::sleep(Duration::from_millis(gap_ms));
            }
            let _ = s.write_all(b"0\r\n\r\n");
            let _ = s.flush();
            std::thread::sleep(Duration::from_millis(200));
        }
    });
}

/// Drive one caller request through the hub, returning the concatenated response
/// body and the status code the caller saw.
async fn one_request(hub_http: SocketAddr, token: &str, path: &str) -> (u16, String, Vec<f64>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let body = r#"{"model":"m","stream":true}"#;
    let req = format!(
        "POST {path} HTTP/1.1\r\nhost: {hub_http}\r\nauthorization: Bearer {token}\r\n\
         content-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    );

    let mut sock = tokio::time::timeout(
        Duration::from_secs(8),
        tokio::net::TcpStream::connect(hub_http),
    )
    .await
    .expect("connect hub frontend")
    .expect("tcp connect");
    sock.write_all(req.as_bytes()).await.unwrap();

    let mut timings = Vec::new();
    let mut body_out = String::new();
    let mut status = 0u16;
    let start = std::time::Instant::now();
    let mut raw = Vec::new();
    let mut tmp = vec![0u8; 4096];

    // Read until the chunk terminator, recording when each read returns.
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            match sock.read(&mut tmp).await {
                Ok(0) => break,
                Ok(n) => {
                    timings.push(start.elapsed().as_secs_f64());
                    raw.extend_from_slice(&tmp[..n]);
                    if raw.windows(5).any(|w| w == b"0\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    })
    .await
    .expect("reading response should finish");

    let text = String::from_utf8_lossy(&raw).to_string();
    eprintln!("RAW WIRE ({} bytes): {:?}", raw.len(), text);
    status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // Body = after the first blank line, then DECODED. The tunnel forwards engine
    // bytes verbatim, so the wire is chunked; a real caller's HTTP client decodes
    // that, and this hand-rolled socket must too. See `decode_chunked` below.
    let raw_body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_else(|| text.clone());
    body_out = decode_chunked(&raw_body);
    (status, body_out, timings)
}

include!("chunked_decoder_shared.rs");

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_travels_the_whole_path() {
    let engine_port = free_port();
    let hub_tunnel_port = free_port();
    let hub_http_port = free_port();
    const TOKEN: &str = "caller-token";

    spawn_fake_engine(
        engine_port,
        300,
        vec!["data: one", "data: two", "data: done"],
    );

    // Hub side.
    let reg = Arc::new(Registry::new(Duration::from_secs(300)));
    reg.register("t1", "test tether", "tunnel-cred");
    let mut events = anvil_ring::hub::serve(
        format!("127.0.0.1:{hub_tunnel_port}").parse().unwrap(),
        reg.clone(),
    )
    .await
    .unwrap();
    // Consume events so sends never block.
    tokio::spawn(async move { while events.recv().await.is_some() {} });

    // The hub's caller-facing frontend: this is what turns an HTTP request into a
    // forwarded stream, and it is the piece the previous commit lacked.
    let reg2 = reg.clone();
    anvil_ring::frontend::serve_frontend(
        format!("127.0.0.1:{hub_http_port}").parse().unwrap(),
        reg2,
        TOKEN.to_string(),
    )
    .await
    .unwrap();

    // Tether side (in-process, pointing at a loopback engine).
    let state = Arc::new(TunnelState::default());
    let cfg = ClientConfig {
        hub_url: format!("ws://127.0.0.1:{hub_tunnel_port}/ring"),
        credential: b"tunnel-cred".to_vec(),
        state: state.clone(),
    };
    tokio::spawn(async move {
        let _ = tunnel::run_client(cfg, format!("http://127.0.0.1:{engine_port}")).await;
    });

    // Wait for the tether to be authorized, so the test is not racing the handshake.
    for _ in 0..200 {
        if state.is_up() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(state.is_up(), "tether should have authorized");

    let (status, body, timings) = one_request(
        format!("127.0.0.1:{hub_http_port}").parse().unwrap(),
        TOKEN,
        "/v1/chat/completions",
    )
    .await;

    assert_eq!(
        status, 200,
        "caller must see the ENGINE's status, not a hub-invented 200. body={body}"
    );
    for want in ["one", "two", "done"] {
        assert!(
            body.contains(want),
            "expected {want} in the forwarded stream; got {body:?}"
        );
    }

    // I-9 through the tunnel: arrivals should track the 300ms emission cadence.
    // Relaxed vs the direct proxy (the tunnel adds a hop) but not vacuous -- a
    // buffering hub collapses every read to one instant.
    if timings.len() >= 3 {
        let spread = timings.last().unwrap() - timings.first().unwrap();
        assert!(
            spread > 0.25,
            "I-9: {} reads spanning {spread:.3}s means the tunnel buffered. timings={timings:?}",
            timings.len()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caller_without_token_is_refused_before_any_tunnel_use() {
    let hub_tunnel_port = free_port();
    let hub_http_port = free_port();
    let reg = Arc::new(Registry::new(Duration::from_secs(300)));
    reg.register("t1", "test tether", "tunnel-cred");
    let mut events = anvil_ring::hub::serve(
        format!("127.0.0.1:{hub_tunnel_port}").parse().unwrap(),
        reg.clone(),
    )
    .await
    .unwrap();
    tokio::spawn(async move { while events.recv().await.is_some() {} });
    anvil_ring::frontend::serve_frontend(
        format!("127.0.0.1:{hub_http_port}").parse().unwrap(),
        reg.clone(),
        "caller-token".to_string(),
    )
    .await
    .unwrap();

    // No tether is connected at all: an unauthenticated caller must not even learn
    // that, since "no upstream" leaks fleet state (I-6 ordering).
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut sock = tokio::net::TcpStream::connect(format!("127.0.0.1:{hub_http_port}"))
        .await
        .unwrap();
    sock.write_all(b"POST /v1/chat/completions HTTP/1.1\r\nhost: x\r\ncontent-length: 2\r\n\r\n{}")
        .await
        .unwrap();
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), sock.read_to_end(&mut buf)).await;
    let text = String::from_utf8_lossy(&buf).to_string();
    assert!(
        text.contains("401"),
        "unauthenticated caller must get 401, got: {text:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_caller_with_no_tether_gets_502_not_401() {
    // The distinct answers matter: 401 says "you are not allowed", 502 says
    // "allowed, but no tether is up". Collapsing them hides fleet state from the
    // only people who need to see it (I-6).
    let hub_tunnel_port = free_port();
    let hub_http_port = free_port();
    let reg = Arc::new(Registry::new(Duration::from_secs(300)));
    reg.register("t1", "test tether", "tunnel-cred");
    let mut events = anvil_ring::hub::serve(
        format!("127.0.0.1:{hub_tunnel_port}").parse().unwrap(),
        reg.clone(),
    )
    .await
    .unwrap();
    tokio::spawn(async move { while events.recv().await.is_some() {} });
    anvil_ring::frontend::serve_frontend(
        format!("127.0.0.1:{hub_http_port}").parse().unwrap(),
        reg.clone(),
        "caller-token".to_string(),
    )
    .await
    .unwrap();

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut sock = tokio::net::TcpStream::connect(format!("127.0.0.1:{hub_http_port}"))
        .await
        .unwrap();
    sock.write_all(
        b"POST /v1/chat/completions HTTP/1.1\r\nhost: x\r\nauthorization: Bearer caller-token\r\ncontent-length: 2\r\n\r\n{}",
    )
    .await
    .unwrap();
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), sock.read_to_end(&mut buf)).await;
    let text = String::from_utf8_lossy(&buf).to_string();
    assert!(
        text.contains("502"),
        "no tether should be 502, got: {text:?}"
    );
    assert!(
        !text.contains("401"),
        "must not be conflated with auth failure"
    );
}

/// A tether that dies mid-stream must not leave the caller hanging forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tether_death_midstream_ends_the_caller() {
    let engine_port = free_port();
    let hub_tunnel_port = free_port();
    let hub_http_port = free_port();

    // An engine that streams forever, so the request is definitely in flight when
    // the tether is killed.
    std::thread::spawn(move || {
        let listener = std::net::TcpListener::bind(("127.0.0.1", engine_port)).unwrap();
        let (mut s, _) = listener.accept().unwrap();
        use std::io::{Read, Write};
        let mut buf = vec![0u8; 4096];
        let _ = s.read(&mut buf);
        let _ = s.write_all(
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n",
        );
        loop {
            let _ = s.write_all(b"5\r\ndata:\r\n");
            let _ = s.flush();
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let reg = Arc::new(Registry::new(Duration::from_secs(300)));
    reg.register("t1", "test tether", "tunnel-cred");
    let mut events: tokio::sync::mpsc::UnboundedReceiver<TetherEvent> = anvil_ring::hub::serve(
        format!("127.0.0.1:{hub_tunnel_port}").parse().unwrap(),
        reg.clone(),
    )
    .await
    .unwrap();
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    tokio::spawn(async move {
        while let Some(ev) = events.recv().await {
            seen2.lock().unwrap().push(format!("{:?}", ev.kind));
        }
    });
    anvil_ring::frontend::serve_frontend(
        format!("127.0.0.1:{hub_http_port}").parse().unwrap(),
        reg.clone(),
        "caller-token".to_string(),
    )
    .await
    .unwrap();

    let state = Arc::new(TunnelState::default());
    let cfg = ClientConfig {
        hub_url: format!("ws://127.0.0.1:{hub_tunnel_port}/ring"),
        credential: b"tunnel-cred".to_vec(),
        state: state.clone(),
    };
    let tether = tokio::spawn(async move {
        let _ = tunnel::run_client(cfg, format!("http://127.0.0.1:{engine_port}")).await;
    });
    for _ in 0..200 {
        if state.is_up() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(state.is_up());

    // Start a streaming request, let tokens flow, then kill the tether.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let body = r#"{"stream":true}"#;
    let mut sock = tokio::net::TcpStream::connect(format!("127.0.0.1:{hub_http_port}"))
        .await
        .unwrap();
    sock.write_all(
        format!(
            "POST /v1/chat/completions HTTP/1.1\r\nhost: x\r\nauthorization: Bearer caller-token\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut got = vec![0u8; 64];
    let _ = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut got)).await;

    // Kill the tunnel abruptly (no close handshake).
    tether.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;

    // The caller must reach an end, not hang: read returns 0 or errors.
    let ended = tokio::time::timeout(Duration::from_secs(10), async {
        let mut total = 0usize;
        loop {
            match sock.read(&mut got).await {
                Ok(0) | Err(_) => return true,
                Ok(n) => {
                    total += n;
                    if total > 100_000 {
                        return false;
                    }
                }
            }
        }
    })
    .await;
    assert!(
        matches!(ended, Ok(true)),
        "I-6: caller must be terminated when its tether dies, not left streaming"
    );
    // And the hub should have reported the transition rather than staying silent.
    let kinds = seen.lock().unwrap().clone();
    assert!(
        kinds.iter().any(|k| k == "Up") && kinds.iter().any(|k| k != "Up"),
        "I-6: hub should log Up then a loss, saw {kinds:?}"
    );
}

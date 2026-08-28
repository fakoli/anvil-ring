//! I-6 under a LEAKED socket: when the tether's task dies but its TCP connection
//! stays open, what does the caller observe, and when?
//!
//! `tunnel::run_client` does `ws.split()` (tunnel.rs), so the writer task owns the
//! sink half. `JoinHandle::abort()` cancels the client task only; the writer keeps
//! the socket open, so the HUB CORRECTLY sees a live connection and receives only
//! keepalives (measured: 74x `stream.next -> Some(Ok(..))`, no None, no Err).
//!
//! That makes this the designed-for case for the liveness watchdog, not a socket
//! close. So the real I-6 contract is: within TETHER_SILENCE, the hub notices, ends
//! the session, and ends every stream it was serving.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anvil_ring::frontend::serve_frontend;
use anvil_ring::hub::{serve, Registry, TetherEvent, TETHER_SILENCE};
use anvil_ring::tunnel::{run_client, ClientConfig, TunnelState};

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caller_terminates_within_the_liveness_window() {
    let engine_port = free_port();
    let hub_tunnel_port = free_port();
    let hub_http_port = free_port();

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
    let mut events: tokio::sync::mpsc::UnboundedReceiver<TetherEvent> = serve(
        format!("127.0.0.1:{hub_tunnel_port}").parse().unwrap(),
        reg.clone(),
    )
    .await
    .unwrap();
    tokio::spawn(async move { while events.recv().await.is_some() {} });
    serve_frontend(
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
        let _ = run_client(cfg, format!("http://127.0.0.1:{engine_port}")).await;
    });
    for _ in 0..200 {
        if state.is_up() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(state.is_up());

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let body = r#"{"stream":true}"#;
    let mut sock = tokio::net::TcpStream::connect(format!("127.0.0.1:{hub_http_port}"))
        .await
        .unwrap();
    sock.write_all(
        format!(
            "POST /v1/chat/completions HTTP/1.1\r\nhost: x\r\nauthorization: Bearer caller-token\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut got = vec![0u8; 64];
    let _ = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut got)).await;

    // Kill the tether's task. Its writer task keeps the sink half alive (the
    // socket was split in run_client), so the TCP connection SURVIVES -- which is
    // the case this probe exists to characterize.
    //
    // NOT COVERED HERE, and deliberately so: the crashed-process case, where both
    // socket halves go away and the peer gets a real RST. That path IS detected --
    // the hub logs 'Connection reset without closing handshake' and, since the
    // decode fix, treats it as terminal rather than swallowing it. A test for it
    // needs control of the client socket (e.g. SO_LINGER=0 on drop), which this
    // harness cannot reach because run_client owns the connection. Wiring that up
    // is worth doing before claiming RST coverage; until then this file should not
    // be read as covering it.
    tether.abort();
    drop(tether);

    let started = Instant::now();
    let mut total = 0usize;
    let ended = tokio::time::timeout(TETHER_SILENCE + Duration::from_secs(20), async {
        loop {
            match sock.read(&mut got).await {
                Ok(0) | Err(_) => break,
                Ok(n) => total += n,
            }
        }
    })
    .await;

    let secs = started.elapsed().as_secs_f64();
    eprintln!("I-6 liveness measurement: ended={ended:?} after {secs:.1}s total_bytes={total}");
    assert!(
        ended.is_ok(),
        "I-6: caller never terminated even after the liveness window ({secs:.1}s, \
         TETHER_SILENCE={TETHER_SILENCE:?}) -- the hub is not ending streams when a \
         tether's socket leaks"
    );
    assert!(
        secs <= TETHER_SILENCE.as_secs_f64() + 12.0,
        "I-6: caller terminated only after {secs:.1}s, far beyond the {TETHER_SILENCE:?} \
         liveness window -- a caller should not wait much longer than the watchdog"
    );
}

//! Full-path HTTP contracts. Fake engines control only their wire behavior;
//! the caller, hub, tether, frame codec, and HTTP body bridge are real.
use anvil_ring::{frontend, hub, tunnel};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

struct Topology {
    frontend: SocketAddr,
    registry: Arc<hub::Registry>,
    tethers: Vec<tokio::task::JoinHandle<()>>,
}
impl Drop for Topology {
    fn drop(&mut self) {
        for tether in &self.tethers {
            tether.abort();
        }
    }
}
impl Topology {
    async fn start(engine: SocketAddr) -> Self {
        Self::start_pool(engine, 1).await
    }

    async fn start_pool(engine: SocketAddr, count: usize) -> Self {
        let registry = Arc::new(hub::Registry::new(Duration::from_secs(300)));
        for index in 1..=count {
            registry.register(&format!("t{index}"), "test", &format!("credential-{index}"));
        }
        let hub_addr = free_addr();
        let mut events = hub::serve(hub_addr, registry.clone()).await.unwrap();
        let frontend = free_addr();
        frontend::serve_frontend(frontend, registry.clone(), "caller".into())
            .await
            .unwrap();
        let tethers = (1..=count)
            .map(|index| {
                let config = tunnel::ClientConfig {
                    hub_url: format!("ws://{hub_addr}/ring"),
                    credential: format!("credential-{index}").into_bytes(),
                    state: Arc::new(tunnel::TunnelState::default()),
                };
                tokio::spawn(async move {
                    let _ = tunnel::run_client(config, format!("http://{engine}")).await;
                })
            })
            .collect();
        tokio::time::timeout(Duration::from_secs(3), async {
            while registry
                .status()
                .iter()
                .filter(|(_, _, state)| matches!(state, hub::TetherState::Up(_)))
                .count()
                != count
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("tether attached");
        tokio::spawn(async move { while events.recv().await.is_some() {} });
        Self {
            frontend,
            registry,
            tethers,
        }
    }

    async fn request(&self) -> hyper::Response<hyper::body::Incoming> {
        let socket = TcpStream::connect(self.frontend).await.unwrap();
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(socket))
                .await
                .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        sender
            .send_request(
                hyper::Request::builder()
                    .uri("/v1/models")
                    .header("host", "caller.example")
                    .header("authorization", "Bearer caller")
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }
}

async fn engine(
    parts: Vec<&'static [u8]>,
    keep_open: bool,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        let mut byte = [0];
        while !head.ends_with(b"\r\n\r\n") {
            socket.read_exact(&mut byte).await.unwrap();
            head.push(byte[0]);
        }
        for part in parts {
            if socket.write_all(part).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if keep_open {
            std::future::pending::<()>().await;
        }
    });
    (addr, task)
}

async fn check_response(parts: Vec<&'static [u8]>, keep_open: bool, expected: &[u8]) {
    let (addr, task) = engine(parts, keep_open).await;
    let topology = Topology::start(addr).await;
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let response = topology.request().await;
        assert_eq!(response.status(), 200);
        response.into_body().collect().await.map(|b| b.to_bytes())
    })
    .await;
    task.abort();
    assert_eq!(
        result
            .expect("HTTP completion must not wait for TCP EOF")
            .unwrap()
            .as_ref(),
        expected
    );
}

#[tokio::test]
async fn fragmented_chunked_head_never_leaks_framing() {
    check_response(
        vec![
            b"HTTP/1.1 200 OK\r\ntransfer-enc",
            b"oding: chunked\r\n\r\n",
            b"2\r\nok\r\n0\r\n\r\n",
        ],
        false,
        b"ok",
    )
    .await;
}

#[tokio::test]
async fn fixed_length_head_and_body_in_one_read_preserve_only_body() {
    check_response(
        vec![b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok"],
        false,
        b"ok",
    )
    .await;
}

#[tokio::test]
async fn chunked_completion_does_not_wait_for_keep_alive_eof() {
    check_response(
        vec![
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n",
            b"2\r\nok\r\n",
            b"0\r\n\r\n",
        ],
        true,
        b"ok",
    )
    .await;
}

#[tokio::test]
async fn truncated_chunked_body_is_an_http_error() {
    let (addr, task) = engine(
        vec![
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n",
            b"2\r\nok\r\n",
        ],
        false,
    )
    .await;
    let topology = Topology::start(addr).await;
    let response = topology.request().await;
    assert_eq!(response.status(), 200);
    let result = tokio::time::timeout(Duration::from_secs(2), response.into_body().collect())
        .await
        .unwrap();
    task.await.unwrap();
    assert!(
        result.is_err(),
        "missing last chunk is not successful HTTP completion"
    );
}

#[tokio::test]
async fn revocation_interrupts_a_live_response() {
    let (addr, task) = engine(
        vec![
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n",
            b"2\r\nok\r\n",
        ],
        true,
    )
    .await;
    let topology = Topology::start(addr).await;
    let mut body = topology.request().await.into_body();
    assert_eq!(
        body.frame().await.unwrap().unwrap().into_data().unwrap(),
        "ok"
    );
    topology.registry.revoke("t1");
    let result = tokio::time::timeout(Duration::from_secs(2), body.collect()).await;
    task.abort();
    assert!(result
        .expect("revocation must wake a live response immediately")
        .is_err());
}

// Runs only in the child launched below. It uses the real tether session with
// abortive socket close enabled so a killed process produces a kernel TCP reset.
#[tokio::test]
async fn crash_fixture() {
    let Ok(hub_url) = std::env::var("ANVIL_RING_TEST_CRASH_HUB") else {
        return;
    };
    let witness = TcpStream::connect(std::env::var("ANVIL_RING_TEST_CRASH_WITNESS").unwrap())
        .await
        .unwrap();
    socket2::SockRef::from(&witness)
        .set_linger(Some(Duration::ZERO))
        .unwrap();
    let address = hub_url
        .strip_prefix("ws://")
        .unwrap()
        .strip_suffix("/ring")
        .unwrap();
    let socket = TcpStream::connect(address).await.unwrap();
    socket2::SockRef::from(&socket)
        .set_linger(Some(Duration::ZERO))
        .unwrap();
    let (ws, _) =
        tokio_tungstenite::client_async(&hub_url, tokio_tungstenite::MaybeTlsStream::Plain(socket))
            .await
            .unwrap();
    let config = tunnel::ClientConfig {
        hub_url,
        credential: b"credential".to_vec(),
        state: Arc::new(tunnel::TunnelState::default()),
    };
    let _ = tunnel::serve_over(
        ws,
        &config,
        &std::env::var("ANVIL_RING_TEST_CRASH_ENGINE").unwrap(),
    )
    .await;
    drop(witness);
}

struct ChildGuard(std::process::Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn killed_tether_process_resets_tcp_and_fails_inflight_http() {
    let (engine, task) = engine(
        vec![
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n",
            b"2\r\nok\r\n",
        ],
        true,
    )
    .await;
    let registry = Arc::new(hub::Registry::new(Duration::from_secs(300)));
    registry.register("t1", "crash test", "credential");
    let hub_addr = free_addr();
    let mut events = hub::serve(hub_addr, registry.clone()).await.unwrap();
    let frontend_addr = free_addr();
    frontend::serve_frontend(frontend_addr, registry.clone(), "caller".into())
        .await
        .unwrap();
    let witness = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut child = ChildGuard(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_fixture", "--nocapture"])
            .env("ANVIL_RING_TEST_CRASH_HUB", format!("ws://{hub_addr}/ring"))
            .env("ANVIL_RING_TEST_CRASH_ENGINE", format!("http://{engine}"))
            .env(
                "ANVIL_RING_TEST_CRASH_WITNESS",
                witness.local_addr().unwrap().to_string(),
            )
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let (mut witness, _) = tokio::time::timeout(Duration::from_secs(5), witness.accept())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap()
            .kind,
        hub::EventKind::Up
    );
    let topology = Topology {
        frontend: frontend_addr,
        registry,
        tethers: vec![tokio::spawn(async {})],
    };
    let mut body = topology.request().await.into_body();
    assert_eq!(
        body.frame().await.unwrap().unwrap().into_data().unwrap(),
        "ok"
    );
    child.0.kill().unwrap();
    assert!(
        !child.0.wait().unwrap().success(),
        "child must be killed, not exit normally"
    );
    let mut byte = [0];
    let reset = tokio::time::timeout(Duration::from_secs(2), witness.read(&mut byte))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        reset.kind(),
        std::io::ErrorKind::ConnectionReset,
        "the fixture must produce a real TCP RST"
    );
    let result = tokio::time::timeout(Duration::from_secs(2), body.collect()).await;
    task.abort();
    assert!(
        result
            .expect("a crashed tether cannot strand the caller")
            .is_err(),
        "partial output cannot be complete"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap()
            .kind,
        hub::EventKind::Down
    );
    assert!(matches!(
        topology.registry.status()[0].2,
        hub::TetherState::Down
    ));
}

#[tokio::test]
async fn response_header_bytes_and_repeated_values_are_preserved() {
    let (addr, task) = engine(vec![b"HTTP/1.1 429 Too Many Requests\r\nx-note: \xe9\r\nset-cookie: a=1\r\nset-cookie: b=2\r\ncontent-length: 2\r\n\r\nno"], false).await;
    let topology = Topology::start(addr).await;
    let response = tokio::time::timeout(Duration::from_secs(1), topology.request()).await;
    task.abort();
    let response = response.expect("valid HTTP header bytes must not strand the response head");
    assert_eq!(response.status(), 429);
    assert_eq!(response.headers()["x-note"].as_bytes(), b"\xe9");
    assert_eq!(response.headers().get_all("set-cookie").iter().count(), 2);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "no"
    );
}

#[tokio::test]
async fn hub_expires_a_lease_even_when_the_tether_keeps_sending_pongs() {
    use anvil_ring::frames::Frame;
    use futures_util::{SinkExt, StreamExt};
    let registry = Arc::new(hub::Registry::new(Duration::from_secs(5)));
    registry.register("t1", "lease test", "credential");
    let addr = free_addr();
    let mut events = hub::serve(addr, registry.clone()).await.unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ring"))
        .await
        .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(
        Frame::Hello {
            credential: b"credential".to_vec(),
        }
        .encode(),
    ))
    .await
    .unwrap();
    assert!(ws.next().await.unwrap().is_ok());
    assert_eq!(events.recv().await.unwrap().kind, hub::EventKind::Up);
    let peer = tokio::spawn(async move {
        loop {
            if ws
                .send(tokio_tungstenite::tungstenite::Message::Binary(
                    Frame::Pong.encode(),
                ))
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let end = tokio::time::timeout(Duration::from_secs(6), events.recv()).await;
    peer.abort();
    assert_eq!(
        end.expect("hub must enforce its lease without trusting a tether watchdog")
            .unwrap()
            .kind,
        hub::EventKind::Down
    );
    assert!(
        registry.authorize(b"credential").is_some(),
        "lease expiry permits fresh authorization"
    );
}

async fn start_stalled_upload(topology: &Topology) -> TcpStream {
    let mut socket = TcpStream::connect(topology.frontend).await.unwrap();
    socket.write_all(b"POST /v1/chat/completions HTTP/1.1\r\nhost: caller\r\nauthorization: Bearer caller\r\ncontent-length: 100\r\n\r\n").await.unwrap();
    socket
}

async fn response_status(socket: &mut TcpStream) -> u16 {
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut line = Vec::new();
        let mut byte = [0];
        while !line.ends_with(b"\r\n") {
            socket.read_exact(&mut byte).await.unwrap();
            line.push(byte[0]);
        }
        String::from_utf8(line)
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap()
    })
    .await
    .expect("response must not wait for the caller to finish uploading")
}

#[tokio::test]
async fn early_engine_rejection_does_not_wait_for_request_body() {
    let (addr, task) = engine(
        vec![b"HTTP/1.1 413 Content Too Large\r\ncontent-length: 0\r\n\r\n"],
        false,
    )
    .await;
    let topology = Topology::start(addr).await;
    let mut socket = start_stalled_upload(&topology).await;
    let status = response_status(&mut socket).await;
    task.abort();
    assert_eq!(status, 413);
}

#[tokio::test]
async fn tether_failure_does_not_wait_for_request_body() {
    let (addr, task) = engine(vec![], true).await;
    let topology = Topology::start(addr).await;
    let mut socket = start_stalled_upload(&topology).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    topology.tethers[0].abort();
    let status = response_status(&mut socket).await;
    task.abort();
    assert_eq!(status, 502);
}

#[tokio::test]
async fn chunked_get_upload_reaches_the_engine_with_end_to_end_headers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        hyper::server::conn::http1::Builder::new()
            .serve_connection(
                hyper_util::rt::TokioIo::new(socket),
                hyper::service::service_fn(
                    move |req: hyper::Request<hyper::body::Incoming>| async move {
                        assert_eq!(req.headers()["host"], addr.to_string());
                        assert_eq!(req.headers()["x-request-id"], "trace-123");
                        assert!(!req.headers().contains_key("x-private"));
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(body)))
                    },
                ),
            )
            .await
            .ok();
    });
    let topology = Topology::start(addr).await;
    let (mut sender, connection) = hyper::client::conn::http1::handshake(
        hyper_util::rt::TokioIo::new(TcpStream::connect(topology.frontend).await.unwrap()),
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let request = hyper::Request::builder()
        .method("GET")
        .uri("/echo")
        .header("host", "caller.example")
        .header("authorization", "Bearer caller")
        .header("transfer-encoding", "chunked")
        .header("connection", "x-private")
        .header("x-private", "never-forward")
        .header("x-request-id", "trace-123")
        .body(Full::new(Bytes::from_static(b"abc")))
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        sender
            .send_request(request)
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
    })
    .await;
    task.abort();
    assert_eq!(result.unwrap(), "abc");
}

#[tokio::test]
async fn unsupported_transfer_coding_is_not_relabelled_as_plain_body() {
    let (addr, task) = engine(
        vec![b"HTTP/1.1 200 OK\r\ntransfer-encoding: gzip, chunked\r\n\r\n3\r\nzip\r\n0\r\n\r\n"],
        false,
    )
    .await;
    let topology = Topology::start(addr).await;
    let response = topology.request().await;
    task.abort();
    assert_eq!(response.status(), 502);
}

#[tokio::test]
async fn hub_lease_expiry_interrupts_a_blocked_websocket_writer() {
    use anvil_ring::frames::Frame;
    use futures_util::{SinkExt, StreamExt};
    let registry = Arc::new(hub::Registry::new(Duration::from_secs(5)));
    registry.register("t1", "blocked writer", "credential");
    let hub_addr = free_addr();
    let mut events = hub::serve(hub_addr, registry.clone()).await.unwrap();
    let frontend = free_addr();
    frontend::serve_frontend(frontend, registry, "caller".into())
        .await
        .unwrap();
    let socket = TcpStream::connect(hub_addr).await.unwrap();
    socket2::SockRef::from(&socket)
        .set_recv_buffer_size(4096)
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::client_async(format!("ws://{hub_addr}/ring"), socket)
        .await
        .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(
        Frame::Hello {
            credential: b"credential".to_vec(),
        }
        .encode(),
    ))
    .await
    .unwrap();
    ws.next().await.unwrap().unwrap();
    assert_eq!(events.recv().await.unwrap().kind, hub::EventKind::Up);
    // Hold the peer socket open without reading any OPEN/DATA/PING frames.
    let mut caller = TcpStream::connect(frontend).await.unwrap();
    caller.write_all(b"POST /v1/test HTTP/1.1\r\nhost: caller\r\nauthorization: Bearer caller\r\ncontent-length: 134217728\r\n\r\n").await.unwrap();
    let (mut reader, mut writer) = caller.into_split();
    let upload = tokio::spawn(async move {
        let bytes = vec![b'x'; 65536];
        for _ in 0..2048 {
            if writer.write_all(&bytes).await.is_err() {
                break;
            }
        }
    });
    let result = tokio::time::timeout(Duration::from_secs(6), events.recv()).await;
    upload.abort();
    assert_eq!(
        result
            .expect("blocked socket writes cannot postpone hub lease expiry")
            .unwrap()
            .kind,
        hub::EventKind::Down
    );
    let mut response = [0; 256];
    let length = tokio::time::timeout(Duration::from_secs(1), reader.read(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&response[..length]).starts_with("HTTP/1.1 502"));
    drop(ws);
}

#[tokio::test]
async fn concurrent_stream_capacity_returns_503_for_new_work() {
    saturate_pool(1).await;
}

#[tokio::test]
async fn two_tethers_use_both_stream_budgets_before_returning_503() {
    saturate_pool(2).await;
}

async fn saturate_pool(count: usize) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let engine = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut byte = [0];
                while !head.ends_with(b"\r\n\r\n") {
                    if socket.read_exact(&mut byte).await.is_err() {
                        return;
                    }
                    head.push(byte[0]);
                }
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2\r\nok\r\n")
                    .await;
                std::future::pending::<()>().await;
            });
        }
    });
    let topology = Topology::start_pool(addr, count).await;
    let mut held = Vec::new();
    let result = tokio::time::timeout(Duration::from_secs(3 * count as u64), async {
        for _ in 0..tunnel::MAX_CONCURRENT_STREAMS * count {
            let response = topology.request().await;
            assert_eq!(response.status(), 200);
            held.push(response.into_body());
        }
        topology.request().await.status()
    })
    .await;
    engine.abort();
    assert_eq!(result.unwrap(), 503);
}

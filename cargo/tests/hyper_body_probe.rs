//! Is hyper the component that truncates?
//!
//! No proxy, no tunnel, no channel: a hand-built body that yields
//!   DATA("data: one\n") then Ready(None)
//! served by hyper over a real connection. If the client sees only one event
//! here, the truncation is hyper's framing/flushing, not anvil-ring's logic.

use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use std::convert::Infallible;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hyper_relay_of_a_finite_stream_reaches_the_client() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let Ok((io, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(io);
                let svc = service_fn(|_req: Request<hyper::body::Incoming>| async move {
                    // Exactly what our TunnelBody produces: one chunk, then end.
                    let chunks: Vec<Result<Frame<Bytes>, Infallible>> =
                        vec![Ok(Frame::data(Bytes::from_static(b"data: one\n")))];
                    let body = StreamBody::new(stream::iter(chunks)).boxed_unsync();
                    Ok::<_, Infallible>(
                        Response::builder()
                            .header(hyper::header::CONTENT_TYPE, "text/event-stream")
                            .body(body)
                            .unwrap(),
                    )
                });
                let _ = http1::Builder::new()
                    .serve_connection(io, svc)
                    .with_upgrades()
                    .await;
            });
        }
    });

    // Read with a REAL client so framing is interpreted, not guessed.
    let io = hyper_util::rt::TokioIo::new(tokio::net::TcpStream::connect(addr).await.unwrap());
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(hyper::header::HOST, addr.to_string())
        .body(StreamBody::new(stream::empty::<
            Result<Frame<Bytes>, Infallible>,
        >()))
        .unwrap();

    let resp = sender.send_request(req).await.unwrap();
    let mut got = Vec::new();
    let mut body = BodyExt::boxed(resp.into_body());
    while let Some(frame) = BodyExt::frame(&mut body).await {
        let f = frame.expect("frame").into_data().expect("data frame");
        eprintln!(
            "CLIENT got {} bytes: {:?}",
            f.len(),
            String::from_utf8_lossy(&f)
        );
        got.extend_from_slice(&f);
    }

    assert_eq!(
        got,
        b"data: one\n".to_vec(),
        "hyper truncated a finite stream body"
    );
}

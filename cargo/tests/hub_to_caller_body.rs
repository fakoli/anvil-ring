//! Does the hub->caller hop actually deliver a multi-chunk stream?
//!
//! Bypasses the tunnel, hyper, and sockets entirely: builds a `TunnelBody::live`
//! over a real mpsc channel, feeds it CHUNK, CHUNK, END, and asserts every chunk
//! comes out. The production symptom is "caller sees one event then a clean
//! end" while the hub reports zero send failures, so this isolates the body
//! bridge (`Stream::poll_next` + the `Mutex<Option<Receiver>>` handoff).

use anvil_ring::frontend::TunnelBody;
use anvil_ring::hub::ChunkOrEnd;
use futures_util::StreamExt;
use tokio::sync::mpsc;

#[tokio::test(flavor = "current_thread")]
async fn live_body_forwards_every_chunk_before_end() {
    let (tx, rx) = mpsc::channel::<ChunkOrEnd>(64);
    let body = TunnelBody::live(rx);

    // Mirror what the hub does on receipt of each DATA frame, then END.
    tokio::spawn(async move {
        tx.send(ChunkOrEnd::Chunk("data: one\n".into())).await.unwrap();
        tx.send(ChunkOrEnd::Chunk("data: two\n".into())).await.unwrap();
        tx.send(ChunkOrEnd::Chunk("data: three\n".into())).await.unwrap();
        tx.send(ChunkOrEnd::End).await.unwrap();
    });

    let mut got: Vec<String> = Vec::new();
    let mut stream = body;
    while let Some(item) = stream.next().await {
        let b = item.expect("chunk");
        got.push(String::from_utf8_lossy(&b).to_string());
    }

    assert_eq!(
        got,
        vec!["data: one\n", "data: two\n", "data: three\n"],
        "hub->caller hop lost chunks: {got:?}"
    );
}

/// The same channel shape, but with chunks arriving AFTER the consumer is
/// already polling (i.e. truly streaming, nothing buffered in advance). The
/// production path is this one: the caller starts polling before the engine has
/// produced a second token.
#[tokio::test(flavor = "current_thread")]
async fn live_body_streams_chunks_that_arrive_later() {
    let (tx, rx) = mpsc::channel::<ChunkOrEnd>(64);
    let body = TunnelBody::live(rx);

    let producer = tokio::spawn(async move {
        for i in 0..5u32 {
            // Yield so the consumer is parked in poll_recv before each send,
            // exactly like tokens arriving over a network.
            tokio::task::yield_now().await;
            tx.send(ChunkOrEnd::Chunk(format!("tok{i}\n").into()))
                .await
                .unwrap();
        }
        tx.send(ChunkOrEnd::End).await.unwrap();
    });

    let mut got: Vec<String> = Vec::new();
    let mut stream = body;
    while let Some(item) = stream.next().await {
        got.push(String::from_utf8_lossy(&item.expect("chunk")).to_string());
    }
    producer.await.unwrap();

    assert_eq!(
        got,
        vec!["tok0\n", "tok1\n", "tok2\n", "tok3\n", "tok4\n"],
        "late-arriving chunks were dropped: {got:?}"
    );
}

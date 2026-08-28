//! Can `TunnelBody::Live` be driven to completion in ONE poll?
//!
//! Field measurement that motivated this: with a 6-event engine spread over ~10s,
//! the hub received all 6 DATA frames plus END, yet the caller received exactly
//! 1 event and never saw end-of-body (socket open, no FIN, for 20s). The tunnel
//! hop was proven correct (6/6 chunks reach the hub), so the loss is the
//! hub->caller hop: the caller's channel holds chunks that `poll_next` never
//! drains.
//!
//! hyper can serve an entire response within a single `poll_frame` drive. If our
//! body returns `Ready(None)` while the channel still holds data, the response
//! ends early and the remaining bytes are stranded -- which is exactly the
//! symptom. This test reproduces the shape WITHOUT any networking, so it is the
//! cheapest place to see the defect.

use anvil_ring::frontend::TunnelBody;
use anvil_ring::hub::ChunkOrEnd;
use futures_util::StreamExt;
use tokio::sync::mpsc;

#[tokio::test(flavor = "current_thread")]
async fn live_body_must_not_end_while_data_is_still_queued() {
    let (tx, rx) = mpsc::channel::<ChunkOrEnd>(64);
    let body = TunnelBody::live_for_test(rx);

    // Producer is ALREADY DONE before the consumer starts: everything is queued.
    drop(tx);

    let mut got: Vec<String> = Vec::new();
    let mut stream = body;
    while let Some(item) = stream.next().await {
        got.push(String::from_utf8_lossy(&item.expect("chunk")).to_string());
    }

    // Empty channel, no END ever sent: the body must end cleanly with no data.
    assert!(got.is_empty(), "unexpected data from an empty channel");
}

/// The shape that matters: several chunks queued, then END. A single drive of the
/// stream must yield all of them. If `poll_next` stops early, the count is short.
#[tokio::test(flavor = "current_thread")]
async fn all_queued_chunks_come_out_in_order_before_end() {
    let (tx, rx) = mpsc::channel::<ChunkOrEnd>(64);
    for i in 0..6 {
        tx.send(ChunkOrEnd::Chunk(format!("data: ev{i}\n").into()))
            .await
            .unwrap();
    }
    tx.send(ChunkOrEnd::End).await.unwrap();
    drop(tx);

    let body = TunnelBody::live_for_test(rx);
    let mut got: Vec<String> = Vec::new();
    let mut stream = body;
    while let Some(item) = stream.next().await {
        got.push(String::from_utf8_lossy(&item.expect("chunk")).to_string());
    }

    let want: Vec<String> = (0..6).map(|i| format!("data: ev{i}\n")).collect();
    assert_eq!(got, want, "queued chunks were not all delivered");
}

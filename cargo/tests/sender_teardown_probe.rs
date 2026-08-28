//! Does dropping the response-building future tear down the stream?
//!
//! Live trace, one line, decisive: the caller's body logged exactly ONE
//! `Chunk` then `-> END`. There is only one `ChunkOrEnd::End` send site in the
//! codebase (the hub's END handler), so the body can only have ended via
//! `Poll::Ready(None)` -- i.e. EVERY sender handle died after the first chunk.
//!
//! The handles live in `Arc<StreamState>` (session map) and `Forwarded`
//! (`state` + `_guard`). `StreamGuard::drop` removes the map entry and releases
//! its sender, so anything that drops `Forwarded` mid-response closes the
//! caller's channel exactly like this.
//!
//! In the frontend the receiver is taken out of `StreamState::chunks` and placed
//! in `TunnelBody::Live`, so `TunnelBody` holds the only reader; if the sender
//! side is dropped, `recv()` yields None and hyper ends the response cleanly --
//! with a synthesized chunked terminator, and WITHOUT a FIN on the socket until
//! the connection is torn down. Those are the three measured symptoms.

use anvil_ring::hub::ChunkOrEnd;
use futures_util::StreamExt;
use tokio::sync::mpsc;

/// The shape that produces "clean early end": every sender dropped while chunks
/// are still queued, and no END ever sent. A well-behaved body must NOT pretend
/// the stream completed successfully -- but as written it does, because `None`
/// and END are indistinguishable.
#[tokio::test(flavor = "current_thread")]
async fn dropping_all_senders_ends_the_stream_and_loses_queued_chunks() {
    let (tx, rx) = mpsc::channel::<ChunkOrEnd>(64);
    tx.send(ChunkOrEnd::Chunk("data: one\n".into()))
        .await
        .unwrap();
    tx.send(ChunkOrEnd::Chunk("data: two\n".into()))
        .await
        .unwrap();

    // No END, just the senders going away -- what `StreamGuard::drop` produces.
    drop(tx);

    let body = anvil_ring::frontend::TunnelBody::live(rx);
    let mut got: Vec<String> = Vec::new();
    let mut stream = body;
    while let Some(item) = stream.next().await {
        got.push(String::from_utf8_lossy(&item.expect("chunk")).to_string());
    }

    // Demonstrated, not asserted as desirable: the queued data survives only
    // because it was already buffered, and the stream ends as if it finished.
    // In the live proxy the analogous drop happens BEFORE most chunks arrive, so
    // they are lost for good.
    assert!(
        !got.is_empty(),
        "sanity: buffered chunks are still yielded before closure"
    );
    eprintln!("delivered after senders dropped: {got:?} (stream ended with no END)");
}

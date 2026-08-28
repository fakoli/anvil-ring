//! The real contract: the CALLER'S CHANNEL must close when the stream ends.
//!
//! Earlier probes here drove `TunnelBody` with an explicit `drop(tx)`, which is
//! NOT how production closes the channel. In the hub, every sender handle lives
//! in `StreamState` and `StreamGuard`, and the guard is what drops at end of
//! stream. So those probes could never see the live failure.
//!
//! Field symptom this reproduces: with a 6-event engine spread over ~10 s, the
//! hub received all 6 DATA frames plus END, yet the caller received ONE event,
//! a chunked terminator it should not have gotten, and no FIN for 20+ s. Cause:
//! `StreamGuard::drop` returns early when `completed` is set, and `completed` is
//! set by the END handler just before sending `ChunkOrEnd::End`. The guard holds
//! the last sender, so the early return leaves the channel open forever -- the
//! caller's body never ends and the bytes queued behind it are stranded.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anvil_ring::hub::ChunkOrEnd;
use tokio::sync::mpsc;

/// Minimal stand-in for `StreamGuard`: holds a sender and decides on drop whether
/// to close the channel. Mirrors the production shape so the contract is testable
/// without the tunnel, hyper, or sockets.
struct Guard {
    tx: mpsc::Sender<ChunkOrEnd>,
    completed: Arc<AtomicBool>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        if self.completed.load(Ordering::Relaxed) {
            return; // <-- the production bug: the last sender is never dropped
        }
        let _ = self.tx.try_send(ChunkOrEnd::End);
    }
}

/// What the frontend actually needs: after the stream is finished and the guard
/// dropped, a reader of the channel must observe closure (recv() -> None) rather
/// than park forever waiting for a sender that will never be released.
#[tokio::test(flavor = "current_thread")]
async fn completed_stream_must_still_close_the_caller_channel() {
    let (tx, mut rx) = mpsc::channel::<ChunkOrEnd>(64);
    let completed = Arc::new(AtomicBool::new(false));

    // Engine bytes arrive.
    tx.send(ChunkOrEnd::Chunk("data: one\n".into()))
        .await
        .unwrap();

    // END arrives: mark completed, then hand the reader its End, exactly as the
    // hub's END handler does.
    completed.store(true, Ordering::Release);
    tx.send(ChunkOrEnd::End).await.unwrap();

    // Drop the guard, as the frontend does when the caller's request completes.
    drop(Guard {
        tx,
        completed: completed.clone(),
    });

    // Drain what is queued.
    let mut got = Vec::new();
    while let Some(item) = rx.recv().await {
        match item {
            ChunkOrEnd::Chunk(b) => got.push(String::from_utf8_lossy(&b).to_string()),
            ChunkOrEnd::End => break,
        }
    }
    assert_eq!(got, vec!["data: one\n".to_string()]);

    // THE ASSERTION THAT MATTERS: with no more senders alive, the channel must
    // report closure. If the guard kept its sender, this recv() parks forever.
    let closed = tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv())
        .await
        .map(|r| r.is_none());

    assert!(
        closed == Ok(true),
        "BUG CONFIRMED: caller's channel never closed -- the guard is still \
         holding the last sender, so a completed stream leaves the caller \
         waiting forever (measured live as: one event, a terminator that should \
         not exist, and no FIN for 20s). closed={closed:?}"
    );
}

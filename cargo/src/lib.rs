//! anvil-ring library root: everything testable without a network harness.
//!
//! The binary in `main.rs` is thin glue; the logic that can violate an invariant
//! lives here so `cargo test` can reach it.

pub mod chunked;
pub mod frames;
pub mod frontend;
pub mod headers;
pub mod hub;
pub mod proxy;
pub mod tunnel;

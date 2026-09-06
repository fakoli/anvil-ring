# Durable hub implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development to implement this plan task-by-task. Preserve the existing dirty worktree and other workers' changes.

**Goal:** Complete durable hub registration, credential lifecycle, local administration, audit, and deterministic multi-tether admission.

**Architecture:** A private SQLite database is controlled through the local operator account. The hub periodically applies validated snapshots, cancels invalidated sessions, and selects the least-loaded eligible tether with an atomic OPEN reservation.

**Tech stack:** Rust 1.85, Tokio/Hyper, rusqlite with bundled SQLite, OS randomness, JSON command output.

**Spec:** `docs/adr/0007-durable-hub-administration.md`.

## Global constraints

- One static musl executable for aarch64 and x86-64; no extra rental runtime.
- No secrets in command arguments, URLs, database plaintext, or logs.
- Credentials last 60–86400 seconds; local operator state directory is mode 0700.
- Existing caller authentication and HTTP streaming/failure behavior remain mandatory.

## Task 1: Durable storage and operator command

Files: create `cargo/src/admin.rs` and storage unit tests in that module.
Parent owns dependency declarations and the `lib.rs` export.

Interfaces:

```rust
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub struct Record {
    pub id: String,
    pub label: String,
    pub credential_hash: String,
    pub active: bool,
    pub expires_at: u64,
}
// Record implements Clone, PartialEq, Eq; Debug excludes credential material.
pub struct Store; // owns one rusqlite Connection
impl Store {
    pub fn init(directory: &std::path::Path) -> Result<Self, Error>;
    pub fn open(directory: &std::path::Path) -> Result<Self, Error>;
    pub fn snapshot(&self) -> Result<Vec<Record>, Error>;
    pub fn register(&mut self, id: &str, label: &str, ttl_secs: u64,
                    output: &std::path::Path) -> Result<(), Error>;
    pub fn rotate(&mut self, id: &str, ttl_secs: u64,
                  output: &std::path::Path) -> Result<(), Error>;
    pub fn revoke(&mut self, id: &str) -> Result<bool, Error>;
}
pub fn run(args: &[String]) -> Result<(), Error>;
```

- [x] Write failing tests using private temporary directories: initialize/reopen,
  persist register/rotate/revoke, reject duplicate ID/output, verify no plaintext
  in SQLite/audit, reject bad permissions/symlinks/schema, and serialize writers.
- [x] Implement schema and transactions, bounded snapshot (10,000 records),
  output-file durability, validation, OS UID audit, and paged audit/list commands.
- [x] Run `cargo +1.85.0 test --lib admin::tests -- --test-threads=1`.

## Task 2: Live registry, expiry, and administration integration

Files: `cargo/src/hub.rs`, `cargo/src/main.rs`, new
`cargo/tests/admin_contract.rs`.

Interfaces:

```rust
impl Registry {
    pub fn replace_records(&self, records: Vec<crate::admin::Record>);
    pub fn clear_records(&self);
}
// main dispatches `admin` to admin::run and durable hub startup to Store::open.
```

- [x] Add failing tests for snapshot rotation/revocation/removal cancelling a
  live session, unchanged snapshots retaining it, and expiry rejecting both
  authorization and attachment while shortening the issued lease.
- [x] Add the snapshot methods under the existing registry lock order and make
  `authorize`, `attach`, `is_active`, and `status` use credential expiry.
- [x] Add `admin` dispatch and fail-fast mutually exclusive durable/demo config.
  Refresh with a 500 ms interval and one blocking read at a time. On read error
  or a two-second timeout clear authorization and cancel sessions.
- [x] Test the shipped CLI and real hub/tethers across register, rotation,
  revocation, restart, expiry, and database loss. Ensure output omits secrets.

## Task 3: Deterministic routing and bounded admission

Files: `cargo/src/hub.rs`, `cargo/src/frontend.rs`,
`cargo/tests/admin_contract.rs`.

Interfaces:

```rust
impl Registry {
    pub fn forward_any(&self, req: http::Request<hyper::body::Incoming>)
        -> Result<Forwarded, ForwardError>;
}
```

- [x] Add failing two-tether tests: a lexical tie chooses the first ID; a held
  response sends new work to the less-loaded tether; a saturated command queue
  does not prevent the other tether from accepting work; all full returns 503.
- [x] Serialize selection plus stream admission. Reserve an owned command queue
  permit before selecting a candidate. Refactor the existing synchronous portion
  of `forward` into a shared helper, preserving upload/body cancellation ownership.
- [x] Replace the frontend's unordered first-Up selection with `forward_any`.
  Do not retry after OPEN or inspect caller input for a tether selection.

## Task 4: Review, documentation, and final evidence

Files: ADR index, MkDocs navigation, runtime help, README, operator/CLI/testing/
rollout/architecture documentation, and `STATE.md`.

- [x] Review implementation against ADR-0007 and the existing transport guarantees.
- [x] Run full Rust stable/MSRV suites, Python tests/Ruff, clippy/rustdoc, strict
  docs, and actionlint. Correct failures before recording evidence.
- [x] Freeze Cargo sources, rebuild/test both Linux musl targets, regenerate
  target-specific SBOMs, package archives, and verify checksums and unprivileged
  execution. Retain the earlier artifacts as historical evidence only.
- [x] Update current documentation with actual commands, polling/timeout bounds,
  OS-authentication assumptions, serving-pool contract, and observed results.
- [x] Keep target-dependent rollout requirements open until independently proved.

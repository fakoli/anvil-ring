//! Shipped-binary contracts for durable administration and hub reloads.
//!
//! These tests intentionally cross process boundaries: every admin invocation
//! reopens SQLite, and the live cases use the real hub and tether commands.

use serde_json::Value;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const CALLER_TOKEN: &str = "admin-contract-caller";

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "anvil-ring-admin-contract-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .expect("create private test directory");
        Self(path)
    }

    fn state(&self) -> PathBuf {
        self.0.join("state")
    }

    fn credential(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_anvil-ring")
}

fn admin(state: &Path, args: &[&str], credential_out: Option<&Path>) -> Output {
    let mut command = Command::new(binary());
    command
        .arg("admin")
        .args(args)
        .env("ANVIL_RING_STATE_DIR", state)
        .env_remove("ANVIL_RING_DEMO_CREDENTIAL")
        .env_remove("ANVIL_RING_CREDENTIAL_OUT");
    if let Some(path) = credential_out {
        command.env("ANVIL_RING_CREDENTIAL_OUT", path);
    }
    command.output().expect("run admin command")
}

fn successful_json(output: Output) -> Value {
    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("command writes JSON")
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path).expect("metadata").mode() & 0o7777
}

fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind ephemeral port")
        .local_addr()
        .unwrap()
        .port()
}

fn spawn_hub(state: &Path, tunnel_port: u16, frontend_port: u16) -> ChildGuard {
    let child = Command::new(binary())
        .arg("hub")
        .env("ANVIL_RING_STATE_DIR", state)
        .env_remove("ANVIL_RING_DEMO_CREDENTIAL")
        .env("ANVIL_RING_HUB_LISTEN", format!("127.0.0.1:{tunnel_port}"))
        .env(
            "ANVIL_RING_FRONTEND_LISTEN",
            format!("127.0.0.1:{frontend_port}"),
        )
        .env("ANVIL_RING_CALLER_TOKEN", CALLER_TOKEN)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            fs::File::create(state.join("hub.log")).expect("create hub event log"),
        ))
        .spawn()
        .expect("spawn durable hub");
    ChildGuard(child)
}

fn spawn_tether(credential: &Path, tunnel_port: u16, engine_port: u16) -> ChildGuard {
    let child = Command::new(binary())
        .arg("tether")
        .env(
            "ANVIL_RING_HUB_URL",
            format!("ws://127.0.0.1:{tunnel_port}/ring"),
        )
        .env("ANVIL_RING_CRED_FILE", credential)
        .env(
            "ANVIL_RING_UPSTREAM",
            format!("http://127.0.0.1:{engine_port}"),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn tether");
    ChildGuard(child)
}

fn request(port: u16, path: &str, authenticated: bool) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to frontend");
    stream
        .set_read_timeout(Some(Duration::from_secs(4)))
        .unwrap();
    let authorization = if authenticated {
        format!("authorization: Bearer {CALLER_TOKEN}\r\n")
    } else {
        String::new()
    };
    let (method, body) = if authenticated {
        ("POST", r#"{"model":"m","stream":true}"#)
    } else {
        ("GET", "")
    };
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\n{authorization}content-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    )
    .expect("write request");
    let mut response = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(length) => {
                response.extend_from_slice(&chunk[..length]);
                if response_is_complete(&response) {
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                panic!("response did not finish before deadline")
            }
            Err(error) => panic!("read response: {error}"),
        }
    }
    String::from_utf8(response).expect("frontend response is UTF-8")
}

fn response_is_complete(response: &[u8]) -> bool {
    let Some(head_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
        return false;
    };
    let body_offset = head_end + 4;
    let head = String::from_utf8_lossy(&response[..head_end]).to_ascii_lowercase();
    if head.contains("transfer-encoding: chunked") {
        return response[body_offset..]
            .windows(5)
            .any(|window| window == b"0\r\n\r\n");
    }
    head.lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse::<usize>().ok())
        .is_some_and(|length| response.len() >= body_offset + length)
}

fn wait_for<F>(deadline: Duration, mut predicate: F) -> bool
where
    F: FnMut() -> bool,
{
    let end = Instant::now() + deadline;
    while Instant::now() < end {
        if predicate() {
            return true;
        }
        thread::sleep(Duration::from_millis(40));
    }
    false
}

fn wait_for_health(port: u16, expected: &str, deadline: Duration) {
    assert!(
        wait_for(deadline, || {
            if TcpStream::connect(("127.0.0.1", port)).is_err() {
                return false;
            }
            request(port, "/healthz", false).contains(expected)
        }),
        "frontend did not report {expected} before deadline"
    );
}

fn wait_for_stable_tether(port: u16) {
    wait_for_health(port, "tether-up", Duration::from_secs(6));
    // Cross at least one durable-store refresh before using the session. This
    // proves unchanged snapshots retain it and avoids observing the handshake
    // between attachment and the first 500 ms refresh.
    thread::sleep(Duration::from_millis(650));
    wait_for_health(port, "tether-up", Duration::from_secs(3));
}

fn read_request(stream: &mut TcpStream) {
    stream
        .set_read_timeout(Some(Duration::from_secs(4)))
        .unwrap();
    let mut request = Vec::new();
    let mut byte = [0_u8; 1];
    while !request.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).expect("read engine request");
        request.push(byte[0]);
        assert!(request.len() < 64 * 1024, "request head is bounded");
    }
    let head = String::from_utf8(request).expect("engine request head is UTF-8");
    let content_length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let mut body = vec![0_u8; content_length];
    stream
        .read_exact(&mut body)
        .expect("drain complete engine request body");
}

fn reply(stream: &mut TcpStream, body: &str) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("write engine response");
    stream.flush().expect("flush engine response");
}

fn spawn_engine(body: &'static str, requests: usize) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind fake engine");
    let port = listener.local_addr().unwrap().port();
    let task = thread::spawn(move || {
        for _ in 0..requests {
            let (mut stream, _) = listener.accept().expect("accept engine request");
            read_request(&mut stream);
            reply(&mut stream, body);
        }
    });
    (port, task)
}

#[test]
fn shipped_admin_cli_persists_lifecycle_and_never_reports_credentials() {
    let temp = TempDir::new();
    let state = temp.state();
    let alpha = temp.credential("alpha.cred");
    let rotated = temp.credential("alpha-rotated.cred");
    let duplicate = temp.credential("duplicate.cred");

    assert_eq!(
        successful_json(admin(&state, &["init"], None)),
        serde_json::json!({"action": "init", "ok": true})
    );
    assert_eq!(mode(&state), 0o700);
    assert_eq!(mode(&state.join("hub.sqlite3")), 0o600);

    let registered = successful_json(admin(
        &state,
        &["register", "alpha-1", "Alpha rental", "60"],
        Some(&alpha),
    ));
    assert_eq!(
        registered,
        serde_json::json!({"action": "register", "id": "alpha-1", "ok": true})
    );
    assert_eq!(mode(&alpha), 0o600);
    let first_secret = fs::read_to_string(&alpha).expect("issued credential");
    assert_eq!(first_secret.trim().len(), 64);
    assert!(
        !String::from_utf8_lossy(&admin(&state, &["list"], None).stdout)
            .contains(first_secret.trim())
    );

    let rejected = admin(
        &state,
        &["register", "alpha-1", "Duplicate", "60"],
        Some(&duplicate),
    );
    assert!(!rejected.status.success(), "duplicate ID must be rejected");
    assert!(
        !duplicate.exists(),
        "failed issuance must remove its output"
    );

    successful_json(admin(&state, &["rotate", "alpha-1", "60"], Some(&rotated)));
    assert_eq!(mode(&rotated), 0o600);
    let second_secret = fs::read_to_string(&rotated).expect("rotated credential");
    assert_ne!(first_secret, second_secret);

    let revoked = successful_json(admin(&state, &["revoke", "alpha-1"], None));
    assert_eq!(revoked["changed"], true);
    let listed = successful_json(admin(&state, &["list"], None));
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["id"], "alpha-1");
    assert_eq!(listed[0]["label"], "Alpha rental");
    assert_eq!(listed[0]["state"], "revoked");
    assert!(listed[0].get("credential_hash").is_none());

    let audit = successful_json(admin(&state, &["audit"], None));
    let events = audit.as_array().expect("audit array");
    assert_eq!(events.len(), 4);
    assert_eq!(events[0]["action"], "init");
    assert_eq!(events[1]["action"], "register");
    assert_eq!(events[2]["action"], "rotate");
    assert_eq!(events[3]["action"], "revoke");
    let after = events[1]["sequence"].as_u64().unwrap().to_string();
    let later = successful_json(admin(&state, &["audit", &after], None));
    assert_eq!(later.as_array().unwrap().len(), 2);

    let public_output = format!("{listed}{audit}{later}");
    assert!(!public_output.contains(first_secret.trim()));
    assert!(!public_output.contains(second_secret.trim()));
}

#[test]
fn hub_rejects_durable_and_demo_configuration_together() {
    let temp = TempDir::new();
    successful_json(admin(&temp.state(), &["init"], None));
    let mut child = Command::new(binary())
        .arg("hub")
        .env("ANVIL_RING_STATE_DIR", temp.state())
        .env("ANVIL_RING_DEMO_CREDENTIAL", "must-not-win")
        .env("ANVIL_RING_HUB_LISTEN", "127.0.0.1:0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn conflicting hub");
    let exited = wait_for(Duration::from_secs(2), || {
        child.try_wait().unwrap().is_some()
    });
    if !exited {
        child.kill().unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(exited, "invalid configuration started a long-running hub");
    assert!(!output.status.success());
    let diagnostics = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(diagnostics.contains("ANVIL_RING_STATE_DIR"));
    assert!(diagnostics.contains("ANVIL_RING_DEMO_CREDENTIAL"));
}

#[test]
fn running_hub_applies_rotation_revocation_store_loss_and_expiry() {
    let temp = TempDir::new();
    let state = temp.state();
    let credential = temp.credential("solo.cred");
    let rotated = temp.credential("solo-rotated.cred");
    let reactivated = temp.credential("solo-reactivated.cred");
    successful_json(admin(&state, &["init"], None));
    successful_json(admin(
        &state,
        &["register", "solo", "Solo", "60"],
        Some(&credential),
    ));

    let (engine_port, engine) = spawn_engine("served-by-solo", 5);
    let tunnel_port = free_port();
    let frontend_port = free_port();
    let hub = spawn_hub(&state, tunnel_port, frontend_port);
    let _tether = spawn_tether(&credential, tunnel_port, engine_port);
    wait_for_stable_tether(frontend_port);

    let response = request(frontend_port, "/v1/models", true);
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("served-by-solo"));

    successful_json(admin(&state, &["rotate", "solo", "60"], Some(&rotated)));
    wait_for_health(frontend_port, "tether-down", Duration::from_secs(3));
    let response = request(frontend_port, "/v1/models", true);
    assert!(response.starts_with("HTTP/1.1 502"), "{response}");
    let _rotated_tether = spawn_tether(&rotated, tunnel_port, engine_port);
    wait_for_stable_tether(frontend_port);
    let response = request(frontend_port, "/v1/models", true);
    assert!(response.contains("served-by-solo"), "{response}");

    // A new hub process must reopen the same registration and accept the current
    // credential without another operator write.
    drop(hub);
    thread::sleep(Duration::from_millis(100));
    let hub = spawn_hub(&state, tunnel_port, frontend_port);
    wait_for_stable_tether(frontend_port);
    let response = request(frontend_port, "/v1/models", true);
    assert!(response.contains("served-by-solo"), "{response}");

    successful_json(admin(&state, &["revoke", "solo"], None));
    wait_for_health(frontend_port, "tether-down", Duration::from_secs(3));
    let response = request(frontend_port, "/v1/models", true);
    assert!(response.starts_with("HTTP/1.1 502"), "{response}");

    // Revocation also survives a hub restart.
    drop(hub);
    thread::sleep(Duration::from_millis(100));
    let hub = spawn_hub(&state, tunnel_port, frontend_port);
    wait_for_health(frontend_port, "tether-down", Duration::from_secs(3));
    let response = request(frontend_port, "/v1/models", true);
    assert!(response.starts_with("HTTP/1.1 502"), "{response}");

    successful_json(admin(&state, &["rotate", "solo", "60"], Some(&reactivated)));
    let _reactivated_tether = spawn_tether(&reactivated, tunnel_port, engine_port);
    wait_for_stable_tether(frontend_port);
    let response = request(frontend_port, "/v1/models", true);
    assert!(response.contains("served-by-solo"), "{response}");

    // Losing the database after startup clears live authorization. Restoring the
    // same file allows the registered tether to reconnect on a later refresh.
    let database = state.join("hub.sqlite3");
    let missing = state.join("hub.sqlite3.missing");
    fs::rename(&database, &missing).expect("simulate lost registration database");
    wait_for_health(frontend_port, "tether-down", Duration::from_secs(3));
    let response = request(frontend_port, "/v1/models", true);
    assert!(response.starts_with("HTTP/1.1 502"), "{response}");
    fs::rename(&missing, &database).expect("restore registration database");
    wait_for_stable_tether(frontend_port);
    let response = request(frontend_port, "/v1/models", true);
    assert!(response.contains("served-by-solo"), "{response}");

    // Set the persisted absolute expiry into the past instead of making the
    // suite sleep for the minimum 60-second credential lifetime.
    let connection = rusqlite::Connection::open(&database).expect("open test database");
    connection
        .execute(
            "UPDATE registrations SET expires_at = strftime('%s','now') - 1 WHERE id = 'solo'",
            [],
        )
        .expect("expire registration");
    drop(connection);
    wait_for_health(frontend_port, "tether-down", Duration::from_secs(3));
    let response = request(frontend_port, "/v1/models", true);
    assert!(response.starts_with("HTTP/1.1 502"), "{response}");
    drop(hub);
    engine.join().expect("fake engine exits");
}

#[test]
fn two_real_tethers_use_lexical_ties_then_the_less_loaded_session() {
    let temp = TempDir::new();
    let state = temp.state();
    let alpha_credential = temp.credential("alpha.cred");
    let beta_credential = temp.credential("beta.cred");
    successful_json(admin(&state, &["init"], None));
    successful_json(admin(
        &state,
        &["register", "alpha", "Alpha", "60"],
        Some(&alpha_credential),
    ));
    successful_json(admin(
        &state,
        &["register", "beta", "Beta", "60"],
        Some(&beta_credential),
    ));

    let alpha_listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let alpha_port = alpha_listener.local_addr().unwrap().port();
    let beta_listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let beta_port = beta_listener.local_addr().unwrap().port();
    let (alpha_started_tx, alpha_started_rx) = mpsc::channel();
    let (release_alpha_tx, release_alpha_rx) = mpsc::channel();
    let alpha_engine = thread::spawn(move || {
        let (mut stream, _) = alpha_listener.accept().unwrap();
        read_request(&mut stream);
        alpha_started_tx.send(()).unwrap();
        release_alpha_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("release held alpha response");
        reply(&mut stream, "served-by-alpha");
    });
    let beta_engine = thread::spawn(move || {
        let (mut stream, _) = beta_listener.accept().unwrap();
        read_request(&mut stream);
        reply(&mut stream, "served-by-beta");
    });

    let tunnel_port = free_port();
    let frontend_port = free_port();
    let _hub = spawn_hub(&state, tunnel_port, frontend_port);
    // Connect beta first so lexical selection is proven independently of attach order.
    let _beta = spawn_tether(&beta_credential, tunnel_port, beta_port);
    wait_for_stable_tether(frontend_port);
    let _alpha = spawn_tether(&alpha_credential, tunnel_port, alpha_port);
    assert!(
        wait_for(Duration::from_secs(4), || {
            fs::read_to_string(state.join("hub.log"))
                .is_ok_and(|log| log.contains("event alpha Up"))
        }),
        "alpha session must attach before checking the lexical tie"
    );

    let first = thread::spawn(move || request(frontend_port, "/v1/models", true));
    alpha_started_rx
        .recv_timeout(Duration::from_secs(4))
        .expect("lexical alpha tether receives tie");
    let second = request(frontend_port, "/v1/models", true);
    assert!(second.starts_with("HTTP/1.1 200"), "{second}");
    assert!(second.contains("served-by-beta"), "{second}");

    release_alpha_tx.send(()).unwrap();
    let first = first.join().unwrap();
    assert!(first.starts_with("HTTP/1.1 200"), "{first}");
    assert!(first.contains("served-by-alpha"), "{first}");
    alpha_engine.join().unwrap();
    beta_engine.join().unwrap();
}

//! Operator-facing contract for the shipped Rust executable.

use std::process::Command;

fn ring(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_anvil-ring"))
        .args(args)
        .output()
        .expect("run anvil-ring")
}

#[test]
fn help_describes_every_shipped_mode_without_obsolete_scaffold_copy() {
    let output = ring(&["--help"]);
    assert!(output.status.success());

    let help = String::from_utf8(output.stdout).expect("help is UTF-8");
    for mode in [
        "anvil-ring proxy",
        "anvil-ring tether",
        "anvil-ring hub",
        "anvil-ring admin",
    ] {
        assert!(
            help.contains(mode),
            "missing shipped mode {mode:?}:\n{help}"
        );
    }

    let lower = help.to_ascii_lowercase();
    assert!(
        !lower.contains("only the proxy half exists") && !lower.contains("scaffold"),
        "help still tells operators the implemented tunnel is absent:\n{help}"
    );
}

#[test]
fn help_keeps_secrets_out_of_command_line_options() {
    let output = ring(&["--help"]);
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("help is UTF-8");

    assert!(!help.contains("--token"), "tokens must never be argv");
    assert!(
        !help.contains("--credential"),
        "credentials must never be argv"
    );
}

#[test]
fn help_points_to_documentation_and_project_portals() {
    let output = ring(&["--help"]);
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("help is UTF-8");

    assert!(help.contains("https://fakoli.github.io/anvil-ring/"));
    assert!(help.contains("https://github.com/fakoli/anvil-ring"));
    assert!(help.contains("ANVIL_RING_STATE_DIR"));
    assert!(help.contains("ANVIL_RING_CREDENTIAL_OUT"));
}

#[test]
fn every_mode_rejects_unexpected_arguments() {
    let output = ring(&["--version", "unexpected"]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("does not accept command-line arguments"));
}

#[test]
fn proxy_startup_rejects_secret_bearing_urls_without_logging_them() {
    use std::process::Stdio;
    for upstream in [
        "http://user:secret-marker@127.0.0.1:8000",
        "http://127.0.0.1:8000?token=secret-marker",
    ] {
        let mut child = Command::new(env!("CARGO_BIN_EXE_anvil-ring"))
            .arg("proxy")
            .env("ANVIL_RING_LISTEN", "127.0.0.1:0")
            .env("ANVIL_RING_TOKEN", "test-only")
            .env("ANVIL_RING_UPSTREAM", upstream)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while child.try_wait().unwrap().is_none() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let exited = child.try_wait().unwrap().is_some();
        if !exited {
            child.kill().unwrap();
        }
        let output = child.wait_with_output().unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !stderr.contains("secret-marker"),
            "configuration secret appeared in startup output"
        );
        assert!(
            exited && !output.status.success(),
            "invalid configuration must fail before listening"
        );
    }
}

"""Contract tests for the Python diagnostics package."""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

import anvil_ring.probe_egress as probe_egress
from anvil_ring.cli import main
from anvil_ring.probe_egress import Probe

ROOT = Path(__file__).resolve().parents[1]


def test_python_compatibility_cli_is_probe_only(capsys) -> None:
    """The Python shim must not shadow the working Rust runtime CLI."""
    rc = main(["--target", "not-a-target"])
    err = capsys.readouterr().err
    assert rc == 2
    assert "expected HOST:PORT[:MODE]" in err


def test_only_probe_console_script_is_packaged() -> None:
    """Installing the diagnostics must leave `anvil-ring` to the Rust binary."""
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert config["project"]["scripts"] == {
        "anvil-ring-probe-egress": "anvil_ring.probe_egress:main"
    }
    assert "data-files" not in config["tool"]["setuptools"]
    assert config["project"]["urls"]["Documentation"] == (
        "https://fakoli.github.io/anvil-ring/"
    )
    assert config["project"]["urls"]["Repository"] == (
        "https://github.com/fakoli/anvil-ring"
    )


def test_probe_help_has_no_secret_flags() -> None:
    """The diagnostic authenticates to nothing, so secrets never belong in argv."""
    result = subprocess.run(
        [sys.executable, "-m", "anvil_ring.probe_egress", "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "anvil-ring-probe-egress" in result.stdout
    assert "--no-defaults" in result.stdout
    assert "https://fakoli.github.io/anvil-ring/" in result.stdout
    assert "https://github.com/fakoli/anvil-ring" in result.stdout
    assert "--token" not in result.stdout
    assert "--credential" not in result.stdout


def test_probe_rejects_unknown_mode_without_touching_network(capsys) -> None:
    rc = probe_egress.main(
        ["--no-defaults", "--target", "ring.example:443:unknown"]
    )
    assert rc == 2
    assert "MODE in tls|tcp|ssh-banner" in capsys.readouterr().err


def test_exact_target_gate_redacts_hostname_by_default(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    calls: list[tuple[str, int, str]] = []

    def fake_probe(target: str, port: int, mode: str) -> Probe:
        calls.append((target, port, mode))
        return Probe(
            target=target,
            port=port,
            mode=mode,
            tcp=True,
            tls=True,
            detail="TLS TLSV1.3",
        )

    monkeypatch.setattr(probe_egress, "_probe_tcp", fake_probe)
    monkeypatch.setattr(probe_egress.platform, "node", lambda: "private-rental")
    out = tmp_path / "egress.json"

    rc = probe_egress.main(
        [
            "--no-defaults",
            "--target",
            "ring.example:443:tls",
            "--out",
            str(out),
        ]
    )

    assert rc == 0
    assert calls == [("ring.example", 443, "tls")]
    assert "private-rental" not in capsys.readouterr().out
    assert "private-rental" not in out.read_text()


def test_ssh_only_does_not_qualify_the_shipped_wss_transport() -> None:
    hint = probe_egress._transport_hint(
        [
            Probe(
                target="github.com",
                port=22,
                mode="ssh-banner",
                tcp=True,
                detail="SSH-2.0-test",
            )
        ]
    )
    assert "not qualified" in hint
    assert "Chisel" not in hint
    assert "ssh -R" not in hint

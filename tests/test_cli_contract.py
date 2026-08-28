"""Tests for the anvil-ring CLI skeleton.

These test the *contract*, not a tunnel: the binary name, argv hygiene (I-8), and
that unimplemented features fail loudly rather than silently succeeding.
"""

from __future__ import annotations

import shutil
import subprocess

import pytest

from anvil_ring.cli import build_parser, main


def test_binary_name_is_always_anvil_prefixed() -> None:
    """Operator directive: the prog name carries the anvil- prefix."""
    assert build_parser().prog == "anvil-ring"


def test_up_without_token_fails_closed(monkeypatch, capsys) -> None:
    """I-8: no token in the env must be a hard error, not a prompt or a stub run."""
    monkeypatch.delenv("ANVIL_RING_TOKEN", raising=False)
    rc = main(["up", "--serve", "http://127.0.0.1:8000"])
    err = capsys.readouterr().err
    assert rc == 2
    assert "ANVIL_RING_TOKEN" in err


def test_unimplemented_subcommands_are_not_silent(monkeypatch, capsys) -> None:
    """A stub must never exit 0 -- that is how skeletons lie."""
    monkeypatch.setenv("ANVIL_RING_TOKEN", "test-token")
    for argv in (["up"], ["list"], ["revoke", "foo"]):
        rc = main(argv)
        assert rc == 2, argv
        assert "not implemented" in capsys.readouterr().err


def test_token_is_not_accepted_as_an_argument() -> None:
    """argv leaks via `ps` and shell history; there must be no flag for it."""
    opts = {
        a
        for action in build_parser()._actions  # noqa: SLF001 - argparse introspection
        for a in (*action.option_strings,)
    }
    assert not any("token" in o.lower() for o in opts), opts


def test_help_output_carries_prefix() -> None:
    """`--help` is the first thing a user reads; it must not teach a bare name."""
    out = build_parser().format_help()
    assert "anvil-ring" in out
    # No line should present a bare `ring` as an invocable command.
    assert not any(line.strip().startswith("ring ") for line in out.splitlines())


@pytest.mark.skipif(shutil.which("ring") is None, reason="no bare `ring` on PATH")
def test_no_bare_ring_alias_was_installed() -> None:
    """Guard against someone adding a bare alias later; skips if a system `ring` exists."""
    pytest.fail(
        "A bare `ring` executable exists on PATH. If it is ours, the naming "
        "directive was violated; if it is another project's, ignore this test."
    )


def test_console_script_entry_point_resolves() -> None:
    """`anvil-ring --version` must work through the real installed entry point."""
    exe = shutil.which("anvil-ring")
    if exe is None:  # not installed in this env; entry point checked in CI instead
        pytest.skip("anvil-ring not installed on PATH")
    out = subprocess.run([exe, "--version"], capture_output=True, text=True, check=True)
    assert out.stdout.strip().startswith("anvil-ring")

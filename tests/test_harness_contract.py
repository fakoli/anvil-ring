"""Contracts for repository-local verification harnesses."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIVE_GATE = ROOT / "cargo" / "scripts" / "live_stream_gate.py"


def test_live_gate_is_repository_relative_and_documents_binary_override() -> None:
    source = LIVE_GATE.read_text()
    assert "workspace-work" not in source
    assert "transfer-encoding: chunked" in source

    result = subprocess.run(
        [sys.executable, LIVE_GATE, "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "--binary" in result.stdout

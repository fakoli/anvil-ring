"""Contracts for the combined vLLM-and-tether rental image."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_combined_image_starts_engine_and_outbound_tether() -> None:
    dockerfile = (ROOT / "deploy" / "Dockerfile").read_text()

    assert 'ENTRYPOINT ["/usr/local/bin/anvil-ring-entrypoint"]' in dockerfile
    assert 'ENTRYPOINT ["/usr/local/bin/anvil-ring", "proxy"]' not in dockerfile
    assert "FROM --platform=$TARGETPLATFORM" in dockerfile
    assert "USER 2000:0" in dockerfile
    assert "EXPOSE 8000" not in dockerfile


def test_entrypoint_supervises_both_processes(tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "calls.log"

    ring = bin_dir / "anvil-ring"
    ring.write_text(
        "#!/bin/sh\n"
        'printf "ring:%s\\n" "$*" >> "$ANVIL_TEST_LOG"\n'
        'test "${ANVIL_RING_CREDENTIAL+x}" = x && printf "ring-secret:present\\n" >> "$ANVIL_TEST_LOG"\n'
        "trap 'exit 0' TERM INT\n"
        "while :; do sleep 0.05; done\n"
    )
    ring.chmod(0o755)

    vllm = bin_dir / "vllm"
    vllm.write_text(
        "#!/bin/sh\n"
        'printf "vllm:%s\\n" "$*" >> "$ANVIL_TEST_LOG"\n'
        'if test "${ANVIL_RING_CREDENTIAL+x}" = x; then secret=present; else secret=absent; fi\n'
        'printf "vllm-secret:%s\\n" "$secret" >> "$ANVIL_TEST_LOG"\n'
        "sleep 0.05\n"
    )
    vllm.chmod(0o755)

    env = os.environ | {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "ANVIL_TEST_LOG": str(log),
        "ANVIL_RING_HUB_URL": "ws://127.0.0.1:8443/ring",
        "ANVIL_RING_CREDENTIAL": "test-credential",
    }
    result = subprocess.run(
        [sys.executable, ROOT / "deploy" / "entrypoint.py", "example/model"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    calls = log.read_text().splitlines()
    assert "ring:tether" in calls
    assert "ring-secret:present" in calls
    assert (
        "vllm:serve example/model --host 127.0.0.1 --port 8000" in calls
    )
    assert "vllm-secret:absent" in calls

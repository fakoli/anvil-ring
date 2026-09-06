"""Release workflow contracts that should stay aligned with the product state."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "release-artifact.yml"
DOCS_WORKFLOW = ROOT / ".github" / "workflows" / "docs.yml"


def test_ci_treats_streaming_regressions_as_ordinary_green_tests() -> None:
    text = WORKFLOW.read_text()
    lower = text.lower()

    assert "known-red" not in lower
    assert "--skip tether_death_midstream_ends_the_caller" not in text
    assert "--skip cancelled_tether_ends_the_caller_response_body" not in text
    assert "cargo test --locked --all-targets" in text
    assert "python3 -m pytest -q" in text
    assert "python3 -m ruff check ." in text


def test_documentation_is_built_strictly_and_deployed_from_main() -> None:
    text = DOCS_WORKFLOW.read_text()

    assert "mkdocs build --strict" in text
    assert "actions/upload-pages-artifact@" in text
    assert "actions/deploy-pages@" in text
    assert "github.ref == 'refs/heads/main'" in text

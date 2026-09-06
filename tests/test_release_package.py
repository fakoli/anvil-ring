"""Download integrity, executable permissions, and release identity contracts."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import struct
import tarfile
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/package_release.py"
SPEC = importlib.util.spec_from_file_location("release_package", SCRIPT)
assert SPEC and SPEC.loader
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
TARGET = "aarch64-unknown-linux-musl"


@pytest.fixture
def inputs(tmp_path: Path) -> dict:
    (tmp_path / "cargo").mkdir()
    (tmp_path / "cargo/Cargo.toml").write_text('[package]\nversion = "0.1.0"\n')
    (tmp_path / "cargo/Cargo.lock").write_text("version = 4\n")
    (tmp_path / "LICENSE").write_text("test license\n")
    binary = tmp_path / "binary"
    elf = bytearray(120)
    elf[:6] = b"\x7fELF\x02\x01"
    struct.pack_into("<H", elf, 18, 183)
    struct.pack_into("<Q", elf, 32, 64)
    struct.pack_into("<HH", elf, 54, 56, 1)
    struct.pack_into("<I", elf, 64, 1)
    binary.write_bytes(elf)
    sbom = tmp_path / "sbom.json"
    local = "path+file:///private/operator/checkout#anvil-ring@0.1.0"
    sbom.write_text(json.dumps({
        "bomFormat": "CycloneDX",
        "metadata": {
            "component": {"name": "anvil-ring", "version": "0.1.0", "bom-ref": local},
            "properties": [{"name": "cdx:rustc:sbom:target:triple", "value": TARGET}],
        },
        "components": [{"name": "tokio", "version": "1.0.0"}],
        "dependencies": [{"ref": local, "dependsOn": []}],
    }))
    return dict(
        root=tmp_path, binary=binary, sbom=sbom, target=TARGET,
        output=tmp_path / "out", revision="a" * 40, dirty=True, epoch=1000,
    )


def test_archive_preserves_executable_and_verifiable_contents(inputs: dict) -> None:
    archive = release.package(**inputs)
    with tarfile.open(archive) as tar:
        assert tar.getmember("anvil-ring").mode == 0o755
        files = {member.name: tar.extractfile(member).read() for member in tar.getmembers()}
    assert files["anvil-ring"] == inputs["binary"].read_bytes()
    for line in files["SHA256SUMS"].decode().splitlines():
        digest, name = line.split("  ")
        assert hashlib.sha256(files[name]).hexdigest() == digest
    info = json.loads(files["build-info.json"])
    assert info["source_tree_dirty"] is True
    assert info["source_revision"] == "a" * 40
    assert info["binary_sha256"] == hashlib.sha256(files["anvil-ring"]).hexdigest()
    inventory = json.loads(files["sbom.cdx.json"])
    assert b"/private/operator" not in files["sbom.cdx.json"]
    assert inventory["metadata"]["component"]["bom-ref"] == inventory["dependencies"][0]["ref"]
    original = archive.read_bytes()
    assert release.package(**inputs).read_bytes() == original


@pytest.mark.parametrize("fault", ["architecture", "loader", "truncated"])
def test_invalid_executable_cannot_be_packaged(inputs: dict, fault: str) -> None:
    elf = bytearray(inputs["binary"].read_bytes())
    if fault == "architecture":
        struct.pack_into("<H", elf, 18, 62)
    elif fault == "loader":
        struct.pack_into("<I", elf, 64, 3)
    else:
        del elf[110:]
    inputs["binary"].write_bytes(elf)
    with pytest.raises(ValueError):
        release.package(**inputs)
    assert not inputs["output"].exists()


def test_other_target_sbom_cannot_be_attached_to_binary(inputs: dict) -> None:
    data = json.loads(inputs["sbom"].read_text())
    data["metadata"]["properties"][0]["value"] = "x86_64-unknown-linux-musl"
    inputs["sbom"].write_text(json.dumps(data))
    with pytest.raises(ValueError, match="SBOM"):
        release.package(**inputs)

#!/usr/bin/env python3
"""Package a verified musl build and its Cargo SBOM for download.

Static-link checks run in the Linux builder before this script. Local dirty-tree
bundles require an explicit flag and are labelled as development evidence.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import struct
import subprocess
import tarfile
import tomllib
from pathlib import Path

TARGETS = {"aarch64-unknown-linux-musl": 183, "x86_64-unknown-linux-musl": 62}


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def package(
    root: Path,
    binary: Path,
    sbom: Path,
    target: str,
    output: Path,
    revision: str,
    dirty: bool,
    epoch: int,
) -> Path:
    executable = binary.read_bytes()
    if (
        len(executable) < 64
        or executable[:6] != b"\x7fELF\x02\x01"
        or struct.unpack_from("<H", executable, 18)[0] != TARGETS[target]
    ):
        raise ValueError("binary does not match the requested ELF architecture")
    # Check every program header rather than trusting a filename or `file` text.
    offset = struct.unpack_from("<Q", executable, 32)[0]
    size, count = struct.unpack_from("<HH", executable, 54)
    if not count or size < 56 or offset + count * size > len(executable):
        raise ValueError("binary has invalid ELF program headers")
    for index in range(count):
        if struct.unpack_from("<I", executable, offset + index * size)[0] == 3:
            raise ValueError("binary requires a dynamic loader")

    manifest = tomllib.loads((root / "cargo/Cargo.toml").read_text())
    version = manifest["package"]["version"]
    inventory = json.loads(sbom.read_text())
    metadata = inventory.get("metadata", {})
    component = metadata.get("component", {})
    properties = {p["name"]: p["value"] for p in metadata.get("properties", [])}
    if (
        inventory.get("bomFormat") != "CycloneDX"
        or component.get("name") != "anvil-ring"
        or component.get("version") != version
        or properties.get("cdx:rustc:sbom:target:triple") != target
        or not inventory.get("components")
    ):
        raise ValueError("SBOM does not describe this binary version and target")
    # Cargo uses an absolute checkout path as its local package ID. Replace that
    # reference consistently (including dependency edges) before distribution.
    local_ref = component["bom-ref"]
    inventory = json.loads(
        json.dumps(inventory).replace(
            json.dumps(local_ref), json.dumps(f"pkg:cargo/anvil-ring@{version}")
        )
    )
    info = {
        "name": "anvil-ring",
        "version": version,
        "target": target,
        "source_revision": revision,
        "source_tree_dirty": dirty,
        "binary_sha256": sha256(executable),
        "cargo_lock_sha256": sha256((root / "cargo/Cargo.lock").read_bytes()),
    }
    contents = {
        "anvil-ring": executable,
        "sbom.cdx.json": (json.dumps(inventory, indent=2) + "\n").encode(),
        "build-info.json": (json.dumps(info, indent=2) + "\n").encode(),
        "Cargo.lock": (root / "cargo/Cargo.lock").read_bytes(),
        "LICENSE": (root / "LICENSE").read_bytes(),
    }
    contents["SHA256SUMS"] = "".join(
        f"{sha256(data)}  {name}\n" for name, data in sorted(contents.items())
    ).encode()
    output.mkdir(parents=True, exist_ok=True)
    archive = output / f"anvil-ring-{version}-{target}.tar.gz"
    with archive.open("wb") as raw, gzip.GzipFile(
        filename="", fileobj=raw, mode="wb", mtime=epoch
    ) as compressed, tarfile.open(fileobj=compressed, mode="w") as tar:
        for name, data in sorted(contents.items()):
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            entry.mode = 0o755 if name == "anvil-ring" else 0o644
            entry.mtime = epoch
            tar.addfile(entry, io.BytesIO(data))
    # Sidecars let CI attest the binary and its SBOM without unpacking the archive.
    for name, data in contents.items():
        (output / name).write_bytes(data)
    (output / "anvil-ring").chmod(0o755)
    (output / "ARCHIVE-SHA256SUMS").write_text(
        f"{sha256(archive.read_bytes())}  {archive.name}\n"
    )
    return archive


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--sbom", type=Path, required=True)
    parser.add_argument("--target", choices=TARGETS, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-dirty", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]

    def git(*arguments: str) -> str:
        return subprocess.check_output(["git", "-C", str(root), *arguments], text=True).strip()

    dirty = bool(git("status", "--porcelain", "--untracked-files=normal"))
    if dirty and not args.allow_dirty:
        parser.error("release source is dirty; use --allow-dirty only for local development evidence")
    try:
        archive = package(
            root, args.binary, args.sbom, args.target, args.output,
            git("rev-parse", "HEAD"), dirty, int(git("log", "-1", "--format=%ct")),
        )
    except (ValueError, KeyError) as error:
        parser.error(str(error))
    print(archive)


if __name__ == "__main__":
    main()

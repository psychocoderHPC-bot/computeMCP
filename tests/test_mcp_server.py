# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import os

import pytest

from compute_mcp.files import parse_mode
from compute_mcp.ssh_backend import SSHError


def test_parse_mode_octal_strings():
    assert parse_mode("644") == 0o644
    assert parse_mode("755") == 0o755
    assert parse_mode("0o644") == 0o644
    assert parse_mode("0755") == 0o755


def test_parse_mode_int_passthrough():
    assert parse_mode(0o600) == 0o600


def test_parse_mode_invalid():
    with pytest.raises(SSHError):
        parse_mode("99z")
    with pytest.raises(SSHError):
        parse_mode("99999")


def test_matches_globs():
    from compute_mcp.mcp_server import _matches

    assert _matches("src/a.c", [], []) is True
    assert _matches("src/a.c", ["src/*"], []) is True
    assert _matches("src/a.o", ["src/*"], ["*.o"]) is False
    assert _matches("build/x.o", [], ["build/*"]) is False


class FakeGatewayClient:
    """In-memory stand-in for the gateway used by the tree helpers."""

    def __init__(self):
        self.dirs = {"/remote"}
        self.files = {}
        self.uploaded = []

    async def request(self, method, path, params=None, json=None):
        if path == "/v1/files/stat":
            p = params["path"]
            if p in self.dirs:
                return {"path": p, "type": "directory"}
            if p in self.files:
                return {"path": p, "type": "file", "size": len(self.files[p])}
            raise RuntimeError("not found")
        if path == "/v1/files/mkdir":
            self.dirs.add(json["path"])
            return {"path": json["path"], "created": True}
        if path == "/v1/files/list":
            p = params["path"]
            entries = []
            for d in self.dirs:
                if os.path.dirname(d) == p and d != p:
                    entries.append({"name": os.path.basename(d), "path": d, "type": "directory"})
            for f in self.files:
                if os.path.dirname(f) == p:
                    entries.append({"name": os.path.basename(f), "path": f, "type": "file"})
            return {"path": p, "entries": entries}
        raise RuntimeError(f"unexpected {path}")

    async def upload(self, target, remote_path, data, length, append=False, parents=False):
        payload = data.read()
        self.files[remote_path] = payload
        self.uploaded.append((remote_path, payload))
        return {"path": remote_path, "written": len(payload)}

    async def download(self, target, remote_path, sink):
        sink.write(self.files[remote_path])
        return {"path": remote_path, "written": len(self.files[remote_path])}


async def test_upload_tree_mirrors_and_filters(tmp_path):
    from compute_mcp.mcp_server import _upload_tree

    root = tmp_path / "src"
    (root / "sub").mkdir(parents=True)
    (root / "a.txt").write_text("A")
    (root / "sub" / "b.txt").write_text("B")
    (root / "sub" / "c.o").write_text("C")

    client = FakeGatewayClient()
    report = await _upload_tree(
        client, "hal", str(root), "/remote/dst",
        append=False, parents=True, overwrite=True, skip_existing=False,
        max_files=0, include=[], exclude=["*.o"],
    )
    assert report["uploaded"] == 2
    assert report["skipped"] == 1
    assert report["failed"] == []
    assert client.files["/remote/dst/a.txt"] == b"A"
    assert client.files["/remote/dst/sub/b.txt"] == b"B"
    assert not any(p.endswith("c.o") for p in client.files)


async def test_upload_tree_skip_existing(tmp_path):
    from compute_mcp.mcp_server import _upload_tree

    root = tmp_path / "src"
    root.mkdir()
    (root / "a.txt").write_text("A")
    (root / "b.txt").write_text("B")

    client = FakeGatewayClient()
    client.files["/remote/dst/a.txt"] = b"old"
    report = await _upload_tree(
        client, "hal", str(root), "/remote/dst",
        append=False, parents=True, overwrite=True, skip_existing=True,
        max_files=0, include=[], exclude=[],
    )
    assert report["uploaded"] == 1 and report["skipped"] == 1
    assert client.files["/remote/dst/a.txt"] == b"old"  # untouched
    assert client.files["/remote/dst/b.txt"] == b"B"


async def test_download_tree_recursive(tmp_path):
    from compute_mcp.mcp_server import _download_tree

    client = FakeGatewayClient()
    client.dirs.update({"/remote/dst", "/remote/dst/sub"})
    client.files["/remote/dst/a.txt"] = b"A"
    client.files["/remote/dst/sub/b.bin"] = b"\x00\x01\x02"

    out = tmp_path / "out"
    report = await _download_tree(client, "hal", "/remote/dst", str(out))
    assert report["downloaded"] == 2
    assert (out / "a.txt").read_bytes() == b"A"
    assert (out / "sub" / "b.bin").read_bytes() == b"\x00\x01\x02"

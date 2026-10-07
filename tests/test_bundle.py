# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Unit tests for the deployable bundle support.

The tests use a fake route connection and a fake SFTP client; no real SSH or
SFTP is involved.  They cover the hash marker (skip vs upload), the
auto-deploy=false pin, the base64 fallback, the SSH public-key derivation, and
the argv construction.
"""

from __future__ import annotations

import base64
import contextlib
import posixpath
import stat as stat_module

import pytest

from compute_mcp import bundle as bundle_module
from compute_mcp.config import (
    BundleConfig,
    ConfigError,
    ContainerConfig,
    TargetConfig,
    TransportConfig,
)
from compute_mcp.bundle import (
    BundleError,
    ensure_deployed,
    load_bundle,
    marker_name,
    provision_argv,
    public_key_for,
    resolve_deploy_dir,
)


class _Result:
    def __init__(self, exit_status=0, stdout=b"", stderr=b""):
        self.exit_status = exit_status
        self.stdout = stdout
        self.stderr = stderr


class _FakeSFTP:
    """In-memory SFTP with the subset bundle.py uses."""

    def __init__(self, store: dict[str, bytes], fail: bool = False):
        self.store = store
        self.fail = fail
        self.mkdirs = []
        self.written: list[str] = []

    async def makedirs(self, path, exist_ok=False):
        if self.fail:
            raise OSError("sftp refused")
        self.mkdirs.append(path)
        return None

    async def lstat(self, path):
        raise FileNotFoundError(path)

    @contextlib.asynccontextmanager
    async def open(self, path, mode):
        if self.fail:
            raise OSError("sftp refused")
        if "w" in mode:
            buffer = bytearray()

            class _Handle:
                async def write(self, data):
                    buffer.extend(data)
                    return None

            try:
                yield _Handle()
            finally:
                self.store[path] = bytes(buffer)
                self.written.append(path)
        else:
            data = self.store[path]

            class _RHandle:
                async def read(self):
                    return data

            yield _RHandle()

    async def chmod(self, path, mode):
        if self.fail:
            raise OSError("sftp refused")

    async def readdir(self, path):
        return [
            _Entry(name)
            for name in self.store
            if posixpath.dirname(name) == path
        ]

    async def remove(self, path):
        self.store.pop(path, None)


class _Entry:
    def __init__(self, filename):
        self.filename = filename


def _sftp_factory(store, fail=False):
    def factory(conn):
        @contextlib.asynccontextmanager
        async def manager():
            yield _FakeSFTP(store, fail=fail)

        return manager()

    return factory


class _FakeConn:
    def __init__(self, store):
        self.store = store
        self.commands: list[str] = []

    async def run(self, command, **kwargs):
        self.commands.append(command)
        # 'test -f <marker>' style probe: report presence from the store.
        if command.startswith("test -f "):
            path = command.split("test -f ", 1)[1].strip().strip("'\"")
            return _Result(0 if path in self.store else 1)
        return _Result(0)


def _bundle_target(tmp_path, **bundle_kwargs):
    return TargetConfig(
        name="rosi",
        user="agent",
        transport=TransportConfig(
            kind="tunnel", ssh_targets=("rosi",), remote_port=2222
        ),
        client_key="/home/user/.ssh/key",
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
        container=ContainerConfig(
            runtime="apptainer", storage_root="/scratch/agent/computemcp"
        ),
        bundle=BundleConfig(source="computemcp-slurm", **bundle_kwargs),
    )


def test_load_bundle_has_provision_script_and_digest():
    contents = load_bundle("computemcp-slurm")
    names = {name for name, _ in contents.files}
    assert "computemcp-provision.sh" in names
    assert "computemcp-job.sh" in names
    assert len(contents.digest) == 64


def test_load_bundle_unknown_source_rejected():
    with pytest.raises(BundleError):
        load_bundle("does-not-exist")


def test_resolve_deploy_dir_prefers_explicit_then_storage_root(tmp_path):
    explicit = _bundle_target(tmp_path, deploy_dir="/scratch/x/b")
    assert resolve_deploy_dir(explicit) == "/scratch/x/b"
    derived = _bundle_target(tmp_path)
    assert resolve_deploy_dir(derived) == "/scratch/agent/computemcp/bundle"


def test_provision_argv_prefers_explicit_command(tmp_path):
    target = _bundle_target(tmp_path)
    assert provision_argv(target, "provision") == (
        "bash",
        "/scratch/agent/computemcp/bundle/computemcp-provision.sh",
        "provision",
    )
    assert provision_argv(target, "stop") == (
        "bash",
        "/scratch/agent/computemcp/bundle/computemcp-provision.sh",
        "stop",
    )


def test_provision_argv_explicit_command_wins(tmp_path):
    target = _bundle_target(tmp_path)
    object.__setattr__(target, "provision_command", ("/custom/prov.sh",))
    object.__setattr__(target, "close_command", ("/custom/close.sh",))
    assert provision_argv(target, "provision") == ("/custom/prov.sh",)
    assert provision_argv(target, "stop") == ("/custom/close.sh",)


@pytest.mark.asyncio
async def test_ensure_deployed_uploads_when_marker_absent(tmp_path):
    target = _bundle_target(tmp_path)
    store: dict[str, bytes] = {}
    conn = _FakeConn(store)
    sftp = _sftp_factory(store)
    deploy_dir = resolve_deploy_dir(target)

    await ensure_deployed(conn, target, sftp_factory=sftp)

    digest = load_bundle("computemcp-slurm").digest
    assert posixpath.join(deploy_dir, marker_name(digest)) in store
    assert posixpath.join(deploy_dir, "computemcp-provision.sh") in store
    assert store[posixpath.join(deploy_dir, "computemcp-provision.sh")].startswith(
        b"#!/usr/bin/env bash"
    )


@pytest.mark.asyncio
async def test_ensure_deployed_skips_when_marker_matches(tmp_path):
    target = _bundle_target(tmp_path)
    digest = load_bundle("computemcp-slurm").digest
    store = {posixpath.join(resolve_deploy_dir(target), marker_name(digest)): b""}
    conn = _FakeConn(store)

    def exploding_factory(_conn):
        raise AssertionError("SFTP must not be used when the marker matches")

    await ensure_deployed(conn, target, sftp_factory=exploding_factory)
    assert conn.commands and conn.commands[0].startswith("test -f ")


@pytest.mark.asyncio
async def test_auto_deploy_false_never_uploads(tmp_path):
    target = _bundle_target(tmp_path, auto_deploy=False)
    store: dict[str, bytes] = {}
    conn = _FakeConn(store)

    def exploding_factory(_conn):
        raise AssertionError("auto-deploy=false must not upload")

    await ensure_deployed(conn, target, sftp_factory=exploding_factory)
    assert store == {}


@pytest.mark.asyncio
async def test_ensure_deployed_falls_back_to_base64(tmp_path):
    target = _bundle_target(tmp_path)
    store: dict[str, bytes] = {}
    conn = _FakeConn(store)
    sftp = _sftp_factory(store, fail=True)

    await ensure_deployed(conn, target, sftp_factory=sftp)

    # The shell path decoded base64 payloads; the connection saw base64 commands.
    assert any("base64 -d" in command for command in conn.commands)
    digest = load_bundle("computemcp-slurm").digest
    marker_path = posixpath.join(resolve_deploy_dir(target), marker_name(digest))
    assert any(digest in command for command in conn.commands)


@pytest.mark.asyncio
async def test_shell_fallback_refuses_symlink(monkeypatch):
    """The base64 fallback must not write through a planted symlink."""
    from compute_mcp import bundle as bundle_module

    commands: list[str] = []

    class _Conn:
        async def run(self, command, **kwargs):
            commands.append(command)
            return _Result(0)

    async def fake_run(conn, command, timeout=120.0):
        return await conn.run(command)

    contents = load_bundle("computemcp-slurm")
    # The generated write command must contain a symlink guard.
    await bundle_module._deploy_shell(
        _Conn(), "/scratch/agent/computemcp/bundle", contents,
        marker_name(contents.digest), fake_run,
    )
    writes = [c for c in commands if "base64 -d" in c]
    assert writes and all("[ -L " in command for command in writes)
    # No write command may bypass the guard.
    assert all("refusing symlink" in command for command in writes)


def test_public_key_for_reads_pub_file(tmp_path):
    key = tmp_path / "id_ed25519"
    key.write_text("PRIVATE\n")
    (tmp_path / "id_ed25519.pub").write_text("ssh-ed25519 AAAA test@host\n")
    target = _bundle_target(tmp_path)
    object.__setattr__(target, "client_key", str(key))
    assert public_key_for(target) == "ssh-ed25519 AAAA test@host"


def test_public_key_for_missing_key_returns_none(tmp_path):
    target = _bundle_target(tmp_path)
    assert public_key_for(target) is None


class _HomeConn:
    """Connection that answers the HOME probe and records run commands."""

    def __init__(self, home="/home/remote"):
        self.home = home
        self.commands = []

    async def run(self, command, **kwargs):
        self.commands.append(command)
        if command == 'printf %s "$HOME"':
            return _Result(0, self.home.encode())
        if command.startswith("test -f "):
            return _Result(1)  # marker absent -> deploy
        return _Result(0)


@pytest.mark.asyncio
async def test_resolve_remote_dir_expands_home():
    from compute_mcp.bundle import resolve_remote_dir

    conn = _HomeConn("/home/remote")
    assert await resolve_remote_dir(conn, "$HOME/computemcp") == "/home/remote/computemcp"
    assert await resolve_remote_dir(conn, "~/computemcp") == "/home/remote/computemcp"
    assert await resolve_remote_dir(conn, "$HOME") == "/home/remote"
    assert await resolve_remote_dir(conn, "/abs/x") == "/abs/x"


@pytest.mark.asyncio
async def test_resolve_remote_dir_rejects_relative():
    from compute_mcp.bundle import BundleError, resolve_remote_dir

    with pytest.raises(BundleError):
        await resolve_remote_dir(_HomeConn(), "relative/path")


@pytest.mark.asyncio
async def test_ensure_deployed_expands_home_for_sftp(tmp_path):
    from compute_mcp.bundle import ensure_deployed, load_bundle, marker_name

    target = _bundle_target(tmp_path, deploy_dir="$HOME/computemcp/bundle")
    store: dict[str, bytes] = {}
    sftp = _sftp_factory(store)
    conn = _HomeConn("/home/remote")
    resolved = await ensure_deployed(conn, target, sftp_factory=sftp)
    assert resolved == "/home/remote/computemcp/bundle"
    digest = load_bundle("computemcp-slurm").digest
    assert f"/home/remote/computemcp/bundle/{marker_name(digest)}" in store

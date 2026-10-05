# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Unit tests for the ``ssh -G`` route resolver in ``compute_mcp.tunnel``.

Everything is hermetic: the ``_ssh_g`` layer (or the subprocess call it wraps)
is stubbed, so no real ``ssh`` binary and no network are used.  Async tests are
bounded by ``asyncio.wait_for`` where a regression could otherwise hang.
"""
from __future__ import annotations

import asyncio
import os

import pytest

from compute_mcp.config import SSHConfig, TargetConfig, TransportConfig
from compute_mcp.tunnel import (
    TunnelError,
    _iter_hops,
    _resolve_route,
    _route_client_keys,
    _split_jump,
    _ssh_g,
)


def _g_output(**fields) -> str:
    """Build a minimal ``ssh -G`` stdout string from key/value pairs."""
    return "\n".join(f"{key} {value}" for key, value in fields.items()) + "\n"


def _route(alias: str, **kw) -> dict:
    base = {
        "alias": alias,
        "hostname": alias,
        "user": None,
        "port": 22,
        "identityfiles": (),
        "jumps": (),
    }
    base.update(kw)
    return base


# ---------------------------------------------------------------------------
# Normal resolution
# ---------------------------------------------------------------------------

async def test_resolve_route_basic_fields(monkeypatch):
    async def fake_ssh_g(alias, ssh):
        assert alias == "hal"
        return _g_output(
            hostname="hal.example.org",
            user="agent",
            port="2200",
            identityfile="~/.ssh/id_ed25519",
        )

    monkeypatch.setattr("compute_mcp.tunnel._ssh_g", fake_ssh_g)
    info = await asyncio.wait_for(
        _resolve_route("hal", SSHConfig()), timeout=5.0
    )
    assert info["alias"] == "hal"
    assert info["hostname"] == "hal.example.org"
    assert info["user"] == "agent"
    assert info["port"] == 2200
    # ``~`` is expanded by the resolver.
    assert info["identityfiles"] == (os.path.expanduser("~/.ssh/id_ed25519"),)
    assert info["jumps"] == ()


async def test_resolve_route_repeated_identityfiles_and_quotes(monkeypatch):
    async def fake_ssh_g(alias, ssh):
        return (
            "hostname hal\n"
            "identityfile \"~/.ssh/id_rsa\"\n"
            "identityfile '/etc/ssh/id_ecdsa'\n"
            "identityfile ~/.ssh/id_rsa\n"
        )

    monkeypatch.setattr("compute_mcp.tunnel._ssh_g", fake_ssh_g)
    info = await _resolve_route("hal", SSHConfig())
    assert info["identityfiles"] == (
        os.path.expanduser("~/.ssh/id_rsa"),
        "/etc/ssh/id_ecdsa",
        os.path.expanduser("~/.ssh/id_rsa"),
    )


# ---------------------------------------------------------------------------
# ProxyJump parsing and recursion
# ---------------------------------------------------------------------------

def test_split_jump_user_host_port_forms():
    assert _split_jump("host") == (None, "host", None)
    assert _split_jump("hop") == (None, "hop", None)
    assert _split_jump("u@host") == ("u", "host", None)
    assert _split_jump("host:2222") == (None, "host", 2222)
    assert _split_jump("u@host:2222") == ("u", "host", 2222)
    assert _split_jump("[2001:db8::1]:2222") == (None, "2001:db8::1", 2222)
    assert _split_jump("[2001:db8::1]") == (None, "2001:db8::1", None)


async def test_proxyjump_chain_order_and_override(monkeypatch):
    """Route -> jump1 -> jump2; resolved client-nearest first, order preserved."""
    outputs = {
        "route": _g_output(
            hostname="route.example",
            user="agent",
            port="22",
            proxyjump="jump1",
        ),
        "jump1": _g_output(
            hostname="jump1.example",
            user="j1",
            port="2200",
            proxyjump="bob@jump2:2222",
        ),
        "jump2": _g_output(hostname="jump2.example", user="j2", port="22"),
    }

    async def fake_ssh_g(alias, ssh):
        return outputs[alias]

    monkeypatch.setattr("compute_mcp.tunnel._ssh_g", fake_ssh_g)
    info = await _resolve_route("route", SSHConfig())
    assert [j["alias"] for j in info["jumps"]] == ["jump1"]
    jump1 = info["jumps"][0]
    assert jump1["hostname"] == "jump1.example"
    assert [j["alias"] for j in jump1["jumps"]] == ["jump2"]
    jump2 = jump1["jumps"][0]
    # The ``bob@jump2:2222`` token overrides the resolved user and port.
    assert jump2["user"] == "bob"
    assert jump2["port"] == 2222


async def test_proxyjump_cycle_is_rejected(monkeypatch):
    outputs = {
        "a": _g_output(hostname="a", proxyjump="b"),
        "b": _g_output(hostname="b", proxyjump="a"),
    }

    async def fake_ssh_g(alias, ssh):
        return outputs[alias]

    monkeypatch.setattr("compute_mcp.tunnel._ssh_g", fake_ssh_g)
    with pytest.raises(TunnelError, match="cycle"):
        await _resolve_route("a", SSHConfig())


# ---------------------------------------------------------------------------
# ProxyCommand rejection
# ---------------------------------------------------------------------------

async def test_proxycommand_is_rejected(monkeypatch):
    async def fake_ssh_g(alias, ssh):
        return _g_output(hostname="hal", proxycommand="nc %h %p")

    monkeypatch.setattr("compute_mcp.tunnel._ssh_g", fake_ssh_g)
    with pytest.raises(TunnelError, match="ProxyCommand"):
        await _resolve_route("hal", SSHConfig())


# ---------------------------------------------------------------------------
# _ssh_g subprocess failure modes
# ---------------------------------------------------------------------------

class _FakeProc:
    def __init__(self, stdout=b"", stderr=b"", returncode=0, hang=False):
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        self._hang = hang
        self.killed = False

    async def communicate(self):
        if self._hang:
            await asyncio.sleep(3600)
        return self.stdout, self.stderr

    def kill(self):
        self.killed = True

    async def wait(self):
        return self.returncode


async def test_ssh_g_nonzero_exit_raises(monkeypatch):
    proc = _FakeProc(stderr=b"bad config", returncode=255)

    async def fake_exec(*argv, **kwargs):
        return proc

    monkeypatch.setattr(
        "compute_mcp.tunnel.asyncio.create_subprocess_exec", fake_exec
    )
    with pytest.raises(TunnelError, match="rc=255"):
        await _ssh_g("hal", SSHConfig())


async def test_ssh_g_timeout_raises_and_kills(monkeypatch):
    proc = _FakeProc(hang=True)

    async def fake_exec(*argv, **kwargs):
        return proc

    monkeypatch.setattr(
        "compute_mcp.tunnel.asyncio.create_subprocess_exec", fake_exec
    )
    ssh = SSHConfig(connect_timeout=0.05)
    with pytest.raises(TunnelError, match="timed out"):
        await asyncio.wait_for(_ssh_g("hal", ssh), timeout=5.0)
    assert proc.killed is True


async def test_ssh_g_exec_failure_raises(monkeypatch):
    async def fake_exec(*argv, **kwargs):
        raise OSError("ssh not found")

    monkeypatch.setattr(
        "compute_mcp.tunnel.asyncio.create_subprocess_exec", fake_exec
    )
    with pytest.raises(TunnelError, match="could not run local ssh"):
        await _ssh_g("hal", SSHConfig())


# ---------------------------------------------------------------------------
# _iter_hops ordering
# ---------------------------------------------------------------------------

def test_iter_hops_yields_jumps_first_then_route():
    jump2 = _route("jump2")
    jump1 = _route("jump1", jumps=(jump2,))
    route = _route("route", jumps=(jump1,))
    assert [h["alias"] for h in _iter_hops(route)] == ["jump2", "jump1", "route"]


# ---------------------------------------------------------------------------
# _route_client_keys filtering
# ---------------------------------------------------------------------------

def test_route_client_keys_skips_missing_and_keeps_target_key(tmp_path):
    present_a = tmp_path / "id_a"
    present_a.write_text("key-a")
    present_b = tmp_path / "id_b"
    present_b.write_text("key-b")
    missing = tmp_path / "does-not-exist"

    target = TargetConfig(
        name="hal",
        user="agent",
        transport=TransportConfig(kind="tunnel", ssh_targets=("hal",)),
        client_key=str(present_b),
    )
    info = _route(
        "route",
        identityfiles=(str(present_a), str(missing)),
        jumps=(
            _route(
                "jump",
                identityfiles=(str(missing), str(present_b)),
            ),
        ),
    )
    keys = _route_client_keys(target, info)
    # Jump hops come first (dial order), missing inherited defaults skipped,
    # duplicates removed, and the target.client_key already present is kept.
    assert keys == [str(present_b), str(present_a)]


def test_route_client_keys_keeps_missing_explicit_client_key(tmp_path):
    missing_explicit = tmp_path / "explicit-missing"
    target = TargetConfig(
        name="hal",
        user="agent",
        transport=TransportConfig(kind="tunnel", ssh_targets=("hal",)),
        client_key=str(missing_explicit),
    )
    keys = _route_client_keys(target, _route("hal"))
    assert keys == [str(missing_explicit)]

# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC

import asyncio
import os
import socket
import stat
import sys
from pathlib import Path

import pytest

from terok_compute.config import SSHConfig, TargetConfig, TransportConfig
from terok_compute.tunnel import (
    TunnelError,
    TunnelManager,
    allocate_loopback_port,
    probe,
)

FAKE_SSH = r'''
import socket, sys, time, signal

args = sys.argv[1:]
if any("broken" in a for a in args):
    sys.stderr.write("simulated failure\n")
    sys.exit(255)

local = None
for i, a in enumerate(args):
    if a == "-L":
        spec = args[i + 1]
        local = int(spec.split(":")[1])
        break

if local is None:
    sys.exit(0)

sock = socket.socket()
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(("127.0.0.1", local))
sock.listen(5)

def _term(*_):
    sock.close()
    sys.exit(0)

signal.signal(signal.SIGTERM, _term)

while True:
    try:
        conn, _ = sock.accept()
        conn.close()
    except OSError:
        break
'''


@pytest.fixture
def fake_ssh(tmp_path, monkeypatch):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    exe = bindir / "ssh"
    exe.write_text(f"#!{sys.executable}\n{FAKE_SSH}")
    exe.chmod(exe.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    monkeypatch.setenv("PATH", str(bindir) + os.pathsep + os.environ.get("PATH", ""))
    return bindir


def make_target(name, ssh_targets, host_key="SHA256:abcdefghijklmnopqrstuvwxyz0123456789"):
    return TargetConfig(
        name=name,
        user="agent",
        transport=TransportConfig(kind="tunnel", ssh_targets=tuple(ssh_targets)),
        client_key="/tmp/key",
        host_key_sha256=host_key,
    )


def test_allocate_loopback_port_avoids_reserved():
    ssh = SSHConfig(internal_port_min=31000, internal_port_max=31005)
    reserved = set()
    ports = [allocate_loopback_port(ssh, reserved) for _ in range(6)]
    assert len(set(ports)) == 6
    with pytest.raises(TunnelError):
        allocate_loopback_port(ssh, reserved)


async def test_failover_uses_first_working_route(fake_ssh):
    mgr = TunnelManager(SSHConfig(internal_port_min=31200, internal_port_max=31300))
    target = make_target("hal", ["broken-route", "good-route"])
    seen = []
    tunnel = await mgr.connect(target, on_route=lambda r, e: seen.append(r))
    try:
        assert tunnel.route == "good-route"
        assert seen == ["broken-route"]
        assert await probe("127.0.0.1", tunnel.local_port)
    finally:
        await tunnel.stop()
        mgr.release(tunnel)


async def test_all_routes_fail(fake_ssh):
    mgr = TunnelManager(SSHConfig(internal_port_min=31310, internal_port_max=31320))
    target = make_target("x", ["broken-a", "broken-b"])
    with pytest.raises(TunnelError):
        await mgr.connect(target)


async def test_stop_releases_port(fake_ssh):
    mgr = TunnelManager(SSHConfig(internal_port_min=31330, internal_port_max=31340))
    target = make_target("hal", ["good-route"])
    tunnel = await mgr.connect(target)
    port = tunnel.local_port
    assert port in mgr.reserved
    await tunnel.stop()
    mgr.release(tunnel)
    assert port not in mgr.reserved
    assert not await probe("127.0.0.1", port, timeout=1.0)


async def test_direct_transport_uses_endpoint(tmp_path):
    # Bind a real loopback listener and point direct transport at it.
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    port = sock.getsockname()[1]
    try:
        mgr = TunnelManager(SSHConfig())
        target = TargetConfig(
            name="local",
            user="agent",
            transport=TransportConfig(
                kind="direct", remote_host="127.0.0.1", remote_port=port
            ),
            host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
        )
        tunnel = await mgr.connect(target)
        assert tunnel.route is None
        assert tunnel.local_port == port
    finally:
        sock.close()


def test_probe_times_out_on_closed_port():
    # Grab a port then close it; probe should fail quickly.
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    assert asyncio.run(probe("127.0.0.1", port, timeout=1.0)) is False

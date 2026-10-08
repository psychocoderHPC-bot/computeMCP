# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import asyncio
import socket

import pytest

from compute_mcp.config import SSHConfig, TargetConfig, TransportConfig
from compute_mcp.ssh_backend import SSHError
from compute_mcp.tunnel import (
    TunnelError,
    TunnelManager,
    allocate_loopback_port,
    probe,
)


class _FakeRunResult:
    def __init__(self, exit_status, stdout, stderr):
        self.exit_status = exit_status
        self.stdout = stdout
        self.stderr = stderr


class _FakeConn:
    """Minimal asyncssh-connection stand-in for unit tests."""

    def __init__(self, addr):
        self._addr = addr
        self._closed = False
        self.forwarded = []

    def is_closed(self):
        return self._closed

    def close(self):
        self._closed = True

    async def wait_closed(self):
        return None

    async def forward_local_port(self, host, port, dest_host, dest_port):
        self.forwarded.append((host, port, dest_host, dest_port))
        return object()

    async def run(self, command, **kwargs):
        return _FakeRunResult(0, b"", b"")


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


async def test_failover_uses_first_working_route(monkeypatch):
    mgr = TunnelManager(SSHConfig(internal_port_min=31200, internal_port_max=31300))
    target = make_target("hal", ["broken-route", "good-route"])
    seen = []

    async def fake_resolve(alias, ssh, _seen=None):
        return {
            "alias": alias,
            "hostname": "127.0.0.1",
            "user": "agent",
            "port": 22,
            "identityfiles": (),
            "jumps": (),
        }

    async def fake_dial_route(**kwargs):
        if kwargs["name"] == "broken-route":
            raise SSHError("route broken")
        return _FakeConn(("127.0.0.1", kwargs["port"]))

    async def fake_probe(host, port, timeout=8.0):
        # Only the working route's forward is considered reachable.
        return port == 22 or port in mgr.reserved

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)
    monkeypatch.setattr("compute_mcp.tunnel.dial_route", fake_dial_route)
    monkeypatch.setattr("compute_mcp.tunnel.probe", fake_probe)

    # Make every allocated local port "reachable" so the failover picks the
    # second route.
    tunnel = await mgr.connect(target, on_route=lambda r, e: seen.append(r))
    try:
        assert tunnel.route == "good-route"
        assert seen == ["broken-route"]
    finally:
        await tunnel.stop()
        mgr.release(tunnel)


async def test_all_routes_fail(monkeypatch):
    mgr = TunnelManager(SSHConfig(internal_port_min=31310, internal_port_max=31320))
    target = make_target("x", ["broken-a", "broken-b"])

    async def fake_resolve(alias, ssh, _seen=None):
        return {
            "alias": alias,
            "hostname": "127.0.0.1",
            "user": "agent",
            "port": 22,
            "identityfiles": (),
            "jumps": (),
        }

    async def fake_dial_route(**kwargs):
        raise SSHError("route broken")

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)
    monkeypatch.setattr("compute_mcp.tunnel.dial_route", fake_dial_route)

    with pytest.raises(TunnelError):
        await mgr.connect(target)


async def test_stop_releases_port(monkeypatch):
    mgr = TunnelManager(SSHConfig(internal_port_min=31330, internal_port_max=31340))
    target = make_target("hal", ["good-route"])

    async def fake_resolve(alias, ssh, _seen=None):
        return {
            "alias": alias,
            "hostname": "127.0.0.1",
            "user": "agent",
            "port": 22,
            "identityfiles": (),
            "jumps": (),
        }

    conn = _FakeConn(("127.0.0.1", 22))

    async def fake_dial_route(**kwargs):
        return conn

    async def fake_probe(host, port, timeout=8.0):
        return True

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)
    monkeypatch.setattr("compute_mcp.tunnel.dial_route", fake_dial_route)
    monkeypatch.setattr("compute_mcp.tunnel.probe", fake_probe)

    tunnel = await mgr.connect(target)
    port = tunnel.local_port
    assert port in mgr.reserved
    assert tunnel.is_alive()
    await tunnel.stop()
    mgr.release(tunnel)
    assert port not in mgr.reserved
    assert not tunnel.is_alive()


async def test_proxy_jump_and_dynamic_endpoint_in_route(monkeypatch):
    """ProxyJump chains and the provisioned endpoint flow through the route."""
    mgr = TunnelManager(SSHConfig(internal_port_min=31400, internal_port_max=31410))
    target = make_target("rosi5", ["rosi5-alias"])
    transport = TransportConfig(
        kind="tunnel",
        ssh_targets=("rosi5-alias",),
        proxy_jump="rosi5",
        remote_host="cn123",
        remote_port=2345,
    )
    dialed = []
    run_calls = []

    async def fake_resolve(alias, ssh, _seen=None):
        if alias == "rosi5-alias":
            return {
                "alias": alias,
                "hostname": "127.0.0.1",
                "user": "agent",
                "port": 22,
                "identityfiles": (),
                "jumps": (
                    {
                        "alias": "rosi5",
                        "hostname": "127.0.0.1",
                        "user": "agent",
                        "port": 2200,
                        "identityfiles": (),
                        "jumps": (),
                    },
                ),
            }
        return {
            "alias": alias,
            "hostname": "127.0.0.1",
            "user": "agent",
            "port": 22,
            "identityfiles": (),
            "jumps": (),
        }

    class _SessionConn(_FakeConn):
        async def run(self, command, **kwargs):
            run_calls.append(command)
            return _FakeRunResult(0, b"cn123:2345\n", b"")

    jump_conn = _FakeConn(("127.0.0.1", 2200))
    final_conn = _SessionConn(("127.0.0.1", 22))

    async def fake_dial_route(**kwargs):
        # The first (jump) hop has tunnel=None; the final hop is dialled by
        # asyncssh directly in _dial_hop, so only the jump hits dial_route.
        dialed.append((kwargs["name"], kwargs["port"]))
        return jump_conn

    async def fake_asyncssh_connect(host, **kwargs):
        dialed.append((host, kwargs.get("port")))
        assert kwargs.get("tunnel") is jump_conn
        return final_conn

    async def fake_probe(host, port, timeout=8.0):
        return True

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)
    monkeypatch.setattr("compute_mcp.tunnel.dial_route", fake_dial_route)
    monkeypatch.setattr("compute_mcp.tunnel.asyncssh.connect", fake_asyncssh_connect)
    monkeypatch.setattr("compute_mcp.tunnel.probe", fake_probe)

    tunnel = await mgr.connect(target, transport=transport)
    try:
        assert tunnel.route == "rosi5-alias"
        # With an explicit transport override (the endpoint is already known)
        # provisioning is skipped, but the forward must still target the
        # transport endpoint through the ProxyJump chain.
        assert final_conn.forwarded == [
            ("127.0.0.1", tunnel.local_port, "cn123", 2345)
        ]
        # The jump hop is dialled through dial_route, and the final route hop
        # through asyncssh with tunnel=jump_conn.
        assert dialed[0] == ("rosi5-alias", 2200)
        assert dialed[1] == ("127.0.0.1", 22)
    finally:
        await tunnel.stop()
        mgr.release(tunnel)


def _hop(alias, jumps=()):
    return {
        "alias": alias,
        "hostname": "127.0.0.1",
        "user": "agent",
        "port": 22,
        "identityfiles": (),
        "jumps": tuple(jumps),
    }


async def test_open_for_route_honors_configured_proxy_jump(monkeypatch):
    """An explicit ``proxy_jump`` dials one ordered two-hop chain."""
    target = TargetConfig(
        name="hal",
        user="agent",
        transport=TransportConfig(
            kind="tunnel", ssh_targets=("hal",), proxy_jump="jump_alias"
        ),
        client_key="/tmp/key",
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    )
    routes = {"hal": _hop("hal"), "jump_alias": _hop("jump_alias")}

    resolved = []

    async def fake_resolve(alias, ssh, _seen=None):
        resolved.append(alias)
        return routes[alias]

    dialed = []
    tunnels = []
    returned = []

    async def fake_dial_hop(info, **kwargs):
        dialed.append(info["alias"])
        tunnels.append(kwargs.get("tunnel"))
        conn = _FakeConn((info["hostname"], info["port"]))
        returned.append(conn)
        return conn

    async def fake_probe(host, port, timeout=8.0):
        return True

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)
    monkeypatch.setattr("compute_mcp.tunnel._dial_hop", fake_dial_hop)
    monkeypatch.setattr("compute_mcp.tunnel.probe", fake_probe)

    mgr = TunnelManager(SSHConfig(internal_port_min=31700, internal_port_max=31710))
    tunnel = await mgr.open_for_route(target, "hal", 31700)
    try:
        # The configured jump is dialed first, the route alias last.
        assert dialed == ["jump_alias", "hal"]
        assert len(dialed) == len(set(dialed))
        assert set(resolved) == {"hal", "jump_alias"}
        # Both hops share one connection chain (no duplicate dialing).
        assert tunnels[0] is None
        assert tunnels[1] is returned[0]
        assert returned[1] is tunnel.connection
    finally:
        await tunnel.stop()


async def test_open_for_route_proxy_jump_precedes_alias_own_chain(monkeypatch):
    """The configured jump is composed ahead of the alias's own ProxyJump chain."""
    target = TargetConfig(
        name="hal",
        user="agent",
        transport=TransportConfig(
            kind="tunnel", ssh_targets=("hal",), proxy_jump="jump_alias"
        ),
        client_key="/tmp/key",
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    )
    route_info = _hop(
        "hal",
        jumps=(_hop("hal_inner"),),
    )
    jump_info = _hop("jump_alias", jumps=(_hop("jump_outer"),))
    routes = {"hal": route_info, "jump_alias": jump_info}

    async def fake_resolve(alias, ssh, _seen=None):
        return routes[alias]

    dialed = []

    async def fake_dial_hop(info, **kwargs):
        dialed.append(info["alias"])
        return _FakeConn((info["hostname"], info["port"]))

    async def fake_probe(host, port, timeout=8.0):
        return True

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)
    monkeypatch.setattr("compute_mcp.tunnel._dial_hop", fake_dial_hop)
    monkeypatch.setattr("compute_mcp.tunnel.probe", fake_probe)

    mgr = TunnelManager(SSHConfig(internal_port_min=31720, internal_port_max=31730))
    tunnel = await mgr.open_for_route(target, "hal", 31720)
    try:
        assert dialed == ["jump_outer", "jump_alias", "hal_inner", "hal"]
        assert len(dialed) == len(set(dialed))
    finally:
        await tunnel.stop()


async def test_open_for_route_without_proxy_jump_is_single_hop(monkeypatch):
    target = TargetConfig(
        name="hal",
        user="agent",
        transport=TransportConfig(kind="tunnel", ssh_targets=("hal",)),
        client_key="/tmp/key",
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    )
    resolved = []

    async def fake_resolve(alias, ssh, _seen=None):
        resolved.append(alias)
        return _hop(alias)

    dialed = []

    async def fake_dial_hop(info, **kwargs):
        dialed.append(info["alias"])
        return _FakeConn((info["hostname"], info["port"]))

    async def fake_probe(host, port, timeout=8.0):
        return True

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)
    monkeypatch.setattr("compute_mcp.tunnel._dial_hop", fake_dial_hop)
    monkeypatch.setattr("compute_mcp.tunnel.probe", fake_probe)

    mgr = TunnelManager(SSHConfig(internal_port_min=31740, internal_port_max=31750))
    tunnel = await mgr.open_for_route(target, "hal", 31740)
    try:
        assert dialed == ["hal"]
        assert resolved == ["hal"]
    finally:
        await tunnel.stop()


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


async def test_run_connect_command_runs_over_route_connection():
    mgr = TunnelManager(SSHConfig(internal_port_min=31500, internal_port_max=31510))
    target = make_target("hal", ["hal"])
    import dataclasses

    target = dataclasses.replace(
        target, connect_command=("/bin/up.sh", "--ensure")
    )
    seen = []

    class _Conn:
        async def run(self, command, **kwargs):
            seen.append(command)
            return _FakeRunResult(0, b"", b"")

    await mgr.run_connect_command(target, "hal", connection=_Conn())
    assert seen == ["/bin/up.sh --ensure"]


async def test_run_connect_command_without_connection_is_noop(caplog):
    mgr = TunnelManager(SSHConfig(internal_port_min=31520, internal_port_max=31530))
    target = make_target("hal", ["hal"])
    import dataclasses

    target = dataclasses.replace(target, connect_command=("/bin/up.sh",))
    with caplog.at_level("WARNING"):
        await mgr.run_connect_command(target, "hal", connection=None)
    assert "without a live route connection" in caplog.text


async def test_provision_on_route_sets_endpoint(monkeypatch):
    """Without a transport override, tunnel.connect provisions on the route."""
    mgr = TunnelManager(SSHConfig(internal_port_min=31540, internal_port_max=31550))
    target = make_target("hal", ["hal"])
    import dataclasses

    target = dataclasses.replace(
        target, provision_command=("printf", "cn7:4321\n")
    )

    class _SessionConn(_FakeConn):
        async def run(self, command, **kwargs):
            return _FakeRunResult(0, b"cn7:4321\n", b"")

    conn = _SessionConn(("127.0.0.1", 22))

    async def fake_resolve(alias, ssh, _seen=None):
        return {
            "alias": alias,
            "hostname": "127.0.0.1",
            "user": "agent",
            "port": 22,
            "identityfiles": (),
            "jumps": (),
        }

    async def fake_dial_route(**kwargs):
        return conn

    async def fake_probe(host, port, timeout=8.0):
        return True

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)
    monkeypatch.setattr("compute_mcp.tunnel.dial_route", fake_dial_route)
    monkeypatch.setattr("compute_mcp.tunnel.probe", fake_probe)

    tunnel = await mgr.connect(target)
    try:
        assert tunnel.provisioned_endpoint == ("cn7", 4321)
        assert conn.forwarded == [
            ("127.0.0.1", tunnel.local_port, "cn7", 4321)
        ]
    finally:
        await tunnel.stop()
        mgr.release(tunnel)


async def test_open_for_route_forwards_provisioned_when_provisioning_ran(monkeypatch):
    """When provisioning ran, the forward targets the PROVISIONED endpoint,
    not the value in the target's static transport.

    The static transport (remote_host:remote_port) is the placeholder the
    config author wrote; provisioning resolves the container's real published
    port, which can differ (and does, after a container restart).
    """
    mgr = TunnelManager(SSHConfig(internal_port_min=31550, internal_port_max=31560))
    target = make_target("hal", ["hal"])
    import dataclasses

    # A static endpoint that differs from what provisioning will report, so we
    # can prove provisioning wins.
    target = dataclasses.replace(
        target,
        transport=TransportConfig(
            kind="tunnel", ssh_targets=("hal",), remote_host="127.0.0.1", remote_port=2299
        ),
        provision_command=("printf", "cn7:4321\n"),
    )

    class _SessionConn(_FakeConn):
        async def run(self, command, **kwargs):
            return _FakeRunResult(0, b"cn7:4321\n", b"")

    conn = _SessionConn(("127.0.0.1", 22))

    async def fake_resolve(alias, ssh, _seen=None):
        return {
            "alias": alias,
            "hostname": "127.0.0.1",
            "user": "agent",
            "port": 22,
            "identityfiles": (),
            "jumps": (),
        }

    async def fake_dial_route(**kwargs):
        return conn

    async def fake_probe(host, port, timeout=8.0):
        return True

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)
    monkeypatch.setattr("compute_mcp.tunnel.dial_route", fake_dial_route)
    monkeypatch.setattr("compute_mcp.tunnel.probe", fake_probe)

    tunnel = await mgr.connect(target)
    try:
        assert tunnel.provisioned_endpoint == ("cn7", 4321)
        # Forward uses the provisioned endpoint, NOT the static 127.0.0.1:2299.
        assert conn.forwarded == [
            ("127.0.0.1", tunnel.local_port, "cn7", 4321)
        ]
        dests = {(h, p) for (_, _, h, p) in conn.forwarded}
        assert ("127.0.0.1", 2299) not in dests
    finally:
        await tunnel.stop()
        mgr.release(tunnel)


async def test_open_for_route_forwards_static_when_no_provisioning(monkeypatch):
    """Without a provision command (and no bundle) the forward targets the
    target's STATIC transport, and no endpoint is reported as provisioned."""
    mgr = TunnelManager(SSHConfig(internal_port_min=31560, internal_port_max=31570))
    target = make_target("hal", ["hal"])
    import dataclasses

    target = dataclasses.replace(
        target,
        transport=TransportConfig(
            kind="tunnel", ssh_targets=("hal",), remote_host="10.0.0.4", remote_port=2244
        ),
    )
    # No provision_command, no bundle: provisioning never runs.

    conn = _FakeConn(("127.0.0.1", 22))

    async def fake_resolve(alias, ssh, _seen=None):
        return {
            "alias": alias,
            "hostname": "127.0.0.1",
            "user": "agent",
            "port": 22,
            "identityfiles": (),
            "jumps": (),
        }

    async def fake_dial_route(**kwargs):
        return conn

    async def fake_probe(host, port, timeout=8.0):
        return True

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)
    monkeypatch.setattr("compute_mcp.tunnel.dial_route", fake_dial_route)
    monkeypatch.setattr("compute_mcp.tunnel.probe", fake_probe)

    tunnel = await mgr.connect(target)
    try:
        # No provisioning ran, so nothing was "provisioned".
        assert tunnel.provisioned_endpoint is None
        # The forward targets the static transport endpoint from config.
        assert conn.forwarded == [
            ("127.0.0.1", tunnel.local_port, "10.0.0.4", 2244)
        ]
    finally:
        await tunnel.stop()
        mgr.release(tunnel)


async def test_bundle_target_deploys_and_runs_derived_argv(monkeypatch):
    """A bundle target deploys over the route, then runs the deployed helper."""
    import dataclasses

    from compute_mcp import bundle as bundle_module
    from compute_mcp.config import BundleConfig, ContainerConfig

    mgr = TunnelManager(SSHConfig(internal_port_min=31560, internal_port_max=31570))
    target = make_target("rosi", ["rosi"])
    target = dataclasses.replace(
        target,
        transport=TransportConfig(
            kind="tunnel", ssh_targets=("rosi",), remote_port=2222
        ),
        container=ContainerConfig(
            runtime="apptainer", storage_root="/scratch/agent/computemcp"
        ),
        bundle=BundleConfig(source="computemcp-slurm"),
    )

    commands: list[str] = []
    deployed: list[str] = []

    async def fake_ensure(conn, tgt, **kwargs):
        deployed.append(bundle_module.resolve_deploy_dir(tgt))
        return deployed[-1]

    monkeypatch.setattr(bundle_module, "ensure_deployed", fake_ensure)

    class _SessionConn(_FakeConn):
        async def run(self, command, **kwargs):
            commands.append(command)
            return _FakeRunResult(0, b"cn9:4321\n", b"")

    conn = _SessionConn(("127.0.0.1", 22))

    async def fake_resolve(alias, ssh, _seen=None):
        return {
            "alias": alias,
            "hostname": "127.0.0.1",
            "user": "agent",
            "port": 22,
            "identityfiles": (),
            "jumps": (),
        }

    async def fake_dial_route(**kwargs):
        return conn

    async def fake_probe(host, port, timeout=8.0):
        return True

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)
    monkeypatch.setattr("compute_mcp.tunnel.dial_route", fake_dial_route)
    monkeypatch.setattr("compute_mcp.tunnel.probe", fake_probe)

    tunnel = await mgr.connect(target)
    try:
        assert deployed == ["/scratch/agent/computemcp/bundle"]
        provision = next(c for c in commands if "computemcp-provision.sh" in c)
        assert provision.endswith(
            "/scratch/agent/computemcp/bundle/computemcp-provision.sh provision"
        )
        assert tunnel.provisioned_endpoint == ("cn9", 4321)
    finally:
        await tunnel.stop()
        mgr.release(tunnel)


async def test_close_command_derives_bundle_stop_when_unset(monkeypatch):
    """A bundle target without close_command releases the allocation via the bundle."""
    import dataclasses

    from compute_mcp.config import BundleConfig, ContainerConfig

    mgr = TunnelManager(SSHConfig(internal_port_min=31580, internal_port_max=31590))
    target = make_target("rosi", ["rosi"])
    target = dataclasses.replace(
        target,
        transport=TransportConfig(
            kind="tunnel", ssh_targets=("rosi",), remote_port=2222
        ),
        container=ContainerConfig(
            runtime="apptainer", storage_root="/scratch/agent/computemcp"
        ),
        bundle=BundleConfig(source="computemcp-slurm"),
    )

    seen: list[str] = []

    class _Conn:
        async def run(self, command, **kwargs):
            seen.append(command)
            return _FakeRunResult(0, b"", b"")

    await mgr.run_close_command(target, "rosi", connection=_Conn())
    assert seen and seen[0].endswith(
        "/scratch/agent/computemcp/bundle/computemcp-provision.sh stop"
    )


async def test_close_command_bundle_prefixes_provision_env():
    """A derived bundle stop carries the COMPUTEMCP_* exports.

    Regression: the gateway ran the helper ``stop`` with no environment, so the
    helper exited 2 ("must be apptainer or docker") and the container leaked.
    The close command must be prefixed with the same shell-quoted exports the
    provision path emits.
    """
    import dataclasses

    from compute_mcp.config import BundleConfig, ContainerConfig

    mgr = TunnelManager(SSHConfig(internal_port_min=31580, internal_port_max=31590))
    target = make_target("rosi", ["rosi"])
    target = dataclasses.replace(
        target,
        transport=TransportConfig(
            kind="tunnel", ssh_targets=("rosi",), remote_port=2222
        ),
        container=ContainerConfig(
            runtime="apptainer", storage_root="/scratch/agent/computemcp"
        ),
        bundle=BundleConfig(source="computemcp-slurm"),
    )

    seen: list[str] = []

    class _Conn:
        async def run(self, command, **kwargs):
            seen.append(command)
            return _FakeRunResult(0, b"", b"")

    env = {
        "COMPUTEMCP_SYSTEM": "rosi",
        "COMPUTEMCP_CONTAINER_RUNTIME": "apptainer",
        "COMPUTEMCP_STORAGE_ROOT": "/scratch/agent/computemcp",
        "COMPUTEMCP_STATE_DIR": "/scratch/agent/computemcp/rosi/state",
        "COMPUTEMCP_CONTAINER_PORT": "2222",
        "COMPUTEMCP_SANDBOX": "true",
        "COMPUTEMCP_HOST_HOME": "",
        "COMPUTEMCP_IMAGE": "",
        "COMPUTEMCP_GPU_VENDORS": "",
        "COMPUTEMCP_PROVISION_ENV": "",
    }
    await mgr.run_close_command(
        target, "rosi", connection=_Conn(), provision_env=env
    )
    assert len(seen) == 1
    command = seen[0]
    assert "export COMPUTEMCP_CONTAINER_RUNTIME=apptainer;" in command
    assert "export COMPUTEMCP_STATE_DIR=/scratch/agent/computemcp/rosi/state;" in command
    assert "export COMPUTEMCP_SYSTEM=rosi;" in command
    # The exports precede the helper argv.
    assert command.endswith(
        "bash /scratch/agent/computemcp/bundle/computemcp-provision.sh stop"
    )


async def test_run_close_command_without_provision_env_unchanged():
    """An explicit close_command with no env keeps the previous exact shape."""
    import dataclasses

    mgr = TunnelManager(SSHConfig(internal_port_min=31600, internal_port_max=31610))
    target = dataclasses.replace(
        make_target("hal", ["hal"]), close_command=("scancel", "--name", "x")
    )
    seen: list[str] = []

    class _Conn:
        async def run(self, command, **kwargs):
            seen.append(command)
            return _FakeRunResult(0, b"", b"")

    await mgr.run_close_command(target, "hal", connection=_Conn())
    assert seen == ["scancel --name x"]


# ---------------------------------------------------------------------------
# run_connect_command failure paths (advisory, never fatal)
# ---------------------------------------------------------------------------
async def test_run_connect_command_nonzero_exit_is_logged_not_raised(caplog):
    mgr = TunnelManager(SSHConfig(internal_port_min=31600, internal_port_max=31610))
    import dataclasses

    target = dataclasses.replace(
        make_target("hal", ["hal"]), connect_command=("/bin/up.sh",)
    )

    class _Conn:
        async def run(self, command, **kwargs):
            return _FakeRunResult(7, b"out\n", b"boom\n")

    with caplog.at_level("WARNING"):
        await mgr.run_connect_command(target, "hal", connection=_Conn())
    assert "exited 7" in caplog.text
    assert "boom" in caplog.text


async def test_run_connect_command_exception_is_logged_not_raised(caplog):
    mgr = TunnelManager(SSHConfig(internal_port_min=31610, internal_port_max=31620))
    import dataclasses

    target = dataclasses.replace(
        make_target("hal", ["hal"]), connect_command=("/bin/up.sh",)
    )

    class _Conn:
        async def run(self, command, **kwargs):
            raise OSError("channel closed")

    with caplog.at_level("WARNING"):
        # Must not raise: recovery is advisory and the caller retries.
        await mgr.run_connect_command(target, "hal", connection=_Conn())
    assert "connect_command failed" in caplog.text


# ---------------------------------------------------------------------------
# run_close_command (advisory release, never fatal)
# ---------------------------------------------------------------------------

async def test_run_close_command_runs_shlex_quoted_with_timeout():
    import dataclasses

    mgr = TunnelManager(SSHConfig(internal_port_min=31660, internal_port_max=31670))
    target = dataclasses.replace(
        make_target("hal", ["hal"]),
        close_command=("scancel", "--name", "my job"),
        close_command_timeout=45.0,
    )
    seen = []

    class _Conn:
        async def run(self, command, **kwargs):
            seen.append((command, kwargs))
            return _FakeRunResult(0, b"", b"")

    await mgr.run_close_command(target, "hal", connection=_Conn())
    assert len(seen) == 1
    command, kwargs = seen[0]
    # argv is shlex-quoted (the embedded space is protected).
    assert command == "scancel --name 'my job'"
    assert kwargs["timeout"] == 45.0
    assert kwargs["check"] is False


async def test_run_close_command_nonzero_exit_is_not_raised(caplog):
    import dataclasses

    mgr = TunnelManager(SSHConfig(internal_port_min=31670, internal_port_max=31680))
    target = dataclasses.replace(
        make_target("hal", ["hal"]), close_command=("scancel",)
    )

    class _Conn:
        async def run(self, command, **kwargs):
            return _FakeRunResult(9, b"out\n", b"nope\n")

    with caplog.at_level("WARNING"):
        await mgr.run_close_command(target, "hal", connection=_Conn())
    assert "exited 9" in caplog.text
    assert "nope" in caplog.text


async def test_run_close_command_without_connection_is_noop(caplog):
    import dataclasses

    mgr = TunnelManager(SSHConfig(internal_port_min=31680, internal_port_max=31690))
    target = dataclasses.replace(
        make_target("hal", ["hal"]), close_command=("scancel",)
    )
    with caplog.at_level("WARNING"):
        await mgr.run_close_command(target, "hal", connection=None)
    assert "without a live route connection" in caplog.text


# ---------------------------------------------------------------------------
# open_for_route failure branches (fake route connection)
# ---------------------------------------------------------------------------

def _install_route(monkeypatch, conn, local_port):
    async def fake_resolve(alias, ssh, _seen=None):
        return {
            "alias": alias,
            "hostname": "127.0.0.1",
            "user": "agent",
            "port": 22,
            "identityfiles": (),
            "jumps": (),
        }

    async def fake_dial_route(**kwargs):
        return conn

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)
    monkeypatch.setattr("compute_mcp.tunnel.dial_route", fake_dial_route)
    return TunnelManager(
        SSHConfig(internal_port_min=local_port, internal_port_max=local_port)
    )


async def test_open_for_route_provision_nonzero_closes_all(monkeypatch):
    class _Conn(_FakeConn):
        async def run(self, command, **kwargs):
            return _FakeRunResult(3, b"", b"nope")

    target = make_target("hal", ["hal"])
    import dataclasses

    target = dataclasses.replace(target, provision_command=("provision",))
    conn = _Conn(("127.0.0.1", 22))
    mgr = _install_route(monkeypatch, conn, 31630)
    with pytest.raises(TunnelError, match="exited 3"):
        await mgr.open_for_route(target, "hal", 31630)
    assert conn.is_closed()


async def test_open_for_route_provision_no_endpoint_closes_all(monkeypatch):
    class _Conn(_FakeConn):
        async def run(self, command, **kwargs):
            return _FakeRunResult(0, b"nothing here\n", b"")

    target = make_target("hal", ["hal"])
    import dataclasses

    target = dataclasses.replace(target, provision_command=("provision",))
    conn = _Conn(("127.0.0.1", 22))
    mgr = _install_route(monkeypatch, conn, 31640)
    with pytest.raises(TunnelError, match="no .*endpoint"):
        await mgr.open_for_route(target, "hal", 31640)
    assert conn.is_closed()


async def test_open_for_route_forward_failure_releases_port(monkeypatch):
    class _Conn(_FakeConn):
        async def forward_local_port(self, *args, **kwargs):
            raise OSError("cannot bind")

    target = make_target("hal", ["hal"])
    conn = _Conn(("127.0.0.1", 22))
    mgr = _install_route(monkeypatch, conn, 31650)
    # connect() reserves a port, fails forwarding, and must release it again.
    with pytest.raises(TunnelError, match="no working route"):
        await mgr.connect(target)
    assert conn.is_closed()
    assert mgr.reserved == frozenset()


# ---------------------------------------------------------------------------
# format_provision_env: shell-quoted export statements
# ---------------------------------------------------------------------------

def test_format_provision_env_none_empty_and_plain_values():
    from compute_mcp.tunnel import format_provision_env

    assert format_provision_env(None) == ""
    assert format_provision_env({}) == ""
    text = format_provision_env(
        {"COMPUTEMCP_SYSTEM": "hal", "COMPUTEMCP_NODES": "1"}
    )
    # Simple word-safe values need no quoting (shlex.quote is a no-op for them).
    assert text == "export COMPUTEMCP_SYSTEM=hal; export COMPUTEMCP_NODES=1;"


def test_format_provision_env_quotes_values_with_spaces_newlines_and_shell_words():
    from compute_mcp.tunnel import format_provision_env

    # The SBATCH args value is one complete argument per line (no trailing
    # newline) plus shell metacharacters: the whole value must survive as a
    # single quoted word so the provision command sees it verbatim.
    value = "--gres=gpu:2\n--mem=378000M --job-name='my job' $(reboot) ;"
    text = format_provision_env({"COMPUTEMCP_SBATCH_ARGS": value})
    assert text.count("export ") == 1
    assert text.startswith("export COMPUTEMCP_SBATCH_ARGS=")
    assert text.endswith(";")
    # The quoted value must reproduce the exact byte sequence.
    quoted = text[len("export COMPUTEMCP_SBATCH_ARGS=") : -1]
    assert quoted.startswith("'") and quoted.endswith("'")
    # shlex.quote: a raw newline embedded inside a single-quoted value.
    assert "\n" in quoted and "$" in quoted


def test_format_provision_env_empty_value_is_quoted_empty():
    from compute_mcp.tunnel import format_provision_env

    text = format_provision_env(
        {"COMPUTEMCP_SBATCH_ARGS": "", "COMPUTEMCP_SANDBOX": "false"}
    )
    assert text == "export COMPUTEMCP_SBATCH_ARGS=''; export COMPUTEMCP_SANDBOX=false;"


def test_format_provision_env_rejects_nul():
    from compute_mcp.tunnel import TunnelError, format_provision_env

    with pytest.raises(TunnelError, match="NUL"):
        format_provision_env({"COMPUTEMCP_X": "a\x00b"})
    with pytest.raises(TunnelError, match="NUL"):
        format_provision_env({"COMPUTEMCP_X\x00": "a"})


async def test_dial_hop_empty_user_uses_asyncssh_sentinel(monkeypatch):
    """An empty target user must reach asyncssh as (), never None.

    _dial_hop resolves the username from the SSH config alias then the target
    fallback; when both are empty it must pass asyncssh's () sentinel so the
    local account is used, because None raises inside saslprep.
    """
    from compute_mcp import tunnel as tunnel_module
    from compute_mcp.config import SSHConfig

    seen = {}

    async def fake_dial_route(**kwargs):
        seen.update(kwargs)
        return object()

    monkeypatch.setattr(tunnel_module, "dial_route", fake_dial_route)
    await tunnel_module._dial_hop(
        {"hostname": "127.0.0.1", "port": 22, "user": ""},
        tunnel=None,
        client_keys=[],
        prompter=None,
        pin=None,
        ssh=SSHConfig(),
        name="route",
        fallback_user="",
    )
    assert seen["username"] == ()

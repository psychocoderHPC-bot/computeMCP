# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import asyncio
import base64
import dataclasses
import json
import time

import pytest
from aiohttp.test_utils import TestClient, TestServer

from compute_mcp.auth import hash_token
from compute_mcp.config import (
    ClientConfig,
    GatewayConfig,
    ServerConfig,
    SessionConfig,
    SSHConfig,
    TargetConfig,
    TransportConfig,
    parse_config,
)
from compute_mcp.gateway import Gateway, TargetRuntime


def make_gateway():
    target = TargetConfig(
        name="hal",
        user="agent",
        transport=TransportConfig(kind="direct", remote_host="127.0.0.1", remote_port=9),
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    )
    target2 = TargetConfig(
        name="gpu03",
        user="agent",
        transport=TransportConfig(kind="direct", remote_host="127.0.0.1", remote_port=9),
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    )
    cfg = GatewayConfig(
        server=ServerConfig(),
        ssh=SSHConfig(),
        sessions=SessionConfig(),
        targets={"hal": target, "gpu03": target2},
        clients={
            "alpaka": ClientConfig(
                client_id="alpaka",
                token_sha256=hash_token("alpaka-token"),
                targets=("hal",),
                label="alpaka project",
            ),
            "admin": ClientConfig(
                client_id="admin",
                token_sha256=hash_token("admin-token"),
                allow_all=True,
            ),
        },
    )
    return Gateway(cfg)


async def make_client(gateway):
    client = TestClient(TestServer(gateway.create_app()))
    await client.start_server()
    return client


def auth(token):
    return {"Authorization": f"Bearer {token}"}


async def test_missing_token_rejected():
    gw = make_gateway()
    client = await make_client(gw)
    try:
        resp = await client.get("/v1/targets")
        assert resp.status == 401
    finally:
        await client.close()


async def test_wrong_token_rejected():
    gw = make_gateway()
    client = await make_client(gw)
    try:
        resp = await client.get("/v1/targets", headers=auth("nope"))
        assert resp.status == 401
    finally:
        await client.close()


async def test_discovery_hides_unauthorized_targets():
    gw = make_gateway()
    client = await make_client(gw)
    try:
        resp = await client.get("/v1/targets", headers=auth("alpaka-token"))
        assert resp.status == 200
        body = await resp.json()
        names = {t["name"] for t in body["targets"]}
        assert names == {"hal"}

        resp = await client.get("/v1/targets", headers=auth("admin-token"))
        body = await resp.json()
        names = {t["name"] for t in body["targets"]}
        assert names == {"hal", "gpu03"}
    finally:
        await client.close()


async def test_unauthorized_target_is_forbidden_not_leaked():
    gw = make_gateway()
    client = await make_client(gw)
    try:
        resp = await client.get("/v1/targets/gpu03", headers=auth("alpaka-token"))
        assert resp.status == 403
        # unknown target behaves the same for a legit client of its own ACL
        resp = await client.get("/v1/targets/does-not-exist", headers=auth("alpaka-token"))
        assert resp.status == 403
    finally:
        await client.close()


async def test_exec_uses_backend(monkeypatch):
    gw = make_gateway()

    class FakeResult:
        exit_status = 0
        stdout = b"container-host\n"
        stderr = b""

    seen = {}

    async def fake_run(conn, command, cwd=None, timeout=None, env=None, stdin=None):
        seen.update(command=command, cwd=cwd, env=env, stdin=stdin)
        return FakeResult()

    async def fake_provider(name):
        return gw.config.targets[name], object()

    monkeypatch.setattr(gw.backend, "run", fake_run)
    monkeypatch.setattr(gw, "_connection_provider", fake_provider)

    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/exec",
            headers=auth("alpaka-token"),
            json={"target": "hal", "command": "hostname"},
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["exit_status"] == 0
        assert body["stdout"] == "container-host\n"

        # env and stdin are forwarded to the backend
        resp = await client.post(
            "/v1/exec",
            headers=auth("alpaka-token"),
            json={
                "target": "hal",
                "command": "cat",
                "env": {"FOO": "bar"},
                "stdin": "hello\n",
            },
        )
        assert resp.status == 200
        assert seen["env"] == {"FOO": "bar"}
        assert seen["stdin"] == "hello\n"

        resp = await client.post(
            "/v1/exec",
            headers=auth("alpaka-token"),
            json={"target": "gpu03", "command": "hostname"},
        )
        assert resp.status == 403

        # a malformed env is rejected
        resp = await client.post(
            "/v1/exec",
            headers=auth("alpaka-token"),
            json={"target": "hal", "command": "x", "env": "not-an-object"},
        )
        assert resp.status == 400
    finally:
        await client.close()


async def test_session_isolation_between_clients(monkeypatch):
    gw = make_gateway()

    class FakeProcess:
        stdin = type("S", (), {"write": lambda self, d: None})()
        channel = type("C", (), {"change_terminal_size": lambda self, c, r: None})()
        exit_status = None

        async def wait_closed(self):
            return None

        def terminate(self):
            return None

    async def fake_provider(name):
        return gw.config.targets[name], object()

    async def fake_create(conn, cwd, columns, rows, term="xterm-256color"):
        return FakeProcess()

    monkeypatch.setattr(gw.sessions, "provider", fake_provider)
    monkeypatch.setattr(gw.backend, "create_session", fake_create)

    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/sessions",
            headers=auth("admin-token"),
            json={"target": "hal"},
        )
        assert resp.status == 200
        sid = (await resp.json())["session_id"]

        # Another client must not see or use it.
        resp = await client.get(f"/v1/sessions/{sid}", headers=auth("alpaka-token"))
        assert resp.status == 404
        resp = await client.post(
            f"/v1/sessions/{sid}/write",
            headers=auth("alpaka-token"),
            json={"data": "x"},
        )
        assert resp.status == 404

        # Owner can.
        resp = await client.get(f"/v1/sessions/{sid}", headers=auth("admin-token"))
        assert resp.status == 200
    finally:
        await client.close()


async def test_session_read_wait_blocks_then_returns(monkeypatch):
    gw = make_gateway()

    class _NeverEndingStdout:
        async def read(self, n):
            await asyncio.sleep(3600)

    class FakeProcess:
        stdin = type("S", (), {"write": lambda self, d: None})()
        channel = type("C", (), {"change_terminal_size": lambda self, c, r: None})()
        exit_status = None
        stdout = _NeverEndingStdout()

        async def wait_closed(self):
            return None

        def terminate(self):
            return None

    async def fake_provider(name):
        return gw.config.targets[name], object()

    async def fake_create(conn, cwd, columns, rows, term="xterm-256color"):
        return FakeProcess()

    monkeypatch.setattr(gw.sessions, "provider", fake_provider)
    monkeypatch.setattr(gw.backend, "create_session", fake_create)

    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/sessions", headers=auth("alpaka-token"), json={"target": "hal"}
        )
        sid = (await resp.json())["session_id"]

        # simulate output arriving shortly after the waiting read starts
        async def produce():
            await asyncio.sleep(0.3)
            session = gw.sessions._sessions[sid]
            session.output_buffer.extend(b"late output\n")
            session.updated_at = time.time()

        producer = asyncio.create_task(produce())

        # non-waiting read returns immediately and empty
        resp = await client.post(
            f"/v1/sessions/{sid}/read",
            headers=auth("alpaka-token"),
            json={"max_bytes": 0, "wait": 0},
        )
        assert (await resp.json())["data"] == ""

        # waiting read blocks until the output appears
        resp = await client.post(
            f"/v1/sessions/{sid}/read",
            headers=auth("alpaka-token"),
            json={"max_bytes": 0, "wait": 2.0},
        )
        assert "late output" in (await resp.json())["data"]
        await producer
    finally:
        await client.close()


async def test_file_chmod_endpoint(monkeypatch):
    gw = make_gateway()

    class FakeSFTP:
        def __init__(self):
            self.calls = []

        def exit(self):
            pass

        async def chmod(self, path, mode):
            self.calls.append((path, mode))

    holder = {}

    class FakeSftpCtx:
        def __init__(self, conn):
            self._sftp = FakeSFTP()
            holder["sftp"] = self._sftp

        async def __aenter__(self):
            return self._sftp

        async def __aexit__(self, *a):
            return False

    async def fake_provider(name):
        return gw.config.targets[name], object()

    monkeypatch.setattr(gw, "_connection_provider", fake_provider)
    monkeypatch.setattr("compute_mcp.gateway.sftp_client", lambda conn: FakeSftpCtx(conn))

    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/files/chmod",
            headers=auth("alpaka-token"),
            params={"target": "hal"},
            json={"path": "/work/x.sh", "mode": "755"},
        )
        assert resp.status == 200, await resp.text()
        assert holder["sftp"].calls == [("/work/x.sh", 0o755)]

        # invalid mode rejected
        resp = await client.post(
            "/v1/files/chmod",
            headers=auth("alpaka-token"),
            params={"target": "hal"},
            json={"path": "/work/x.sh", "mode": "99z"},
        )
        assert resp.status == 502
    finally:
        await client.close()


async def test_reload_add_remove_unchanged(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(
        """
        [clients.admin]
        token = "admin-token"
        targets = ["*"]

        [targets.hal]
        transport = "direct"
        remote_host = "127.0.0.1"
        remote_port = 9
        user = "agent"
        host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
        """
    )
    from compute_mcp.config import load_config

    cfg = load_config(cfg_file)
    gw = Gateway(cfg)
    assert set(gw.runtimes) == {"hal"}

    # add gpu03, remove hal
    cfg_file.write_text(
        """
        [clients.admin]
        token = "admin-token"
        targets = ["*"]

        [targets.gpu03]
        transport = "direct"
        remote_host = "127.0.0.1"
        remote_port = 9
        user = "agent"
        host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
        """
    )
    report = await gw.reload()
    assert report["added"] == ["gpu03"]
    assert report["removed"] == ["hal"]
    assert set(gw.runtimes) == {"gpu03"}


async def test_reload_malformed_keeps_old_config(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(
        """
        [clients.admin]
        token = "admin-token"
        targets = ["*"]

        [targets.hal]
        transport = "direct"
        remote_host = "127.0.0.1"
        remote_port = 9
        user = "agent"
        host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
        """
    )
    from compute_mcp.config import ConfigError, load_config

    cfg = load_config(cfg_file)
    gw = Gateway(cfg)
    cfg_file.write_text("this is = = not toml")
    with pytest.raises(ConfigError):
        await gw.reload()
    assert set(gw.runtimes) == {"hal"}


async def test_health_no_auth_required():
    gw = make_gateway()
    client = await make_client(gw)
    try:
        resp = await client.get("/v1/health")
        assert resp.status == 200
    finally:
        await client.close()


def test_file_helpers_base64_roundtrip():
    data = bytes(range(256))
    encoded = base64.b64encode(data).decode("ascii")
    assert base64.b64decode(encoded) == data


def make_dedicated_gateway():
    target = TargetConfig(
        name="hal",
        user="agent",
        transport=TransportConfig(kind="direct", remote_host="127.0.0.1", remote_port=9),
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
        connect_mode="dedicated",
    )
    cfg = GatewayConfig(
        server=ServerConfig(),
        ssh=SSHConfig(),
        sessions=SessionConfig(),
        targets={"hal": target},
        clients={
            "admin": ClientConfig(
                client_id="admin",
                token_sha256=hash_token("admin-token"),
                allow_all=True,
            ),
        },
    )
    return Gateway(cfg)


async def test_dedicated_exec_opens_and_closes_connection(monkeypatch):
    gw = make_dedicated_gateway()
    opened = []
    closed = []

    class FakeResult:
        exit_status = 0
        stdout = b"ok\n"
        stderr = b""

    async def fake_open(target, host, port, prompter=None):
        opened.append(target.name)
        return object()

    async def fake_run(conn, command, cwd=None, timeout=None, env=None, stdin=None):
        return FakeResult()

    async def fake_close(conn):
        closed.append(True)

    monkeypatch.setattr(gw.backend, "open_connection", fake_open)
    monkeypatch.setattr(gw.backend, "run", fake_run)
    monkeypatch.setattr(gw.backend, "close_connection", fake_close)
    monkeypatch.setattr(gw, "ensure_connected", lambda name: _noop())
    monkeypatch.setattr(gw, "_endpoint", lambda rt: ("127.0.0.1", 9))

    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/exec",
            headers=auth("admin-token"),
            json={"target": "hal", "command": "hostname"},
        )
        assert resp.status == 200
        assert opened == ["hal"], opened
        assert closed == [True], closed
    finally:
        await client.close()


async def _noop():
    return None


async def test_dedicated_session_closes_connection_on_close(monkeypatch):
    gw = make_dedicated_gateway()
    opened = []
    closed = []
    dedicated = []

    class FakeProcess:
        stdin = type("S", (), {"write": lambda self, d: None})()
        channel = type("C", (), {"change_terminal_size": lambda self, c, r: None})()
        exit_status = None

        async def wait_closed(self):
            return None

        def terminate(self):
            return None

    async def fake_dedicated_provider(name):
        opened.append(name)
        conn = object()
        dedicated.append(conn)
        return gw.config.targets[name], conn

    async def fake_close(conn):
        closed.append(conn)

    async def fake_create(conn, cwd, columns, rows, term="xterm-256color"):
        return FakeProcess()

    monkeypatch.setattr(gw.sessions, "dedicated_provider", fake_dedicated_provider)
    monkeypatch.setattr(gw.backend, "create_session", fake_create)
    monkeypatch.setattr(gw.backend, "close_connection", fake_close)

    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/sessions", headers=auth("admin-token"), json={"target": "hal"}
        )
        assert resp.status == 200, await resp.text()
        sid = (await resp.json())["session_id"]
        assert opened == ["hal"]

        resp = await client.get("/v1/sessions", headers=auth("admin-token"))
        sessions = (await resp.json())["sessions"]
        assert sessions[0]["connection"] == "dedicated"

        resp = await client.delete(f"/v1/sessions/{sid}", headers=auth("admin-token"))
        assert resp.status == 200
        assert closed == dedicated, (closed, dedicated)
    finally:
        await client.close()


async def test_upload_endpoint_streams_body_to_sftp(monkeypatch):
    gw = make_gateway()
    stored = bytearray()

    class FakeHandle:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def write(self, data):
            stored.extend(data)

    class FakeSFTP:
        def exit(self):
            pass

        def open(self, path, mode):
            return FakeHandle()

        async def stat(self, path):
            return type(
                "A",
                (),
                {"permissions": 0o100644, "size": len(stored), "uid": 1, "gid": 1, "mtime": 0},
            )()

    async def fake_provider(name):
        return gw.config.targets[name], object()

    class FakeSftpCtx:
        def __init__(self, conn):
            self._sftp = FakeSFTP()

        async def __aenter__(self):
            return self._sftp

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(gw, "_connection_provider", fake_provider)
    monkeypatch.setattr(
        "compute_mcp.gateway.sftp_client", lambda conn: FakeSftpCtx(conn)
    )

    client = await make_client(gw)
    try:
        payload = bytes(range(256)) * 32
        resp = await client.put(
            "/v1/files/upload",
            headers=auth("alpaka-token"),
            params={"target": "hal", "path": "/tmp/x.bin"},
            data=payload,
        )
        assert resp.status == 200, await resp.text()
        body = await resp.json()
        assert body["written"] == len(payload)
        assert bytes(stored) == payload
    finally:
        await client.close()


def test_parse_provision_endpoint():
    from compute_mcp.tunnel import parse_provision_endpoint

    assert parse_provision_endpoint("cn123:2345\n") == ("cn123", 2345)
    assert parse_provision_endpoint("ENDPOINT 10.0.0.5:2222\n") == ("10.0.0.5", 2222)
    assert parse_provision_endpoint("job 123 running\nENDPOINT cn1:9\n") == ("cn1", 9)
    assert parse_provision_endpoint("no endpoint here\n") is None
    assert parse_provision_endpoint("host:99999\n") is None
    # the first valid line wins
    assert parse_provision_endpoint("a:1\nb:2\n") == ("a", 1)


async def test_provision_runs_command_and_overrides_endpoint(monkeypatch):
    """Provisioning now happens inside tunnel.connect; the gateway stores it."""
    gw = make_gateway()
    target = _tunnel_target(provision_command=("printf", "cmd01:2200\n"))
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    seen = {}

    async def fake_connect(target, on_route=None, factor=None, **kwargs):
        seen["kwargs"] = kwargs
        return _FakeTunnel(
            target, route="rosi5", local_port=30000,
            provisioned_endpoint=("cmd01", 2200),
        )

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)
    runtime = gw.runtimes["hal"]
    await gw._connect_locked(gw.config.targets["hal"], runtime)
    # No transport override: a non-None transport would make TunnelManager skip
    # provisioning (provision=False) and the provision_command would never run.
    assert "transport" not in seen["kwargs"]
    assert runtime.provisioned_endpoint == "cmd01:2200"


async def test_provision_failure_raises(monkeypatch):
    from compute_mcp.tunnel import TunnelError

    gw = make_gateway()
    target = _tunnel_target(provision_command=("printf", "no endpoint\n"))
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )

    async def fake_connect(target, on_route=None, transport=None, factor=None):
        raise TunnelError("provisioning printed no host:port endpoint")

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)
    runtime = gw.runtimes["hal"]
    with pytest.raises(TunnelError):
        await gw._connect_locked(gw.config.targets["hal"], runtime)
    assert runtime.state == "failed"
    assert "no host:port endpoint" in runtime.last_error


async def test_connect_uses_provisioned_transport(monkeypatch):
    gw = make_gateway()
    target = _tunnel_target(provision_command=("printf", "cn9:3210\n"))
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    seen = {}

    async def fake_connect(target, on_route=None, factor=None, **kwargs):
        seen["kwargs"] = kwargs
        return _FakeTunnel(
            target, route="rosi5", local_port=30001,
            provisioned_endpoint=("cn9", 3210),
        )

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)
    runtime = gw.runtimes["hal"]
    await gw._connect_locked(gw.config.targets["hal"], runtime)
    # Provisioning lives inside tunnel.connect: the gateway must NOT pass a
    # transport override, otherwise TunnelManager.connect would skip
    # provision_command (provision=False) and the static endpoint would win.
    assert "transport" not in seen["kwargs"]
    assert runtime.provisioned_endpoint == "cn9:3210"
    assert gw.public_status("hal")["provisioned_endpoint"] == "cn9:3210"


async def test_tunnel_connect_provision_flag_depends_on_override(monkeypatch):
    """TunnelManager provisions only when no transport override is supplied."""
    from compute_mcp.config import SSHConfig
    from compute_mcp.tunnel import TunnelManager

    mgr = TunnelManager(SSHConfig(internal_port_min=31700, internal_port_max=31710))
    target = _tunnel_target(provision_command=("printf", "cn:1\n"))
    flags = []

    async def fake_open_for_route(
        self, target, route, local_port, transport=None, *, factor=None, provision=True
    ):
        flags.append(provision)
        return _FakeTunnel(target, route=route, local_port=local_port)

    monkeypatch.setattr(TunnelManager, "open_for_route", fake_open_for_route)

    # No override: the target's provision_command must run.
    await mgr.connect(target)
    assert flags == [True]

    # Explicit override: the endpoint is already known, provisioning is skipped.
    override = dataclasses.replace(
        target.transport, remote_host="cn", remote_port=2222
    )
    await mgr.connect(target, transport=override)
    assert flags == [True, False]


async def test_connect_target_does_not_suppress_provisioning(monkeypatch):
    """connect_target must let TunnelManager run provision_command.

    Regression guard: passing ``transport=target.transport`` made
    ``TunnelManager.connect`` treat the endpoint as already discovered and call
    ``open_for_route(..., provision=False)``, so provisioning never ran.
    """
    gw = make_gateway()
    target = _tunnel_target(provision_command=("printf", "127.0.0.1:2224\n"))
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    seen = {}

    async def fake_connect(target, on_route=None, transport=None, factor=None, **kwargs):
        seen["transport"] = transport
        seen["kwargs"] = kwargs
        return _FakeTunnel(
            target, route="r", local_port=1234,
            provisioned_endpoint=("127.0.0.1", 2224), connection=None,
        )

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)
    result = await gw.connect_target("hal")
    # No transport override: provisioning is not suppressed.
    assert seen["transport"] is None
    assert "transport" not in seen["kwargs"]
    assert gw.runtimes["hal"].provisioned_endpoint == "127.0.0.1:2224"
    assert result["state"] == "connected"


class _FakeTunnel:
    """Stand-in for tunnel.RouteTunnel with the attributes the gateway reads."""

    def __init__(
        self,
        target,
        route="direct",
        local_port=9,
        provisioned_endpoint=None,
        connection=None,
        alive=True,
    ):
        self.target = target
        self.route = route
        self.local_port = local_port
        self.provisioned_endpoint = provisioned_endpoint
        self.connection = connection
        self.process = None
        self._alive = alive
        self.stopped = False

    def is_alive(self):
        return self._alive

    async def stop(self):
        self.stopped = True
        self._alive = False


async def test_interactive_prompter_prompts_when_console_active(monkeypatch):
    gw = make_gateway()
    gw._interactive = True
    prompts = []

    async def fake_prompt_line(prompt, echo):
        prompts.append((prompt, echo))
        return "otp-123"

    monkeypatch.setattr(gw, "_prompt_line", fake_prompt_line)
    result = await gw._prompter("hal", "Password: ", False)
    assert result == "otp-123"
    assert prompts == [("[hal] Password: ", False)]


async def test_prompter_caps_attempts(monkeypatch):
    gw = make_gateway()
    gw._interactive = True
    prompts = []

    async def fake_prompt_line(prompt, echo):
        prompts.append(prompt)
        return "bad"

    monkeypatch.setattr(gw, "_prompt_line", fake_prompt_line)
    prompter = gw._make_prompter("hal")
    results = [await prompter("Password: ", False) for _ in range(5)]
    assert results[:3] == ["bad", "bad", "bad"]
    assert results[3:] == [None, None]
    assert len(prompts) == 3


async def test_interactive_prompter_returns_none_when_headless(monkeypatch):
    gw = make_gateway()
    gw._interactive = False

    async def fail_line(prompt, echo):
        raise AssertionError("must not prompt when headless")

    monkeypatch.setattr(gw, "_prompt_line", fail_line)
    assert await gw._prompter("hal", "Password: ", False) is None


async def test_headless_interactive_target_raises_clear_error(monkeypatch):
    from compute_mcp.gateway import InteractiveAuthRequired
    from compute_mcp.ssh_backend import SSHError

    gw = make_gateway()
    gw._interactive = False
    # mark the target as requiring interactive auth
    target = gw.config.targets["hal"]
    import dataclasses

    gw.config = dataclasses.replace(
        gw.config,
        targets={
            **gw.config.targets,
            "hal": dataclasses.replace(target, interactive_auth=True),
        },
    )

    async def boom(target, host, port, prompter=None):
        raise SSHError("Permission denied")

    async def no_connect(name):
        return object()

    monkeypatch.setattr(gw.backend, "connection", boom)
    monkeypatch.setattr(gw, "ensure_connected", no_connect)
    monkeypatch.setattr(gw, "_endpoint", lambda rt: ("127.0.0.1", 9))

    with pytest.raises(InteractiveAuthRequired):
        await gw._open_container_conn("hal")


async def test_interactive_target_connects_with_factor(monkeypatch):
    """An interactive target dials the route with the supplied factor."""
    import dataclasses

    gw = make_gateway()
    target = gw.config.targets["hal"]
    gw.config = dataclasses.replace(
        gw.config,
        targets={
            **gw.config.targets,
            "hal": dataclasses.replace(target, interactive_auth=True),
        },
    )
    seen = {}

    async def fake_connect(target, on_route=None, transport=None, factor=None):
        seen["factor"] = factor
        return _FakeTunnel(target, route="hal", local_port=31000)

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)
    result = await gw.connect_target("hal", factor="SECRET")
    assert seen["factor"] == "SECRET"
    assert result["state"] == "connected"
    assert "warning" not in result
    # the factor must never be exposed in the public status
    assert "SECRET" not in json.dumps(result)


async def test_interactive_target_without_factor_warns_and_skips(monkeypatch):
    import dataclasses

    gw = make_gateway()
    target = gw.config.targets["hal"]
    gw.config = dataclasses.replace(
        gw.config,
        targets={
            **gw.config.targets,
            "hal": dataclasses.replace(target, interactive_auth=True),
        },
    )
    called = []

    async def fake_connect(target, on_route=None, transport=None, factor=None):
        called.append(factor)
        return _FakeTunnel(target)

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)
    result = await gw.connect_target("hal", factor=None)
    assert called == []  # never dialled without a factor
    assert result["state"] == "disconnected"
    assert "warning" in result
    assert "interactive authentication" in result["warning"]


async def test_sharing_exposed_in_target_status_and_discovery(monkeypatch):
    import dataclasses

    gw = make_gateway()
    hal = gw.config.targets["hal"]
    gw.config = dataclasses.replace(
        gw.config,
        targets={**gw.config.targets, "hal": dataclasses.replace(hal, sharing="exclusive")},
    )
    client = await make_client(gw)
    try:
        resp = await client.get("/v1/targets/hal", headers=auth("alpaka-token"))
        assert (await resp.json())["sharing"] == "exclusive"

        resp = await client.get("/v1/targets", headers=auth("admin-token"))
        body = await resp.json()
        by_name = {t["name"]: t for t in body["targets"]}
        assert by_name["hal"]["sharing"] == "exclusive"
        assert by_name["gpu03"]["sharing"] == "unknown"
    finally:
        await client.close()


def test_public_status_node_info_empty_for_runtime_without_target():
    gw = make_gateway()
    gw.runtimes["orphan"] = TargetRuntime(name="orphan")
    assert "orphan" not in gw.config.targets
    assert gw.public_status("orphan")["node_info"] == []


async def test_node_info_exposed_in_target_status_and_discovery(monkeypatch):
    import dataclasses

    gw = make_gateway()
    hal = gw.config.targets["hal"]
    gw.config = dataclasses.replace(
        gw.config,
        targets={
            **gw.config.targets,
            "hal": dataclasses.replace(hal, node_info=("GPU nvidia", "x86 CPU")),
        },
    )
    client = await make_client(gw)
    try:
        resp = await client.get("/v1/targets/hal", headers=auth("alpaka-token"))
        assert (await resp.json())["node_info"] == ["GPU nvidia", "x86 CPU"]

        resp = await client.get("/v1/targets", headers=auth("admin-token"))
        body = await resp.json()
        by_name = {t["name"]: t for t in body["targets"]}
        assert by_name["hal"]["node_info"] == ["GPU nvidia", "x86 CPU"]
        assert by_name["gpu03"]["node_info"] == []
        assert "node_info" in by_name["gpu03"]
    finally:
        await client.close()


def test_public_status_agent_empty_for_runtime_without_target():
    gw = make_gateway()
    gw.runtimes["orphan"] = TargetRuntime(name="orphan")
    assert "orphan" not in gw.config.targets
    assert gw.public_status("orphan")["agent"] == []


async def test_agent_exposed_in_target_status_and_discovery(monkeypatch):
    import dataclasses

    gw = make_gateway()
    hal = gw.config.targets["hal"]
    gw.config = dataclasses.replace(
        gw.config,
        targets={
            **gw.config.targets,
            "hal": dataclasses.replace(
                hal, agent=(("opencode", "GWen 3.5"), ("codex", "Sole"))
            ),
        },
    )
    client = await make_client(gw)
    try:
        resp = await client.get("/v1/targets/hal", headers=auth("alpaka-token"))
        assert (await resp.json())["agent"] == [
            {"agent": "opencode", "model": "GWen 3.5"},
            {"agent": "codex", "model": "Sole"},
        ]

        resp = await client.get("/v1/targets", headers=auth("admin-token"))
        body = await resp.json()
        assert isinstance(body["targets"], list)
        by_name = {t["name"]: t for t in body["targets"]}
        assert by_name["hal"]["agent"] == [
            {"agent": "opencode", "model": "GWen 3.5"},
            {"agent": "codex", "model": "Sole"},
        ]
        assert by_name["gpu03"]["agent"] == []
        assert "agent" in by_name["gpu03"]
    finally:
        await client.close()


async def test_clients_endpoint_admin_only_and_lists_acl(monkeypatch):
    gw = make_gateway()
    client = await make_client(gw)
    try:
        # non-admin denied
        resp = await client.get("/v1/clients", headers=auth("alpaka-token"))
        assert resp.status == 403

        resp = await client.get("/v1/clients", headers=auth("admin-token"))
        assert resp.status == 200
        body = await resp.json()
        by_name = {c["name"]: c for c in body["clients"]}
        assert set(by_name) == {"alpaka", "admin"}
        assert by_name["alpaka"]["label"] == "alpaka project"
        assert by_name["alpaka"]["targets"] == ["hal"]
        assert by_name["alpaka"]["allow_all"] is False
        assert by_name["admin"]["allow_all"] is True
        assert by_name["admin"]["targets"] == ["gpu03", "hal"]
        assert by_name["alpaka"]["token_fingerprint"].startswith("sha256:")
        assert "alpaka-token" not in json.dumps(body)

        resp = await client.get("/v1/clients/alpaka", headers=auth("admin-token"))
        assert resp.status == 200
        assert (await resp.json())["name"] == "alpaka"

        resp = await client.get("/v1/clients/nope", headers=auth("admin-token"))
        assert resp.status == 404
    finally:
        await client.close()


async def test_admin_sees_all_sessions_but_client_only_own(monkeypatch):
    gw = make_gateway()

    class FakeProcess:
        stdin = type("S", (), {"write": lambda self, d: None})()
        channel = type("C", (), {"change_terminal_size": lambda self, c, r: None})()
        exit_status = None

        async def wait_closed(self):
            return None

        def terminate(self):
            return None

    async def fake_provider(name):
        return gw.config.targets[name], object()

    async def fake_create(conn, cwd, columns, rows, term="xterm-256color"):
        return FakeProcess()

    monkeypatch.setattr(gw.sessions, "provider", fake_provider)
    monkeypatch.setattr(gw.backend, "create_session", fake_create)

    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/sessions", headers=auth("alpaka-token"), json={"target": "hal"}
        )
        sid = (await resp.json())["session_id"]

        # owner sees one
        resp = await client.get("/v1/sessions", headers=auth("alpaka-token"))
        assert len((await resp.json())["sessions"]) == 1

        # non-admin cannot ask for the all view
        resp = await client.get("/v1/sessions?all=true", headers=auth("alpaka-token"))
        assert resp.status == 403

        # admin sees it and the owner field
        resp = await client.get("/v1/sessions?all=true", headers=auth("admin-token"))
        sessions = (await resp.json())["sessions"]
        assert len(sessions) == 1
        assert sessions[0]["client"] == "alpaka"

        # admin can fetch and close another client's session
        resp = await client.get(f"/v1/sessions/{sid}", headers=auth("admin-token"))
        assert resp.status == 200
        assert (await resp.json())["client"] == "alpaka"

        resp = await client.get(f"/v1/clients/alpaka/sessions", headers=auth("admin-token"))
        assert len((await resp.json())["sessions"]) == 1

        resp = await client.delete(f"/v1/sessions/{sid}", headers=auth("admin-token"))
        assert resp.status == 200

        resp = await client.delete(
            "/v1/clients/alpaka/sessions", headers=auth("admin-token")
        )
        assert (await resp.json())["closed"] == 0
    finally:
        await client.close()


async def test_clients_kill_closes_sessions(monkeypatch):
    gw = make_gateway()

    class FakeProcess:
        stdin = type("S", (), {"write": lambda self, d: None})()
        channel = type("C", (), {"change_terminal_size": lambda self, c, r: None})()
        exit_status = None

        async def wait_closed(self):
            return None

        def terminate(self):
            return None

    async def fake_provider(name):
        return gw.config.targets[name], object()

    async def fake_create(conn, cwd, columns, rows, term="xterm-256color"):
        return FakeProcess()

    monkeypatch.setattr(gw.sessions, "provider", fake_provider)
    monkeypatch.setattr(gw.backend, "create_session", fake_create)

    client = await make_client(gw)
    try:
        for _ in range(2):
            await client.post(
                "/v1/sessions", headers=auth("alpaka-token"), json={"target": "hal"}
            )
        resp = await client.delete(
            "/v1/clients/alpaka/sessions", headers=auth("admin-token")
        )
        assert (await resp.json())["closed"] == 2
        resp = await client.get("/v1/clients/alpaka", headers=auth("admin-token"))
        assert (await resp.json())["session_count"] == 0
    finally:
        await client.close()


async def test_reload_endpoint_admin_only(tmp_path):
    from compute_mcp.config import load_config

    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(
        """
        [clients.admin]
        token = "admin-token"
        targets = ["*"]

        [targets.hal]
        transport = "direct"
        remote_host = "127.0.0.1"
        remote_port = 9
        user = "agent"
        host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
        """
    )
    gw = Gateway(load_config(cfg_file))
    client = await make_client(gw)
    try:
        resp = await client.post("/v1/reload", headers=auth("admin-token"))
        assert resp.status == 200
        assert (await resp.json())["reloaded"] is True
    finally:
        await client.close()


async def test_console_clients_and_actions(monkeypatch):
    gw = make_gateway()
    import io
    import contextlib

    # console listing
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        await gw._console_command("clients")
    out = buf.getvalue()
    assert "alpaka" in out and "admin" in out and "alpaka project" in out

    # client-refresh only touches allowed targets of that client
    calls = []

    async def fake_refresh(target):
        calls.append(target)
        return {"state": "connected", "active_route": target}

    monkeypatch.setattr(gw, "refresh_target", fake_refresh)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        await gw._console_command("client-refresh alpaka")
    assert calls == ["hal"], calls


async def test_console_target_commands_without_argument_print_usage():
    """Bare connect/refresh/reconnect/stop/client must not raise IndexError."""
    import io
    import contextlib

    gw = make_gateway()
    for cmd in ("connect", "refresh", "reconnect", "stop", "client"):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            keep_going = await gw._console_command(cmd)
        out = buf.getvalue()
        assert keep_going is True
        assert out.startswith(f"usage: {cmd} <"), (cmd, out)


async def test_generate_tokens_writes_hashes_and_authenticates(tmp_path):
    from compute_mcp.auth import hash_token
    from compute_mcp.gateway import generate_tokens
    import io
    import contextlib as _ctx

    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(
        """
        [clients.alpaka]
        targets = ["hal"]

        [targets.hal]
        transport = "direct"
        remote_host = "127.0.0.1"
        remote_port = 9
        user = "agent"
        host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
        """
    )
    out = tmp_path / "tokens.toml"
    buf = io.StringIO()
    with _ctx.redirect_stdout(buf):
        rc = generate_tokens(str(cfg_file), str(out), None)
    assert rc == 0
    text = out.read_text()
    assert "sha256:" in text and "alpaka" in text
    # extract plaintext token from printed output and check it hashes to stored
    import re

    match = re.search(r"alpaka:\s+(\S+)", buf.getvalue())
    assert match
    assert hash_token(match.group(1)) in text
    # token file mode must be restrictive
    assert (out.stat().st_mode & 0o077) == 0


async def test_upload_requires_target_acl(monkeypatch):
    gw = make_gateway()
    client = await make_client(gw)
    try:
        resp = await client.put(
            "/v1/files/upload",
            headers=auth("alpaka-token"),
            params={"target": "gpu03", "path": "/tmp/x"},
            data=b"data",
        )
        assert resp.status == 403
    finally:
        await client.close()


async def _enroll_flow(tmp_path, monkeypatch):
    """Build a gateway backed by real files so enrollment can persist."""
    from compute_mcp.config import load_config
    from compute_mcp.gateway import Gateway

    cfg = tmp_path / "config.toml"
    cfg.write_text(
        """
        [clients.ci]
        token = "ci-secret"
        targets = ["hal"]

        [clients.admin]
        token = "admin-token"
        targets = ["*"]

        [targets.hal]
        transport = "direct"
        remote_host = "127.0.0.1"
        remote_port = 9
        user = "agent"
        host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
        """
    )
    gw = Gateway(load_config(cfg))
    return gw, cfg


async def test_enroll_request_approve_delivers_token(tmp_path, monkeypatch):
    gw, cfg = await _enroll_flow(tmp_path, monkeypatch)
    client = await make_client(gw)
    try:
        # Unauthenticated request to enroll.
        resp = await client.post(
            "/v1/enroll",
            json={"client_id": "picongpu-bot-dev2", "targets": ["hal"], "label": "PIC"},
        )
        assert resp.status == 200, await resp.text()
        created = await resp.json()
        rid, secret = created["request_id"], created["poll_secret"]

        # Pending until approved.
        resp = await client.get(f"/v1/enroll/{rid}", headers={"X-Enroll-Secret": secret})
        assert (await resp.json())["status"] == "pending"

        # Admin can see it and approve it.
        resp = await client.get("/v1/enroll-requests", headers=auth("admin-token"))
        assert [r["client_id"] for r in (await resp.json())["pending"]] == [
            "picongpu-bot-dev2"
        ]
        resp = await client.post(
            f"/v1/enroll-requests/{rid}/approve", headers=auth("admin-token")
        )
        assert resp.status == 200, await resp.text()

        # The requester now receives the token exactly once.
        resp = await client.get(f"/v1/enroll/{rid}", headers={"X-Enroll-Secret": secret})
        body = await resp.json()
        assert body["status"] == "approved" and body["token"]
        token = body["token"]

        # And the delivered token authenticates against the live gateway.
        resp = await client.get("/v1/targets", headers=auth(token))
        assert resp.status == 200
        names = [t["name"] for t in (await resp.json())["targets"]]
        assert names == ["hal"]

        # Second poll: token already consumed.
        resp = await client.get(f"/v1/enroll/{rid}", headers={"X-Enroll-Secret": secret})
        assert (await resp.json())["status"] == "consumed"

        # Config file gained the client, comment-free but parseable.
        assert "[clients.picongpu-bot-dev2]" in cfg.read_text()
    finally:
        await client.close()


async def test_enroll_poll_requires_secret(tmp_path, monkeypatch):
    gw, _ = await _enroll_flow(tmp_path, monkeypatch)
    client = await make_client(gw)
    try:
        resp = await client.post("/v1/enroll", json={"client_id": "x", "targets": []})
        rid = (await resp.json())["request_id"]
        resp = await client.get(f"/v1/enroll/{rid}", headers={"X-Enroll-Secret": "nope"})
        assert resp.status == 404
    finally:
        await client.close()


async def test_enroll_rejects_existing_client_and_unknown_target(tmp_path, monkeypatch):
    gw, _ = await _enroll_flow(tmp_path, monkeypatch)
    client = await make_client(gw)
    try:
        resp = await client.post("/v1/enroll", json={"client_id": "ci", "targets": []})
        assert resp.status == 409
        resp = await client.post(
            "/v1/enroll", json={"client_id": "newci", "targets": ["nope"]}
        )
        assert resp.status == 400
    finally:
        await client.close()


async def test_enroll_disabled_returns_403(tmp_path):
    from compute_mcp.config import load_config
    from compute_mcp.gateway import Gateway

    cfg = tmp_path / "config.toml"
    cfg.write_text(
        """
        [server]
        allow_enrollment = false

        [clients.ci]
        token = "ci-secret"
        targets = ["hal"]

        [targets.hal]
        transport = "direct"
        remote_host = "127.0.0.1"
        remote_port = 9
        user = "agent"
        host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
        """
    )
    gw = Gateway(load_config(cfg))
    client = await make_client(gw)
    try:
        resp = await client.post("/v1/enroll", json={"client_id": "x", "targets": []})
        assert resp.status == 403
    finally:
        await client.close()


async def test_enroll_deny_is_reported(tmp_path, monkeypatch):
    gw, _ = await _enroll_flow(tmp_path, monkeypatch)
    client = await make_client(gw)
    try:
        resp = await client.post("/v1/enroll", json={"client_id": "x", "targets": []})
        created = await resp.json()
        rid, secret = created["request_id"], created["poll_secret"]
        await client.post(f"/v1/enroll-requests/{rid}/deny", headers=auth("admin-token"))
        resp = await client.get(f"/v1/enroll/{rid}", headers={"X-Enroll-Secret": secret})
        assert (await resp.json())["status"] == "denied"
    finally:
        await client.close()


async def test_approve_rolls_back_token_when_config_append_fails(tmp_path, monkeypatch):
    """A failed config append must not leave an orphan token hash behind."""
    from compute_mcp.config import load_config, load_tokens
    from compute_mcp.gateway import Gateway
    import compute_mcp.gateway as gwmod

    cfg = tmp_path / "config.toml"
    cfg.write_text(
        """
        [clients.admin]
        token = "admin-token"
        targets = ["*"]

        [targets.hal]
        transport = "direct"
        remote_host = "127.0.0.1"
        remote_port = 9
        user = "agent"
        host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
        """
    )
    gw = Gateway(load_config(cfg))
    pending, _ = gw.enrollments.create("newci", ("hal",), None, None)

    def boom(*a, **k):
        raise RuntimeError("simulated append failure")

    # Fail only the config append, after the token hash was already written.
    monkeypatch.setattr(gwmod, "append_client", boom)
    with pytest.raises(RuntimeError):
        await gw.approve_enrollment(pending.request_id)

    tokens = tmp_path / "tokens.toml"
    assert tokens.exists()
    assert "newci" not in load_tokens(tokens)


# -- connect_command recovery ------------------------------------------------

def _tunnel_target(name="hal", **overrides):
    import dataclasses

    base = TargetConfig(
        name=name,
        user="agent",
        transport=TransportConfig(kind="tunnel", ssh_targets=("hal",)),
        client_key="/tmp/key",
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    )
    if overrides:
        base = dataclasses.replace(base, **overrides)
    return base


async def test_connect_command_on_failure_runs_and_retries(monkeypatch):
    from compute_mcp.ssh_backend import SSHError

    gw = make_gateway()
    import dataclasses

    target = _tunnel_target(connect_command=("/bin/up.sh",))
    gw.config = dataclasses.replace(gw.config, targets={**gw.config.targets, "hal": target})

    calls = {"connect_cmd": 0, "attempt": 0}
    route_conn = object()

    async def fake_ensure(name):
        # Simulate an existing live tunnel whose route connection is used for
        # the recovery command.
        gw.runtimes[name].tunnel = _FakeTunnel(
            target, route="hal", local_port=2222, connection=route_conn
        )
        return gw.runtimes[name]

    def fake_endpoint(runtime):
        return "127.0.0.1", 2222

    async def fake_run_connect_command(t, route, connection=None):
        calls["connect_cmd"] += 1
        calls["route"] = route
        calls["connection"] = connection

    async def fake_connection(t, host, port, prompter=None):
        calls["attempt"] += 1
        if calls["attempt"] == 1:
            raise SSHError("container down")
        return "CONN"

    async def fake_disconnect(name):
        return None

    monkeypatch.setattr(gw, "ensure_connected", fake_ensure)
    monkeypatch.setattr(gw, "_endpoint", fake_endpoint)
    monkeypatch.setattr(gw.tunnels, "run_connect_command", fake_run_connect_command)
    monkeypatch.setattr(gw.backend, "connection", fake_connection)
    monkeypatch.setattr(gw.backend, "disconnect", fake_disconnect)

    t, conn = await gw._open_container_conn("hal")
    assert conn == "CONN"
    assert calls["connect_cmd"] == 1
    assert calls["attempt"] == 2
    assert calls["route"] == "hal"
    assert calls["connection"] is route_conn


async def test_connect_command_on_failure_not_run_on_success(monkeypatch):
    gw = make_gateway()
    import dataclasses

    target = _tunnel_target(connect_command=("/bin/up.sh",))
    gw.config = dataclasses.replace(gw.config, targets={**gw.config.targets, "hal": target})
    calls = {"connect_cmd": 0}

    async def fake_ensure(name):
        return gw.runtimes[name]

    def fake_endpoint(runtime):
        return "127.0.0.1", 2222

    async def fake_run_connect_command(t, route):
        calls["connect_cmd"] += 1

    async def fake_connection(t, host, port, prompter=None):
        return "CONN"

    monkeypatch.setattr(gw, "ensure_connected", fake_ensure)
    monkeypatch.setattr(gw, "_endpoint", fake_endpoint)
    monkeypatch.setattr(gw.tunnels, "run_connect_command", fake_run_connect_command)
    monkeypatch.setattr(gw.backend, "connection", fake_connection)

    t, conn = await gw._open_container_conn("hal")
    assert conn == "CONN"
    assert calls["connect_cmd"] == 0


async def test_connect_command_always_runs_before_open(monkeypatch):
    gw = make_gateway()
    import dataclasses

    target = _tunnel_target(
        connect_command=("/bin/up.sh",), connect_command_mode="always"
    )
    gw.config = dataclasses.replace(gw.config, targets={**gw.config.targets, "hal": target})
    calls = {"connect_cmd": 0}
    route_conn = object()

    async def fake_ensure(name):
        gw.runtimes[name].tunnel = _FakeTunnel(
            target, route="hal", local_port=2222, connection=route_conn
        )
        return gw.runtimes[name]

    def fake_endpoint(runtime):
        return "127.0.0.1", 2222

    async def fake_run_connect_command(t, route, connection=None):
        calls["connect_cmd"] += 1
        calls["connection"] = connection

    async def fake_connection(t, host, port, prompter=None):
        return "CONN"

    monkeypatch.setattr(gw, "ensure_connected", fake_ensure)
    monkeypatch.setattr(gw, "_endpoint", fake_endpoint)
    monkeypatch.setattr(gw.tunnels, "run_connect_command", fake_run_connect_command)
    monkeypatch.setattr(gw.backend, "connection", fake_connection)

    t, conn = await gw._open_container_conn("hal")
    assert conn == "CONN"
    assert calls["connect_cmd"] == 1
    assert calls["connection"] is route_conn


# ============================================================================
# 2FA factor matrix and auto-connect / route-loss guards (fake route tunnel)
# ============================================================================

def _interactive_gateway(**target_overrides):
    """A gateway whose `hal` target is interactive_auth with tunnel transport."""
    gw = make_gateway()
    target = _tunnel_target(interactive_auth=True, **target_overrides)
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    return gw, target


def _install_fake_connect(monkeypatch, gw, recorder):
    async def fake_connect(target, on_route=None, transport=None, factor=None):
        recorder["calls"].append({"target": target.name, "factor": factor})
        return _FakeTunnel(
            target, route=target.transport.ssh_targets[0], local_port=31000,
            provisioned_endpoint=None,
        )

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)


async def test_factor_matrix_interactive_with_factor(monkeypatch):
    gw, _ = _interactive_gateway()
    rec = {"calls": []}
    _install_fake_connect(monkeypatch, gw, rec)

    result = await gw.connect_target("hal", factor="SECRET")
    assert rec["calls"] == [{"target": "hal", "factor": "SECRET"}]
    assert result["state"] == "connected"
    assert "warning" not in result


async def test_factor_matrix_interactive_without_factor(monkeypatch):
    gw, _ = _interactive_gateway()
    rec = {"calls": []}
    _install_fake_connect(monkeypatch, gw, rec)

    result = await gw.connect_target("hal", factor=None)
    assert rec["calls"] == []  # no dial
    assert result["state"] == "disconnected"
    assert result["warning"]
    assert "interactive authentication" in result["warning"]


async def test_factor_matrix_non_interactive_with_factor(monkeypatch):
    gw = make_gateway()
    target = _tunnel_target(interactive_auth=False)
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    rec = {"calls": []}
    _install_fake_connect(monkeypatch, gw, rec)

    result = await gw.connect_target("hal", factor="SECRET")
    # The ignored factor must not be forwarded downstream (key-only connect).
    assert rec["calls"] == [{"target": "hal", "factor": None}]
    assert result["state"] == "connected"
    assert "warning" in result
    assert "does not use interactive_auth" in result["warning"]


async def test_factor_matrix_non_interactive_without_factor(monkeypatch):
    gw = make_gateway()
    target = _tunnel_target(interactive_auth=False)
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    rec = {"calls": []}
    _install_fake_connect(monkeypatch, gw, rec)

    result = await gw.connect_target("hal", factor=None)
    assert rec["calls"] == [{"target": "hal", "factor": None}]
    assert result["state"] == "connected"
    assert "warning" not in result


async def test_refresh_interactive_no_factor_no_teardown(monkeypatch):
    gw, _ = _interactive_gateway()
    rec = {"calls": []}
    _install_fake_connect(monkeypatch, gw, rec)

    disconnected = []
    stopped = []

    async def fake_disconnect(name):
        disconnected.append(name)

    async def fake_stop():
        stopped.append("tunnel")

    # Simulate an existing live tunnel that must NOT be torn down.
    gw.runtimes["hal"].state = "connected"
    live = _FakeTunnel(
        gw.config.targets["hal"], route="hal", local_port=31000, connection=object()
    )
    live.stop = fake_stop
    gw.runtimes["hal"].tunnel = live

    monkeypatch.setattr(gw.backend, "disconnect", fake_disconnect)
    result = await gw.refresh_target("hal", factor=None)
    assert rec["calls"] == []  # no reconnect dial
    assert disconnected == []
    assert stopped == []  # no teardown
    assert result["state"] == "connected"
    assert "warning" in result


# -- auto_connect guard -----------------------------------------------------

async def test_start_skips_auto_connect_for_interactive(monkeypatch):
    gw, _ = _interactive_gateway(auto_connect=True)
    scheduled = []

    def fake_safe_connect(name):
        scheduled.append(name)

        async def _done():
            return None

        return _done()

    monkeypatch.setattr(gw, "_safe_connect", fake_safe_connect)
    monkeypatch.setattr(gw.sessions, "start", lambda: None)

    async def fake_watch():
        return None

    monkeypatch.setattr(gw, "_watch_tunnels", fake_watch)
    await gw.start()
    # The interactive target must not be auto-connected.
    assert scheduled == []
    assert gw.runtimes["hal"].state != "connecting"


async def test_apply_config_skips_auto_connect_for_interactive(monkeypatch):
    gw, target = _interactive_gateway(auto_connect=True)
    target2 = dataclasses.replace(
        gw.config.targets["gpu03"], auto_connect=True, interactive_auth=False
    )
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "gpu03": target2}
    )
    # Remove all runtimes so both targets are "added".
    gw.runtimes.clear()
    scheduled = []

    def fake_safe_connect(name):
        scheduled.append(name)

        async def _done():
            return None

        return _done()

    monkeypatch.setattr(gw, "_safe_connect", fake_safe_connect)

    report = await gw.apply_config(gw.config)
    assert set(report["added"]) == {"hal", "gpu03"}
    # Only the non-interactive target got a scheduled connect.
    assert scheduled == ["gpu03"]


# -- route loss behaviour ---------------------------------------------------

async def test_route_loss_non_interactive_schedules_reconnect(monkeypatch):
    gw = make_gateway()
    target = _tunnel_target(interactive_auth=False)
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    gw.runtimes["hal"].state = "connected"
    gw.runtimes["hal"].tunnel = _FakeTunnel(target, route="hal", local_port=31000)

    scheduled = []

    def fake_backoff(name):
        scheduled.append(name)

        async def _done():
            return None

        return _done()

    monkeypatch.setattr(gw.backend, "disconnect", _async_noop)
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)
    monkeypatch.setattr(gw, "_reconnect_with_backoff", fake_backoff)

    await gw._handle_loss("hal", "tunnel connection closed")
    runtime = gw.runtimes["hal"]
    assert runtime.needs_refresh is True
    assert runtime.state == "disconnected"
    assert scheduled == ["hal"]


async def test_route_loss_interactive_fails_closed(monkeypatch):
    gw, target = _interactive_gateway()
    gw.runtimes["hal"].state = "connected"
    gw.runtimes["hal"].tunnel = _FakeTunnel(target, route="hal", local_port=31000)

    scheduled = []
    monkeypatch.setattr(gw.backend, "disconnect", _async_noop)
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)
    monkeypatch.setattr(
        gw,
        "_reconnect_with_backoff",
        lambda name: scheduled.append(name) or _noop(),
    )

    await gw._handle_loss("hal", "tunnel connection closed")
    runtime = gw.runtimes["hal"]
    assert runtime.needs_refresh is False
    assert runtime.state == "disconnected"
    assert runtime.last_error
    assert "second factor" in runtime.last_error
    # Fail closed: no backoff/reconnect scheduled.
    assert scheduled == []


async def _async_noop(*a, **k):
    return None


async def _async_noop_kw(*a, **k):
    return None


# -- console 2FA parsing -----------------------------------------------------

async def test_console_connect_factor_before_and_after_target(monkeypatch):
    gw = make_gateway()
    seen = []

    async def fake_connect(name, factor=None):
        seen.append((name, factor))
        return {"state": "connected"}

    monkeypatch.setattr(gw, "connect_target", fake_connect)
    import io
    import contextlib

    for line in ("connect --2fa SECRET t", "connect t --2fa SECRET"):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            await gw._console_command(line)
        assert "SECRET" not in buf.getvalue()
    assert seen == [("t", "SECRET"), ("t", "SECRET")]


async def test_console_refresh_factor(monkeypatch):
    gw = make_gateway()
    seen = []

    async def fake_refresh(name, factor=None):
        seen.append((name, factor))
        return {"state": "connected"}

    monkeypatch.setattr(gw, "refresh_target", fake_refresh)
    import io
    import contextlib

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        await gw._console_command("refresh --2fa SECRET t")
    assert "SECRET" not in buf.getvalue()
    assert seen == [("t", "SECRET")]


async def test_console_2fa_without_value_is_usage_error(capsys):
    gw = make_gateway()
    keep_going = await gw._console_command("connect --2fa")
    out = capsys.readouterr().out
    assert keep_going is True
    assert out.startswith("usage: connect")


async def test_factor_never_in_status_or_logs(monkeypatch, caplog):
    gw, _ = _interactive_gateway()
    rec = {"calls": []}
    _install_fake_connect(monkeypatch, gw, rec)

    with caplog.at_level("DEBUG"):
        result = await gw.connect_target("hal", factor="TOP-SECRET")
    assert "TOP-SECRET" not in json.dumps(result)
    assert "TOP-SECRET" not in caplog.text
    assert "TOP-SECRET" not in json.dumps(gw.public_status("hal"))


# -- HTTP factor body --------------------------------------------------------

async def test_http_connect_factor_accepted(monkeypatch):
    gw, _ = _interactive_gateway()
    rec = {"calls": []}
    _install_fake_connect(monkeypatch, gw, rec)

    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/targets/hal/connect",
            headers=auth("alpaka-token"),
            json={"factor": "SECRET"},
        )
        assert resp.status == 200, await resp.text()
        body = await resp.json()
        assert rec["calls"] == [{"target": "hal", "factor": "SECRET"}]
        assert "SECRET" not in json.dumps(body)
    finally:
        await client.close()


async def test_http_refresh_factor_accepted(monkeypatch):
    gw, _ = _interactive_gateway()
    rec = {"calls": []}
    _install_fake_connect(monkeypatch, gw, rec)

    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/targets/hal/refresh",
            headers=auth("alpaka-token"),
            json={"factor": "SECRET"},
        )
        assert resp.status == 200, await resp.text()
        assert rec["calls"] == [{"target": "hal", "factor": "SECRET"}]
    finally:
        await client.close()


async def test_http_malformed_factor_body_rejected():
    gw = make_gateway()
    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/targets/hal/connect",
            headers={**auth("alpaka-token"), "Content-Type": "application/json"},
            data="not json",
        )
        assert resp.status == 400
        resp = await client.post(
            "/v1/targets/hal/connect",
            headers=auth("alpaka-token"),
            json={"factor": 1234},
        )
        assert resp.status == 400
        resp = await client.post(
            "/v1/targets/hal/refresh",
            headers=auth("alpaka-token"),
            json={"factor": ["x"]},
        )
        assert resp.status == 400
    finally:
        await client.close()


async def test_http_factor_not_echoed_on_warning(monkeypatch):
    gw, _ = _interactive_gateway()
    rec = {"calls": []}
    _install_fake_connect(monkeypatch, gw, rec)

    client = await make_client(gw)
    try:
        # interactive + no factor -> warning response, factor absent everywhere.
        resp = await client.post(
            "/v1/targets/hal/connect", headers=auth("alpaka-token"), json={}
        )
        assert resp.status == 200
        body = await resp.json()
        assert "warning" in body
        assert rec["calls"] == []
    finally:
        await client.close()


# ============================================================================
# F1: real tunnel errors vs InteractiveAuthRequired; awaiting_factor lifecycle
# ============================================================================

async def test_open_container_conn_reraises_real_tunnel_error(monkeypatch):
    from compute_mcp.gateway import InteractiveAuthRequired
    from compute_mcp.tunnel import TunnelError

    gw, _ = _interactive_gateway()

    async def boom(name):
        raise TunnelError("dial exploded")

    monkeypatch.setattr(gw, "ensure_connected", boom)
    assert gw.runtimes["hal"].awaiting_factor is False
    with pytest.raises(TunnelError) as excinfo:
        await gw._open_container_conn("hal")
    # The original error must survive; it must not be masked as 2FA-needed.
    assert type(excinfo.value) is TunnelError
    assert not isinstance(excinfo.value, InteractiveAuthRequired)


async def test_open_container_conn_reraises_real_ssh_error(monkeypatch):
    from compute_mcp.ssh_backend import SSHError

    gw, _ = _interactive_gateway()

    async def boom(name):
        raise SSHError("host key mismatch")

    monkeypatch.setattr(gw, "ensure_connected", boom)
    assert gw.runtimes["hal"].awaiting_factor is False
    with pytest.raises(SSHError) as excinfo:
        await gw._open_container_conn("hal")
    assert type(excinfo.value) is SSHError


async def test_open_container_conn_maps_to_interactive_when_awaiting(monkeypatch):
    from compute_mcp.gateway import InteractiveAuthRequired
    from compute_mcp.tunnel import TunnelError

    gw, _ = _interactive_gateway()
    gw.runtimes["hal"].awaiting_factor = True

    async def boom(name):
        raise TunnelError("dial exploded")

    monkeypatch.setattr(gw, "ensure_connected", boom)
    with pytest.raises(InteractiveAuthRequired):
        await gw._open_container_conn("hal")


def test_public_status_exposes_awaiting_factor():
    gw, _ = _interactive_gateway()
    assert gw.public_status("hal")["awaiting_factor"] is False
    gw.runtimes["hal"].awaiting_factor = True
    assert gw.public_status("hal")["awaiting_factor"] is True


async def test_awaiting_factor_set_on_skip_cleared_on_success_and_stop(monkeypatch):
    gw, _ = _interactive_gateway()
    rec = {"calls": []}
    _install_fake_connect(monkeypatch, gw, rec)
    monkeypatch.setattr(gw.backend, "disconnect", _async_noop)
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)

    # skip without a factor -> awaiting_factor set and exposed
    result = await gw.connect_target("hal", factor=None)
    assert gw.runtimes["hal"].awaiting_factor is True
    assert result["awaiting_factor"] is True

    # stop -> cleared
    stopped = await gw.stop_target("hal")
    assert gw.runtimes["hal"].awaiting_factor is False
    assert stopped["awaiting_factor"] is False

    # skip again -> set again (not connected, so the fail-closed path runs)
    await gw.connect_target("hal", factor=None)
    assert gw.runtimes["hal"].awaiting_factor is True

    # successful connect with a factor -> cleared
    await gw.connect_target("hal", factor="SECRET")
    assert gw.runtimes["hal"].awaiting_factor is False


# ============================================================================
# F2: reconnect task dedup / cancellation / obsolete runtime
# ============================================================================

def _non_interactive_tunnel_gateway(**overrides):
    gw = make_gateway()
    target = _tunnel_target(interactive_auth=False, **overrides)
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    return gw, target


async def test_schedule_recovery_is_deduplicated(monkeypatch):
    import contextlib

    gw, _ = _non_interactive_tunnel_gateway()
    started = []

    async def slow(name):
        started.append(name)
        await asyncio.sleep(30)

    monkeypatch.setattr(gw, "_reconnect_with_backoff", slow)
    runtime = gw.runtimes["hal"]
    gw._schedule_recovery("hal", runtime)
    first = runtime.recovery_task
    gw._schedule_recovery("hal", runtime)
    assert runtime.recovery_task is first
    await asyncio.sleep(0)  # let the single task start
    assert started == ["hal"]
    first.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await first


async def test_stop_target_cancels_recovery_and_loop_does_not_dial(monkeypatch):
    gw, _ = _non_interactive_tunnel_gateway(connect_backoff_initial=30.0)
    monkeypatch.setattr(gw.backend, "disconnect", _async_noop)
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)
    dials = []

    async def fake_ensure(name):
        dials.append(name)
        return gw.runtimes[name]

    monkeypatch.setattr(gw, "ensure_connected", fake_ensure)
    runtime = gw.runtimes["hal"]
    gw._schedule_recovery("hal", runtime)
    task = runtime.recovery_task
    await asyncio.sleep(0)  # task captures the runtime and starts sleeping
    assert task is not None and not task.done()
    await gw.stop_target("hal")
    assert task.cancelled() or task.done()
    await asyncio.sleep(0.05)
    assert dials == []


async def test_reconnect_skips_dial_when_runtime_replaced(monkeypatch):
    gw, _ = _non_interactive_tunnel_gateway(connect_backoff_initial=0.05)
    dials = []

    async def fake_ensure(name):
        dials.append(name)
        return gw.runtimes[name]

    monkeypatch.setattr(gw, "ensure_connected", fake_ensure)
    task = asyncio.create_task(gw._reconnect_with_backoff("hal"))
    await asyncio.sleep(0)  # task captures the old runtime and starts sleeping
    gw.runtimes["hal"] = TargetRuntime(name="hal")  # replacement
    await asyncio.wait_for(task, timeout=5.0)
    assert dials == []


async def test_reconnect_skips_dial_when_stopping(monkeypatch):
    gw, _ = _non_interactive_tunnel_gateway(connect_backoff_initial=0.0)
    dials = []

    async def fake_ensure(name):
        dials.append(name)
        return gw.runtimes[name]

    monkeypatch.setattr(gw, "ensure_connected", fake_ensure)
    gw._stopping = True
    await asyncio.wait_for(gw._reconnect_with_backoff("hal"), timeout=5.0)
    assert dials == []


async def test_interactive_loss_no_recovery_task_and_actionable_error(monkeypatch):
    gw, target = _interactive_gateway()
    gw.runtimes["hal"].state = "connected"
    gw.runtimes["hal"].tunnel = _FakeTunnel(target, route="hal", local_port=31000)
    monkeypatch.setattr(gw.backend, "disconnect", _async_noop)
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)

    await gw._handle_loss("hal", "tunnel connection closed")
    runtime = gw.runtimes["hal"]
    assert runtime.recovery_task is None
    assert runtime.awaiting_factor is True
    assert runtime.needs_refresh is False
    assert "target-connect --2fa" in runtime.last_error
    assert gw.public_status("hal")["awaiting_factor"] is True


# ============================================================================
# Console --2fa missing value (refresh variant) and factor leakage
# ============================================================================

async def test_console_refresh_2fa_without_value_is_usage(monkeypatch, capsys):
    gw = make_gateway()
    called = []

    async def fake_refresh(name, factor=None):
        called.append(name)
        return {"state": "connected"}

    monkeypatch.setattr(gw, "refresh_target", fake_refresh)
    keep_going = await gw._console_command("refresh --2fa")
    out = capsys.readouterr().out
    assert keep_going is True
    assert out.startswith("usage: refresh")
    assert called == []


async def test_factor_not_leaked_on_route_dial_failure(monkeypatch, caplog):
    from compute_mcp.tunnel import TunnelError

    gw, _ = _interactive_gateway()

    async def boom(target, on_route=None, transport=None, factor=None):
        raise TunnelError("all routes failed")

    monkeypatch.setattr(gw.tunnels, "connect", boom)
    with caplog.at_level("DEBUG"):
        with pytest.raises(TunnelError):
            await gw.connect_target("hal", factor="TOP-SECRET")
    assert "TOP-SECRET" not in caplog.text
    assert gw.runtimes["hal"].last_error is not None
    assert "TOP-SECRET" not in gw.runtimes["hal"].last_error
    assert "TOP-SECRET" not in json.dumps(gw.public_status("hal"))

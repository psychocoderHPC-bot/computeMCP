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
    ConfigError,
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
        [server]
        allow_enrollment = true

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


async def test_enroll_approve_include_based_config_succeeds(tmp_path, monkeypatch):
    """Targets living in an included file must approve through /enroll."""
    from compute_mcp.config import load_config
    from compute_mcp.gateway import Gateway

    cfg = tmp_path / "config.toml"
    included = tmp_path / "systems" / "hal.toml"
    included.parent.mkdir(parents=True, exist_ok=True)
    included.write_text(
        """
        [targets.hal]
        transport = "direct"
        remote_host = "127.0.0.1"
        remote_port = 9
        user = "agent"
        host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
        """
    )
    cfg.write_text(
        """
        include = ["systems/hal.toml"]

        [server]
        allow_enrollment = true

        [clients.admin]
        token = "admin-token"
        targets = ["*"]
        """
    )
    gw = Gateway(load_config(cfg))
    before_included = included.read_text()
    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/enroll", json={"client_id": "newci", "targets": ["hal"]}
        )
        assert resp.status == 200, await resp.text()
        rid = (await resp.json())["request_id"]
        resp = await client.post(
            f"/v1/enroll-requests/{rid}/approve", headers=auth("admin-token")
        )
        assert resp.status == 200, await resp.text()
        body = await resp.json()
        assert body["client_id"] == "newci"
        assert body["targets"] == ["hal"]
        assert "[clients.newci]" in cfg.read_text()
        # Only the entry file was written to; the include file is untouched.
        assert included.read_text() == before_included
        loaded = load_config(cfg)
        assert loaded.clients["newci"].may_access("hal")
    finally:
        await client.close()


async def test_enroll_approve_unknown_request_id_returns_404(tmp_path, monkeypatch):
    """A stale/unknown id must yield 404, not a 500 from the middleware."""
    gw, _ = await _enroll_flow(tmp_path, monkeypatch)
    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/enroll-requests/00000000000/approve", headers=auth("admin-token")
        )
        assert resp.status == 404
        assert resp.status != 500
        assert "unknown enrollment request" in await resp.text()
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


def _colliding_gateway():
    """Two clients share a token: a limited one and an admin one."""
    target = TargetConfig(
        name="hal",
        user="agent",
        transport=TransportConfig(kind="direct", remote_host="127.0.0.1", remote_port=9),
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    )
    cfg = GatewayConfig(
        server=ServerConfig(),
        ssh=SSHConfig(),
        sessions=SessionConfig(),
        targets={"hal": target},
        clients={
            "limited": ClientConfig(
                client_id="limited",
                token_sha256=hash_token("shared-token"),
                targets=("hal",),
            ),
            "extra": ClientConfig(
                client_id="extra",
                token_sha256=hash_token("shared-token"),
                allow_all=True,
            ),
        },
    )
    return Gateway(cfg)


async def test_second_client_with_duplicate_token_cannot_overreach():
    gw = _colliding_gateway()
    client = await make_client(gw)
    try:
        # The colliding token must not authenticate at all: no union/upgrade.
        resp = await client.get("/v1/targets", headers=auth("shared-token"))
        assert resp.status == 401
    finally:
        await client.close()


async def test_admin_acl_not_widened_by_token_collision():
    gw = _colliding_gateway()
    client = await make_client(gw)
    try:
        # A request that the admin ACL would allow still fails closed.
        resp = await client.get("/v1/clients", headers=auth("shared-token"))
        assert resp.status == 401
    finally:
        await client.close()


async def test_enroll_disabled_by_default_returns_403():
    target = TargetConfig(
        name="hal",
        user="agent",
        transport=TransportConfig(kind="direct", remote_host="127.0.0.1", remote_port=9),
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
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
    gw = Gateway(cfg)
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


# ============================================================================
# close_command: release on stop/shutdown/refresh, not on config-reload removal
# ============================================================================

def _close_gateway(close_command=("scancel", "--name", "terok-dev")):
    gw = make_gateway()
    target = _tunnel_target(close_command=close_command)
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    return gw, target


def _record_close(gw, monkeypatch, calls=None, *, boom=False):
    calls = calls if calls is not None else []

    async def fake_run_close_command(target, route, connection=None, **kwargs):
        calls.append(
            {
                "target": target.name,
                "route": route,
                "connection": connection,
                "tunnel_alive": gw.runtimes[target.name].tunnel is not None,
                "provision_env": kwargs.get("provision_env"),
            }
        )
        if boom:
            raise RuntimeError("close boom")

    monkeypatch.setattr(gw.tunnels, "run_close_command", fake_run_close_command)
    return calls


def _connect_fake_tunnel(gw, target, connection):
    live = _FakeTunnel(target, route="hal", local_port=31000, connection=connection)
    runtime = gw.runtimes["hal"]
    runtime.state = "connected"
    runtime.active_route = "hal"
    runtime.tunnel = live
    return live


async def test_stop_target_runs_close_command_before_teardown(monkeypatch):
    gw, target = _close_gateway()
    monkeypatch.setattr(gw.backend, "disconnect", _async_noop)
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)
    conn = object()
    live = _connect_fake_tunnel(gw, target, conn)
    calls = _record_close(gw, monkeypatch)

    await gw.stop_target("hal")

    assert len(calls) == 1
    assert calls[0]["route"] == "hal"
    assert calls[0]["connection"] is conn
    # It ran while the route connection was still alive, before teardown.
    assert calls[0]["tunnel_alive"] is True
    # A non-bundle close_command carries no derived provision env.
    assert calls[0]["provision_env"] is None
    assert gw.runtimes["hal"].tunnel is None
    assert live.stopped is True


async def test_refresh_target_runs_close_command(monkeypatch):
    gw, target = _close_gateway()
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)
    rec = {"calls": []}
    _install_fake_connect(monkeypatch, gw, rec)
    conn = object()
    _connect_fake_tunnel(gw, target, conn)
    calls = _record_close(gw, monkeypatch)

    result = await gw.refresh_target("hal")

    assert len(calls) == 1
    assert calls[0]["connection"] is conn
    assert calls[0]["tunnel_alive"] is True
    # A fresh route was provisioned after the close.
    assert rec["calls"] == [{"target": "hal", "factor": None}]
    assert result["state"] == "connected"


async def test_refresh_interactive_no_factor_does_not_run_close(monkeypatch):
    gw = make_gateway()
    target = _tunnel_target(interactive_auth=True, close_command=("scancel",))
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    conn = object()
    _connect_fake_tunnel(gw, target, conn)
    calls = _record_close(gw, monkeypatch)

    result = await gw.refresh_target("hal", factor=None)

    # A skipped refresh must not release the still-active allocation.
    assert calls == []
    assert result["awaiting_factor"] is True
    assert gw.runtimes["hal"].tunnel is not None


async def test_stop_target_runs_bundle_stop_when_close_command_unset(monkeypatch):
    """A bundle target with no close_command still releases on stop.

    Regression: `_run_close_command` used to return early when close_command was
    empty, so a deployed-bundle allocation leaked on stop/refresh.
    """
    from compute_mcp.config import BundleConfig, ContainerConfig

    gw = make_gateway()
    target = _tunnel_target(
        container=ContainerConfig(
            runtime="apptainer", storage_root="/scratch/agent/computemcp"
        ),
        bundle=BundleConfig(source="computemcp-slurm"),
    )
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    monkeypatch.setattr(gw.backend, "disconnect", _async_noop)
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)
    conn = object()
    _connect_fake_tunnel(gw, target, conn)
    calls = _record_close(gw, monkeypatch)

    await gw.stop_target("hal")

    assert len(calls) == 1
    assert calls[0]["route"] == "hal"
    assert calls[0]["tunnel_alive"] is True
    assert gw.runtimes["hal"].tunnel is None
    # The bundle close must carry the COMPUTEMCP_* contract, not an empty env:
    # without it the helper exits "must be apptainer or docker".
    env = calls[0]["provision_env"]
    assert env is not None
    assert env["COMPUTEMCP_CONTAINER_RUNTIME"] == "apptainer"
    assert env["COMPUTEMCP_STORAGE_ROOT"] == "/scratch/agent/computemcp"
    assert env["COMPUTEMCP_SYSTEM"] == "hal"


async def test_gateway_stop_runs_close_command_for_connected_target(monkeypatch):
    gw, target = _close_gateway()
    monkeypatch.setattr(gw.backend, "disconnect", _async_noop)
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)
    monkeypatch.setattr(gw.sessions, "stop", _async_noop)
    monkeypatch.setattr(gw.backend, "close_all", _async_noop)
    conn = object()
    _connect_fake_tunnel(gw, target, conn)
    calls = _record_close(gw, monkeypatch)

    await gw.stop()

    assert len(calls) == 1
    assert calls[0]["connection"] is conn


async def test_apply_config_removal_does_not_run_close_command(monkeypatch):
    gw, target = _close_gateway()
    monkeypatch.setattr(gw.backend, "disconnect", _async_noop)
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)
    conn = object()
    _connect_fake_tunnel(gw, target, conn)
    calls = _record_close(gw, monkeypatch)

    new_config = dataclasses.replace(
        gw.config, targets={"gpu03": gw.config.targets["gpu03"]}
    )
    report = await gw.apply_config(new_config)

    assert report["removed"] == ["hal"]
    # Removing a target by config reload must not release the allocation.
    assert calls == []


async def test_close_command_skipped_without_live_connection(monkeypatch, caplog):
    gw, target = _close_gateway()
    monkeypatch.setattr(gw.backend, "disconnect", _async_noop)
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)
    gw.runtimes["hal"].state = "connected"
    gw.runtimes["hal"].tunnel = None
    calls = _record_close(gw, monkeypatch)

    with caplog.at_level("WARNING"):
        result = await gw.stop_target("hal")

    assert calls == []  # not invoked without a connection
    assert "no live route connection" in caplog.text
    assert result["state"] == "disconnected"


async def test_failing_close_command_still_stops(monkeypatch, caplog):
    gw, target = _close_gateway()
    monkeypatch.setattr(gw.backend, "disconnect", _async_noop)
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)
    _connect_fake_tunnel(gw, target, object())
    calls = _record_close(gw, monkeypatch, boom=True)

    with caplog.at_level("WARNING"):
        result = await gw.stop_target("hal")

    assert len(calls) == 1
    assert "close_command failed" in caplog.text
    assert result["state"] == "disconnected"


async def test_failing_close_command_still_refreshes(monkeypatch, caplog):
    gw, target = _close_gateway()
    monkeypatch.setattr(gw.backend, "disconnect", _async_noop)
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)
    rec = {"calls": []}
    _install_fake_connect(monkeypatch, gw, rec)
    _connect_fake_tunnel(gw, target, object())
    _record_close(gw, monkeypatch, boom=True)

    with caplog.at_level("WARNING"):
        result = await gw.refresh_target("hal")

    assert "close_command failed" in caplog.text
    assert result["state"] == "connected"


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


# ============================================================================
# Slurm allocation: provision env, plan signature, preview, refresh gating
# ============================================================================

def _allocation_target(name="hal", **overrides):
    from compute_mcp.config import (
        AllocationConfig,
        ContainerConfig,
        NodeConfig,
        SlurmConfig,
        SlurmStageConfig,
    )

    kwargs = {
        "name": name,
        "user": "agent",
        "transport": TransportConfig(
            kind="direct", remote_host="127.0.0.1", remote_port=9
        ),
        "host_key_sha256": "SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
        "node": NodeConfig(cpus=24, gpus=4, memory="378000M"),
        "allocation": AllocationConfig(single_node="gpu-proportional"),
        "slurm": SlurmConfig(
            sbatch=SlurmStageConfig(
                options={"ntasks-per-node": 1},
                mapping={"nodes": "nodes", "gpus-per-node": "gres"},
            ),
            srun=SlurmStageConfig(),
        ),
        "container": ContainerConfig(
            runtime="apptainer",
            image="docker://ubuntu:24.04",
            gpus=("nvidia",),
            sandbox=True,
        ),
    }
    kwargs.update(overrides)
    return TargetConfig(**kwargs)


def _allocation_gateway(**target_overrides):
    """Gateway whose `hal` carries a full allocation/container description."""
    gw = make_gateway()
    target = _allocation_target(**target_overrides)
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    return gw, target


def test_build_provision_env_includes_derived_public_key_for_bundle(tmp_path):
    from compute_mcp.allocation import compute_plan, render_args
    from compute_mcp.config import BundleConfig, ContainerConfig
    from compute_mcp.config import TransportConfig as TR
    from compute_mcp.gateway import build_provision_env

    key = tmp_path / "id_ed25519"
    key.write_text("PRIVATE\n")
    (tmp_path / "id_ed25519.pub").write_text("ssh-ed25519 AAAA test@host\n")
    target = _allocation_target(
        client_key=str(key),
        transport=TR(kind="tunnel", ssh_targets=("hal",), remote_port=2222),
        container=ContainerConfig(
            runtime="apptainer", storage_root="/scratch/agent/computemcp"
        ),
        bundle=BundleConfig(source="computemcp-slurm"),
    )
    plan = compute_plan(target)
    sbatch, srun = render_args(target, plan)
    env = build_provision_env(target, plan, sbatch, srun)
    assert env["COMPUTEMCP_SSH_PUBLIC_KEY"] == "ssh-ed25519 AAAA test@host"


def test_build_provision_env_omits_public_key_without_bundle(tmp_path):
    from compute_mcp.allocation import compute_plan, render_args
    from compute_mcp.gateway import build_provision_env

    key = tmp_path / "id_ed25519"
    key.write_text("PRIVATE\n")
    (tmp_path / "id_ed25519.pub").write_text("ssh-ed25519 AAAA test@host\n")
    target = _allocation_target(client_key=str(key))
    plan = compute_plan(target)
    sbatch, srun = render_args(target, plan)
    env = build_provision_env(target, plan, sbatch, srun)
    assert "COMPUTEMCP_SSH_PUBLIC_KEY" not in env


def test_build_provision_env_joins_bundle_provision_env(tmp_path):
    from compute_mcp.allocation import compute_plan, render_args
    from compute_mcp.config import BundleConfig, ContainerConfig
    from compute_mcp.config import TransportConfig as TR
    from compute_mcp.gateway import build_provision_env

    key = tmp_path / "id_ed25519"
    key.write_text("PRIVATE\n")
    (tmp_path / "id_ed25519.pub").write_text("ssh-ed25519 AAAA test@host\n")
    target = _allocation_target(
        client_key=str(key),
        transport=TR(kind="tunnel", ssh_targets=("hal",), remote_port=2222),
        container=ContainerConfig(
            runtime="apptainer", storage_root="/scratch/agent/computemcp"
        ),
        bundle=BundleConfig(
            source="computemcp-slurm",
            provision_env=("module load apptainer", "source /etc/profile.d/spack.sh"),
        ),
    )
    plan = compute_plan(target)
    sbatch, srun = render_args(target, plan)
    env = build_provision_env(target, plan, sbatch, srun)
    assert env["COMPUTEMCP_PROVISION_ENV"] == (
        "module load apptainer\nsource /etc/profile.d/spack.sh"
    )


def test_build_provision_env_provision_env_absent_without_bundle():
    from compute_mcp.allocation import compute_plan, render_args
    from compute_mcp.gateway import build_provision_env

    target = _allocation_target()
    plan = compute_plan(target)
    sbatch, srun = render_args(target, plan)
    env = build_provision_env(target, plan, sbatch, srun)
    assert "COMPUTEMCP_PROVISION_ENV" not in env


def test_env_configured_includes_bundle_only_target():
    from compute_mcp.config import BundleConfig, ContainerConfig
    from compute_mcp.config import TargetConfig as TC
    from compute_mcp.config import TransportConfig as TR

    target = TC(
        name="b",
        user="agent",
        transport=TR(kind="tunnel", ssh_targets=("b",)),
        client_key="/home/user/.ssh/key",
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
        container=ContainerConfig(runtime="apptainer", storage_root="/scratch/b"),
        bundle=BundleConfig(source="computemcp-slurm"),
    )
    assert Gateway._env_configured(target) is True


def test_build_provision_env_exact_contract():
    from compute_mcp.allocation import compute_plan, render_args
    from compute_mcp.gateway import build_provision_env

    target = _allocation_target()
    plan = compute_plan(target, {"gpus-per-node": 2})
    sbatch, srun = render_args(target, plan)
    env = build_provision_env(target, plan, sbatch, srun)
    assert env["COMPUTEMCP_SYSTEM"] == "hal"
    assert env["COMPUTEMCP_NODES"] == "1"
    assert env["COMPUTEMCP_CPUS_PER_NODE"] == "12"
    assert env["COMPUTEMCP_GPUS_PER_NODE"] == "2"
    assert env["COMPUTEMCP_MEMORY_PER_NODE_MIB"] == "189000"
    assert env["COMPUTEMCP_EXCLUSIVE"] == "false"
    assert env["COMPUTEMCP_MODE"] == "gpu-proportional"
    assert env["COMPUTEMCP_SBATCH_ARGS"] == "\n".join(sbatch)
    assert env["COMPUTEMCP_SRUN_ARGS"] == ""
    assert env["COMPUTEMCP_CONTAINER_RUNTIME"] == "apptainer"
    assert env["COMPUTEMCP_IMAGE"] == "docker://ubuntu:24.04"
    assert env["COMPUTEMCP_STORAGE_ROOT"] == ""
    assert env["COMPUTEMCP_GPU_VENDORS"] == "nvidia"
    assert env["COMPUTEMCP_HOST_HOME"] == ""
    assert env["COMPUTEMCP_SANDBOX"] == "true"
    # The container hop dials this account; the helper creates/AllowUsers it.
    assert env["COMPUTEMCP_SSH_USER"] == "agent"


def test_build_provision_env_emits_container_user_override():
    from compute_mcp.allocation import compute_plan, render_args
    from compute_mcp.gateway import build_provision_env

    target = _allocation_target(container_user="dev")
    plan = compute_plan(target)
    sbatch, srun = render_args(target, plan)
    env = build_provision_env(target, plan, sbatch, srun)
    assert env["COMPUTEMCP_SSH_USER"] == "dev"


def test_build_provision_env_omits_container_user_without_container():
    """A plain target with no container/bundle does not emit the key."""
    from compute_mcp.allocation import ResolvedPlan
    from compute_mcp.config import TargetConfig, TransportConfig
    from compute_mcp.gateway import build_provision_env

    target = TargetConfig(
        name="bare",
        user="rwidera",
        transport=TransportConfig(kind="direct", remote_host="127.0.0.1", remote_port=9),
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    )
    env = build_provision_env(target, ResolvedPlan(nodes=1), (), ())
    assert "COMPUTEMCP_SSH_USER" not in env


def test_target_provision_env_emits_container_user():
    """The fallback helper emits the same key/value as build_provision_env."""
    gw = make_gateway()

    def _boom(target, overrides):
        raise ValueError("no allocation")

    gw._resolve_allocation = _boom
    assert (
        gw._target_provision_env(_allocation_target(container_user="dev"))[
            "COMPUTEMCP_SSH_USER"
        ]
        == "dev"
    )
    assert (
        gw._target_provision_env(_allocation_target())["COMPUTEMCP_SSH_USER"]
        == "agent"
    )


def test_build_provision_env_empty_args_and_missing_plan_values():
    from compute_mcp.allocation import ResolvedPlan
    from compute_mcp.config import TargetConfig, TransportConfig
    from compute_mcp.gateway import build_provision_env

    target = TargetConfig(
        name="bare",
        user="agent",
        transport=TransportConfig(kind="direct", remote_host="127.0.0.1", remote_port=9),
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    )
    plan = ResolvedPlan(nodes=1)
    env = build_provision_env(target, plan, (), ())
    assert env["COMPUTEMCP_SYSTEM"] == "bare"
    assert env["COMPUTEMCP_NODES"] == "1"
    assert env["COMPUTEMCP_CPUS_PER_NODE"] == ""
    assert env["COMPUTEMCP_GPUS_PER_NODE"] == ""
    assert env["COMPUTEMCP_MEMORY_PER_NODE_MIB"] == ""
    assert env["COMPUTEMCP_EXCLUSIVE"] == "false"
    assert env["COMPUTEMCP_SBATCH_ARGS"] == ""
    assert env["COMPUTEMCP_SRUN_ARGS"] == ""
    assert env["COMPUTEMCP_CONTAINER_RUNTIME"] == ""
    assert env["COMPUTEMCP_STORAGE_ROOT"] == ""
    assert env["COMPUTEMCP_IMAGE"] == ""
    assert env["COMPUTEMCP_GPU_VENDORS"] == ""
    assert env["COMPUTEMCP_HOST_HOME"] == ""
    assert env["COMPUTEMCP_SANDBOX"] == "false"


def test_build_provision_env_rejects_null_and_carriage_return_in_values():
    """CR must be refused alongside NUL in every provision-env value (the
    NUL-only check left a CR-in-value shell-line-smuggling hole)."""
    from compute_mcp.allocation import ResolvedPlan
    from compute_mcp.config import ConfigError, TargetConfig, TransportConfig
    from compute_mcp.gateway import build_provision_env

    target = TargetConfig(
        name="bare",
        user="agent",
        transport=TransportConfig(kind="direct", remote_host="127.0.0.1", remote_port=9),
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    )
    plan = ResolvedPlan(nodes=1)
    for bad_sbatch, bad_srun in (
        ("x\0y", ()),
        ("x\ry", ()),
    ):
        with pytest.raises(ConfigError, match="COMPUTEMCP_SBATCH_ARGS"):
            build_provision_env(target, plan, bad_sbatch, bad_srun)
    # A CR smuggled into the srun deck is refused just as thoroughly.
    with pytest.raises(ConfigError, match="COMPUTEMCP_SRUN_ARGS"):
        build_provision_env(target, plan, (), ("x\ry",))


def test_build_provision_env_allows_newlines_in_arg_decks():
    """Newlines are the intentional delimiter of the two ARGS decks and must
    NOT be refused; only NUL and CR are."""
    from compute_mcp.allocation import ResolvedPlan
    from compute_mcp.config import TargetConfig, TransportConfig
    from compute_mcp.gateway import build_provision_env

    target = TargetConfig(
        name="bare",
        user="agent",
        transport=TransportConfig(kind="direct", remote_host="127.0.0.1", remote_port=9),
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    )
    plan = ResolvedPlan(nodes=1)
    env = build_provision_env(target, plan, ("--nodes=2", "--gres=gpu:1"), ("--overlap",))
    assert env["COMPUTEMCP_SBATCH_ARGS"] == "--nodes=2\n--gres=gpu:1"
    assert env["COMPUTEMCP_SRUN_ARGS"] == "--overlap"


async def test_connect_with_invalid_override_leaves_state_failed_not_connecting():
    """A connect with a bad --set override on an allocation target must not
    leave the runtime wedged in state=='connecting'; last_error records what
    failed and the exception propagates (mapped to 400 at the HTTP layer)."""
    gw, target = _allocation_gateway()
    rt = gw.runtimes["hal"]
    assert rt.state == "disconnected"
    with pytest.raises(ConfigError, match="unknown --set key"):
        await gw.connect_target("hal", overrides={"bogus": 1})
    # The regression: state must NOT be "connecting" after a failed connect.
    assert rt.state != "connecting"
    assert rt.state in ("failed", "disconnected")
    assert "unknown --set key" in (rt.last_error or "")


async def test_connect_with_invalid_override_propagates_on_http_endpoint(monkeypatch):
    """The HTTP edge maps the same ConfigError to a 400 and leaves the
    runtime in a terminal error state, not "connecting"."""
    gw, target = _allocation_gateway()
    rt = gw.runtimes["hal"]
    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/targets/hal/connect", headers=auth("alpaka-token"),
            json={"set": {"bogus": 1}},
        )
        assert resp.status == 400
    finally:
        await client.close()
    assert rt.state != "connecting"
    assert rt.state in ("failed", "disconnected")
    assert rt.last_error and "unknown --set key" in rt.last_error


def test_plan_signature_stability_and_none():
    from compute_mcp.gateway import plan_signature

    summary = {
        "plan": {
            "nodes": 2,
            "cpus_per_node": 16,
            "gpus_per_node": 1,
            "memory_per_node_mib": 94500,
            "exclusive": False,
            "mode": "full",
        }
    }
    assert plan_signature(summary) == (2, 16, 1, 94500, False, "full")
    assert plan_signature(summary) == plan_signature(dict(summary))
    assert plan_signature(None) is None
    assert plan_signature({"plan": {}}) == (None, None, None, None, None, None)


def test_preview_target_without_override_on_disconnected_target():
    gw, target = _allocation_gateway()
    assert gw.runtimes["hal"].state == "disconnected"
    result = gw.preview_target("hal")
    assert result["target"] == "hal"
    assert result["connected"] is False
    assert "needs_refresh" not in result
    planned = result["planned"]["plan"]
    assert planned["gpus_per_node"] == 1
    assert planned["cpus_per_node"] == 6
    assert planned["memory_per_node_mib"] == 94500
    assert result["sbatch_args"]
    assert result["srun_args"] == []
    assert result["provision_env"]["COMPUTEMCP_NODES"] == "1"


def test_preview_connected_target_with_differing_override_needs_refresh():
    """A preview that differs from the active allocation requires a refresh."""
    gw, target = _allocation_gateway()

    # Manually wire a connected runtime via the real resolution path:
    rt = gw.runtimes["hal"]
    from compute_mcp.allocation import plan_summary

    plan, _ = gw._resolve_allocation(target, None)
    rt.state = "connected"
    rt.resolved_plan = plan_summary(target, plan)
    rt.resolved_overrides = dict(plan.overrides)

    # Same effective settings: no refresh flag.
    result = gw.preview_target("hal")
    assert result["connected"] is True
    assert "needs_refresh" not in result

    # Differing settings: flagged.
    result = gw.preview_target("hal", overrides={"gpus-per-node": 2})
    assert result["needs_refresh"] is True
    assert "target-refresh" in result["warning"]
    assert result["planned"]["plan"]["gpus_per_node"] == 2

    # The preview must never disturb the retained allocation.
    assert rt.state == "connected"
    assert rt.resolved_plan["plan"]["gpus_per_node"] == 1

def test_validate_config_allocations_rejects_conflicting_config():
    """A gateway must not start with a mapping/manual conflict in any target."""
    from compute_mcp.config import ConfigError
    from compute_mcp.config import (
        GatewayConfig as GwCfg,
        ServerConfig,
        SessionConfig,
        SSHConfig as SshCfg,
        SlurmConfig,
        SlurmStageConfig,
    )
    from compute_mcp.gateway import validate_config_allocations

    base = GwCfg(
        server=ServerConfig(),
        ssh=SshCfg(),
        sessions=SessionConfig(),
        targets={
            "hal": TargetConfig(
                name="hal",
                user="agent",
                transport=TransportConfig(
                    kind="direct", remote_host="127.0.0.1", remote_port=9
                ),
                host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
            )
        },
        clients={
            "admin": ClientConfig(
                client_id="admin", token_sha256=hash_token("admin-token"),
                allow_all=True,
            )
        },
    )
    validate_config_allocations(base)

    bad_target = dataclasses.replace(
        base.targets["hal"],
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(
                options={"mem": "64G"}, mapping={"memory-per-node": "mem"}
            ),
            srun=SlurmStageConfig(),
        ),
    )
    bad = dataclasses.replace(base, targets={"hal": bad_target})
    with pytest.raises(ConfigError, match="conflicts"):
        validate_config_allocations(bad)
    # Gateway construction fails closed on the same config.
    with pytest.raises(ConfigError, match="conflicts"):
        Gateway(bad)


async def test_http_preview_endpoint_returns_plan_and_needs_refresh(monkeypatch):
    gw, target = _allocation_gateway()
    rt = gw.runtimes["hal"]
    from compute_mcp.allocation import plan_summary

    plan, _ = gw._resolve_allocation(target, None)
    rt.state = "connected"
    rt.resolved_plan = plan_summary(target, plan)
    rt.resolved_overrides = dict(plan.overrides)

    client = await make_client(gw)
    try:
        # Plain preview of the active settings: no refresh required.
        resp = await client.post(
            "/v1/targets/hal/preview", headers=auth("alpaka-token"), json={
                "set": {"gpus-per-node": 1}
            }
        )
        assert resp.status == 200, await resp.text()
        body = await resp.json()
        assert body["connected"] is True
        assert "needs_refresh" not in body
        assert body["planned"]["plan"]["gpus_per_node"] == 1

        # A different effective request flags the refresh.
        resp = await client.post(
            "/v1/targets/hal/preview", headers=auth("alpaka-token"), json={
                "set": {"gpus-per-node": 2}
            }
        )
        assert resp.status == 200, await resp.text()
        body = await resp.json()
        assert body["needs_refresh"] is True
        assert "COMPUTEMCP_SBATCH_ARGS" in body["provision_env"]

        # Unknown override keys are a 400 (allocation layer validation).
        resp = await client.post(
            "/v1/targets/hal/preview", headers=auth("alpaka-token"), json={
                "set": {"bogus": 1}
            }
        )
        assert resp.status == 400

        # A non-object "set" is a 400.
        resp = await client.post(
            "/v1/targets/hal/preview", headers=auth("alpaka-token"), json={
                "set": [1]
            }
        )
        assert resp.status == 400
    finally:
        await client.close()


async def test_connect_with_overrides_on_connected_target_warns_and_keeps_allocation(monkeypatch):
    """connect with differing overrides on a live target must not swap it."""
    gw, target = _allocation_gateway()
    rec = {"dials": 0, "teardowns": 0}

    class _Tunnel:
        def __init__(self, connection):
            self.connection = connection

        def is_alive(self):
            return True

        async def stop(self):
            rec["teardowns"] += 1

    async def fake_connect(t, on_route=None, transport=None, factor=None, **kwargs):
        rec["dials"] += 1
        raise AssertionError("a connect on a live target must not dial")

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)
    rt = gw.runtimes["hal"]
    rt.tunnel = _Tunnel(connection=object())
    rt.active_route = "hal"
    rt.local_port = 31990
    from compute_mcp.allocation import plan_summary

    plan, _ = gw._resolve_allocation(target, None)
    rt.state = "connected"
    rt.resolved_plan = plan_summary(target, plan)
    rt.resolved_overrides = dict(plan.overrides)

    # Same effective settings: plain no-op (defaults_used is not stable per
    # request, but the effective request tuple is, so no refresh is flagged).
    result = await gw.connect_target("hal", overrides={"gpus-per-node": 1})
    assert result["state"] == "connected"
    assert result["needs_refresh"] is False

    # Different settings: warning, no dial, no teardown.
    result = await gw.connect_target("hal", overrides={"gpus-per-node": 2})
    assert result["state"] == "connected"
    assert result["needs_refresh"] is True
    assert "target-refresh" in result.get("warning", "")
    assert rec == {"dials": 0, "teardowns": 0}



# ============================================================================
# --bootstrap dispatch (setup wizard, no server start)
# ============================================================================

def test_parser_accepts_bootstrap_flags():
    from compute_mcp.gateway import build_parser

    parser = build_parser()
    args = parser.parse_args(["--bootstrap", "--config-dir", "/tmp/cfg", "--force"])
    assert args.bootstrap is True
    assert args.config_dir == "/tmp/cfg"
    assert args.force is True
    args = parser.parse_args(["--bootstrap", "--non-interactive"])
    assert args.non_interactive is True


def test_bootstrap_dispatches_to_setup(monkeypatch, tmp_path):
    import compute_mcp.gateway as gateway_mod
    import compute_mcp.setup as setup_mod

    seen = {}

    def fake_run_bootstrap(config_path, *, force=False, wizard=None):
        seen["path"] = str(config_path)
        seen["force"] = force
        seen["terminal"] = wizard.terminal if wizard is not None else None
        return 0

    def explode(*a, **k):  # the server must never start
        raise AssertionError("gateway server must not start during --bootstrap")

    monkeypatch.setattr(setup_mod, "run_bootstrap", fake_run_bootstrap)
    monkeypatch.setattr(gateway_mod, "_amain", explode)

    rc = gateway_mod.main(["--bootstrap", "--config-dir", str(tmp_path)])
    assert rc == 0
    assert seen["path"] == str(tmp_path / "config.toml")
    assert seen["force"] is False
    # Without --non-interactive the wizard's terminal state follows isatty().
    assert seen["terminal"] in (True, False)


def test_bootstrap_non_interactive_disables_terminal(monkeypatch, tmp_path):
    import compute_mcp.gateway as gateway_mod
    import compute_mcp.setup as setup_mod

    seen = {}

    def fake_run_bootstrap(config_path, *, force=False, wizard=None):
        seen["terminal"] = wizard.terminal
        return 0

    monkeypatch.setattr(setup_mod, "run_bootstrap", fake_run_bootstrap)
    rc = gateway_mod.main(
        ["--bootstrap", "--config-dir", str(tmp_path), "--non-interactive"]
    )
    assert rc == 0
    assert seen["terminal"] is False


def test_bootstrap_abort_returns_2(monkeypatch, tmp_path):
    import compute_mcp.gateway as gateway_mod
    import compute_mcp.setup as setup_mod

    def fake_run_bootstrap(config_path, *, force=False, wizard=None):
        raise setup_mod.WizardAbort("stop")

    monkeypatch.setattr(setup_mod, "run_bootstrap", fake_run_bootstrap)
    rc = gateway_mod.main(["--bootstrap", "--config-dir", str(tmp_path)])
    assert rc == 2


# ============================================================================
# Bundle target provisioning idempotency ("setup if needed / start if not
# running / connect")
# ============================================================================
def _bundle_target(**overrides):
    """A bundle-only target with a container block: no provision_command."""
    from compute_mcp.config import BundleConfig, ContainerConfig

    base = _tunnel_target(
        container=ContainerConfig(runtime="docker", storage_root="/scratch/hal"),
        bundle=BundleConfig(source="computemcp-slurm"),
    )
    if overrides:
        base = dataclasses.replace(base, **overrides)
    return base


def test_bundle_target_without_provision_command_uses_helper_argv():
    """A bundle target needs no provision_command: the argv is the deployed helper."""
    from compute_mcp.bundle import provision_argv, public_key_for

    target = _bundle_target()
    assert target.provision_command == ()
    argv = provision_argv(target, "provision")
    assert argv and argv[0] == "bash"
    assert argv[-1] == "provision"
    assert "computemcp-provision.sh" in argv[1]
    # It is the helper, not an empty argv, that makes the tunnel gate run.
    assert argv != (
        target.provision_command
    )


def test_bundle_only_target_resolves_allocation_without_node_block():
    """Container+bundle with no node/allocation/slurm still resolves an env."""
    from compute_mcp.gateway import Gateway, build_provision_env

    gw = make_gateway()
    target = _bundle_target()
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    assert Gateway._env_configured(target) is True
    plan, env = gw._resolve_allocation(target, None)
    assert plan is not None
    # An empty provision-env is a no-op, but the key is present for a bundle.
    assert env["COMPUTEMCP_PROVISION_ENV"] == ""
    assert env["COMPUTEMCP_CONTAINER_RUNTIME"] == "docker"


async def test_target_connect_provisions_then_reuses(monkeypatch):
    """connect_target provisions once; a second call reuses the live tunnel.

    This is the build-if-needed / start-if-not-running / connect contract: the
    first connect goes through the provision path (which runs the helper), a
    second connect while still connected must not re-run provisioning.
    """
    gw = make_gateway()
    target = _bundle_target()
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": target}
    )
    calls = {"connect": 0}

    async def fake_connect(target, on_route=None, factor=None, **kwargs):
        calls["connect"] += 1
        return _FakeTunnel(
            target, route="hal", local_port=32000,
            provisioned_endpoint=("127.0.0.1", 2222),
        )

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)

    first = await gw.connect_target("hal")
    assert first["state"] == "connected"
    assert calls["connect"] == 1

    second = await gw.connect_target("hal")
    assert second["state"] == "connected"
    # Reuse: an already-connected target must not be provisioned again.
    assert calls["connect"] == 1

    assert gw.public_status("hal")["provisioned_endpoint"] == "127.0.0.1:2222"


# ============================================================================
# Endpoint lifecycle hardening on connect/refresh.  A container restart can
# change the published SSH port; a refresh re-provisions to the new endpoint.
# The runtime must track the latest successful provision and a reconnect must
# dial the NEW port, not the cached connection's stale one.
# ============================================================================


def _endpoint_gateway(kind="tunnel", **target_overrides):
    """A provision-capable target (non-Slurm docker shape) wired into the gw."""
    gw = make_gateway()
    if kind == "tunnel":
        base = _tunnel_target(provision_command=("printf", "127.0.0.1:3010\n"))
    elif kind == "direct":
        base = TargetConfig(
            name="hal",
            user="agent",
            transport=TransportConfig(
                kind="direct", remote_host="127.0.0.1", remote_port=3010
            ),
            provision_command=("printf", "127.0.0.1:3010\n"),
            host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
        )
    else:
        raise ValueError(kind)
    final = dataclasses.replace(base, **target_overrides) if target_overrides else base
    gw.config = dataclasses.replace(
        gw.config, targets={**gw.config.targets, "hal": final}
    )
    return gw, final


def _wire_refresh_noops(gw, monkeypatch):
    """Null out close_command / sessions during a refresh so it stays pure."""
    monkeypatch.setattr(gw.tunnels, "run_close_command", _async_noop_kw)
    monkeypatch.setattr(gw.sessions, "close_for_target", _async_noop_kw)
    monkeypatch.setattr(gw.backend, "disconnect", _async_noop)


async def test_stop_locked_clears_provisioned_endpoint(monkeypatch):
    """Tearing down a connected tunnel drops the retained endpoint.

    A later reconnect that skips re-provision must not dial the previous
    (possibly re-published) container port.
    """
    gw, _ = _endpoint_gateway()
    gw.runtimes["hal"].state = "connected"
    gw.runtimes["hal"].active_route = "hal"
    gw.runtimes["hal"].local_port = 31000
    gw.runtimes["hal"].provisioned_endpoint = "127.0.0.1:3010"
    gw.runtimes["hal"].tunnel = _FakeTunnel(
        gw.config.targets["hal"], route="hal", local_port=31000,
        provisioned_endpoint=("127.0.0.1", 3010),
    )

    await gw._stop_locked("hal")

    assert gw.runtimes["hal"].state == "disconnected"
    assert gw.runtimes["hal"].provisioned_endpoint is None
    assert gw.runtimes["hal"].local_port is None


async def test_connect_locked_reprovisions_and_updates_endpoint(monkeypatch):
    """A re-connect / re-provision overwrites a stale runtime.endpoint with
    the fresh tunnel endpoints, even when the old one was non-None."""
    gw, target = _endpoint_gateway()
    # A previous provision left this endpoint behind.
    gw.runtimes["hal"].provisioned_endpoint = "127.0.0.1:3010"
    runtime = gw.runtimes["hal"]

    async def fake_connect(target2, on_route=None, factor=None, **kwargs):
        return _FakeTunnel(
            target2, route="hal", local_port=31000,
            provisioned_endpoint=("10.0.0.9", 9001),
        )

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)
    await gw._connect_locked(target, runtime)

    # The freshly resolved endpoint is authoritative.
    assert runtime.provisioned_endpoint == "10.0.0.9:9001"
    assert runtime.state == "connected"


async def test_refresh_reprovisions_and_dials_new_tunnel_forward(monkeypatch):
    """Tunnel target: an endpoint-changing refresh re-dials the NEW loopback
    forward port and does not hand out the connection cached on the previous
    forward.  The provisioned (published host) endpoint is tracked on the
    runtime.
    """
    gw, target = _endpoint_gateway(kind="tunnel")
    # Publish ports: first connect -> 3010, refresh -> 9501.
    provisioned = iterator = iter([
        ("127.0.0.1", 3010),
        ("127.0.0.1", 9501),
    ])

    async def fake_connect(t2, on_route=None, factor=None, **kwargs):
        ep = next(iterator)
        # Each tunnel gets a FRESH loopback forward port (31000 then 31100).
        return _FakeTunnel(
            t2, route="hal", local_port=(31000 if ep == ("127.0.0.1", 3010) else 31100),
            provisioned_endpoint=ep, connection=object(),
        )

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)
    _wire_refresh_noops(gw, monkeypatch)

    # First connect: published port 3010, loopback forward 31000.
    result1 = await gw.connect_target("hal")
    assert result1["provisioned_endpoint"] == "127.0.0.1:3010"
    assert gw.runtimes["hal"].local_port == 31000

    # An exec while connected dials the current forward (127.0.0.1:31000).
    dialed = []

    async def fake_conn(t, host, port, prompter=None):
        dialed.append((host, port))
        return f"CONN-{host}-{port}"

    monkeypatch.setattr(gw.backend, "connection", fake_conn)
    await gw._open_container_conn("hal")
    assert dialed == [("127.0.0.1", 31000)]
    dialed.clear()

    # Refresh: re-provision publishes 9501 via the NEW forward 31100.
    result2 = await gw.refresh_target("hal")
    assert result2["state"] == "connected"
    # The runtime endpoint is replaced by the freshest provision.
    assert result2["provisioned_endpoint"] == "127.0.0.1:9501"
    assert gw.runtimes["hal"].local_port == 31100
    # The next exec dials the NEW forward, not the old loopback.
    await gw._open_container_conn("hal")
    assert dialed == [("127.0.0.1", 31100)]


async def test_refresh_drops_cached_container_conn_real_backend(monkeypatch):
    """502 guard, tunnel target, production cache.

    After an exec caches a container connection on the current loopback
    forward, a refresh re-provisions to a NEW published port via a NEW forward.
    The next exec must NOT be handed the cached connection dialled to the old
    forward: the production ``SSHBackend`` cache is keyed to the (host, port)
    it dialed, so it dials the new forward and the old entry is dropped.
    """
    from compute_mcp.ssh_backend import SSHBackend

    gw, target = _endpoint_gateway(kind="tunnel")
    # Provision order: connect publishes 3010 (forward 31000); refresh
    # re-publishes 3500 (forward 31100).  The container hops always arrive at
    # the gateway via the loopback forward (127.0.0.1:local_port).
    provisioned = iter([
        (("127.0.0.1", 3010), 31000),
        (("127.0.0.1", 3500), 31100),
    ])

    async def fake_connect(t2, on_route=None, factor=None, **kwargs):
        (ep, forward) = next(provisioned)
        return _FakeTunnel(
            t2, route="hal", local_port=forward,
            provisioned_endpoint=ep, connection=object(),
        )

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)
    _wire_refresh_noops(gw, monkeypatch)

    # Track each real dial through the production SSHBackend._dial.
    dialed = []
    minted = []

    class _ConnStub:
        def __init__(self, n):
            self.n = n
            self._closed = False

        def is_closed(self):
            return self._closed

        def close(self):
            self._closed = True

        async def wait_closed(self):
            return None

    async def fake_dial(self, tgt, host, port, prompter=None, passphrase=None, username=None):
        dialed.append((host, port))
        conn = _ConnStub(len(minted))
        minted.append(conn)
        return conn

    monkeypatch.setattr(SSHBackend, "_dial", fake_dial)
    assert isinstance(gw.backend, SSHBackend)  # exercise the real cache

    result1 = await gw.connect_target("hal")
    assert result1["provisioned_endpoint"] == "127.0.0.1:3010"

    # First exec opens+gates the cached container connection on forward 31000.
    _, first_conn = await gw._open_container_conn("hal")
    assert dialed == [("127.0.0.1", 31000)]
    assert first_conn is minted[0]

    # Refresh while the cached connection is still "live": re-provision moves
    # the forward to 31100.  The old cache entry must be dropped so the next
    # exec cannot reuse it.
    result2 = await gw.refresh_target("hal")
    assert result2["state"] == "connected"
    assert result2["provisioned_endpoint"] == "127.0.0.1:3500"

    # Second exec: a fresh dial to the NEW forward, and a NEW connection object
    # (the cached 31000 one was invalidated, not returned).
    _, second_conn = await gw._open_container_conn("hal")
    assert dialed == [("127.0.0.1", 31000), ("127.0.0.1", 31100)]
    assert second_conn is minted[1]
    assert second_conn is not first_conn
    # The STALE connection was closed by the invalidation.
    assert first_conn.is_closed()


async def test_connect_target_early_return_keeps_endpoint(monkeypatch):
    """A plain connect while connected does NOT re-provision (idempotency);
    an explicit refresh DOES re-provision and updates the endpoint."""
    gw, target = _endpoint_gateway(kind="tunnel")
    # Already connected with a previously resolved endpoint.
    gw.runtimes["hal"].state = "connected"
    gw.runtimes["hal"].active_route = "hal"
    gw.runtimes["hal"].local_port = 31000
    gw.runtimes["hal"].provisioned_endpoint = "127.0.0.1:3010"
    gw.runtimes["hal"].tunnel = _FakeTunnel(
        target, route="hal", local_port=31000,
        provisioned_endpoint=("127.0.0.1", 3010),
    )

    connect_calls = 0

    async def fake_connect(t2, on_route=None, factor=None, **kwargs):
        nonlocal connect_calls
        connect_calls += 1
        # If a plain connect sneaked past the early return, we would re-provision.
        return _FakeTunnel(
            t2, route="hal", local_port=31000,
            provisioned_endpoint=("127.0.0.1", 3010),
        )

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)

    # Plain connect: returns early, no re-provision, keeps the endpoint it
    # already holds.
    result = await gw.connect_target("hal")
    assert result["state"] == "connected"
    assert result["provisioned_endpoint"] == "127.0.0.1:3010"
    assert connect_calls == 0, "plain connect while connected must not re-provision"

    # Explicit refresh re-provisions and moves the endpoint.
    _wire_refresh_noops(gw, monkeypatch)

    async def fake_connect9501(t2, on_route=None, factor=None, **kwargs):
        return _FakeTunnel(
            t2, route="hal", local_port=31000,
            provisioned_endpoint=("127.0.0.1", 9501),
        )

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect9501)
    result = await gw.refresh_target("hal")
    assert result["state"] == "connected"
    assert result["provisioned_endpoint"] == "127.0.0.1:9501"

# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import asyncio
import base64
import json
import time

import pytest
from aiohttp.test_utils import TestClient, TestServer

from terok_compute.auth import hash_token
from terok_compute.config import (
    ClientConfig,
    GatewayConfig,
    ServerConfig,
    SessionConfig,
    SSHConfig,
    TargetConfig,
    TransportConfig,
    parse_config,
)
from terok_compute.gateway import Gateway


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
    monkeypatch.setattr("terok_compute.gateway.sftp_client", lambda conn: FakeSftpCtx(conn))

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
    from terok_compute.config import load_config

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
    from terok_compute.config import ConfigError, load_config

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
        "terok_compute.gateway.sftp_client", lambda conn: FakeSftpCtx(conn)
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
    from terok_compute.gateway import _parse_provision_endpoint

    assert _parse_provision_endpoint("cn123:2345\n") == ("cn123", 2345)
    assert _parse_provision_endpoint("ENDPOINT 10.0.0.5:2222\n") == ("10.0.0.5", 2222)
    assert _parse_provision_endpoint("job 123 running\nENDPOINT cn1:9\n") == ("cn1", 9)
    assert _parse_provision_endpoint("no endpoint here\n") is None
    assert _parse_provision_endpoint("host:99999\n") is None
    # the first valid line wins
    assert _parse_provision_endpoint("a:1\nb:2\n") == ("a", 1)


async def test_provision_runs_command_and_overrides_endpoint(tmp_path, monkeypatch):
    gw = make_gateway()
    import dataclasses

    target = gw.config.targets["hal"]
    target = dataclasses.replace(
        target, transport=TransportConfig(kind="tunnel", ssh_targets=("rosi5",))
    )
    gw.config = dataclasses.replace(
        gw.config,
        targets={
            **gw.config.targets,
            "hal": dataclasses.replace(
                target, provision_command=("printf", "cmd01:2200\n")
            ),
        },
    )
    transport = await gw._provision(gw.config.targets["hal"])
    assert transport.remote_host == "cmd01"
    assert transport.remote_port == 2200


async def test_provision_failure_raises(tmp_path, monkeypatch):
    from terok_compute.gateway import ProvisionError

    gw = make_gateway()
    import dataclasses

    target = gw.config.targets["hal"]
    target = dataclasses.replace(
        target, transport=TransportConfig(kind="tunnel", ssh_targets=("rosi5",))
    )
    gw.config = dataclasses.replace(
        gw.config,
        targets={
            **gw.config.targets,
            "hal": dataclasses.replace(
                target,
                provision_command=(
                    "python3", "-c", "import sys; sys.stderr.write('no nodes'); sys.exit(3)"
                ),
            ),
        },
    )
    with pytest.raises(ProvisionError):
        await gw._provision(gw.config.targets["hal"])


async def test_connect_uses_provisioned_transport(monkeypatch):
    gw = make_gateway()
    import dataclasses

    target = gw.config.targets["hal"]
    target = dataclasses.replace(
        target, transport=TransportConfig(kind="tunnel", ssh_targets=("rosi5",))
    )
    gw.config = dataclasses.replace(
        gw.config,
        targets={
            **gw.config.targets,
            "hal": dataclasses.replace(
                target, provision_command=("printf", "cn9:3210\n")
            ),
        },
    )
    seen = {}

    async def fake_connect(target, on_route=None, transport=None):
        seen["transport"] = transport or target.transport
        return _FakeTunnel(target, transport or target.transport)

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)
    runtime = gw.runtimes["hal"]
    await gw._connect_locked(gw.config.targets["hal"], runtime)
    assert seen["transport"].remote_host == "cn9"
    assert seen["transport"].remote_port == 3210
    assert runtime.provisioned_endpoint == "cn9:3210"


class _FakeTunnel:
    def __init__(self, target, transport):
        self.target = target
        self.route = "direct"
        self.local_port = transport.remote_port
        self.process = None

    async def stop(self):
        return None


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
    from terok_compute.gateway import InteractiveAuthRequired
    from terok_compute.ssh_backend import SSHError

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


async def test_interactive_target_preauth_prompts_on_connect(monkeypatch):
    gw = make_gateway()
    import dataclasses

    target = gw.config.targets["hal"]
    gw.config = dataclasses.replace(
        gw.config,
        targets={
            **gw.config.targets,
            "hal": dataclasses.replace(target, interactive_auth=True),
        },
    )
    gw.runtimes["hal"].state = "connected"
    gw.runtimes["hal"].local_port = 9
    called = []

    async def fake_conn(target, host, port, prompter=None):
        called.append(prompter is not None)
        return object()

    monkeypatch.setattr(gw.backend, "connection", fake_conn)
    await gw._preauth_if_interactive("hal")
    assert called == [True]


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
    from terok_compute.config import load_config

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
    from terok_compute.auth import hash_token
    from terok_compute.gateway import generate_tokens
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
    from terok_compute.config import load_config
    from terok_compute.gateway import Gateway

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
    from terok_compute.config import load_config
    from terok_compute.gateway import Gateway

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
    from terok_compute.config import load_config, load_tokens
    from terok_compute.gateway import Gateway
    import terok_compute.gateway as gwmod

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

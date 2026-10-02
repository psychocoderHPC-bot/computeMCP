# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import base64
import json

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

    async def fake_run(conn, command, cwd=None, timeout=None):
        assert cwd is None
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

        resp = await client.post(
            "/v1/exec",
            headers=auth("alpaka-token"),
            json={"target": "gpu03", "command": "hostname"},
        )
        assert resp.status == 403
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

    async def fake_open(target, host, port):
        opened.append(target.name)
        return object()

    async def fake_run(conn, command, cwd=None, timeout=None):
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

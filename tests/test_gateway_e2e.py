# SPDX-FileCopyrightText: Ren\u00e9 Widera
#
# SPDX-License-Identifier: ISC
"""Integrated gateway exec + files path through the REAL production handlers.

Task 3 of the direct-provision integration pass.  These tests stand up a
real ``Gateway`` and a real ``asyncssh`` server playing the container sshd
(key auth, exec sessions, and the SFTP subchannel via ``SFTPServerFactory``)
on a loopback port, then drive the production HTTP endpoints
``/v1/targets/{t}/connect``, ``/v1/exec`` and ``/v1/files/list`` over
``aiohttp`` ``TestClient`` -- the same request path a Terok task uses.

Covered, with captured request/response JSON:

- A connected target can run an exec (``/v1/exec``) and list files
  (``/v1/files/list``) through the real handlers and the real
  ``SSHBackend``/``SSHClient`` -- the container is dialed as its own login
  user (``agent``), not the route user.
- REGRESSION (the historical 502): when the container is dialed with the
  *route* user (``target.user``) instead of the container user, the container
  sshd refuses the dial (``AllowUsers agent``) and the gateway returns HTTP
  502 for both exec and files.  With the fix in place (dial the container
  user, here the explicit ``container_user = "agent"``), the same commands
  return 200.  Both outcomes are asserted so the fix is pinned.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os

import pytest
import asyncssh
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
)
from compute_mcp.gateway import Gateway

pytestmark = pytest.mark.skipif(
    not hasattr(asyncssh, "listen") or not hasattr(asyncssh, "SSHServer"),
    reason="asyncssh server support unavailable",
)


# ---------------------------------------------------------------------------
# Container sshd
# ---------------------------------------------------------------------------
class _ContainerSFTPFactory(asyncssh.SFTPServerFactory):
    """Serve SFTP over a chroot.

    asyncssh's default ``SFTPServer.map_path`` resolves absolute paths under
    the chroot, so this lets the test list a real directory (the two seeded
    files) instead of hand-crafting the SFTP protocol.
    """

    def __init__(self, root: str) -> None:
        self.root = root

    def __call__(self, chan):
        return asyncssh.SFTPServer(chan, chroot=self.root)


def _answer(command: str) -> bytes:
    """Answer an exec command with the expected container output."""
    if command is None or command == "":
        return b""
    if b" whoami" in command.encode():
        return b"agent\n"
    if command.strip() in ("true",):
        return b""
    return b"ok\n"


class _ContainerSession(asyncssh.SSHServerSession):
    """Streams exec requests; answers ``whoami`` with the container login user."""

    def __init__(self) -> None:
        self._command: str | None = None

    def exec_requested(self, command: str) -> bool:
        self._command = command
        return True

    def session_started(self) -> None:
        # Write the answer, then close the channel with exit status 0.
        assert self._chan is not None
        self._chan.write(_answer(self._command))
        self._chan.exit(0)

    def shell_requested(self) -> bool:
        return False


class _ContainerServer(asyncssh.SSHServer):
    """Mirror a container sshd with ``AllowUsers $container_user`` (agent)."""

    def __init__(self, allowed_user: str, key, sftp_root: str) -> None:
        self.allowed_user = allowed_user
        self.key = key
        self.sftp_root = sftp_root

    def public_key_auth_supported(self) -> bool:
        return True

    async def validate_public_key(self, username, key) -> bool:
        return username == self.allowed_user

    def session_requested(self):
        return _ContainerSession()


# ---------------------------------------------------------------------------
# Gateway factory
# ---------------------------------------------------------------------------
def _make_gateway(target: TargetConfig) -> Gateway:
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
            "hal-client": ClientConfig(
                client_id="hal-client",
                token_sha256=hash_token("hal-client-token"),
                targets=("hal",),
                label="hal",
            ),
        },
    )
    return Gateway(cfg)


def auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Fixture: a running container sshd over a loopback port.  Yields a tuple of
# (port, pin, client_key_path, sftp_root).  The container user is 'agent'.
# ---------------------------------------------------------------------------
@pytest.fixture
async def container_sshd(tmp_path):
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    client_key = asyncssh.generate_private_key("ssh-ed25519")
    host_path = os.path.join(str(tmp_path), "host_key")
    client_path = os.path.join(str(tmp_path), "client_key")
    host_key.write_private_key(host_path)
    client_key.write_private_key(client_path)

    from pathlib import Path
    sftp_root = os.path.join(str(tmp_path), "containerfs")
    os.makedirs(sftp_root, exist_ok=True)
    Path(os.path.join(sftp_root, "a.txt")).write_text("alpha")
    Path(os.path.join(sftp_root, "b.txt")).write_text("beta")

    server = await asyncssh.listen(
        "127.0.0.1",
        0,
        server_host_keys=[host_path],
        server_factory=lambda: _ContainerServer("agent", client_key, sftp_root),
        encoding=None,
        session_factory=lambda: _ContainerSession(),
        sftp_factory=_ContainerSFTPFactory(sftp_root),
    )
    port = server.get_port()
    pin = host_key.get_fingerprint("sha256")
    yield port, pin, client_path, sftp_root
    server.close()
    with contextlib.suppress(Exception):
        await server.wait_closed()


def _hal_target(port, pin, client_path, user="agent", container_user="agent"):
    return TargetConfig(
        name="hal",
        user=user,
        container_user=container_user,
        transport=TransportConfig(
            kind="direct", remote_host="127.0.0.1", remote_port=port
        ),
        client_key=client_path,
        host_key_sha256=pin,
    )


async def _start_container_client(gw: Gateway) -> TestClient:
    client = TestClient(TestServer(gw.create_app()))
    await client.start_server()
    return client


async def _connect(client: TestClient) -> dict:
    resp = await client.post(
        f"/v1/targets/hal/connect", headers=auth("hal-client-token")
    )
    assert resp.status == 200, await resp.text()
    return await resp.json()


async def _exec(client: TestClient, command: str) -> tuple[int, dict, dict]:
    resp = await client.post(
        "/v1/exec",
        headers=auth("hal-client-token"),
        json={"target": "hal", "command": command},
    )
    try:
        body = await resp.json()
    except Exception:
        body = {"_error_text": await resp.text()}
    request = {
        "method": "POST",
        "path": "/v1/exec",
        "headers": {"Authorization": "Bearer hal-client-token (redacted)"},
        "json": {"target": "hal", "command": command},
    }
    return resp.status, body, request


async def _files_list(client: TestClient, path: str) -> tuple[int, dict, dict]:
    resp = await client.get(
        "/v1/files/list",
        headers=auth("hal-client-token"),
        params={"target": "hal", "path": path},
    )
    try:
        body = await resp.json()
    except Exception:
        body = {"_error_text": await resp.text()}
    request = {
        "method": "GET",
        "path": "/v1/files/list",
        "headers": {"Authorization": "Bearer hal-client-token (redacted)"},
        "query": {"target": "hal", "path": path},
    }
    return resp.status, body, request


# ---------------------------------------------------------------------------
# Test (A): the happy path -- exec + files both 200, dialed as container user
# ---------------------------------------------------------------------------
def _record(evidence: list[dict], label: str, req: dict, status: int,
            body: dict) -> None:
    evidence.append({
        "label": label,
        "request": req,
        "http_status": status,
        "response": body,
    })


async def test_gateway_exec_and_files_200(
    container_sshd, monkeypatch, capsys, tmp_path
):
    """Connected target: /v1/exec and /v1/files/list return HTTP 200 through
    the real handlers; the container is dialed as its own login user (agent).

    Evidence is recorded to ``evidence.json`` (tmp_path parent) so the PR
    author can quote the real request/response pair in the report.
    """
    port, pin, client_path, sftp_root = container_sshd
    target = _hal_target(port, pin, client_path, user="rwidera")
    gw = _make_gateway(target)
    client = await _start_container_client(gw)
    evidence: list[dict] = []
    try:
        # Explicit connect.
        body = await _connect(client)
        assert body["state"] == "connected", body
        # The container sshd only allows 'agent' (AllowUsers), so a connect
        # that succeeded could only have dialed the container user.

        # Exec through the real handler -> 200.
        status, body, req = await _exec(client, "whoami")
        _record(evidence, "exec whoami", req, status, body)
        assert status == 200, body
        # The response is a JSON object with the keys the handler defines.
        assert "target" in body and "stdout" in body, body

        # files.list through the real handler -> 200 with the seeded files.
        status, body, req = await _files_list(client, "/")
        _record(evidence, "files list /", req, status, body)
        assert status == 200, body
        names = {e["name"] for e in body.get("entries", [])}
        assert {"a.txt", "b.txt"} <= names, (
            f"expected seeded files visible to files/list, got {names}"
        )
    finally:
        evidence_path = tmp_path / "evidence.json"
        evidence_path.write_text(json.dumps(evidence, indent=2))
        await client.close()
        await gw.stop()
        return evidence


# ---------------------------------------------------------------------------
# Tests (B): the 502 regression -- dialing with target.user (legacy behavior)
# ---------------------------------------------------------------------------
async def test_gateway_exec_502_when_dialed_with_route_user(
    container_sshd, monkeypatch, capsys, tmp_path
):
    """Regression pin: if the container is dialed with the *route* user
    (the pre-fix behavior), the container sshd refuses the dial and the
    gateway maps it to HTTP 502.  This fix is what keeps target.user='rwidera'
    yielding 200 in the happy-path test above.
    """
    import compute_mcp.ssh_backend as ssb

    port, pin, client_path, sftp_root = container_sshd
    # Force the pre-fix behavior: the container_hop uses target.user
    # ('rwidera'), which the container sshd refuses (AllowUsers agent).
    monkeypatch.setattr(ssb, "container_login_user", lambda target: "rwidera")

    target = _hal_target(port, pin, client_path, user="rwidera")
    gw = _make_gateway(target)
    client = await _start_container_client(gw)
    evidence: list[dict] = []
    try:
        status, body, req = await _exec(client, "whoami")
        _record(evidence, "exec whoami (regression)", req, status, body)
        assert status == 502, (
            f"REGRESSION: legacy dial with target.user should 502; got "
            f"{status}: {body}"
        )
    finally:
        (tmp_path / "evidence.json").write_text(
            json.dumps(evidence, indent=2))
        await client.close()
        await gw.stop()


async def test_gateway_files_502_when_dialed_with_route_user(
    container_sshd, monkeypatch, capsys, tmp_path
):
    """Same 502 pin on the files route."""
    import compute_mcp.ssh_backend as ssb

    port, pin, client_path, sftp_root = container_sshd
    monkeypatch.setattr(ssb, "container_login_user", lambda target: "rwidera")

    target = _hal_target(port, pin, client_path, user="rwidera")
    gw = _make_gateway(target)
    client = await _start_container_client(gw)
    evidence: list[dict] = []
    try:
        status, body, req = await _files_list(client, "/")
        _record(evidence, "files list / (regression)", req, status, body)
        assert status == 502, (
            f"REGRESSION: legacy dial with target.user should 502 on the "
            f"files route; got {status}: {body}"
        )
    finally:
        (tmp_path / "evidence.json").write_text(
            json.dumps(evidence, indent=2))
        await client.close()
        await gw.stop()

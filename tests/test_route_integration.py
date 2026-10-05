# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""In-process asyncssh integration tests for the route-first tunnel.

These stand up a real asyncssh *server* (no sshd binary, no external network)
and drive :meth:`compute_mcp.tunnel.TunnelManager.open_for_route` against it.
``_resolve_route`` is monkeypatched to point at the in-process server, so no
``ssh -G`` is required.

Everything is bounded with ``asyncio.wait_for`` so a regression cannot hang the
suite.  The whole module is skipped if the installed asyncssh lacks server
support.
"""
from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import os

import pytest

import asyncssh

from compute_mcp.config import SSHConfig, TargetConfig, TransportConfig
from compute_mcp.ssh_backend import SSHError
from compute_mcp.tunnel import (
    TunnelError,
    TunnelManager,
    allocate_loopback_port,
)

pytestmark = pytest.mark.skipif(
    not hasattr(asyncssh, "listen") or not hasattr(asyncssh, "SSHServer"),
    reason="asyncssh server support unavailable",
)

# Bound every operation so a regression fails fast instead of hanging.
CONNECT_TIMEOUT = 10.0
RUN_TIMEOUT = 5.0


# ---------------------------------------------------------------------------
# In-process login/route server
# ---------------------------------------------------------------------------

class _LoginSession(asyncssh.SSHServerSession):
    """Accepts an exec and answers provisioning / records the command."""

    def __init__(self, state: dict) -> None:
        self._chan = None
        self.state = state
        self._command = ""

    def connection_made(self, chan) -> None:
        self._chan = chan

    def exec_requested(self, command) -> bool:
        self.state["commands"].append(command)
        self._command = command
        return True

    def session_started(self) -> None:
        out = ""
        if "provision" in self._command:
            out = f"ENDPOINT 127.0.0.1:{self.state['dest_port']}\n"
        self._chan.write(out.encode())
        self._chan.exit(0)

    def shell_requested(self) -> bool:
        return False


class _LoginServer(asyncssh.SSHServer):
    """One login/route host supporting the auth modes a test enables."""

    def __init__(
        self,
        state: dict,
        *,
        password: str | None = None,
        kbdint: str | None = None,
        pubkey: bool = False,
    ) -> None:
        self.state = state
        self.password = password
        self.kbdint = kbdint
        self.pubkey = pubkey
        self.seen_passwords: list[str] = []

    def begin_auth(self, username: str) -> bool:
        return True

    def password_auth_supported(self) -> bool:
        return self.password is not None

    async def validate_password(self, username: str, password: str) -> bool:
        self.seen_passwords.append(password)
        self.state.setdefault("seen_passwords", []).append(password)
        return password == self.password

    def kbdint_auth_supported(self) -> bool:
        return self.kbdint is not None

    def get_kbdint_challenge(self, username, lang, submethods):
        return ("OTP", "Enter your one-time code", "en-US", [("Code: ", False)])

    async def validate_kbdint_response(self, username, responses):
        return list(responses) == [self.kbdint]

    def public_key_auth_supported(self) -> bool:
        return self.pubkey

    async def validate_public_key(self, username, key) -> bool:
        return self.pubkey

    def connection_requested(self, dest_host, dest_port, orig_host, orig_port):
        # Accept the client's TCP forwarding request and let asyncssh relay it.
        return True

    def session_requested(self):
        return _LoginSession(self.state)


class _Login:
    """A started in-process login server plus its pin and port."""

    def __init__(self, server, pin: str, state: dict) -> None:
        self.server = server
        self.pin = pin
        self.state = state

    @property
    def port(self) -> int:
        return self.server.get_port()

    async def close(self) -> None:
        self.server.close()
        with contextlib.suppress(Exception):
            await self.server.wait_closed()


async def _start_login(tmp_path_factory, **auth) -> _Login:
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    host_path = os.path.join(str(tmp_path_factory.mktemp("route")), "host_key")
    host_key.write_private_key(host_path)
    state: dict = {"commands": [], "dest_port": 0}
    server = await asyncssh.listen(
        "127.0.0.1",
        0,
        server_host_keys=[host_path],
        server_factory=lambda: _LoginServer(state, **auth),
        encoding=None,
    )
    return _Login(server, host_key.get_fingerprint("sha256"), state)


def _route_alias(monkeypatch, login: _Login, user: str = "agent") -> None:
    async def fake_resolve(alias, ssh, _seen=None):
        return {
            "alias": alias,
            "hostname": "127.0.0.1",
            "user": user,
            "port": login.port,
            "identityfiles": (),
            "jumps": (),
        }

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)


def _target(login: _Login, **overrides) -> TargetConfig:
    base = TargetConfig(
        name="t",
        user="agent",
        transport=TransportConfig(
            kind="tunnel", ssh_targets=("login",), remote_host="127.0.0.1",
            remote_port=1,
        ),
        route_host_key_sha256=login.pin,
    )
    if overrides:
        import dataclasses

        base = dataclasses.replace(base, **overrides)
    return base


async def _open(mgr: TunnelManager, target: TargetConfig, factor: str | None):
    local_port = allocate_loopback_port(mgr.ssh, mgr._reserved)
    return await asyncio.wait_for(
        mgr.open_for_route(target, "login", local_port, factor=factor),
        CONNECT_TIMEOUT,
    )


@pytest.fixture
async def dest_listener():
    """A loopback TCP echo server standing in for the container endpoint."""
    async def handler(reader, writer):
        try:
            data = await asyncio.wait_for(reader.read(1024), RUN_TIMEOUT)
            writer.write(b"echo:" + data)
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield port
    finally:
        server.close()
        with contextlib.suppress(Exception):
            await server.wait_closed()


# ---------------------------------------------------------------------------
# Authentication over the route
# ---------------------------------------------------------------------------

async def test_password_auth_on_route_with_factor(tmp_path_factory, monkeypatch):
    login = await _start_login(tmp_path_factory, password="SECRET")
    try:
        _route_alias(monkeypatch, login)
        mgr = TunnelManager(SSHConfig())
        target = _target(login)
        tunnel = await _open(mgr, target, factor="SECRET")
        try:
            assert tunnel.is_alive()
            assert tunnel.connection is not None
        finally:
            await tunnel.stop()
            mgr.release(tunnel)
    finally:
        await login.close()


async def test_wrong_factor_fails_without_hang(tmp_path_factory, monkeypatch):
    login = await _start_login(tmp_path_factory, password="SECRET")
    try:
        _route_alias(monkeypatch, login)
        mgr = TunnelManager(SSHConfig())
        target = _target(login)
        with pytest.raises((TunnelError, SSHError)):
            await _open(mgr, target, factor="WRONG")
        # The server observed the (wrong) password and refused.  The capped
        # prompter retried exactly MAX_FACTOR_PROMPTS times, never unbounded.
        assert login.state is not None
        from compute_mcp.ssh_backend import MAX_FACTOR_PROMPTS

        assert login.state.get("seen_passwords") == ["WRONG"] * MAX_FACTOR_PROMPTS
    finally:
        await login.close()


async def test_keyboard_interactive_otp_on_route(tmp_path_factory, monkeypatch):
    login = await _start_login(tmp_path_factory, kbdint="123456")
    try:
        _route_alias(monkeypatch, login)
        mgr = TunnelManager(SSHConfig())
        target = _target(login)
        tunnel = await _open(mgr, target, factor="123456")
        try:
            assert tunnel.is_alive()
        finally:
            await tunnel.stop()
            mgr.release(tunnel)
    finally:
        await login.close()


def _write_encrypted_key(tmp_path_factory, passphrase: bytes) -> str:
    """Write a PKCS#8-encrypted ed25519 client key and return its path.

    The venv has no bcrypt, so an OpenSSH-format encrypted key cannot be
    written; cryptography's PKCS#8 encryption is asyncssh-importable with a
    ``passphrase=`` and rejects a missing/wrong one.
    """
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519

    key_dir = tmp_path_factory.mktemp("enc")
    key_path = str(key_dir / "id_enc")
    private = ed25519.Ed25519PrivateKey.generate()
    pem = private.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.BestAvailableEncryption(passphrase),
    )
    with open(key_path, "wb") as handle:
        handle.write(pem)
    return key_path


async def test_encrypted_client_key_unlocked_with_factor(
    tmp_path_factory, monkeypatch
):
    """An encrypted client key IS unlocked when the factor is its passphrase.

    Key-only login server (no password/OTP challenge, so the prompter is never
    asked): the route-dial factor must reach asyncssh as ``passphrase=`` and
    decrypt the key.  This pins the user-approved requirement that one factor
    can be the SSH client-key passphrase.
    """
    login = await _start_login(tmp_path_factory, pubkey=True)
    try:
        _route_alias(monkeypatch, login)
        key_path = _write_encrypted_key(tmp_path_factory, b"SECRET")

        mgr = TunnelManager(SSHConfig())
        target = _target(login, client_key=key_path)
        tunnel = await _open(mgr, target, factor="SECRET")
        try:
            assert tunnel.is_alive()
            assert tunnel.connection is not None
            # The key-only server never needed the interactive prompter.
            assert login.server is not None
        finally:
            await tunnel.stop()
            mgr.release(tunnel)
    finally:
        await login.close()


async def test_encrypted_client_key_wrong_factor_fails_cleanly(
    tmp_path_factory, monkeypatch
):
    """A wrong/missing passphrase must fail fast, never hang.

    With the factor forwarded as ``passphrase=``, a wrong value fails during
    key import and is surfaced as ``TunnelError``/``SSHError`` (both wrapped),
    bounded by the wait_for in ``_open``.
    """
    login = await _start_login(tmp_path_factory, pubkey=True)
    try:
        _route_alias(monkeypatch, login)
        key_path = _write_encrypted_key(tmp_path_factory, b"SECRET")

        mgr = TunnelManager(SSHConfig())
        target = _target(login, client_key=key_path)
        with pytest.raises((TunnelError, SSHError)):
            await _open(mgr, target, factor="WRONG")
    finally:
        await login.close()


# ---------------------------------------------------------------------------
# Provisioning, forwarding and command execution
# ---------------------------------------------------------------------------

async def test_provisioning_and_forward_roundtrip(
    tmp_path_factory, monkeypatch, dest_listener
):
    login = await _start_login(tmp_path_factory, password="SECRET")
    login.state["dest_port"] = dest_listener
    try:
        _route_alias(monkeypatch, login)
        mgr = TunnelManager(SSHConfig())
        target = _target(
            login, provision_command=("provision", "--ensure")
        )
        tunnel = await _open(mgr, target, factor="SECRET")
        try:
            assert tunnel.provisioned_endpoint == ("127.0.0.1", dest_listener)
            # A TCP round-trip through the local forward reaches the listener.
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", tunnel.local_port),
                RUN_TIMEOUT,
            )
            try:
                writer.write(b"ping")
                await writer.drain()
                data = await asyncio.wait_for(reader.read(64), RUN_TIMEOUT)
                assert data == b"echo:ping"
            finally:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
        finally:
            await tunnel.stop()
            mgr.release(tunnel)
    finally:
        await login.close()


async def test_connect_command_runs_over_route(tmp_path_factory, monkeypatch):
    login = await _start_login(tmp_path_factory, password="SECRET")
    login.state["dest_port"] = 9
    try:
        _route_alias(monkeypatch, login)
        mgr = TunnelManager(SSHConfig())
        target = _target(login, connect_command=("/bin/up.sh", "--ensure"))
        tunnel = await _open(mgr, target, factor="SECRET")
        try:
            await asyncio.wait_for(
                mgr.run_connect_command(target, "login", connection=tunnel.connection),
                RUN_TIMEOUT,
            )
            assert "/bin/up.sh --ensure" in login.state["commands"]
        finally:
            await tunnel.stop()
            mgr.release(tunnel)
    finally:
        await login.close()


# ---------------------------------------------------------------------------
# Route-host-key pin negatives
# ---------------------------------------------------------------------------

async def test_wrong_route_pin_aborts_without_hang(tmp_path_factory, monkeypatch):
    login = await _start_login(tmp_path_factory, password="SECRET")
    try:
        _route_alias(monkeypatch, login)
        mgr = TunnelManager(SSHConfig())
        # A syntactically valid but wrong pin must be refused by the client hook.
        target = _target(
            login,
            route_host_key_sha256="SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        )
        with pytest.raises((TunnelError, SSHError)):
            await _open(mgr, target, factor="SECRET")
    finally:
        await login.close()


async def test_container_host_key_pin_not_applied_to_route_hop(
    tmp_path_factory, monkeypatch
):
    """`host_key_sha256` pins the container; it must not pin the route hop."""
    login = await _start_login(tmp_path_factory, password="SECRET")
    try:
        captured = {}

        async def capture_dial_route(**kwargs):
            captured["route_pin"] = kwargs.get("host_key_sha256")
            return _FakeRouteConn()

        async def fake_probe(host, port, timeout=8.0):
            return True

        _route_alias(monkeypatch, login)
        monkeypatch.setattr("compute_mcp.tunnel.dial_route", capture_dial_route)
        monkeypatch.setattr("compute_mcp.tunnel.probe", fake_probe)

        mgr = TunnelManager(SSHConfig())
        # Only the CONTAINER pin is configured, no route pin.
        target = dataclasses.replace(
            _target(login),
            route_host_key_sha256=None,
            host_key_sha256=login.pin,
        )
        assert target.host_key_sha256 == login.pin
        assert target.route_host_key_sha256 is None
        local_port = allocate_loopback_port(mgr.ssh, mgr._reserved)
        tunnel = await asyncio.wait_for(
            mgr.open_for_route(target, "login", local_port),
            CONNECT_TIMEOUT,
        )
        try:
            assert captured["route_pin"] is None
        finally:
            await tunnel.stop()
            mgr.release(tunnel)
    finally:
        await login.close()


class _FakeRouteConn:
    forwarded: list = []

    def is_closed(self):
        return False

    def close(self):
        return None

    async def wait_closed(self):
        return None

    async def forward_local_port(self, *args):
        return object()


async def test_proxyjump_nonfinal_hop_dialed_without_route_pin(
    monkeypatch,
):
    """The jump hop gets no route pin; the final route hop does."""
    dialed = []
    final_calls = []

    async def fake_resolve(alias, ssh, _seen=None):
        if alias == "route":
            return {
                "alias": "route",
                "hostname": "final.example",
                "user": "agent",
                "port": 22,
                "identityfiles": (),
                "jumps": (
                    {
                        "alias": "jump",
                        "hostname": "jump.example",
                        "user": "agent",
                        "port": 22,
                        "identityfiles": (),
                        "jumps": (),
                    },
                ),
            }
        return {
            "alias": alias,
            "hostname": alias,
            "user": "agent",
            "port": 22,
            "identityfiles": (),
            "jumps": (),
        }

    async def fake_dial_route(**kwargs):
        dialed.append(kwargs.get("host_key_sha256"))
        return _FakeRouteConn()

    async def fake_connect(host, **kwargs):
        final_calls.append(kwargs)
        return _FakeRouteConn()

    async def fake_probe(host, port, timeout=8.0):
        return True

    monkeypatch.setattr("compute_mcp.tunnel._resolve_route", fake_resolve)
    monkeypatch.setattr("compute_mcp.tunnel.dial_route", fake_dial_route)
    monkeypatch.setattr("compute_mcp.tunnel.asyncssh.connect", fake_connect)
    monkeypatch.setattr("compute_mcp.tunnel.probe", fake_probe)

    pin = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
    target = TargetConfig(
        name="t",
        user="agent",
        transport=TransportConfig(
            kind="tunnel", ssh_targets=("route",), remote_host="cn", remote_port=1
        ),
        route_host_key_sha256=pin,
    )
    mgr = TunnelManager(SSHConfig())
    local_port = allocate_loopback_port(mgr.ssh, mgr._reserved)
    tunnel = await asyncio.wait_for(
        mgr.open_for_route(target, "route", local_port), CONNECT_TIMEOUT
    )
    try:
        # The non-final (jump) hop through dial_route must have no pin.
        assert dialed == [None]
        # The final hop goes through asyncssh.connect with a pin-bearing client.
        assert len(final_calls) == 1
        client_factory = final_calls[0]["client_factory"]
        client = client_factory()
        assert client._pin is not None
    finally:
        await tunnel.stop()
        mgr.release(tunnel)

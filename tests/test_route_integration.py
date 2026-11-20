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
        self.state["live_channels"] = (
            self.state.get("live_channels", 0) + 1
        )
        self.state["max_channels"] = max(
            self.state.get("max_channels", 0),
            self.state["live_channels"],
        )

    def connection_lost(self, exc) -> None:
        self.state["live_channels"] = max(
            0, self.state.get("live_channels", 0) - 1
        )

    def exec_requested(self, command) -> bool:
        self.state["commands"].append(command)
        self._command = command
        if "provision" in command:
            self.state["provision_requests"] = (
                self.state.get("provision_requests", 0) + 1
            )
        return True

    def session_started(self) -> None:
        out = ""
        if "provision" in self._command:
            self.state["provision_count"] = (
                self.state.get("provision_count", 0) + 1
            )
            ports = self.state.get("endpoint_ports") or [
                self.state["dest_port"]
            ]
            port = ports[
                min(self.state["provision_count"] - 1, len(ports) - 1)
            ]
            self.state["dest_port"] = port
            out = f"ENDPOINT 127.0.0.1:{port}\n"
        elif "close" in self._command:
            self.state["close_count"] = (
                self.state.get("close_count", 0) + 1
            )
            if self.state.get("drop_on_close"):
                conn = self.state.get("conn")
                if conn is not None:
                    conn.close()
        self._chan.write(out.encode())
        self._chan.exit(0)

    def shell_requested(self) -> bool:
        return False


class _ForwardSession(asyncssh.SSHTCPSession):
    """Count and relay one direct-tcpip channel to the requested destination."""

    def __init__(self, state: dict, dest_host: str, dest_port: int) -> None:
        self.state = state
        self.dest = (dest_host, dest_port)
        self._chan = None
        self._writer = None
        self._pending = bytearray()
        self._opened = False
        self._open_task = None
        self._reader_task = None

    def connection_made(self, chan) -> None:
        self._chan = chan
        self.state["live_channels"] = (
            self.state.get("live_channels", 0) + 1
        )
        self.state["max_channels"] = max(
            self.state.get("max_channels", 0),
            self.state["live_channels"],
        )
        self._open_task = asyncio.create_task(self._connect())

    async def _connect(self) -> None:
        try:
            reader, writer = await asyncio.open_connection(*self.dest)
        except OSError:
            if self._chan is not None:
                self._chan.close()
            return
        self._writer = writer
        self._opened = True
        if self._pending:
            writer.write(self._pending)
            self._pending.clear()
        self._reader_task = asyncio.create_task(self._pump(reader))

    async def _pump(self, reader) -> None:
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                if self._chan is not None:
                    self._chan.write(data)
        except Exception:
            pass
        finally:
            if self._chan is not None:
                with contextlib.suppress(Exception):
                    self._chan.write_eof()

    def data_received(self, data, datatype) -> None:
        if self._opened and self._writer is not None:
            self._writer.write(data)
        else:
            self._pending.extend(data)

    def eof_received(self):
        if self._writer is not None:
            with contextlib.suppress(Exception):
                self._writer.write_eof()
        return False

    def connection_lost(self, exc) -> None:
        self.state["live_channels"] = max(
            0, self.state.get("live_channels", 0) - 1
        )
        if self._reader_task is not None:
            self._reader_task.cancel()
            self._reader_task = None
        if self._open_task is not None and not self._open_task.done():
            self._open_task.cancel()
            self._open_task = None
        if self._writer is not None:
            with contextlib.suppress(Exception):
                self._writer.close()
            self._writer = None


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

    def connection_made(self, conn) -> None:
        self.state["connections"] = self.state.get("connections", 0) + 1
        self.state["conn"] = conn

    def begin_auth(self, username: str) -> bool:
        return True

    def auth_completed(self) -> None:
        self.state["auths"] = self.state.get("auths", 0) + 1

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
        # Enforce the site's one-open-channel limit in the fixture.  A refresh
        # that leaves an old direct-tcpip channel open cannot open its session.
        if self.state.get("live_channels", 0) >= 1:
            return False
        return _ForwardSession(self.state, dest_host, dest_port)

    def session_requested(self):
        if self.state.get("live_channels", 0) >= 1:
            return False
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


class _ContainerSession(asyncssh.SSHServerSession):
    """Minimal container sshd session used to prove the forward still works."""

    def __init__(self, state: dict) -> None:
        self.state = state
        self._chan = None
        self._command = ""

    def connection_made(self, chan) -> None:
        self._chan = chan

    def exec_requested(self, command) -> bool:
        self._command = command
        self.state["commands"].append(command)
        if "provision" in command:
            self.state["provision_requests"] = (
                self.state.get("provision_requests", 0) + 1
            )
        return True

    def session_started(self) -> None:
        self._chan.write(f"container:{self._command}\n".encode())
        self._chan.exit(0)

    def shell_requested(self) -> bool:
        return False


class _ContainerServer(asyncssh.SSHServer):
    """Key-only container endpoint; accepts any offered public key."""

    def __init__(self, state: dict) -> None:
        self.state = state

    def begin_auth(self, username: str) -> bool:
        return True

    def public_key_auth_supported(self) -> bool:
        return True

    async def validate_public_key(self, username, key) -> bool:
        return True

    def session_requested(self):
        return _ContainerSession(self.state)


async def _start_login(tmp_path_factory, **auth) -> _Login:
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    host_path = os.path.join(str(tmp_path_factory.mktemp("route")), "host_key")
    host_key.write_private_key(host_path)
    state: dict = {
        "commands": [],
        "dest_port": 0,
        "connections": 0,
        "auths": 0,
        "live_channels": 0,
        "max_channels": 0,
        "provision_count": 0,
        "provision_requests": 0,
        "close_count": 0,
    }
    server = await asyncssh.listen(
        "127.0.0.1",
        0,
        server_host_keys=[host_path],
        server_factory=lambda: _LoginServer(state, **auth),
        encoding=None,
    )
    return _Login(server, host_key.get_fingerprint("sha256"), state)


def _route_alias(
    monkeypatch,
    login: _Login,
    user: str = "agent",
    identityfiles: tuple[str, ...] = (),
) -> None:
    async def fake_resolve(alias, ssh, _seen=None):
        return {
            "alias": alias,
            "hostname": "127.0.0.1",
            "user": user,
            "port": login.port,
            "identityfiles": tuple(identityfiles),
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
        # The server observed the (wrong) password exactly once: a single-use
        # factor must never be replayed on a second prompt.
        assert login.state is not None
        assert login.state.get("seen_passwords") == ["WRONG"]
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
        key_path = _write_encrypted_key(tmp_path_factory, b"SECRET")
        _route_alias(monkeypatch, login, identityfiles=(key_path,))

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
        key_path = _write_encrypted_key(tmp_path_factory, b"SECRET")
        _route_alias(monkeypatch, login, identityfiles=(key_path,))

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


async def test_proxyjump_factor_is_used_only_on_the_final_hop(monkeypatch):
    """The 2FA factor must not be spent on a ProxyJump hop."""
    jump_calls = []
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
        jump_calls.append(kwargs)
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

    target = TargetConfig(
        name="t",
        user="agent",
        transport=TransportConfig(
            kind="tunnel", ssh_targets=("route",), remote_host="cn", remote_port=1
        ),
        route_host_key_sha256=(
            "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
        ),
    )
    mgr = TunnelManager(SSHConfig())
    local_port = allocate_loopback_port(mgr.ssh, mgr._reserved)
    tunnel = await asyncio.wait_for(
        mgr.open_for_route(target, "route", local_port, factor="SECRET"),
        CONNECT_TIMEOUT,
    )
    try:
        assert jump_calls[0]["prompter"] is None
        assert jump_calls[0]["passphrase"] is None
        assert final_calls[0]["passphrase"] == "SECRET"
        client = final_calls[0]["client_factory"]()
        assert client._prompter is not None
    finally:
        await tunnel.stop()
        mgr.release(tunnel)


# ---------------------------------------------------------------------------
# Live target-refresh over the retained login connection
# ---------------------------------------------------------------------------

@contextlib.asynccontextmanager
async def _live_refresh_gateway(tmp_path, tmp_path_factory, monkeypatch):
    """Start the login fixture, two container endpoints and a live gateway."""
    login = await _start_login(tmp_path_factory, password="SECRET")
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    host_key_path = tmp_path / "container_host_key"
    host_key.write_private_key(str(host_key_path))
    container_pin = host_key.get_fingerprint("sha256")
    client_key = asyncssh.generate_private_key("ssh-ed25519")
    client_key_path = tmp_path / "client_key"
    client_key.write_private_key(str(client_key_path))
    container_states = [{"commands": []}, {"commands": []}]
    containers = []
    gw = None
    runtime = None
    try:
        for state in container_states:
            server = await asyncssh.listen(
                "127.0.0.1",
                0,
                server_host_keys=[str(host_key_path)],
                server_factory=lambda st=state: _ContainerServer(st),
                encoding=None,
            )
            containers.append(server)
        ports = [server.get_port() for server in containers]
        login.state["endpoint_ports"] = ports
        login.state["dest_port"] = ports[0]
        _route_alias(monkeypatch, login)
        target = TargetConfig(
            name="hal",
            user="agent",
            container_user="ubuntu",
            transport=TransportConfig(
                kind="tunnel",
                ssh_targets=("login",),
                remote_host="127.0.0.1",
                remote_port=ports[0],
            ),
            route_host_key_sha256=login.pin,
            host_key_sha256=container_pin,
            client_key=str(client_key_path),
            provision_command=("provision",),
            close_command=("close",),
            interactive_auth=True,
        )
        cfg = GatewayConfig(
            server=ServerConfig(),
            ssh=SSHConfig(),
            sessions=SessionConfig(),
            targets={"hal": target},
            clients={
                "test": ClientConfig(
                    client_id="test",
                    token_sha256=hash_token("test-token"),
                    allow_all=True,
                )
            },
        )
        gw = Gateway(cfg)
        runtime = gw.runtimes["hal"]
        yield login, containers, container_states, gw, runtime, ports
    finally:
        if gw is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(gw.backend.disconnect("hal"), 5)
            if runtime is not None and runtime.tunnel is not None:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(runtime.tunnel.stop(), 10)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(gw.sessions.stop(), 10)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(login.close(), 5)
        for server in containers:
            server.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(server.wait_closed(), 5)


async def test_live_refresh_reuses_login_connection_and_one_channel(
    tmp_path, tmp_path_factory, monkeypatch
):
    """A live refresh must not authenticate again or overlap outer channels.

    The login fixture enforces MaxSessions=1 for both session and
    direct-tcpip channels and counts every channel until connection_lost.  The
    gateway starts connected with an active container forward, refreshes
    without --2fa, and then successfully runs a container command against the
    replacement endpoint.
    """
    async with _live_refresh_gateway(
        tmp_path, tmp_path_factory, monkeypatch
    ) as (login, _containers, states, gw, runtime, ports):
        await asyncio.wait_for(
            gw.connect_target("hal", factor="SECRET"), 20
        )
        assert runtime.state == "connected"
        assert runtime.provisioned_endpoint == f"127.0.0.1:{ports[0]}"
        assert login.state["auths"] == 1
        assert login.state["connections"] == 1
        assert login.state["provision_count"] == 1
        assert login.state["provision_requests"] == 1
        old_conn = runtime.tunnel.connection

        _, conn = await asyncio.wait_for(
            gw._open_container_conn("hal"), 15
        )
        result = await asyncio.wait_for(
            conn.run("before", encoding=None), 10
        )
        assert result.stdout == b"container:before\n"
        assert states[0]["commands"] == ["before"]
        assert login.state["live_channels"] == 1

        refreshed = await asyncio.wait_for(
            gw.refresh_target("hal", factor=None), 30
        )
        assert refreshed["state"] == "connected"
        assert login.state["auths"] == 1
        assert login.state["connections"] == 1
        assert login.state["close_count"] == 1
        assert login.state["provision_count"] == 2
        assert login.state["max_channels"] == 1
        assert runtime.provisioned_endpoint == f"127.0.0.1:{ports[1]}"
        # The same outer login connection is retained, not a new login.
        assert runtime.tunnel.connection is old_conn
        assert states[0]["commands"] == ["before"]

        _, conn2 = await asyncio.wait_for(
            gw._open_container_conn("hal"), 15
        )
        result = await asyncio.wait_for(
            conn2.run("after", encoding=None), 10
        )
        assert result.stdout == b"container:after\n"
        assert states[1]["commands"] == ["after"]
        assert states[0]["commands"] == ["before"]
        assert login.state["auths"] == 1
        assert login.state["max_channels"] == 1


async def test_container_dial_racing_refresh_uses_the_new_forward(
    tmp_path, tmp_path_factory, monkeypatch
):
    """A dial that passed ensure_connected before refresh must not be used."""
    async with _live_refresh_gateway(
        tmp_path, tmp_path_factory, monkeypatch
    ) as (login, _containers, states, gw, runtime, ports):
        await asyncio.wait_for(
            gw.connect_target("hal", factor="SECRET"), 20
        )
        original = gw.backend.connection
        calls = []
        first_conns = []
        first_dialed = asyncio.Event()
        release = asyncio.Event()

        async def delayed(target, host, port, prompter=None):
            calls.append((host, port))
            if len(calls) == 1:
                conn = await original(
                    target, host, port, prompter=prompter
                )
                first_conns.append(conn)
                first_dialed.set()
                await release.wait()
                return conn
            return await original(target, host, port, prompter=prompter)

        monkeypatch.setattr(gw.backend, "connection", delayed)
        task = asyncio.create_task(gw._open_container_conn("hal"))
        await asyncio.wait_for(first_dialed.wait(), 10)
        # The old forward now has an accepted channel; refresh must tear it
        # down and still finish while the racing dial is held open.
        assert login.state["live_channels"] == 1
        refreshed = await asyncio.wait_for(
            gw.refresh_target("hal", factor=None), 30
        )
        assert refreshed["state"] == "connected"
        assert login.state["auths"] == 1
        assert first_conns and first_conns[0].is_closed()

        release.set()
        _, conn = await asyncio.wait_for(task, 20)
        assert len(calls) == 2
        result = await asyncio.wait_for(
            conn.run("race", encoding=None), 10
        )
        assert result.stdout == b"container:race\n"
        assert states[1]["commands"] == ["race"]
        assert states[0]["commands"] == []
        assert login.state["auths"] == 1
        assert login.state["max_channels"] == 1


async def test_refresh_route_loss_mid_refresh_reports_stage_without_retry(
    tmp_path, tmp_path_factory, monkeypatch
):
    """Losing the retained login connection must not start a second allocation."""
    from compute_mcp.tunnel import TunnelError

    async with _live_refresh_gateway(
        tmp_path, tmp_path_factory, monkeypatch
    ) as (login, _containers, _states, gw, runtime, _ports):
        await asyncio.wait_for(
            gw.connect_target("hal", factor="SECRET"), 20
        )
        login.state["drop_on_close"] = True
        with pytest.raises(
            TunnelError, match="refresh provision stage failed"
        ):
            await asyncio.wait_for(
                gw.refresh_target("hal", factor=None), 30
            )
        assert runtime.state == "failed"
        assert "provision" in (runtime.last_error or "")
        assert runtime.tunnel is None
        assert runtime.awaiting_factor is True
        # No OTP replay or second allocation: one login and one provision.
        assert login.state["auths"] == 1
        assert login.state["connections"] == 1
        assert login.state["provision_count"] == 1


async def test_refresh_drains_a_raw_forward_channel(
    tmp_path, tmp_path_factory, monkeypatch
):
    """A forwarded channel that is not a cached container SSH conn must close.

    This exercises the private asyncssh channel discovery and drain path: the
    raw TCP client has no cached container connection for backend.disconnect to
    close, so only the drain can release the MaxSessions slot before close.
    """
    async with _live_refresh_gateway(
        tmp_path, tmp_path_factory, monkeypatch
    ) as (login, _containers, states, gw, runtime, ports):
        await asyncio.wait_for(
            gw.connect_target("hal", factor="SECRET"), 20
        )
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", runtime.local_port
        )
        try:
            for _ in range(200):
                if login.state["live_channels"] == 1:
                    break
                await asyncio.sleep(0.01)
            assert login.state["live_channels"] == 1

            refreshed = await asyncio.wait_for(
                gw.refresh_target("hal", factor=None), 30
            )
            assert refreshed["state"] == "connected"
            assert login.state["auths"] == 1
            assert login.state["close_count"] == 1
            assert login.state["provision_count"] == 2
            assert login.state["max_channels"] == 1

            async def read_to_eof():
                try:
                    while True:
                        data = await reader.read(4096)
                        if not data:
                            return True
                except (ConnectionError, asyncio.IncompleteReadError):
                    return True

            assert await asyncio.wait_for(read_to_eof(), 10) is True
            _, conn = await asyncio.wait_for(
                gw._open_container_conn("hal"), 15
            )
            result = await asyncio.wait_for(
                conn.run("after-drain", encoding=None), 10
            )
            assert result.stdout == b"container:after-drain\n"
            assert states[1]["commands"] == ["after-drain"]
            assert login.state["max_channels"] == 1
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


async def test_dial_started_during_refresh_teardown_does_not_use_old_forward(
    tmp_path, tmp_path_factory, monkeypatch
):
    """A dial blocked before connect is released only after the old fence.

    It may fail on the closed listener or re-dial the replacement endpoint; it
    must never run a container command on the old endpoint.
    """
    from compute_mcp.ssh_backend import SSHError

    async with _live_refresh_gateway(
        tmp_path, tmp_path_factory, monkeypatch
    ) as (login, _containers, states, gw, runtime, ports):
        await asyncio.wait_for(
            gw.connect_target("hal", factor="SECRET"), 20
        )
        original = gw.backend.connection
        calls = []
        blocked = asyncio.Event()
        release = asyncio.Event()

        async def delayed(target, host, port, prompter=None):
            calls.append((host, port))
            if len(calls) == 1:
                blocked.set()
                await release.wait()
            return await original(target, host, port, prompter=prompter)

        monkeypatch.setattr(gw.backend, "connection", delayed)
        task = asyncio.create_task(gw._open_container_conn("hal"))
        await asyncio.wait_for(blocked.wait(), 10)

        refresh_task = asyncio.create_task(
            gw.refresh_target("hal", factor=None)
        )
        for _ in range(500):
            if "close" in login.state["commands"]:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("refresh did not reach the close stage")
        # The old listener has been fenced and drained; release the dial now.
        release.set()
        refreshed = await asyncio.wait_for(refresh_task, 30)
        assert refreshed["state"] == "connected"

        conn = None
        try:
            _, conn = await asyncio.wait_for(task, 20)
        except (SSHError, OSError):
            conn = None
        if conn is not None:
            result = await asyncio.wait_for(
                conn.run("race-during-teardown", encoding=None), 10
            )
            assert result.stdout == b"container:race-during-teardown\n"
            assert states[1]["commands"] == ["race-during-teardown"]
        assert states[0]["commands"] == []
        assert login.state["auths"] == 1
        assert login.state["max_channels"] == 1

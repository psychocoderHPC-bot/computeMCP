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
from compute_mcp.sessions import SessionManager
from compute_mcp.mux import Multiplexer
from compute_mcp.ssh_backend import SSHError, SSHBackend
from compute_mcp.tunnel import (
    TunnelError,
    TunnelManager,
    allocate_loopback_port,
    mux_helper_argv,
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

class _ChannelReader:
    """``read``-only adapter over an asyncssh server session channel."""

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._eof = False
        self._wake = asyncio.Event()

    def feed(self, data: bytes) -> None:
        self._buffer.extend(data)
        self._wake.set()

    def feed_eof(self) -> None:
        self._eof = True
        self._wake.set()

    async def read(self, n: int = -1) -> bytes:
        while True:
            if self._buffer:
                if n is None or n < 0:
                    data = bytes(self._buffer)
                    self._buffer.clear()
                else:
                    data = bytes(self._buffer[:n])
                    del self._buffer[:n]
                return data
            if self._eof:
                return b""
            self._wake.clear()
            if self._buffer or self._eof:
                continue
            await self._wake.wait()


class _ChannelWriter:
    """``write``/``drain`` adapter over an asyncssh server session channel."""

    def __init__(self, chan) -> None:
        self._chan = chan

    def write(self, data: bytes) -> None:
        self._chan.write(data)

    async def drain(self) -> None:
        await asyncio.sleep(0)

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._chan.close()

    async def wait_closed(self) -> None:
        with contextlib.suppress(Exception):
            await self._chan.wait_closed()


class _MuxHelperPeer:
    """In-process stand-in for ``computemcp-mux.py`` on the login node.

    It speaks the real gateway wire protocol with ``initiator=False`` and
    forwards each ``OPEN(job_id)`` to the loopback endpoint in the test
    registry.  Using the real :class:`Multiplexer` keeps the wire format and
    flow control under test; the stdlib helper itself is covered by
    ``test_mux_helper.py``.
    """

    def __init__(self, session: "_LoginSession") -> None:
        self.session = session
        self.state = session.state
        self.reader = session._mux_reader
        self.writer = _ChannelWriter(session._chan)
        self.mux = None
        self._tasks: set[asyncio.Task] = set()
        self._serving: set[asyncio.Task] = set()

    def _track(self, task: asyncio.Task) -> None:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def run(self) -> None:
        self.mux = Multiplexer(self.reader, self.writer, initiator=False)
        await self.mux.start()
        self.state["mux_starts"] = self.state.get("mux_starts", 0) + 1
        try:
            while True:
                stream = await self.mux.accept()
                self.state["mux_streams"] = (
                    self.state.get("mux_streams", 0) + 1
                )
                task = asyncio.create_task(self._serve(stream))
                self._serving.add(task)
                task.add_done_callback(self._serving.discard)
        except Exception:
            return
        finally:
            if self.mux is not None and not self.mux.closed:
                with contextlib.suppress(Exception):
                    await self.mux.close()
            for task in list(self._serving):
                task.cancel()
            if self._serving:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(
                        asyncio.gather(
                            *self._serving, return_exceptions=True
                        ),
                        2.0,
                    )

    async def _serve(self, stream) -> None:
        registry = self.state.get("relay_registry") or {}
        endpoint = registry.get(stream.job_id)
        if endpoint is None:
            await stream.reset(f"job {stream.job_id!r} not registered")
            return
        writer = None
        relay_reader = None
        try:
            relay_reader, writer = await asyncio.wait_for(
                asyncio.open_connection(*endpoint), 5.0
            )
        except Exception:
            await stream.reset("relay unavailable")
            return

        async def relay_to_stream():
            while True:
                data = await relay_reader.read(65536)
                if not data:
                    await stream.close()
                    return
                await stream.write(data)

        async def stream_to_relay():
            while True:
                try:
                    data = await stream.read(65536)
                except Exception:
                    return
                if not data:
                    return
                writer.write(data)
                await writer.drain()

        tasks = [
            asyncio.create_task(relay_to_stream()),
            asyncio.create_task(stream_to_relay()),
        ]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            with contextlib.suppress(Exception):
                writer.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), 2.0
                )

    async def close(self) -> None:
        if self.mux is not None and not self.mux.closed:
            with contextlib.suppress(Exception):
                await self.mux.close()


class _LoginSession(asyncssh.SSHServerSession):
    """Accepts an exec and answers provisioning / records the command."""

    def __init__(self, state: dict) -> None:
        self._chan = None
        self.state = state
        self._command = ""
        self._is_mux = False
        self._mux_task: asyncio.Task | None = None
        self._mux_reader: _ChannelReader | None = None
        self._mux_helper: _MuxHelperPeer | None = None

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
        if self._mux_reader is not None:
            self._mux_reader.feed_eof()
        if self._mux_task is not None and not self._mux_task.done():
            self._mux_task.cancel()
            self._mux_task = None

    def exec_requested(self, command) -> bool:
        self.state["commands"].append(command)
        self._command = command
        if "provision" in command:
            self.state["provision_requests"] = (
                self.state.get("provision_requests", 0) + 1
            )
        if "computemcp-mux" in command:
            self._is_mux = True
            self.state["mux_execs"] = self.state.get("mux_execs", 0) + 1
        return True

    def session_started(self) -> None:
        if self._is_mux:
            self._mux_reader = _ChannelReader()
            self._mux_helper = _MuxHelperPeer(self)
            self._mux_task = asyncio.create_task(self._mux_helper.run())
            return
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

    def data_received(self, data, datatype) -> None:
        if self._mux_reader is not None:
            self._mux_reader.feed(data)

    def eof_received(self):
        if self._mux_reader is not None:
            self._mux_reader.feed_eof()
        return False

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

    def auth_completed(self) -> None:
        # Count authenticated container SSH connections, not readiness probes
        # (which open a raw TCP socket and never complete SSH auth).
        self.state["connections"] = self.state.get("connections", 0) + 1

    def begin_auth(self, username: str) -> bool:
        return True

    def public_key_auth_supported(self) -> bool:
        return True

    async def validate_public_key(self, username, key) -> bool:
        return True

    def session_requested(self):
        return _ContainerSession(self.state)


class _BulkSession(asyncssh.SSHServerSession):
    """Container session that identifies its job and can emit a large blob."""

    def __init__(self, state: dict) -> None:
        self.state = state
        self._chan = None
        self._command = ""

    def connection_made(self, chan) -> None:
        self._chan = chan

    def exec_requested(self, command) -> bool:
        self._command = command
        self.state["commands"].append(command)
        return True

    def session_started(self) -> None:
        label = self.state.get("label", "job").encode()
        if self._command == "bulk":
            size = int(self.state.get("bulk_bytes", 300 * 1024))
            self._chan.write(label + b":" + b"Z" * size)
        else:
            self._chan.write(label + b":" + self._command.encode() + b"\n")
        self._chan.exit(0)

    def shell_requested(self) -> bool:
        return False


class _BulkContainerServer(asyncssh.SSHServer):
    """Key-only container endpoint whose sessions identify their job."""

    def __init__(self, state: dict) -> None:
        self.state = state

    def auth_completed(self) -> None:
        self.state["connections"] = self.state.get("connections", 0) + 1

    def begin_auth(self, username: str) -> bool:
        return True

    def public_key_auth_supported(self) -> bool:
        return True

    async def validate_public_key(self, username, key) -> bool:
        return True

    def session_requested(self):
        return _BulkSession(self.state)


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
        assert login.state["commands"] == ["provision", "close", "provision"]
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
        assert states[0].get("connections", 0) == 1
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
        # A second provision exec attempt on the retained (now dead)
        # connection would be visible here even if it never reached
        # session_started.
        assert login.state["provision_requests"] == 1


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
        assert states[0].get("connections", 0) == 0
        assert login.state["auths"] == 1
        assert login.state["max_channels"] == 1


# ---------------------------------------------------------------------------
# Single-channel mux transport: job-keyed cache, generation fencing, isolation
# ---------------------------------------------------------------------------

async def _start_bulk_containers(tmp_path, jobs):
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    host_key_path = tmp_path / "bulk_container_host_key"
    host_key.write_private_key(str(host_key_path))
    client_key = asyncssh.generate_private_key("ssh-ed25519")
    client_key_path = tmp_path / "bulk_client_key"
    client_key.write_private_key(str(client_key_path))
    states: dict[str, dict] = {}
    servers = []
    registry: dict[str, tuple[str, int]] = {}
    for job in jobs:
        state = {"commands": [], "label": job, "bulk_bytes": 300 * 1024}
        server = await asyncssh.listen(
            "127.0.0.1",
            0,
            server_host_keys=[str(host_key_path)],
            server_factory=lambda st=state: _BulkContainerServer(st),
            encoding=None,
        )
        servers.append(server)
        states[job] = state
        registry[job] = ("127.0.0.1", server.get_port())
    return (
        servers,
        states,
        registry,
        host_key.get_fingerprint("sha256"),
        str(client_key_path),
    )


@contextlib.asynccontextmanager
async def _mux_route(
    tmp_path,
    tmp_path_factory,
    monkeypatch,
    jobs,
    *,
    provision=False,
):
    """Start a login fixture, per-job container endpoints and a mux route."""
    login = await _start_login(tmp_path_factory, password="SECRET")
    servers, states, registry, container_pin, client_key = (
        await _start_bulk_containers(tmp_path, jobs)
    )
    route_tunnel = None
    backend = None
    login.state["relay_registry"] = registry
    login.state["dest_port"] = next(iter(registry.values()))[1]
    _route_alias(monkeypatch, login)
    try:
        target = TargetConfig(
            name="slurm",
            user="agent",
            container_user="ubuntu",
            transport=TransportConfig(
                kind="tunnel",
                ssh_targets=("login",),
                remote_host="127.0.0.1",
                remote_port=login.state["dest_port"],
            ),
            route_host_key_sha256=login.pin,
            host_key_sha256=container_pin,
            client_key=client_key,
            provision_command=("provision",) if provision else (),
            interactive_auth=True,
        )
        mgr = TunnelManager(SSHConfig())
        route_tunnel = await asyncio.wait_for(
            mgr.open_job_route(
                target,
                "login",
                helper_argv=("python3", "/remote/computemcp-mux.py"),
                factor="SECRET",
                provision=provision,
            ),
            CONNECT_TIMEOUT,
        )
        backend = SSHBackend()
        yield login, target, mgr, route_tunnel, backend, states, registry
    finally:
        if backend is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(backend.close_all(), 10)
        if route_tunnel is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(route_tunnel.stop(), 10)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(login.close(), 5)
        for server in servers:
            server.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(server.wait_closed(), 5)


async def _wait_for(predicate, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


async def test_mux_route_bootstraps_then_one_channel_two_jobs(
    tmp_path, tmp_path_factory, monkeypatch
):
    """Provision then helper: one live outer channel and one authentication.

    Two job streams transfer concurrently.  The login fixture counts live
    session channels and auth completions; both must stay at one.
    """
    async with _mux_route(
        tmp_path, tmp_path_factory, monkeypatch, ("A", "B"), provision=True
    ) as (login, target, _mgr, route, backend, states, registry):
        assert route.is_alive()
        assert login.state["provision_count"] == 1
        assert login.state["mux_execs"] == 1
        assert login.state["auths"] == 1
        assert login.state["connections"] == 1
        assert login.state["max_channels"] == 1

        host, port = registry["A"]
        conn_a = await asyncio.wait_for(
            backend.job_connection(
                target, "A", host, port, generation=1, transport=route.mux
            ),
            15,
        )
        host_b, port_b = registry["B"]
        conn_b = await asyncio.wait_for(
            backend.job_connection(
                target, "B", host_b, port_b, generation=1, transport=route.mux
            ),
            15,
        )
        assert conn_a is not conn_b
        assert login.state["mux_streams"] == 2

        result_a, result_b = await asyncio.wait_for(
            asyncio.gather(
                conn_a.run("bulk", encoding=None),
                conn_b.run("bulk", encoding=None),
            ),
            20,
        )
        assert result_a.stdout.startswith(b"A:")
        assert result_b.stdout.startswith(b"B:")
        assert len(result_a.stdout) > 200 * 1024
        assert len(result_b.stdout) > 200 * 1024
        assert states["A"]["commands"] == ["bulk"]
        assert states["B"]["commands"] == ["bulk"]
        assert login.state["max_channels"] == 1
        assert login.state["auths"] == 1
        assert backend.known_targets() == []
        assert sorted(backend.known_jobs()) == [("A", 1), ("B", 1)]


async def test_mux_job_cache_and_stop_b_keeps_a(
    tmp_path, tmp_path_factory, monkeypatch
):
    """Cache is per job/generation; stopping B leaves A's conn/SFTP/streams."""
    async with _mux_route(
        tmp_path, tmp_path_factory, monkeypatch, ("A", "B")
    ) as (_login, target, _mgr, route, backend, states, registry):
        host, port = registry["A"]
        conn_a = await asyncio.wait_for(
            backend.job_connection(
                target, "A", host, port, generation=1, transport=route.mux
            ),
            15,
        )
        again = await asyncio.wait_for(
            backend.job_connection(
                target, "A", host, port, generation=1, transport=route.mux
            ),
            15,
        )
        assert again is conn_a

        host_b, port_b = registry["B"]
        conn_b = await asyncio.wait_for(
            backend.job_connection(
                target, "B", host_b, port_b, generation=1, transport=route.mux
            ),
            15,
        )
        assert (await conn_a.run("a1", encoding=None)).stdout == b"A:a1\n"

        # SFTP state is keyed by the job key while the connection is cached.
        backend._sftp_sem(conn_a, 4)
        backend._sftp_sem(conn_b, 4)
        backend._sftp_open_lock(conn_a)
        backend._sftp_open_lock(conn_b)
        assert ("A", 1) in backend._sftp_locks
        assert ("B", 1) in backend._sftp_locks

        await asyncio.wait_for(backend.disconnect_job("B"), 10)
        assert conn_b.is_closed()
        assert conn_a.is_closed() is False
        assert ("B", 1) not in backend._sftp_locks
        assert ("B", 1) not in backend._sftp_sems
        assert ("A", 1) in backend._sftp_locks

        # A still uses the same cached connection and stream.
        assert (await conn_a.run("a2", encoding=None)).stdout == b"A:a2\n"
        assert backend.known_jobs() == [("A", 1)]
        assert states["B"]["commands"] == []
        assert route.is_alive()


async def test_mux_generation_fencing_and_cross_job_isolation(
    tmp_path, tmp_path_factory, monkeypatch
):
    """A newer generation closes the old one; other jobs are untouched."""
    async with _mux_route(
        tmp_path, tmp_path_factory, monkeypatch, ("A", "B")
    ) as (_login, target, _mgr, route, backend, _states, registry):
        host, port = registry["A"]
        conn_a1 = await asyncio.wait_for(
            backend.job_connection(
                target, "A", host, port, generation=1, transport=route.mux
            ),
            15,
        )
        host_b, port_b = registry["B"]
        conn_b = await asyncio.wait_for(
            backend.job_connection(
                target, "B", host_b, port_b, generation=1, transport=route.mux
            ),
            15,
        )
        backend._sftp_sem(conn_a1, 4)

        conn_a2 = await asyncio.wait_for(
            backend.job_connection(
                target, "A", host, port, generation=2, transport=route.mux
            ),
            15,
        )
        assert conn_a2 is not conn_a1
        assert conn_a1.is_closed()
        assert ("A", 1) not in backend._sftp_locks
        assert sorted(backend.known_jobs()) == [("A", 2), ("B", 1)]
        assert (await conn_b.run("b", encoding=None)).stdout == b"B:b\n"

        # A request naming the superseded generation is refused, and the new
        # connection stays cached (a stale redial cannot resurrect the old one).
        with pytest.raises(SSHError, match="stale"):
            await backend.job_connection(
                target, "A", host, port, generation=1, transport=route.mux
            )
        assert sorted(backend.known_jobs()) == [("A", 2), ("B", 1)]


async def test_mux_route_redial_replaces_one_channel(
    tmp_path, tmp_path_factory, monkeypatch
):
    """A replacement route closes the old helper and keeps one live channel."""
    async with _mux_route(
        tmp_path, tmp_path_factory, monkeypatch, ("A",)
    ) as (login, target, mgr, route1, backend, _states, registry):
        host, port = registry["A"]
        conn1 = await asyncio.wait_for(
            backend.job_connection(
                target, "A", host, port, generation=1, transport=route1.mux
            ),
            15,
        )
        await asyncio.wait_for(route1.stop(), 10)
        assert conn1.is_closed()

        route2 = await asyncio.wait_for(
            mgr.open_job_route(
                target,
                "login",
                helper_argv=("python3", "/remote/computemcp-mux.py"),
                generation=2,
                factor="SECRET",
                provision=False,
            ),
            CONNECT_TIMEOUT,
        )
        try:
            conn2 = await asyncio.wait_for(
                backend.job_connection(
                    target, "A", host, port, generation=2,
                    transport=route2.mux,
                ),
                15,
            )
            assert conn2 is not conn1
            assert (await conn2.run("redial", encoding=None)).stdout == b"A:redial\n"
            assert login.state["max_channels"] == 1
            # Each route authenticated exactly once: a healthy route never
            # re-authenticates for probe/exec/teardown.
            assert login.state["connections"] == 2
            assert login.state["auths"] == 2
            assert route2.is_alive()
        finally:
            await asyncio.wait_for(route2.stop(), 10)


async def test_mux_stream_reset_and_eof_do_not_leak_or_hang(
    tmp_path, tmp_path_factory, monkeypatch
):
    """A reset stream closes its inner connection and frees mux state."""
    async with _mux_route(
        tmp_path, tmp_path_factory, monkeypatch, ("A", "B")
    ) as (_login, target, _mgr, route, backend, _states, registry):
        host, port = registry["A"]
        conn_a = await asyncio.wait_for(
            backend.job_connection(
                target, "A", host, port, generation=1, transport=route.mux
            ),
            15,
        )
        host_b, port_b = registry["B"]
        conn_b = await asyncio.wait_for(
            backend.job_connection(
                target, "B", host_b, port_b, generation=1, transport=route.mux
            ),
            15,
        )
        entry = backend._job_connections[("A", 1)]
        await asyncio.wait_for(entry.stream.reset("test reset"), 5)
        assert await _wait_for(lambda: conn_a.is_closed())
        # B is unaffected by A's reset.
        assert (await conn_b.run("still", encoding=None)).stdout == b"B:still\n"
        await asyncio.wait_for(backend.disconnect_job("A"), 10)
        assert backend.known_jobs() == [("B", 1)]
        assert await _wait_for(
            lambda: route.mux.mux.stats.open_streams == 1
        )
        assert route.is_alive()


class _SessionStdout:
    async def read(self, size=-1):
        await asyncio.sleep(0)
        return b""


class _FakeProcess:
    def __init__(self):
        self.stdout = _SessionStdout()
        self.exit_status = 0
        self.stdin = None

    def terminate(self):
        return None

    async def wait_closed(self):
        return None

    def channel(self):
        return None


async def test_session_manager_job_scoped_close_keeps_other_job(
    monkeypatch,
):
    """close_for_job(B) must not close A's PTY session."""
    target = TargetConfig(
        name="slurm",
        user="agent",
        transport=TransportConfig(
            kind="direct", remote_host="127.0.0.1", remote_port=1
        ),
    )
    backend = SSHBackend()

    async def fake_create_session(self, conn, cwd, columns, rows, term="x"):
        return _FakeProcess()

    monkeypatch.setattr(SSHBackend, "create_session", fake_create_session)

    seen = []

    async def provider(name):
        return target, object()

    async def job_provider(name, job_id):
        seen.append((name, job_id))
        return target, object()

    manager = SessionManager(
        backend,
        provider,
        SessionConfig(),
        job_provider=job_provider,
    )
    sess_a = await manager.create(
        "c", target, None, 80, 24, job_id="A"
    )
    sess_b = await manager.create(
        "c", target, None, 80, 24, job_id="B"
    )
    assert sess_a.job_id == "A"
    assert sess_b.job_id == "B"
    assert seen == [("slurm", "A"), ("slurm", "B")]
    # Separate job connections were requested, so PTYs cannot share a
    # transport that stopping one job would tear down.
    assert sess_a.process is not sess_b.process

    closed = await manager.close_for_job("B")
    assert closed == 1
    assert sess_a.id in manager._sessions
    assert sess_b.id not in manager._sessions
    await manager.close_for_job("A")
    assert manager._sessions == {}

# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""AsyncSSH backend: container SSH connections, exec, PTY sessions and SFTP.

Host-key verification is mandatory.  A target must configure either an explicit
``host_key_sha256`` pin or a ``known_hosts`` file; connecting without one raises
:class:`HostKeyError`.  This prevents silently trusting whatever key appears on
a local forwarded port.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field

import asyncssh

from .config import TargetConfig

log = logging.getLogger("terok_compute.ssh")


class SSHError(RuntimeError):
    pass


class HostKeyError(SSHError):
    pass


@dataclass
class ManagedSession:
    id: str
    client_id: str
    target: str
    process: asyncssh.SSHClientProcess
    created_at: float
    last_activity: float
    output_buffer: bytearray = field(default_factory=bytearray)
    exit_status: int | None = None
    closed: bool = False
    dedicated_conn: asyncssh.SSHClientConnection | None = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class PinnedHostKeyClient(asyncssh.SSHClient):
    """SSH client that only accepts a host key matching a SHA256 fingerprint.

    AsyncSSH consults :meth:`validate_host_public_key` when a server key is not
    already present in ``known_hosts``.  We pair this client with an empty
    ``known_hosts`` provider so every key is checked against the pin instead of
    being accepted blindly.
    """

    def __init__(self, pin: str) -> None:
        self._pin = _b64_normalize(pin[len("SHA256:"):] if pin.startswith("SHA256:") else pin)

    def validate_host_public_key(self, host, addr, port, key) -> bool:
        actual = key.get_fingerprint("sha256")
        actual = actual[len("SHA256:"):] if actual.startswith("SHA256:") else actual
        return _b64_normalize(actual) == self._pin


def _empty_known_hosts(host: str, addr, port) -> tuple:
    # AsyncSSH expects seven sequences: trusted/revoked public keys,
    # trusted/revoked certificates, and trusted/revoked certificate subjects.
    return ((), (), (), (), (), (), ())


def _b64_normalize(value: str) -> str:
    return value.rstrip("=")


class SSHBackend:
    """Caches one live SSH connection per (client independent) target."""

    def __init__(self) -> None:
        self._connections: dict[str, asyncssh.SSHClientConnection] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock(self, target: str) -> asyncio.Lock:
        return self._locks.setdefault(target, asyncio.Lock())

    async def disconnect(self, target: str) -> None:
        conn = self._connections.pop(target, None)
        if conn is not None:
            conn.close()
            with contextlib.suppress(Exception):
                await conn.wait_closed()

    async def close_all(self) -> None:
        for name in list(self._connections):
            await self.disconnect(name)

    def known_targets(self) -> list[str]:
        return list(self._connections)

    async def connection(
        self,
        target: TargetConfig,
        host: str,
        port: int,
    ) -> asyncssh.SSHClientConnection:
        async with self._lock(target.name):
            existing = self._connections.get(target.name)
            if existing is not None and not existing.is_closed():
                return existing
            conn = await self._dial(target, host, port)
            self._connections[target.name] = conn
            return conn

    async def open_connection(
        self,
        target: TargetConfig,
        host: str,
        port: int,
    ) -> asyncssh.SSHClientConnection:
        """Open a fresh, uncached SSH connection owned by the caller.

        Used by ``connect_mode = "dedicated"`` so that each exec/session gets
        its own transport and is not bounded by the remote sshd's
        ``MaxSessions`` limit on a single connection.  The caller must close it.
        """
        return await self._dial(target, host, port)

    @staticmethod
    async def close_connection(conn: asyncssh.SSHClientConnection | None) -> None:
        if conn is None:
            return
        with contextlib.suppress(Exception):
            conn.close()
        with contextlib.suppress(Exception):
            await conn.wait_closed()

    async def _dial(
        self,
        target: TargetConfig,
        host: str,
        port: int,
    ) -> asyncssh.SSHClientConnection:
        if target.host_key_sha256:
            # Pin the key: give AsyncSSH an empty known_hosts provider and let a
            # custom client perform the fingerprint check.
            known_hosts = _empty_known_hosts
            client_factory = lambda: PinnedHostKeyClient(target.host_key_sha256)
        elif target.known_hosts:
            known_hosts = target.known_hosts
            client_factory = None
        else:
            raise HostKeyError(
                f"target {target.name!r} has no host_key_sha256 or known_hosts; "
                "refusing to connect without host-key verification"
            )

        client_keys = [target.client_key] if target.client_key else None
        server_host_key_algs = target.host_key_algorithms or ()
        try:
            return await asyncssh.connect(
                host,
                port=port,
                username=target.user,
                client_keys=client_keys,
                known_hosts=known_hosts,
                client_factory=client_factory,
                server_host_key_algs=server_host_key_algs,
                connect_timeout=10.0,
                keepalive_interval=30,
                keepalive_count_max=3,
            )
        except (asyncssh.Error, OSError) as exc:
            raise SSHError(f"SSH connection to target {target.name!r} failed: {exc}") from exc

    async def run(
        self,
        conn: asyncssh.SSHClientConnection,
        command: str,
        cwd: str | None = None,
        timeout: float | None = None,
    ):
        full = command
        if cwd:
            # The command string is intentionally interpreted by the shell
            # *inside the remote container*; it is never interpolated into a
            # local shell.
            full = f"cd {_shquote(cwd)} && {command}"
        try:
            return await asyncio.wait_for(
                conn.run(full, check=False, encoding=None), timeout=timeout
            )
        except asyncio.TimeoutError as exc:
            raise SSHError("remote command timed out") from exc

    async def create_session(
        self,
        conn: asyncssh.SSHClientConnection,
        cwd: str | None,
        columns: int,
        rows: int,
        term: str = "xterm-256color",
    ) -> asyncssh.SSHClientProcess:
        command = None
        if cwd:
            command = f"cd {_shquote(cwd)} && exec ${{SHELL:-/bin/bash}} -l"
        try:
            return await conn.create_process(
                command,
                term_type=term,
                term_size=(columns, rows),
                encoding=None,
                request_pty=True,
            )
        except asyncssh.Error as exc:
            raise SSHError(f"failed to create PTY session: {exc}") from exc

    async def sftp(self, conn: asyncssh.SSHClientConnection) -> asyncssh.SFTPClient:
        try:
            return await conn.start_sftp_client()
        except asyncssh.Error as exc:
            raise SSHError(f"failed to start SFTP subsystem: {exc}") from exc


def _shquote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"

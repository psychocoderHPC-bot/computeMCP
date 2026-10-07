# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""AsyncSSH backend: container SSH connections, exec, PTY sessions and SFTP.

Host-key verification is controlled per target by ``host_key_check``.  The
default (``"on"``) requires an explicit ``host_key_sha256`` pin or a
``known_hosts`` file and refuses to connect without one, which prevents
silently trusting whatever key appears on a local forwarded port.  A target may
opt out with ``host_key_check = "off"``; this accepts any host key and is only
safe when the forwarded endpoint itself is trusted (e.g. a single-user dev box).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from dataclasses import dataclass, field
from typing import Awaitable, Protocol

import asyncssh

from .config import TargetConfig

log = logging.getLogger("compute_mcp.ssh")


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
    updated_at: float = 0.0
    exit_status: int | None = None
    closed: bool = False
    dedicated_conn: asyncssh.SSHClientConnection | None = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class _Prompter(Protocol):
    """Interactive authentication callback used by the gateway console."""

    def __call__(self, prompt: str, echo: bool) -> Awaitable[str | None]:
        ...


class InteractiveSSHClient(asyncssh.SSHClient):
    """SSH client with host-key pinning and interactive authentication.

    AsyncSSH consults :meth:`validate_host_public_key` for the host key and the
    ``password``/``kbdint`` hooks when the server requests them.  The hooks
    delegate to an injected ``prompter`` so a second factor (password, OTP, or
    an interactive challenge) can be collected by the operator console.  Without
    a prompter, or when the target does not opt in, no secret is supplied and
    key/agent authentication is used as usual.
    """

    def __init__(
        self,
        pin: str | None = None,
        prompter: _Prompter | None = None,
        accept_any: bool = False,
    ) -> None:
        self._pin = (
            _b64_normalize(pin[len("SHA256:"):] if pin.startswith("SHA256:") else pin)
            if pin
            else None
        )
        # accept_any disables identity checking entirely (host_key_check="off").
        # It only has an effect when asyncssh actually consults this hook, which
        # it does while a trusted-host-key set is present; _dial passes an empty
        # set for that case.
        self._accept_any = accept_any
        self._prompter = prompter

    def validate_host_public_key(self, host, addr, port, key) -> bool:
        if self._accept_any:
            return True
        if self._pin is None:
            return False
        actual = key.get_fingerprint("sha256")
        actual = actual[len("SHA256:"):] if actual.startswith("SHA256:") else actual
        return _b64_normalize(actual) == self._pin

    async def password_auth_requested(self) -> str | None:
        if self._prompter is None:
            return None
        return await self._prompter("Password: ", False)

    async def kbdint_auth_requested(self) -> str:
        # Advertise keyboard-interactive so the server can issue a challenge
        # (commonly used for OTP / second factor).
        return ""

    async def kbdint_challenge_received(self, name, instructions, lang, prompts):
        if self._prompter is None:
            return None
        responses = []
        for prompt, echo in prompts:
            answer = await self._prompter(prompt.rstrip(), bool(echo))
            if answer is None:
                return None
            responses.append(answer)
        return responses


# Backwards-compatible name used by earlier code paths / tests.
PinnedHostKeyClient = InteractiveSSHClient

# Maximum number of interactive factor prompts answered before giving up.
# Mirrors ``Gateway.MAX_AUTH_PROMPTS`` so the route dial and the container dial
# bound operator prompts identically.
MAX_FACTOR_PROMPTS = 3


def make_factor_prompter(secret: str) -> _Prompter:
    """Return a prompter that answers every auth prompt with ``secret``.

    Used for a per-request second factor (password / OTP) on the gateway ->
    login-node (route) connection.  The same value is returned for password and
    keyboard-interactive prompts; attempts are capped at
    :data:`MAX_FACTOR_PROMPTS`, after which ``None`` is returned so the SSH
    library stops instead of retrying forever.  The secret is never logged.
    """
    attempts = 0

    async def prompter(prompt: str, echo: bool) -> str | None:
        nonlocal attempts
        attempts += 1
        if attempts > MAX_FACTOR_PROMPTS:
            log.warning(
                "route authentication: giving up after %d interactive "
                "attempt(s)",
                MAX_FACTOR_PROMPTS,
            )
            return None
        return secret

    return prompter


def _empty_known_hosts(host: str, addr, port) -> tuple:
    # AsyncSSH expects seven sequences: trusted/revoked public keys,
    # trusted/revoked certificates, and trusted/revoked certificate subjects.
    return ((), (), (), (), (), (), ())


def _b64_normalize(value: str) -> str:
    return value.rstrip("=")


async def dial_route(
    *,
    name: str,
    host: str,
    port: int,
    username: str | tuple,
    client_keys: list[str] | tuple[str, ...] | None,
    passphrase: str | None,
    prompter: _Prompter | None,
    host_key_sha256: str | None,
    known_hosts,
    host_key_algorithms: tuple[str, ...],
    host_key_check: str = "on",
    connect_timeout: float = 10.0,
    keepalive_interval: int = 30,
    keepalive_count_max: int = 3,
) -> asyncssh.SSHClientConnection:
    """Dial the gateway -> login/route host (the hop before the container).

    This is the route-first primitive node T2 (``tunnel.py``) calls before any
    container endpoint exists.  Unlike :meth:`SSHBackend._dial` it takes raw
    connection parameters instead of a :class:`TargetConfig`, and pins the
    *route* host key (``host_key_sha256`` here) independently of the container
    pin.

    ``prompter`` supplies a per-request second factor.  Authentication /
    2FA failures are wrapped in :class:`SSHError` with a message that says so
    when a prompter or passphrase was supplied.
    """
    accept_any = host_key_check == "off"
    pin = host_key_sha256

    client_factory = lambda: InteractiveSSHClient(
        pin, prompter, accept_any=accept_any
    )

    if known_hosts is not None:
        selected_known_hosts = known_hosts
    elif pin or accept_any:
        # A pin or disabled verification must not consult the user's
        # ~/.ssh/known_hosts; pass an empty trusted-key set so asyncssh still
        # calls the client hook and host_key_algorithms can restrict negotiation.
        selected_known_hosts = _empty_known_hosts
    else:
        # No explicit source and no pin: fall back to asyncssh's default
        # (~/.ssh/known_hosts); the client hook then validates against it.
        selected_known_hosts = None

    try:
        return await asyncssh.connect(
            host,
            port=port,
            username=username,
            client_keys=list(client_keys) if client_keys else None,
            passphrase=passphrase,
            known_hosts=selected_known_hosts,
            client_factory=client_factory,
            server_host_key_algs=host_key_algorithms or (),
            connect_timeout=connect_timeout,
            keepalive_interval=keepalive_interval,
            keepalive_count_max=keepalive_count_max,
        )
    except (asyncssh.Error, OSError) as exc:
        if prompter is not None or passphrase is not None:
            raise SSHError(
                f"SSH route connection to {name!r} failed during authentication "
                f"(interactive/2FA may be required): {exc}"
            ) from exc
        raise SSHError(f"SSH route connection to {name!r} failed: {exc}") from exc


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
        prompter: _Prompter | None = None,
    ) -> asyncssh.SSHClientConnection:
        async with self._lock(target.name):
            existing = self._connections.get(target.name)
            if existing is not None and not existing.is_closed():
                return existing
            conn = await self._dial(target, host, port, prompter)
            self._connections[target.name] = conn
            return conn

    async def open_connection(
        self,
        target: TargetConfig,
        host: str,
        port: int,
        prompter: _Prompter | None = None,
    ) -> asyncssh.SSHClientConnection:
        """Open a fresh, uncached SSH connection owned by the caller.

        Used by ``connect_mode = "dedicated"`` so that each exec/session gets
        its own transport and is not bounded by the remote sshd's
        ``MaxSessions`` limit on a single connection.  The caller must close it.
        """
        return await self._dial(target, host, port, prompter)

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
        prompter: _Prompter | None = None,
        passphrase: str | None = None,
    ) -> asyncssh.SSHClientConnection:
        # Only pass the interactive prompter when the target opts in; otherwise
        # authentication stays key/agent-only and no secret can be injected.
        active_prompter = prompter if target.interactive_auth else None
        pin = target.host_key_sha256 if target.host_key_sha256 else None
        accept_any = target.host_key_check == "off"
        if pin:
            known_hosts = _empty_known_hosts
        elif target.known_hosts:
            known_hosts = target.known_hosts
        elif accept_any:
            # Verification intentionally disabled.  Pass an empty trusted-key
            # set (not None) so asyncssh still consults the client hook, which
            # accepts any key, and so host_key_algorithms can restrict
            # negotiation if set.
            known_hosts = _empty_known_hosts
            log.warning(
                "target %r has host_key_check='off': accepting any host key "
                "(not recommended for shared hosts)",
                target.name,
            )
        else:
            raise HostKeyError(
                f"target {target.name!r} has no host_key_sha256 or known_hosts; "
                "refusing to connect without host-key verification "
                "(set host_key_check='off' to disable explicitly)"
            )
        client_factory = lambda: InteractiveSSHClient(
            pin, active_prompter, accept_any=accept_any
        )

        client_keys = [target.client_key] if target.client_key else None
        server_host_key_algs = target.host_key_algorithms or ()
        try:
            return await asyncssh.connect(
                host,
                port=port,
                username=target.user or (),
                client_keys=client_keys,
                passphrase=passphrase,
                known_hosts=known_hosts,
                client_factory=client_factory,
                server_host_key_algs=server_host_key_algs,
                connect_timeout=10.0,
                keepalive_interval=30,
                keepalive_count_max=3,
            )
        except (asyncssh.Error, OSError) as exc:
            if active_prompter is not None:
                raise SSHError(
                    f"SSH connection to target {target.name!r} failed during "
                    f"authentication (interactive/2FA may be required): {exc}"
                ) from exc
            raise SSHError(f"SSH connection to target {target.name!r} failed: {exc}") from exc

    async def run(
        self,
        conn: asyncssh.SSHClientConnection,
        command: str,
        cwd: str | None = None,
        timeout: float | None = None,
        env: dict[str, str] | None = None,
        stdin: bytes | str | None = None,
    ):
        full = command
        if env:
            # Export in the *remote* shell rather than relying on the remote
            # sshd's AcceptEnv (commonly disabled), so env works everywhere.
            # Values are shell-quoted; the resulting string is interpreted by
            # the shell inside the container and never by a local shell.
            exports = " ".join(
                f"export {_sh_identifier(k)}={_shquote(str(v))};"
                for k, v in env.items()
            )
            full = f"{exports} {full}"
        if cwd:
            # The command string is intentionally interpreted by the shell
            # *inside the remote container*; it is never interpolated into a
            # local shell.
            full = f"cd {_shquote(cwd)} && {full}"
        if isinstance(stdin, str):
            stdin = stdin.encode()
        try:
            return await asyncio.wait_for(
                conn.run(full, check=False, encoding=None, input=stdin),
                timeout=timeout,
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


_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _sh_identifier(name: str) -> str:
    """Validate an environment variable name so it cannot inject shell syntax."""
    if not _ENV_NAME_RE.match(name):
        raise SSHError(f"invalid environment variable name {name!r}")
    return name

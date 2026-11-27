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
import shlex
import socket
from dataclasses import dataclass, field
from typing import Awaitable, Protocol

import asyncssh

from . import mux as mux_module
from .config import TargetConfig, container_login_user

log = logging.getLogger("compute_mcp.ssh")


class SSHError(RuntimeError):
    pass


# Bounded retry and channel budget for the SFTP subsystem.  The remote sshd
# caps session channels per connection (``MaxSessions``); a burst of file
# operations on one shared connection can exhaust that budget, so opens are
# serialized, retried a few times, and capped in number.
SFTP_OPEN_MAX_ATTEMPTS = 3
SFTP_OPEN_BACKOFF_BASE = 0.25
MAX_SFTP_SESSIONS = 8

# SSH_OPEN_ADMINISTRATIVELY_PROHIBITED (code 1): the server refuses the
# subsystem outright, so retrying and redialing cannot help.
_SFTP_OPEN_PROHIBITED = asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED


class HostKeyError(SSHError):
    pass


def is_channel_open_error(exc: BaseException) -> bool:
    """Return True when *exc* is asyncssh's channel-open failure.

    ``asyncssh.ChannelOpenError`` is exported at the top level in asyncssh
    2.24.1 (and also as ``asyncssh.misc.ChannelOpenError``).  Resolve it at call
    time so the check stays correct if a future version relocates the name,
    without hard-coding a version-specific import path.
    """
    channel_open_error = getattr(asyncssh, "ChannelOpenError", None)
    return channel_open_error is not None and isinstance(exc, channel_open_error)


# A route connection can transiently refuse the next session channel while the
# previous one is still being torn down (OpenSSH MaxSessions).  A refused open
# means the command was never sent, so a bounded retry is safe.
ROUTE_SESSION_ATTEMPTS = 3
ROUTE_SESSION_BACKOFF_BASE = 0.25


async def with_channel_open_retry(
    operation,
    *,
    attempts: int = ROUTE_SESSION_ATTEMPTS,
    base_delay: float = ROUTE_SESSION_BACKOFF_BASE,
):
    """Await ``operation()``, retrying only a refused session-channel open.

    ``operation`` must open a fresh channel and run its command.  A
    ``ChannelOpenError`` is raised before the command reaches the remote side,
    so retrying cannot duplicate side effects, including for provisioning.
    Other errors propagate unchanged.
    """
    for attempt in range(1, attempts + 1):
        try:
            return await operation()
        except asyncssh.Error as exc:
            if not is_channel_open_error(exc):
                raise
            if attempt >= attempts:
                raise
            log.warning(
                "route session channel refused (%s); retry %d/%d",
                exc,
                attempt,
                attempts - 1,
            )
            await asyncio.sleep(base_delay * 2 ** (attempt - 1))


def _channel_open_message(prefix: str, exc: BaseException) -> str:
    return (
        f"{prefix} (remote sshd MaxSessions limit may be reached); consider "
        "connect_mode='dedicated' or retrying: "
        f"{exc}"
    )


# -- single-channel mux transport -------------------------------------------
#
# On a Slurm target every per-job transport is an opaque byte stream carried by
# the login-node multiplexer over ONE outer SSH session channel.  The channel
# itself is a single asyncssh ``create_process`` running the helper; all
# per-job container SSH connections are rebuilt on top of mux streams, so the
# outer channel count stays at one for bootstrap (transient), probe and the
# helper.  These limits bound both directions; the mux enforces per-stream
# credit and the outer channel enforces its own SSH window.
MUX_RECV_WINDOW = 256 * 1024
MUX_SEND_BUFFER = 256 * 1024
MUX_CREDIT_THRESHOLD = 64 * 1024
MUX_MAX_DATA_FRAME = 64 * 1024
MUX_CHANNEL_WINDOW = 256 * 1024
MUX_MAX_PKTSIZE = 32 * 1024
MUX_HANDSHAKE_TIMEOUT = 15.0
MUX_STDERR_KEEP = 16 * 1024
# How long to wait for a bootstrap/probe session channel to disappear before
# the persistent helper channel is opened.  A login node may cap MaxSessions at
# one, so an overlapping bootstrap channel would make the helper unopenable.
MUX_BOOTSTRAP_DRAIN_TIMEOUT = 5.0


class MuxTransportError(SSHError):
    """The route mux helper could not be started or is no longer usable."""


def validate_helper_argv(helper_argv) -> list[str]:
    """Validate a helper argv for ``create_process``.

    A helper argv must be a non-empty sequence of strings.  The check is kept
    here so a malformed command fails before an SSH channel is opened.
    """
    if isinstance(helper_argv, str):
        argv = [helper_argv]
    else:
        argv = list(helper_argv or ())
    if not argv or not all(isinstance(part, str) and part for part in argv):
        raise MuxTransportError("mux helper argv must be non-empty strings")
    return argv


async def _wait_for_no_session_channels(
    connection: asyncssh.SSHClientConnection,
    timeout: float = MUX_BOOTSTRAP_DRAIN_TIMEOUT,
) -> None:
    """Wait until no session channel remains on *connection*.

    Provisioning and bundle deployment open short-lived channels on the route
    connection.  The persistent helper must be the only live channel when it
    starts, otherwise a ``MaxSessions=1`` login node refuses the helper.  The
    channel table is private asyncssh state; a version that moves it degrades to
    a no-op rather than breaking the route.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, timeout)
    while True:
        channels = getattr(connection, "_channels", None)
        if not isinstance(channels, dict):
            return
        if not channels:
            return
        if loop.time() >= deadline:
            log.warning(
                "route still has %d open SSH channel(s) after %.1fs; "
                "opening the mux helper anyway",
                len(channels),
                timeout,
            )
            return
        await asyncio.sleep(0.01)


@dataclass
class MuxLimits:
    """Bounded sizes for one mux transport (gateway side)."""

    recv_window: int = MUX_RECV_WINDOW
    send_buffer: int = MUX_SEND_BUFFER
    credit_threshold: int = MUX_CREDIT_THRESHOLD
    max_data_frame: int = MUX_MAX_DATA_FRAME
    channel_window: int = MUX_CHANNEL_WINDOW
    max_pktsize: int = MUX_MAX_PKTSIZE
    handshake_timeout: float = MUX_HANDSHAKE_TIMEOUT


class RouteMuxTransport:
    """One persistent helper process over a single outer SSH channel.

    ``start`` opens exactly one ``create_process`` channel with binary pipes and
    no PTY, then runs the gateway-side :class:`compute_mcp.mux.Multiplexer` over
    its stdin/stdout.  stderr is drained separately into a bounded buffer and
    never parsed as framed data.  ``open_stream`` opens an inner per-job stream;
    the caller adapts it to a container SSH connection.
    """

    def __init__(
        self,
        connection: asyncssh.SSHClientConnection,
        helper_argv,
        *,
        limits: MuxLimits | None = None,
        stderr_keep: int = MUX_STDERR_KEEP,
    ) -> None:
        self.connection = connection
        self.helper_argv = validate_helper_argv(helper_argv)
        self.limits = limits or MuxLimits()
        self._stderr_keep = stderr_keep
        self.process: asyncssh.SSHClientProcess | None = None
        self.mux: mux_module.Multiplexer | None = None
        self._started = False
        self._closed = False
        self._start_lock = asyncio.Lock()
        self._stderr_task: asyncio.Task | None = None
        self.stderr_tail = bytearray()

    @property
    def started(self) -> bool:
        return self._started

    def is_alive(self) -> bool:
        mux = self.mux
        return (
            self._started
            and not self._closed
            and mux is not None
            and not mux.closed
            and self.connection is not None
            and not self.connection.is_closed()
        )

    async def start(self) -> int:
        """Open the single helper channel and complete the mux handshake."""
        async with self._start_lock:
            if self._started:
                if self.mux is None:
                    return 1
                return self.mux.negotiated_version or 1
            if self._closed:
                raise MuxTransportError("mux transport is closed")
            if self.connection.is_closed():
                raise MuxTransportError("route connection is closed")
            await _wait_for_no_session_channels(self.connection)
            try:
                process = await self.connection.create_process(
                    shlex.join(self.helper_argv),
                    encoding=None,
                    request_pty=False,
                    stdin=asyncssh.PIPE,
                    stdout=asyncssh.PIPE,
                    stderr=asyncssh.PIPE,
                    window=self.limits.channel_window,
                    max_pktsize=self.limits.max_pktsize,
                )
            except (asyncssh.Error, OSError) as exc:
                raise MuxTransportError(
                    f"could not start the route mux helper: {exc}"
                ) from exc
            self.process = process
            self._stderr_task = asyncio.create_task(self._drain_stderr(process))
            mux = mux_module.Multiplexer(
                process.stdout,
                process.stdin,
                recv_window=self.limits.recv_window,
                send_buffer_limit=self.limits.send_buffer,
                credit_threshold=self.limits.credit_threshold,
                max_data_frame=self.limits.max_data_frame,
                handshake_timeout=self.limits.handshake_timeout,
                initiator=True,
            )
            try:
                version = await mux.start()
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    process.close()
                raise
            except Exception as exc:
                with contextlib.suppress(Exception):
                    process.close()
                raise MuxTransportError(
                    "route mux helper did not complete its version handshake"
                ) from exc
            self.mux = mux
            self._started = True
            return version

    async def open_stream(self, job_id: str, *, timeout: float | None = None):
        """Open one inner stream addressed to *job_id*."""
        if not self._started or self.mux is None:
            raise MuxTransportError("mux transport not started")
        if self.mux.closed:
            raise MuxTransportError("mux transport closed")
        return await self.mux.open_stream(job_id, timeout=timeout)

    async def ping(self, timeout: float = MUX_HANDSHAKE_TIMEOUT) -> None:
        """Probe the helper control stream without opening a new SSH channel."""
        if not self._started or self.mux is None:
            raise MuxTransportError("mux transport not started")
        await self.mux.ping(timeout)

    async def _drain_stderr(self, process: asyncssh.SSHClientProcess) -> None:
        """Drain stderr into a bounded tail; never treat it as framed data."""
        try:
            while True:
                data = await process.stderr.read(4096)
                if not data:
                    return
                self.stderr_tail.extend(data)
                overflow = len(self.stderr_tail) - self._stderr_keep
                if overflow > 0:
                    del self.stderr_tail[:overflow]
        except (asyncssh.Error, OSError, asyncio.CancelledError):
            return

    async def close(self) -> None:
        """Close the mux, the helper channel and its pump, in that order."""
        if self._closed:
            return
        self._closed = True
        mux = self.mux
        if mux is not None:
            with contextlib.suppress(Exception):
                await mux.close()
        process = self.process
        if process is not None:
            with contextlib.suppress(Exception):
                process.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(process.wait_closed(), 5.0)
        task = self._stderr_task
        self._stderr_task = None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self.mux = None
        self.process = None

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"RouteMuxTransport(started={self._started}, closed={self._closed}, "
            f"job_streams={self.mux.stats.open_streams if self.mux else 0})"
        )


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
    job_id: str | None = None
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

# A per-request second factor is single use.  Submitting a one-time code more
# than once counts as a failed login at the site and can lock the account.
FACTOR_MAX_USES = 1


class FactorPrompter:
    """One-shot prompter for a per-request second factor.

    The secret is cleared as soon as it is submitted.  A second prompt, whether
    from a retry, a second DNS name in the route list, or another hop, returns
    ``None`` instead of replaying a single-use code.
    """

    def __init__(self, secret: str) -> None:
        self._secret: str | None = secret
        self._uses = 0

    @property
    def used(self) -> bool:
        return self._uses > 0

    @property
    def exhausted(self) -> bool:
        return self._uses >= FACTOR_MAX_USES

    def clear(self) -> None:
        """Drop the secret when the connect request is over.

        A key-only route can succeed without ever calling the prompter.  Clear
        it before returning the tunnel so the connection client cannot hold a
        usable factor for a later authentication request.
        """
        self._secret = None

    async def __call__(self, prompt: str, echo: bool) -> str | None:
        self._uses += 1
        if self._uses > FACTOR_MAX_USES or self._secret is None:
            log.warning(
                "route authentication: refusing to replay a single-use second "
                "factor; aborting this attempt"
            )
            return None
        secret, self._secret = self._secret, None
        return secret


def make_factor_prompter(secret: str) -> FactorPrompter:
    """Return a prompter that submits ``secret`` at most once.

    Used for a per-request second factor (password / OTP) on the gateway ->
    login-node (route) connection.  One-time codes are single use, so a second
    prompt is not answered: replaying the same code would only add a failed
    authentication attempt at the site.  The prompter therefore returns ``None``
    after the first use, which stops that auth method instead of retrying with
    the same secret.  The secret is cleared after the first submission and is
    never logged.
    """
    return FactorPrompter(secret)


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
            # Prefer the natural 2FA method.  When a server offers both
            # ``password`` and ``keyboard-interactive``, spending a single-use
            # factor on the wrong method would leave the real prompt unanswered.
            preferred_auth=("publickey", "keyboard-interactive", "password"),
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


@dataclass
class _JobConnection:
    """One cached inner container connection for (job_id, generation).

    ``stream``/``socket``/``pumps`` are only set for the mux path.  On that
    path the inner ``conn`` is a real asyncssh client connection whose bytes
    travel over the stream, so exec/SFTP/PTY continue to use asyncssh directly
    while the outer route keeps exactly one channel.
    """

    job_id: str
    generation: int
    endpoint_key: str
    conn: asyncssh.SSHClientConnection
    stream: object | None = None
    pump_writer: object | None = None
    pump_tasks: tuple[asyncio.Task, ...] = ()
    transport: object | None = None

    @property
    def key(self) -> tuple[str, int]:
        return (self.job_id, self.generation)


class SSHBackend:
    """Caches one live container SSH connection per target, keyed by endpoint.

    A cached connection is only valid for the exact container endpoint
    (host:port) it was dialed to.  The container's published port can change
    after a container restart, and a refresh/recovery re-provisions to a new
    endpoint; a cached connection dialed to a stale port must never be reused.
    ``connection`` therefore records the endpoint each cached connection was
    dialed to and redials when the requested endpoint differs.
    """

    def __init__(self) -> None:
        # Legacy/direct Docker path: maps target name -> (connection, endpoint).
        # The target name is the correct key only when one target has exactly
        # one container; a Slurm target multiplexes several jobs and must use
        # the job-keyed cache below instead.
        self._connections: dict[str, tuple[asyncssh.SSHClientConnection, str]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        # Slurm/mux path: maps (job_id, endpoint_generation) -> JobConnection.
        # The generation fences a stale endpoint: a newer generation evicts and
        # closes older entries for the same job, and a stopped job only ever
        # touches its own entries.
        self._job_connections: dict[tuple[str, int], "_JobConnection"] = {}
        self._job_locks: dict[tuple[str, int], asyncio.Lock] = {}
        # Highest generation ever requested per job.  A late request for an
        # older generation is stale and is refused instead of redialing it.
        self._job_generations: dict[str, int] = {}
        # Reverse lookup so a job connection can be identified from its object.
        self._conn_job_keys: dict[int, tuple[str, int]] = {}
        # SFTP open serialization and channel budget.  Legacy/dedicated
        # connections are keyed by id(conn); job connections by their
        # (job_id, generation) key so cleanup follows the job cache.
        self._sftp_locks: dict[object, asyncio.Lock] = {}
        self._sftp_sems: dict[object, asyncio.Semaphore] = {}

    @staticmethod
    def _endpoint_key(host: str, port: int) -> str:
        return f"{host}:{port}"

    def _lock(self, target: str) -> asyncio.Lock:
        return self._locks.setdefault(target, asyncio.Lock())

    def _sftp_state_key(self, conn: asyncssh.SSHClientConnection) -> object:
        """Key SFTP locks/semaphores by job when the connection is job-owned."""
        job_key = self._conn_job_keys.get(id(conn))
        return job_key if job_key is not None else id(conn)

    def _release_sftp_state(self, conn: asyncssh.SSHClientConnection) -> None:
        key = self._sftp_state_key(conn)
        self._sftp_locks.pop(key, None)
        self._sftp_sems.pop(key, None)

    def _job_lock(self, key: tuple[str, int]) -> asyncio.Lock:
        return self._job_locks.setdefault(key, asyncio.Lock())

    async def disconnect(self, target: str) -> None:
        entry = self._connections.pop(target, None)
        if entry is not None:
            conn = entry[0]
            self._release_sftp_state(conn)
            conn.close()
            with contextlib.suppress(Exception):
                await conn.wait_closed()

    async def discard_connection(
        self, name_or_conn: str | asyncssh.SSHClientConnection
    ) -> None:
        """Drop a cached connection so the next request redials.

        Accepts either a target name or a live connection object.  Used when a
        cached SHARED connection can no longer open session channels (the
        remote sshd's per-connection ``MaxSessions`` cap was reached): keeping
        it cached would make every later request fail the same way.

        A job-owned connection is evicted only for its own job/generation;
        other jobs and the legacy target cache are untouched.
        """
        if isinstance(name_or_conn, str):
            name = name_or_conn
        else:
            job_key = self._conn_job_keys.get(id(name_or_conn))
            if job_key is not None:
                await self._drop_job_key(job_key, reason="discarded")
                return
            name = self._target_for_conn(name_or_conn)
            if name is None:
                # Not a cached connection (e.g. a dedicated one owned by the
                # caller): nothing to invalidate here.
                return
        await self.disconnect(name)

    def _target_for_conn(
        self, conn: asyncssh.SSHClientConnection
    ) -> str | None:
        """Return the cached target name for *conn*, or None if not cached."""
        for cached_name, (cached_conn, _key) in self._connections.items():
            if cached_conn is conn:
                return cached_name
        return None

    def _job_key_for_conn(
        self, conn: asyncssh.SSHClientConnection
    ) -> tuple[str, int] | None:
        return self._conn_job_keys.get(id(conn))

    async def close_all(self) -> None:
        for name in list(self._connections):
            await self.disconnect(name)
        for key in list(self._job_connections):
            await self._drop_job_key(key, reason="shutdown")

    def known_targets(self) -> list[str]:
        return list(self._connections)

    def known_jobs(self) -> list[tuple[str, int]]:
        return list(self._job_connections)

    async def connection(
        self,
        target: TargetConfig,
        host: str,
        port: int,
        prompter: _Prompter | None = None,
    ) -> asyncssh.SSHClientConnection:
        async with self._lock(target.name):
            existing = self._connections.get(target.name)
            key = self._endpoint_key(host, port)
            if (
                existing is not None
                and existing[1] == key  # same endpoint a refresh did not change
                and not existing[0].is_closed()
            ):
                return existing[0]

            # No cached connection, it is dead, or it was dialed to a different
            # (stale) endpoint.  Keep the old cached connection in place until
            # the replacement is established: a failed redial must not destroy
            # a still-usable connection and turn an exec into a 502.
            old = existing
            if old is not None and not old[0].is_closed():
                log.info(
                    "target %s: redialing container connection from %s to %s; "
                    "keeping the old connection until the new one is established",
                    target.name,
                    old[1],
                    key,
                )
            try:
                conn = await self._dial(target, host, port, prompter)
            except Exception:
                if old is not None:
                    if not old[0].is_closed():
                        log.warning(
                            "target %s: container redial to %s failed; keeping "
                            "the existing connection to %s",
                            target.name,
                            key,
                            old[1],
                        )
                    else:
                        self._connections.pop(target.name, None)
                        self._release_sftp_state(old[0])
                raise
            self._connections[target.name] = (conn, key)
            if old is not None and old[0] is not conn:
                self._release_sftp_state(old[0])
                await self.close_connection(old[0])
            return conn

    async def job_connection(
        self,
        target: TargetConfig,
        job_id: str,
        host: str,
        port: int,
        *,
        generation: int = 0,
        transport: object | None = None,
        prompter: _Prompter | None = None,
    ) -> asyncssh.SSHClientConnection:
        """Return the cached container connection for one job generation.

        The cache key is ``(job_id, generation)`` rather than the target name:
        a Slurm target owns several independent containers, so target-keyed
        caching would let one job's redial or stop invalidate another job's
        connection.  Requesting a newer generation fences and closes every
        older generation for the same job before dialing.

        With a :class:`RouteMuxTransport` the container SSH bytes travel over a
        mux stream addressed to ``job_id`` (one outer route channel total).  A
        ``None`` transport keeps the legacy direct dial but still caches per
        job/generation, which is useful for a direct per-job endpoint.
        """
        job_id = str(job_id)
        if not job_id:
            raise SSHError("job-scoped connection requires a non-empty job id")
        generation = int(generation)
        key = (job_id, generation)
        async with self._job_lock(key):
            highest = self._job_generations.get(job_id)
            if highest is not None and generation < highest:
                raise SSHError(
                    f"job {job_id!r}: generation {generation} is stale "
                    f"(current {highest}); refusing to reuse an old endpoint"
                )
            if highest is None or generation > highest:
                self._job_generations[job_id] = generation
            await self._fence_older_generations(job_id, generation)
            existing = self._job_connections.get(key)
            endpoint = self._endpoint_key(host, port)
            if (
                existing is not None
                and existing.endpoint_key == endpoint
                and not existing.conn.is_closed()
            ):
                return existing.conn
            if existing is not None:
                await self._drop_job_entry(existing, reason="redial")
            if transport is None:
                conn = await self._dial(target, host, port, prompter)
                entry = _JobConnection(
                    job_id=job_id,
                    generation=generation,
                    endpoint_key=endpoint,
                    conn=conn,
                )
            else:
                entry = await self._dial_job_over_mux(
                    target,
                    job_id,
                    generation,
                    host,
                    port,
                    endpoint,
                    transport,
                    prompter,
                )
            self._job_connections[key] = entry
            self._conn_job_keys[id(entry.conn)] = key
            return entry.conn

    async def _dial_job_over_mux(
        self,
        target: TargetConfig,
        job_id: str,
        generation: int,
        host: str,
        port: int,
        endpoint: str,
        transport,
        prompter: _Prompter | None,
    ) -> _JobConnection:
        """Open one mux stream and run a real container SSH client over it."""
        stream = await transport.open_stream(job_id)
        try:
            gateway_sock, pump_sock = socket.socketpair()
        except OSError as exc:
            with contextlib.suppress(Exception):
                await stream.reset("no socketpair")
            raise SSHError(f"could not create a mux pump socket: {exc}") from exc
        pump_reader = pump_writer = None
        try:
            pump_reader, pump_writer = await asyncio.open_connection(
                sock=pump_sock
            )
        except BaseException:
            with contextlib.suppress(Exception):
                gateway_sock.close()
            with contextlib.suppress(Exception):
                pump_sock.close()
            with contextlib.suppress(Exception):
                await stream.reset("no pump")
            raise
        # Start the pumps before the SSH handshake: the client and server
        # banners both travel through the stream, so a dial that ran first
        # would wait forever for bytes nobody is moving.
        tasks = (
            asyncio.create_task(
                self._pump_socket_to_stream(
                    pump_reader, stream, job_id=job_id
                )
            ),
            asyncio.create_task(
                self._pump_stream_to_socket(
                    stream, pump_writer, job_id=job_id
                )
            ),
        )
        try:
            conn = await self._dial(
                target, host, port, prompter, sock=gateway_sock
            )
        except BaseException:
            for task in tasks:
                task.cancel()
            with contextlib.suppress(Exception):
                pump_writer.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(pump_writer.wait_closed(), 2.0)
            with contextlib.suppress(Exception):
                await stream.reset("container dial failed")
            with contextlib.suppress(Exception):
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), 2.0
                )
            raise
        return _JobConnection(
            job_id=job_id,
            generation=generation,
            endpoint_key=endpoint,
            conn=conn,
            stream=stream,
            pump_writer=pump_writer,
            pump_tasks=tasks,
            transport=transport,
        )

    @staticmethod
    async def _pump_socket_to_stream(reader, stream, *, job_id: str) -> None:
        """Socket (inner SSH client) -> mux stream, half-closing on EOF."""
        try:
            while True:
                data = await reader.read(MUX_MAX_DATA_FRAME)
                if not data:
                    with contextlib.suppress(Exception):
                        await stream.close()
                    return
                await stream.write(data)
        except (asyncio.CancelledError, OSError):
            return
        except Exception as exc:  # noqa: BLE001 - isolate one job
            log.debug("job %s mux socket->stream pump ended: %s", job_id, exc)
            with contextlib.suppress(Exception):
                await stream.reset("pump failed")

    @staticmethod
    async def _pump_stream_to_socket(stream, writer, *, job_id: str) -> None:
        """Mux stream -> socket, closing the socket on EOF/reset."""
        try:
            while True:
                try:
                    data = await stream.read(MUX_MAX_DATA_FRAME)
                except Exception as exc:  # StreamReset / ConnectionClosed
                    log.debug("job %s mux stream reset: %s", job_id, exc)
                    return
                if not data:
                    return
                writer.write(data)
                await writer.drain()
        except (asyncio.CancelledError, OSError):
            return
        finally:
            with contextlib.suppress(Exception):
                writer.close()

    async def _fence_older_generations(
        self, job_id: str, generation: int
    ) -> None:
        """Close every cached generation of *job_id* older than *generation*."""
        for key, entry in list(self._job_connections.items()):
            if entry.job_id == job_id and entry.generation != generation:
                log.info(
                    "job %s: fencing stale connection generation %d "
                    "(requested %d)",
                    job_id,
                    entry.generation,
                    generation,
                )
                await self._drop_job_entry(entry, reason="generation fenced")

    async def _drop_job_entry(self, entry: _JobConnection, *, reason: str) -> None:
        self._job_connections.pop(entry.key, None)
        # Release the job-keyed SFTP state before dropping the reverse
        # mapping, which is what resolves the key.
        self._release_sftp_state(entry.conn)
        self._conn_job_keys.pop(id(entry.conn), None)
        for task in entry.pump_tasks:
            if not task.done():
                task.cancel()
        await self.close_connection(entry.conn)
        stream = entry.stream
        if stream is not None:
            with contextlib.suppress(Exception):
                await stream.reset(reason)
        writer = entry.pump_writer
        if writer is not None:
            with contextlib.suppress(Exception):
                writer.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(writer.wait_closed(), 2.0)
        if entry.pump_tasks:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(
                    asyncio.gather(
                        *entry.pump_tasks, return_exceptions=True
                    ),
                    3.0,
                )

    async def _drop_job_key(
        self, key: tuple[str, int], *, reason: str
    ) -> None:
        async with self._job_lock(key):
            entry = self._job_connections.get(key)
            if entry is not None:
                await self._drop_job_entry(entry, reason=reason)

    async def disconnect_job(
        self, job_id: str, *, generation: int | None = None
    ) -> None:
        """Drop one job's cached connection(s); never touches other jobs."""
        for key in list(self._job_connections):
            if key[0] != job_id:
                continue
            if generation is not None and key[1] != int(generation):
                continue
            await self._drop_job_key(key, reason="job stopped")

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
        username: str | tuple | None = None,
        sock: socket.socket | None = None,
    ) -> asyncssh.SSHClientConnection:
        # This dials the CONTAINER hop.  The container sshd accepts only its own
        # login account (``container_user``, default ``ubuntu``), which is a
        # different account from ``target.user`` (the gateway -> login/route
        # account).  When no explicit username is threaded through, resolve the
        # container user here.  The direct-transport case reaches the container
        # directly, so it too must use the container user rather than the route
        # user.
        if username is None:
            username = container_login_user(target) or ()
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
                "container for target %r has host_key_check='off': accepting "
                "any host key (not recommended for shared hosts)",
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
        extra: dict[str, object] = {}
        if sock is not None:
            # The bytes come from a mux stream pumped into this socket; the
            # host/port still drive host-key checking and the reported peer.
            extra["sock"] = sock
        try:
            return await asyncssh.connect(
                host,
                port=port,
                **extra,
                username=username,
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
        except asyncssh.Error as exc:
            # ChannelOpenError (a subclass of asyncssh.Error) means the remote
            # sshd refused a new session channel on this connection, typically
            # because its per-connection MaxSessions cap was reached.  Drop the
            # cached connection so the next request can redial.
            if is_channel_open_error(exc):
                await self.discard_connection(conn)
                raise SSHError(
                    _channel_open_message(
                        f"{self._conn_slot(conn)}failed to open SSH session channel",
                        exc,
                    )
                ) from exc
            raise SSHError(f"{self._conn_slot(conn)}remote command failed: {exc}") from exc

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
            if is_channel_open_error(exc):
                await self.discard_connection(conn)
                raise SSHError(
                    _channel_open_message(
                        f"{self._conn_slot(conn)}failed to open SSH session channel",
                        exc,
                    )
                ) from exc
            raise SSHError(
                f"{self._conn_slot(conn)}failed to create PTY session: {exc}"
            ) from exc

    def _conn_slot(self, conn: asyncssh.SSHClientConnection) -> str:
        """Return an identifying prefix for a cached connection."""
        job_key = self._conn_job_keys.get(id(conn))
        if job_key is not None:
            return f"job {job_key[0]!r} (generation {job_key[1]}): "
        name = self._target_for_conn(conn)
        return f"target {name!r}: " if name is not None else ""

    def _sftp_open_lock(self, conn: asyncssh.SSHClientConnection) -> asyncio.Lock:
        key = self._sftp_state_key(conn)
        return self._sftp_locks.setdefault(key, asyncio.Lock())

    def _sftp_sem(
        self, conn: asyncssh.SSHClientConnection, max_sessions: int
    ) -> asyncio.Semaphore:
        key = self._sftp_state_key(conn)
        return self._sftp_sems.setdefault(key, asyncio.Semaphore(max_sessions))

    @contextlib.asynccontextmanager
    async def sftp_client(
        self,
        conn: asyncssh.SSHClientConnection,
        max_sessions: int = MAX_SFTP_SESSIONS,
    ):
        """Open an SFTP subsystem session bounded per connection.

        At most *max_sessions* SFTP channels stay open per connection.  Channel
        opens are serialized on a per-connection lock that is held only around
        ``start_sftp_client`` (never across the backoff sleep or ``exit``), so
        concurrent file operations queue instead of racing the sshd session
        budget.  Transient channel-open failures are retried before the cached
        connection is discarded.
        """
        async with self._sftp_sem(conn, max_sessions):
            client = None
            try:
                for attempt in range(1, SFTP_OPEN_MAX_ATTEMPTS + 1):
                    async with self._sftp_open_lock(conn):
                        try:
                            client = await conn.start_sftp_client()
                            break
                        except (asyncssh.Error, OSError) as exc:
                            # A bare OSError is not retried: only asyncssh's
                            # channel-open refusal has evidence of transience.
                            transient = (
                                is_channel_open_error(exc)
                                and getattr(exc, "code", None) != _SFTP_OPEN_PROHIBITED
                            )
                            if attempt >= SFTP_OPEN_MAX_ATTEMPTS or not transient:
                                if transient:
                                    await self.discard_connection(conn)
                                    msg = _channel_open_message(
                                        f"{self._conn_slot(conn)}failed to open "
                                        "SFTP subsystem",
                                        exc,
                                    )
                                else:
                                    msg = (
                                        f"{self._conn_slot(conn)}failed to start "
                                        f"SFTP subsystem: {exc}"
                                    )
                                raise SSHError(msg) from exc
                            log.warning(
                                "sftp channel open attempt %d/%d failed on %s: %s",
                                attempt,
                                SFTP_OPEN_MAX_ATTEMPTS,
                                conn,
                                exc,
                            )
                    # The open lock is released here: backoff must not block
                    # other open attempts on the same connection.
                    await asyncio.sleep(
                        SFTP_OPEN_BACKOFF_BASE * 2 ** (attempt - 1)
                    )
                yield client
            finally:
                if client is not None:
                    with contextlib.suppress(Exception):
                        client.exit()
                    # Release the bounded slot only after the server has
                    # dropped the channel; otherwise the next open can race a
                    # connection whose session budget is still exhausted.
                    with contextlib.suppress(Exception):
                        await client.wait_closed()
                if (
                    self._target_for_conn(conn) is None
                    and self._job_key_for_conn(conn) is None
                ):
                    # Dedicated/one-shot connections are closed by the caller
                    # after this context exits; drop their bound state now.
                    self._release_sftp_state(conn)


def _shquote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _sh_identifier(name: str) -> str:
    """Validate an environment variable name so it cannot inject shell syntax."""
    if not _ENV_NAME_RE.match(name):
        raise SSHError(f"invalid environment variable name {name!r}")
    return name

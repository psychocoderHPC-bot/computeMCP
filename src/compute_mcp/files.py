# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""SFTP file operations inside a remote development container.

Every path is a path inside the selected container.  The gateway host
filesystem is never exposed: all operations go through the SSH connection's
SFTP subsystem.

The module keeps two layers:

* ``stat``/``read``/``write``/... are the small, stateless helpers that operate
  on an already-open :class:`asyncssh.SFTPClient`.
* :class:`FileService` is the job-scoped entry point.  Every public operation
  takes an explicit ``job_id`` and resolves it through injected async callables
  before touching the network.  The gateway injects those callables, so this
  module never decides which job is the caller's default: an omitted
  ``job_id`` is a caller error, not an implicit private-job selection.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import posixpath
import stat as stat_module
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from typing import Any

import asyncssh

from .ssh_backend import SSHError

log = logging.getLogger("compute_mcp.files")

MAX_INLINE_READ = 8 * 1024 * 1024
DEFAULT_MAX_SFTP_SESSIONS = 8

# Public message shared by every unknown or foreign job id.  The gateway must
# not add detail that would let a caller distinguish "does not exist" from
# "exists but belongs to someone else".
JOB_ACCESS_DENIED_MESSAGE = "job not found or not accessible"


class JobAccessError(RuntimeError):
    """One indistinguishable error for an unknown or foreign job id.

    The gateway maps this to its public authorization failure.  Deliberately
    not an :class:`~compute_mcp.ssh_backend.SSHError`: an inaccessible job is
    an authorization result, not an upstream SSH failure.
    """

    def __init__(self) -> None:
        super().__init__(JOB_ACCESS_DENIED_MESSAGE)


ConnectionResolver = Callable[[str], Awaitable[asyncssh.SSHClientConnection]]
JobAuthorizer = Callable[[str], Awaitable[bool]]


def _sftp_error(exc: Exception) -> SSHError:
    if isinstance(exc, asyncssh.SFTPError):
        return SSHError(f"SFTP error: {exc}")
    return SSHError(f"file operation failed: {exc}")


async def read(sftp: asyncssh.SFTPClient, path: str) -> tuple[bytes, dict]:
    try:
        info = await sftp.stat(path)
        async with sftp.open(path, "rb") as handle:
            data = await handle.read()
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    return data, _stat_dict(path, info)


async def write(sftp: asyncssh.SFTPClient, path: str, content: bytes) -> dict:
    try:
        async with sftp.open(path, "wb") as handle:
            await handle.write(content)
        info = await sftp.stat(path)
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    return _stat_dict(path, info)


async def list_dir(sftp: asyncssh.SFTPClient, path: str) -> list[dict]:
    try:
        names = await sftp.readdir(path)
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    entries = []
    for entry in names:
        name = entry.filename
        if name in (".", ".."):
            continue
        entries.append(
            {
                "name": name,
                "path": posixpath.join(path, name),
                "type": _type_of(entry.attrs),
                "size": entry.attrs.size,
                "mode": oct(entry.attrs.permissions & 0o7777) if entry.attrs.permissions else None,
            }
        )
    entries.sort(key=lambda item: (item["type"] != "directory", item["name"]))
    return entries


async def stat(sftp: asyncssh.SFTPClient, path: str) -> dict:
    try:
        info = await sftp.stat(path)
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    return _stat_dict(path, info)


async def mkdir(sftp: asyncssh.SFTPClient, path: str) -> dict:
    try:
        await sftp.makedirs(path, exist_ok=False)
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    return {"path": path, "created": True}


async def remove(sftp: asyncssh.SFTPClient, path: str) -> dict:
    try:
        info = await sftp.stat(path)
        if stat_module.S_ISDIR(info.permissions) if info.permissions else False:
            await sftp.rmtree(path)
        else:
            await sftp.remove(path)
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    return {"path": path, "removed": True}


async def rename(sftp: asyncssh.SFTPClient, source: str, destination: str) -> dict:
    try:
        await sftp.rename(source, destination)
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    return {"source": source, "destination": destination, "renamed": True}


async def chmod(sftp: asyncssh.SFTPClient, path: str, mode: int) -> dict:
    try:
        await sftp.chmod(path, mode)
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    return {"path": path, "mode": oct(mode)}


def parse_mode(mode: str | int) -> int:
    """Accept ``644``, ``"644"`` or ``"0o644"`` and return an int mode."""
    if isinstance(mode, int):
        return mode
    text = str(mode).strip().lower()
    if text.startswith("0o"):
        text = text[2:]
    try:
        value = int(text, 8)
    except ValueError as exc:
        raise SSHError(f"invalid mode {mode!r}") from exc
    if not 0 <= value <= 0o7777:
        raise SSHError(f"mode out of range: {mode!r}")
    return value


# Internal aliases so the service methods can call the stateless helpers even
# though the method names are the same.  A method body resolves unqualified
# names globally, but explicit aliases keep the intent obvious to reviewers.
_stat = stat
_list_dir = list_dir
_read = read
_write = write
_mkdir = mkdir
_remove = remove
_rename = rename
_chmod = chmod


@contextlib.asynccontextmanager
async def _default_sftp_session(conn: asyncssh.SSHClientConnection):
    """Open one SFTP channel on *conn* and close it on exit."""
    client = await conn.start_sftp_client()
    try:
        yield client
    finally:
        with contextlib.suppress(Exception):
            client.exit()
        with contextlib.suppress(Exception):
            await client.wait_closed()


SftpSessionFactory = Callable[
    [asyncssh.SSHClientConnection], Any
]


class _JobState:
    """Per-job SFTP bookkeeping, never shared between jobs."""

    __slots__ = ("lock", "sem", "active", "closing")

    def __init__(self, max_sessions: int) -> None:
        self.lock = asyncio.Lock()
        self.sem = asyncio.Semaphore(max_sessions)
        self.active = 0
        self.closing = False


class FileService:
    """Job-scoped file operations with injected resolution and authorization.

    ``resolve_job`` and ``authorize_job`` are supplied by the gateway.  Their
    contract:

    * ``authorize_job(job_id) -> bool`` returns ``False`` for an unknown id and
      for an id owned by another client.  It runs before any connection lookup
      or network I/O and must not distinguish the two cases.
    * ``resolve_job(job_id) -> connection`` returns the job-owned SSH
      connection.  It is only called after authorization succeeds.
    * ``sftp_session(connection)`` is an optional async context-manager factory
      returning an SFTP client.  The default opens ``start_sftp_client`` and
      closes the channel; the gateway may pass ``SSHBackend.sftp_client`` to
      keep its retry and channel-budget behaviour.
    """

    def __init__(
        self,
        resolve_job: ConnectionResolver,
        authorize_job: JobAuthorizer,
        *,
        max_sessions: int = DEFAULT_MAX_SFTP_SESSIONS,
        sftp_session: SftpSessionFactory | None = None,
    ) -> None:
        if max_sessions < 1:
            raise ValueError("max_sessions must be positive")
        self.resolve_job = resolve_job
        self.authorize_job = authorize_job
        self.max_sessions = max_sessions
        self._sftp_session = sftp_session or _default_sftp_session
        self._states: dict[str, _JobState] = {}

    # -- lifecycle ---------------------------------------------------------
    def _require_job_id(self, job_id: str | None) -> str:
        if job_id is None:
            # A missing id is a bug in the caller, not a signal to pick a
            # private job.  The gateway resolves the shared default first.
            raise ValueError(
                "job_id is required; resolve the shared or explicit job "
                "before calling a file operation"
            )
        text = str(job_id)
        if not text:
            raise ValueError("job_id must be a non-empty string")
        return text

    def _state(self, job_id: str) -> _JobState:
        state = self._states.get(job_id)
        if state is None:
            state = _JobState(self.max_sessions)
            self._states[job_id] = state
        return state

    def _release(self, job_id: str, state: _JobState) -> None:
        state.active -= 1
        if state.closing and state.active <= 0:
            if self._states.get(job_id) is state:
                self._states.pop(job_id, None)

    async def close_job(self, job_id: str) -> bool:
        """Drop the per-job SFTP state for *job_id* only.

        In-flight operations hold their own ``_JobState`` reference and finish
        (or fail with their job's connection); the state is removed once the
        last one leaves.  No other job's state, connection or channel is
        touched.  Returns ``True`` when state existed.
        """
        job_id = self._require_job_id(job_id)
        state = self._states.get(job_id)
        if state is None:
            return False
        state.closing = True
        if state.active <= 0:
            self._states.pop(job_id, None)
        return True

    async def close_all(self) -> None:
        """Retire all per-job state; used by gateway shutdown."""
        for job_id in list(self._states):
            await self.close_job(job_id)

    def job_count(self) -> int:
        return len(self._states)

    # -- resolution --------------------------------------------------------
    async def _authorize(self, job_id: str) -> None:
        try:
            allowed = await self.authorize_job(job_id)
        except JobAccessError:
            raise
        except Exception as exc:  # noqa: BLE001 - never become an oracle
            log.debug("job authorization failed for %r: %s", job_id, exc)
            raise JobAccessError() from exc
        if not allowed:
            raise JobAccessError()

    async def _resolve(self, job_id: str) -> asyncssh.SSHClientConnection:
        try:
            conn = await self.resolve_job(job_id)
        except (KeyError, LookupError) as exc:
            # A lookup race must not turn into "this id used to exist".
            raise JobAccessError() from exc
        if conn is None:
            raise JobAccessError()
        return conn

    @contextlib.asynccontextmanager
    async def _sftp(self, job_id: str | None):
        job_id = self._require_job_id(job_id)
        # Authorization strictly precedes resolution and any network I/O.
        await self._authorize(job_id)
        state = self._state(job_id)
        state.active += 1
        try:
            conn = await self._resolve(job_id)
            async with state.sem:
                async with contextlib.AsyncExitStack() as stack:
                    # The lock only guards opening the channel.  It is never
                    # held across a transfer, so a slow read on this job does
                    # not block another job (each job has its own lock/sem).
                    async with state.lock:
                        sftp = await stack.enter_async_context(
                            self._sftp_session(conn)
                        )
                    yield sftp
        finally:
            self._release(job_id, state)

    # -- operations --------------------------------------------------------
    async def stat(self, job_id: str | None, path: str) -> dict:
        async with self._sftp(job_id) as sftp:
            return await _stat(sftp, path)

    async def list_dir(self, job_id: str | None, path: str) -> list[dict]:
        async with self._sftp(job_id) as sftp:
            return await _list_dir(sftp, path)

    async def read(
        self,
        job_id: str | None,
        path: str,
        *,
        max_bytes: int = MAX_INLINE_READ,
    ) -> tuple[bytes, dict]:
        async with self._sftp(job_id) as sftp:
            info = await _stat(sftp, path)
            size = info.get("size")
            if size is not None and size > max_bytes:
                raise SSHError(
                    f"file is too large for an inline read ({size} > "
                    f"{max_bytes} bytes); use download"
                )
            data, info = await _read(sftp, path)
            return data, info

    async def write(
        self, job_id: str | None, path: str, content: bytes
    ) -> dict:
        async with self._sftp(job_id) as sftp:
            return await _write(sftp, path, content)

    async def upload(
        self,
        job_id: str | None,
        path: str,
        source: Any,
        *,
        append: bool = False,
        parents: bool = False,
        chunk_size: int = 262144,
    ) -> dict:
        """Stream *source* into *path* without buffering the whole body.

        *source* may be an object with an async ``readany()`` (an aiohttp
        request body), an async iterable of ``bytes``, an object with an async
        ``read(n)``, or a sync iterable of ``bytes``.
        """
        async with self._sftp(job_id) as sftp:
            if parents:
                parent = posixpath.dirname(path)
                if parent:
                    with contextlib.suppress(Exception):
                        await sftp.makedirs(parent, exist_ok=True)
            written = 0
            try:
                async with sftp.open(path, "ab" if append else "wb") as handle:
                    async for chunk in _iter_source(source, chunk_size):
                        await handle.write(chunk)
                        written += len(chunk)
                info = await _stat(sftp, path)
            except (asyncssh.Error, OSError) as exc:
                raise _sftp_error(exc) from exc
        return {"path": path, "written": written, "stat": info}

    async def download(
        self,
        job_id: str | None,
        path: str,
        *,
        chunk_size: int = 262144,
    ) -> AsyncIterator[bytes]:
        """Yield a regular file in bounded chunks."""
        async with self._sftp(job_id) as sftp:
            try:
                info = await _stat(sftp, path)
                if info["type"] != "file":
                    raise SSHError("not a regular file")
                async with sftp.open(path, "rb") as handle:
                    while True:
                        chunk = await handle.read(chunk_size)
                        if not chunk:
                            break
                        yield chunk
            except (asyncssh.Error, OSError) as exc:
                raise _sftp_error(exc) from exc

    async def mkdir(self, job_id: str | None, path: str) -> dict:
        async with self._sftp(job_id) as sftp:
            return await _mkdir(sftp, path)

    async def remove(self, job_id: str | None, path: str) -> dict:
        async with self._sftp(job_id) as sftp:
            return await _remove(sftp, path)

    async def rename(
        self, job_id: str | None, source: str, destination: str
    ) -> dict:
        async with self._sftp(job_id) as sftp:
            return await _rename(sftp, source, destination)

    async def chmod(self, job_id: str | None, path: str, mode: int) -> dict:
        async with self._sftp(job_id) as sftp:
            return await _chmod(sftp, path, mode)

    async def exists(self, job_id: str | None, path: str) -> bool:
        async with self._sftp(job_id) as sftp:
            try:
                await sftp.stat(path)
            except asyncssh.SFTPNoSuchFile:
                return False
            except (asyncssh.Error, OSError) as exc:
                raise _sftp_error(exc) from exc
        return True


async def _iter_source(source: Any, chunk_size: int) -> AsyncIterator[bytes]:
    """Adapt the source shapes the gateway uses into an async byte stream."""
    if hasattr(source, "readany"):
        while True:
            chunk = await source.readany()
            if not chunk:
                break
            yield chunk
        return
    if hasattr(source, "__aiter__"):
        async for chunk in source:
            if chunk:
                yield chunk
        return
    read = getattr(source, "read", None)
    if read is not None:
        while True:
            chunk = read(chunk_size)
            if hasattr(chunk, "__await__"):
                chunk = await chunk
            if not chunk:
                break
            yield chunk
        return
    for chunk in source:  # type: ignore[union-attr]
        if chunk:
            yield chunk


def _type_of(attrs) -> str:
    perms = attrs.permissions or 0
    if stat_module.S_ISDIR(perms):
        return "directory"
    if stat_module.S_ISLNK(perms):
        return "symlink"
    return "file"


def _stat_dict(path: str, info) -> dict:
    return {
        "path": path,
        "type": _type_of(info),
        "size": info.size,
        "mode": oct(info.permissions & 0o7777) if info.permissions else None,
        "uid": info.uid,
        "gid": info.gid,
        "mtime": info.mtime,
    }

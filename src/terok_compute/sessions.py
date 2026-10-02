# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Persistent PTY session manager.

Sessions are independent of each other and of the MCP request loop, so a long
build in one session never blocks another call.  Output is drained by a
background task into a bounded buffer; ``read`` returns and clears it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets
import time
from typing import Awaitable, Callable

import asyncssh

from .config import SessionConfig, TargetConfig
from .ssh_backend import ManagedSession, SSHBackend, SSHError

log = logging.getLogger("terok_compute.sessions")

ConnectionProvider = Callable[[str], Awaitable[tuple[TargetConfig, asyncssh.SSHClientConnection]]]


class SessionError(RuntimeError):
    pass


class SessionLimitError(SessionError):
    pass


class SessionNotFound(SessionError):
    pass


class SessionForbidden(SessionError):
    pass


class SessionManager:
    def __init__(
        self,
        backend: SSHBackend,
        provider: ConnectionProvider,
        config: SessionConfig,
        dedicated_provider: ConnectionProvider | None = None,
    ) -> None:
        self.backend = backend
        self.provider = provider
        self.dedicated_provider = dedicated_provider
        self.config = config
        self._sessions: dict[str, ManagedSession] = {}
        self._readers: dict[str, asyncio.Task] = {}
        self._janitor: asyncio.Task | None = None

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        if self._janitor is None:
            self._janitor = asyncio.create_task(self._reap_idle())

    async def stop(self) -> None:
        if self._janitor is not None:
            self._janitor.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._janitor
            self._janitor = None
        for session_id in list(self._sessions):
            await self.close(session_id, owner=None)

    async def close_for_target(self, target: str, reason: str = "tunnel lost") -> None:
        for session_id, session in list(self._sessions.items()):
            if session.target == target:
                await self.close(session_id, owner=None, reason=reason)

    # -- operations --------------------------------------------------------
    def _client_sessions(self, client_id: str) -> list[ManagedSession]:
        return [s for s in self._sessions.values() if s.client_id == client_id]

    async def create(
        self,
        client_id: str,
        target: TargetConfig,
        cwd: str | None,
        columns: int,
        rows: int,
    ) -> ManagedSession:
        if len(self._client_sessions(client_id)) >= self.config.max_per_client:
            raise SessionLimitError("per-client session limit reached")
        if not (1 <= columns <= 1000) or not (1 <= rows <= 1000):
            raise SessionError("invalid terminal size")

        dedicated_conn = None
        try:
            if target.connect_mode == "dedicated" and self.dedicated_provider is not None:
                _, conn = await self.dedicated_provider(target.name)
                dedicated_conn = conn
            else:
                _, conn = await self.provider(target.name)
            process = await self.backend.create_session(conn, cwd, columns, rows)
        except Exception:
            if dedicated_conn is not None:
                await self.backend.close_connection(dedicated_conn)
            raise
        now = time.time()
        session_id = secrets.token_urlsafe(18)
        session = ManagedSession(
            id=session_id,
            client_id=client_id,
            target=target.name,
            process=process,
            created_at=now,
            last_activity=now,
            dedicated_conn=dedicated_conn,
        )
        self._sessions[session_id] = session
        self._readers[session_id] = asyncio.create_task(self._drain(session))
        log.info(
            "session %s created for client %s on %s (%s)",
            session_id,
            client_id,
            target.name,
            "dedicated" if dedicated_conn is not None else "shared",
        )
        return session

    def _get(
        self, session_id: str, client_id: str, *, admin: bool = False
    ) -> ManagedSession:
        session = self._sessions.get(session_id)
        if session is None:
            raise SessionNotFound(session_id)
        if not admin and session.client_id != client_id:
            # Do not reveal whether the session exists.
            raise SessionNotFound(session_id)
        return session

    def get_public(
        self, session_id: str, client_id: str, *, admin: bool = False
    ) -> ManagedSession:
        return self._get(session_id, client_id, admin=admin)

    async def close_all_for_client(self, client_id: str, reason: str = "admin") -> int:
        ids = [s.id for s in self._sessions.values() if s.client_id == client_id]
        for session_id in ids:
            with contextlib.suppress(SessionError):
                await self.close(session_id, owner=None, reason=reason)
        return len(ids)

    async def write(self, session_id: str, client_id: str, data: str | bytes) -> int:
        session = self._get(session_id, client_id)
        if session.closed:
            raise SessionError("session is closed")
        payload = data.encode() if isinstance(data, str) else data
        session.process.stdin.write(payload)
        session.last_activity = time.time()
        return len(payload)

    def read(self, session_id: str, client_id: str, max_bytes: int) -> dict:
        session = self._get(session_id, client_id)
        session.last_activity = time.time()
        buf = session.output_buffer
        if max_bytes <= 0:
            chunk = bytes(buf)
            buf.clear()
        else:
            chunk = bytes(buf[:max_bytes])
            del buf[:max_bytes]
        return {
            "session_id": session.id,
            "data": chunk.decode("utf-8", errors="replace"),
            "encoding": "utf-8-replace",
            "exit_status": session.exit_status,
            "closed": session.closed,
            "buffered_bytes": len(buf),
        }

    async def resize(self, session_id: str, client_id: str, columns: int, rows: int) -> None:
        session = self._get(session_id, client_id)
        if not (1 <= columns <= 1000) or not (1 <= rows <= 1000):
            raise SessionError("invalid terminal size")
        session.process.channel.change_terminal_size(columns, rows)
        session.last_activity = time.time()

    async def close(self, session_id: str, owner: str | None, reason: str = "closed") -> None:
        session = self._sessions.pop(session_id, None)
        if session is None:
            raise SessionNotFound(session_id)
        if owner is not None and session.client_id != owner:
            self._sessions[session_id] = session
            raise SessionForbidden(session_id)
        session.closed = True
        reader = self._readers.pop(session_id, None)
        if reader is not None:
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reader
        with contextlib.suppress(Exception):
            try:
                session.process.terminate()
            except Exception:
                pass
        with contextlib.suppress(Exception):
            await asyncio.wait_for(session.process.wait_closed(), timeout=3)
        if session.dedicated_conn is not None:
            await self.backend.close_connection(session.dedicated_conn)
            session.dedicated_conn = None
        log.info("session %s closed (%s)", session_id, reason)

    def _public(self, s: ManagedSession, now: float) -> dict:
        return {
            "session_id": s.id,
            "client": s.client_id,
            "target": s.target,
            "age": round(now - s.created_at, 3),
            "idle": round(now - s.last_activity, 3),
            "closed": s.closed,
            "exit_status": s.exit_status,
            "buffered_bytes": len(s.output_buffer),
            "connection": "dedicated" if s.dedicated_conn is not None else "shared",
        }

    def list(self, client_id: str, target: str | None = None) -> list[dict]:
        now = time.time()
        return [
            self._public(s, now)
            for s in self._sessions.values()
            if s.client_id == client_id and (target is None or s.target == target)
        ]

    def list_all(self, target: str | None = None, client_id: str | None = None) -> list[dict]:
        now = time.time()
        return [
            self._public(s, now)
            for s in self._sessions.values()
            if (target is None or s.target == target)
            and (client_id is None or s.client_id == client_id)
        ]

    def count_for_client(self, client_id: str) -> int:
        return sum(1 for s in self._sessions.values() if s.client_id == client_id)

    def count_for_target(self, target: str) -> int:
        return sum(1 for s in self._sessions.values() if s.target == target)

    # -- internals ---------------------------------------------------------
    async def _drain(self, session: ManagedSession) -> None:
        try:
            while True:
                try:
                    data = await session.process.stdout.read(65536)
                except (asyncssh.Error, OSError):
                    break
                if not data:
                    break
                buf = session.output_buffer
                buf.extend(data)
                overflow = len(buf) - self.config.output_buffer_bytes
                if overflow > 0:
                    del buf[:overflow]
            with contextlib.suppress(Exception):
                await session.process.wait_closed()
            rc = session.process.exit_status
            session.exit_status = rc
            session.closed = True
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("session %s reader failed", session.id)
            session.closed = True

    async def _reap_idle(self) -> None:
        try:
            while True:
                await asyncio.sleep(30)
                now = time.time()
                for session_id, session in list(self._sessions.items()):
                    if now - session.last_activity > self.config.idle_timeout:
                        with contextlib.suppress(SessionError):
                            await self.close(session_id, owner=None, reason="idle timeout")
        except asyncio.CancelledError:
            raise

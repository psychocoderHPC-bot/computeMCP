# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Job scoping for persistent PTY sessions.

The tests use fake connections and processes, so no SSH server or container is
needed.  They pin the SessionManager contract:

* an explicit ``job_id`` is authorized before any connection work and is
  forwarded to ``job_provider``;
* an unknown id and a foreign id produce the same public error;
* an omitted id uses the shared provider and never the caller's private job;
* ``close_for_job`` closes only that job's sessions and streams;
* sessions on different jobs have independent processes and are tagged with
  their job id in the public view.
"""

from __future__ import annotations

import asyncio

import pytest

from compute_mcp.config import SessionConfig, TargetConfig, TransportConfig
from compute_mcp.files import JOB_ACCESS_DENIED_MESSAGE, JobAccessError
from compute_mcp.sessions import SessionError, SessionManager

pytestmark = pytest.mark.asyncio


class _BlockingStdout:
    """A PTY stdout that stays open until the session is closed."""

    def __init__(self):
        self._event = asyncio.Event()

    async def read(self, size=-1):
        await self._event.wait()
        return b""


class _Stdin:
    def __init__(self):
        self.written = []

    def write(self, data):
        self.written.append(bytes(data))


class _Channel:
    def __init__(self):
        self.sizes = []

    def change_terminal_size(self, columns, rows):
        self.sizes.append((columns, rows))


class FakeProcess:
    def __init__(self):
        self.stdout = _BlockingStdout()
        self.stdin = _Stdin()
        self.channel = _Channel()
        self.exit_status = None
        self.terminated = 0
        self._closed = asyncio.Event()

    def terminate(self):
        self.terminated += 1
        self._closed.set()

    async def wait_closed(self):
        await self._closed.wait()


class FakeBackend:
    def __init__(self):
        self.created = []
        self.closed_connections = []

    async def create_session(self, conn, cwd, columns, rows):
        process = FakeProcess()
        self.created.append((conn, cwd, columns, rows, process))
        return process

    async def close_connection(self, conn):
        self.closed_connections.append(conn)


class FakeConnection:
    def __init__(self, job_id):
        self.job_id = job_id
        self.closed = False


def _target(name="slurm"):
    return TargetConfig(
        name=name,
        user="agent",
        transport=TransportConfig(
            kind="direct", remote_host="127.0.0.1", remote_port=1
        ),
    )


class Harness:
    def __init__(self, *, authorized=None, with_job_provider=True):
        self.backend = FakeBackend()
        self.target = _target()
        self.provider_calls = []
        self.job_calls = []
        self.authorize_calls = []
        self.authorized = authorized or (lambda target, job_id: True)

        async def provider(name):
            self.provider_calls.append(name)
            return self.target, FakeConnection("shared")

        async def job_provider(name, job_id):
            self.job_calls.append((name, job_id))
            return self.target, FakeConnection(job_id)

        self.manager = SessionManager(
            self.backend,
            provider,
            SessionConfig(),
            job_provider=job_provider if with_job_provider else None,
            job_authorizer=self._authorize,
        )

    async def _authorize(self, target, job_id):
        self.authorize_calls.append((target, job_id))
        return self.authorized(target, job_id)

    async def create(self, client_id="c", job_id=None):
        return await self.manager.create(
            client_id, self.target, None, 80, 24, job_id=job_id
        )


async def _cleanup(harness):
    for session_id in list(harness.manager._sessions):
        await harness.manager.close(session_id, owner=None)


async def test_job_id_is_forwarded_and_recorded():
    harness = Harness()
    session = await harness.create(job_id="A")

    assert session.job_id == "A"
    assert harness.job_calls == [("slurm", "A")]
    assert harness.provider_calls == []
    # Authorization runs before the connection provider.
    assert harness.authorize_calls == [("slurm", "A")]
    public = harness.manager.list("c")
    assert public and public[0]["job_id"] == "A"
    await _cleanup(harness)


async def test_omitted_job_id_uses_shared_provider_not_a_private_job():
    harness = Harness()
    session = await harness.create()

    assert session.job_id is None
    assert harness.provider_calls == ["slurm"]
    assert harness.job_calls == []
    await _cleanup(harness)


async def test_unknown_and_foreign_job_ids_are_indistinguishable_and_pre_io():
    harness = Harness(authorized=lambda target, job_id: job_id == "shared")

    errors = []
    for bad in ("missing", "foreign-private"):
        with pytest.raises(JobAccessError) as info:
            await harness.create(job_id=bad)
        errors.append(info.value)

    assert str(errors[0]) == str(errors[1]) == JOB_ACCESS_DENIED_MESSAGE
    assert harness.authorize_calls == [
        ("slurm", "missing"),
        ("slurm", "foreign-private"),
    ]
    # No provider, connection or PTY work happened.
    assert harness.job_calls == []
    assert harness.provider_calls == []
    assert harness.manager._sessions == {}
    assert harness.backend.created == []


async def test_explicit_job_without_a_job_provider_is_refused():
    harness = Harness(with_job_provider=False)

    with pytest.raises(SessionError):
        await harness.create(job_id="A")

    # It must not fall back to the shared provider.
    assert harness.provider_calls == []
    assert harness.manager._sessions == {}


async def test_close_for_job_closes_only_that_jobs_sessions():
    harness = Harness()
    session_a = await harness.create(job_id="A")
    session_b = await harness.create(job_id="B")

    assert session_a.process is not session_b.process
    assert {s.job_id for s in harness.manager._sessions.values()} == {"A", "B"}

    closed = await harness.manager.close_for_job("B")
    assert closed == 1
    assert session_b.id not in harness.manager._sessions
    assert session_a.id in harness.manager._sessions
    # A's process/stream was not touched.
    assert session_a.process.terminated == 0
    assert session_b.process.terminated == 1

    await harness.manager.close_for_job("A")
    assert harness.manager._sessions == {}


async def test_close_for_job_is_idempotent_and_target_wide_close_still_works():
    harness = Harness()
    session_a = await harness.create(job_id="A")
    session_b = await harness.create(job_id="B")

    assert await harness.manager.close_for_job("A") == 1
    assert await harness.manager.close_for_job("A") == 0
    assert session_b.id in harness.manager._sessions

    await harness.manager.close_for_target("slurm", reason="route lost")
    assert harness.manager._sessions == {}
    assert session_b.process.terminated == 1

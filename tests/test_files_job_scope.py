# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Job scoping for every SFTP file operation.

These tests use fake connections/SFTP clients, so they run without SSH or a
container.  They pin the contract the gateway must satisfy:

* every operation carries an explicit job id through to the resolver;
* an unknown and a foreign id produce one identical public error and do no
  network work;
* a missing id never selects a private job;
* per-job SFTP state, locks and semaphores are independent, and retiring one
  job leaves another job's transfer running;
* paths are forwarded unchanged (traversal behaviour is not altered here).
"""

from __future__ import annotations

import asyncio
import contextlib
import stat as stat_module

import asyncssh
import pytest

from compute_mcp.files import (
    JOB_ACCESS_DENIED_MESSAGE,
    FileService,
    JobAccessError,
)
from compute_mcp.ssh_backend import SSHError


class _Attrs:
    def __init__(self, size=0, permissions=stat_module.S_IFREG | 0o644):
        self.size = size
        self.permissions = permissions
        self.uid = 1000
        self.gid = 1000
        self.mtime = 1234


class _DirEntry:
    def __init__(self, filename, attrs):
        self.filename = filename
        self.attrs = attrs


class _Handle:
    def __init__(self, sftp, path, mode):
        self.sftp = sftp
        self.path = path
        self.mode = mode
        self.pos = 0
        self._writes = bytearray()

    async def __aenter__(self):
        self.sftp.calls.append(("open", self.path, self.mode))
        return self

    async def __aexit__(self, *exc):
        if "w" in self.mode or "a" in self.mode:
            self.sftp.files[self.path] = bytes(self._writes)
        return False

    async def read(self, size=-1):
        if self.path in self.sftp.gated_paths:
            self.sftp.started.set()
            await self.sftp.gate.wait()
        data = self.sftp.files.get(self.path, b"")
        if size is None or size < 0:
            chunk = data[self.pos:]
        else:
            chunk = data[self.pos:self.pos + size]
        self.pos += len(chunk)
        return chunk

    async def write(self, data):
        self._writes.extend(data)
        return len(data)


class FakeSFTP:
    """Small in-memory SFTP client with call recording and optional gating."""

    def __init__(self, files=None, entries=None):
        self.files = dict(files or {})
        self.entries = entries or {}
        self.calls = []
        self.exited = 0
        self.closed = 0
        self.gate = asyncio.Event()
        self.started = asyncio.Event()
        self.gated_paths: set[str] = set()

    async def stat(self, path):
        self.calls.append(("stat", path))
        if path in self.entries:
            return _Attrs(0, stat_module.S_IFDIR | 0o755)
        if path not in self.files:
            raise asyncssh.SFTPNoSuchFile("nope")
        return _Attrs(len(self.files[path]))

    def open(self, path, mode="rb"):
        return _Handle(self, path, mode)

    async def readdir(self, path):
        self.calls.append(("readdir", path))
        names = self.entries.get(path, [])
        result = [_DirEntry(".", _Attrs(0, stat_module.S_IFDIR | 0o755)),
                  _DirEntry("..", _Attrs(0, stat_module.S_IFDIR | 0o755))]
        for name in names:
            perms = (
                stat_module.S_IFDIR | 0o755
                if name in self.entries
                else stat_module.S_IFREG | 0o644
            )
            result.append(_DirEntry(name, _Attrs(0, perms)))
        return result

    async def makedirs(self, path, exist_ok=False):
        self.calls.append(("makedirs", path, exist_ok))

    async def remove(self, path):
        self.calls.append(("remove", path))
        self.files.pop(path, None)

    async def rmtree(self, path):
        self.calls.append(("rmtree", path))

    async def rename(self, source, destination):
        self.calls.append(("rename", source, destination))

    async def chmod(self, path, mode):
        self.calls.append(("chmod", path, mode))

    def exit(self):
        self.exited += 1

    async def wait_closed(self):
        self.closed += 1


class FakeConnection:
    def __init__(self, job_id, sftp):
        self.job_id = job_id
        self.sftp = sftp
        self.closed = False

    async def start_sftp_client(self):
        return self.sftp


class Harness:
    """Resolver/authorizer harness with recorded calls."""

    def __init__(self, jobs, *, authorized=None):
        self.jobs = jobs
        self.authorized = authorized or (lambda job_id: True)
        self.resolve_calls = []
        self.authorize_calls = []
        self.service = FileService(
            self._resolve, self._authorize, sftp_session=self._session
        )

    async def _resolve(self, job_id):
        self.resolve_calls.append(job_id)
        if job_id not in self.jobs:
            raise KeyError(job_id)
        return self.jobs[job_id]

    async def _authorize(self, job_id):
        self.authorize_calls.append(job_id)
        return self.authorized(job_id)

    @contextlib.asynccontextmanager
    async def _session(self, conn):
        yield conn.sftp


def _harness(*jobs, authorized=None):
    connections = {job_id: FakeConnection(job_id, FakeSFTP()) for job_id in jobs}
    return Harness(connections, authorized=authorized)


async def test_every_operation_forwards_job_id():
    harness = _harness("A")
    sftp = harness.jobs["A"].sftp
    sftp.files["/f"] = b"hello"
    sftp.entries["/"] = ["sub"]

    await harness.service.stat("A", "/f")
    await harness.service.list_dir("A", "/")
    await harness.service.read("A", "/f")
    await harness.service.write("A", "/new", b"x")
    await harness.service.mkdir("A", "/dir")
    await harness.service.remove("A", "/new")
    await harness.service.rename("A", "/f", "/g")
    await harness.service.chmod("A", "/f", 0o600)
    assert await harness.service.exists("A", "/missing") is False
    await harness.service.upload("A", "/up", [b"a", b"b"])
    chunks = [chunk async for chunk in harness.service.download("A", "/f")]

    assert b"".join(chunks) == b"hello"
    # Every resolver/authorizer call carried the explicit id.
    assert set(harness.resolve_calls) == {"A"}
    assert set(harness.authorize_calls) == {"A"}
    assert harness.resolve_calls.count("A") >= 11


async def test_unknown_and_foreign_are_indistinguishable_and_do_no_io():
    harness = _harness("shared", authorized=lambda job_id: job_id == "shared")

    errors = []
    for bad in ("missing", "foreign-private"):
        with pytest.raises(JobAccessError) as info:
            await harness.service.stat(bad, "/f")
        errors.append(info.value)

    assert str(errors[0]) == str(errors[1]) == JOB_ACCESS_DENIED_MESSAGE
    # Authorization runs for both, but the resolver (and hence any connection
    # or SFTP work) was never reached.
    assert harness.authorize_calls == ["missing", "foreign-private"]
    assert harness.resolve_calls == []


async def test_resolver_lookup_race_uses_the_same_public_error():
    harness = _harness("shared")

    with pytest.raises(JobAccessError) as info:
        await harness.service.stat("ghost", "/f")

    assert str(info.value) == JOB_ACCESS_DENIED_MESSAGE
    # Authorization succeeded, then the lookup failed; the caller still sees
    # exactly the same error as an unauthorized id.
    assert harness.resolve_calls == ["ghost"]


async def test_missing_job_id_never_defaults_to_a_private_job():
    harness = _harness("private")

    for missing in (None, ""):
        with pytest.raises(ValueError):
            await harness.service.stat(missing, "/f")

    assert harness.authorize_calls == []
    assert harness.resolve_calls == []


async def test_per_job_state_is_independent_and_close_leaves_no_state():
    harness = _harness("A", "B")
    harness.jobs["A"].sftp.files["/f"] = b"a"
    harness.jobs["B"].sftp.files["/f"] = b"b"

    await harness.service.stat("A", "/f")
    await harness.service.stat("B", "/f")

    state_a = harness.service._states["A"]
    state_b = harness.service._states["B"]
    assert state_a.sem is not state_b.sem
    assert state_a.lock is not state_b.lock

    assert await harness.service.close_job("B") is True
    assert "B" not in harness.service._states
    assert "A" in harness.service._states
    # Closing B did not touch A's connection or SFTP channel.
    assert harness.jobs["A"].sftp.exited == 0

    assert await harness.service.close_job("A") is True
    assert harness.service.job_count() == 0


async def test_long_transfer_on_a_survives_close_of_b():
    harness = _harness("A", "B")
    payload = b"0123456789" * 4096
    sftp_a = harness.jobs["A"].sftp
    sftp_a.files["/big"] = payload
    sftp_a.gated_paths.add("/big")
    harness.jobs["B"].sftp.files["/small"] = b"b"

    received = bytearray()

    async def consume():
        async for chunk in harness.service.download("A", "/big"):
            received.extend(chunk)

    task = asyncio.create_task(consume())
    await asyncio.wait_for(sftp_a.started.wait(), 2)
    assert not task.done()

    # Stopping/retiring job B must not cancel or close job A's transfer.
    await harness.service.close_job("B")
    assert not task.done()
    assert harness.jobs["A"].sftp.exited == 0

    sftp_a.gate.set()
    await asyncio.wait_for(task, 2)
    assert bytes(received) == payload


async def test_ptyless_transfer_and_second_job_run_concurrently():
    harness = _harness("A", "B")
    sftp_a = harness.jobs["A"].sftp
    sftp_a.files["/big"] = b"x" * 16
    sftp_a.gated_paths.add("/big")
    harness.jobs["B"].sftp.files["/other"] = b"y"

    async def consume():
        return b"".join([c async for c in harness.service.download("A", "/big")])

    task = asyncio.create_task(consume())
    await asyncio.wait_for(sftp_a.started.wait(), 2)

    # B has its own lock/semaphore, so its operation completes while A's
    # channel is still blocked.
    data, _ = await asyncio.wait_for(
        harness.service.read("B", "/other"), 2
    )
    assert data == b"y"
    assert not task.done()

    sftp_a.gate.set()
    assert await asyncio.wait_for(task, 2) == b"x" * 16


async def test_paths_are_forwarded_unchanged():
    harness = _harness("A")
    sftp = harness.jobs["A"].sftp
    sftp.files["../escape"] = b"z"
    sftp.entries["/"] = ["file"]

    await harness.service.stat("A", "../escape")
    await harness.service.exists("A", "../../missing")
    await harness.service.upload("A", "/../up", [b"z"], parents=True)
    await harness.service.list_dir("A", "/")

    stat_paths = [call[1] for call in sftp.calls if call[0] == "stat"]
    assert "../escape" in stat_paths
    assert "../../missing" in stat_paths
    assert ("open", "/../up", "wb") in sftp.calls
    # The directory listing still drops "."/".." entries as before.
    listed = await harness.service.list_dir("A", "/")
    assert [entry["name"] for entry in listed] == ["file"]


async def test_inline_read_is_bounded():
    harness = _harness("A")
    harness.jobs["A"].sftp.files["/big"] = b"0123456789"

    with pytest.raises(SSHError):
        await harness.service.read("A", "/big", max_bytes=4)


async def test_default_sftp_session_opens_and_closes_the_channel():
    sftp = FakeSFTP({"/f": b"data"})
    conn = FakeConnection("A", sftp)

    async def resolve(job_id):
        return conn

    async def authorize(job_id):
        return True

    service = FileService(resolve, authorize)
    info = await service.stat("A", "/f")
    assert info["size"] == 4
    assert sftp.exited == 1
    assert sftp.closed == 1

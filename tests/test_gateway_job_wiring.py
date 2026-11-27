# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Gateway wiring of the mux transport and job-scoped services (T6c).

An in-process asyncssh login server counts the actual outer channels and
authentications.  Each Slurm job is a separate in-process asyncssh container
endpoint reached through the mux stream, so the tests prove the acceptance
properties without a real cluster or Docker:

* two jobs run exec and file transfer concurrently while the login route shows
  ``max_channels == 1`` and exactly one authentication;
* exec, SFTP and PTY use the job-keyed connection, never the target cache;
* stopping one job closes only its sessions/files and leaves another job's
  transfer and PTY usable;
* file routes reject unknown and foreign ids with one identical answer and an
  omitted id selects the shared job only;
* a direct Docker target keeps the legacy transport and job service untouched.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from types import SimpleNamespace

import asyncssh
import pytest
from aiohttp.test_utils import TestClient, TestServer

from compute_mcp import jobs
from compute_mcp.auth import hash_token
from compute_mcp.config import (
    BundleConfig,
    ClientConfig,
    GatewayConfig,
    ServerConfig,
    SessionConfig,
    SlurmConfig,
    SlurmStageConfig,
    SSHConfig,
    TargetConfig,
    TransportConfig,
)
from compute_mcp.files import JOB_ACCESS_DENIED_MESSAGE
from compute_mcp.gateway import Gateway
from compute_mcp.mux import Multiplexer

pytestmark = pytest.mark.skipif(
    not hasattr(asyncssh, "listen") or not hasattr(asyncssh, "SSHServer"),
    reason="asyncssh server support unavailable",
)

CONNECT_TIMEOUT = 15.0
FINGERPRINT = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"


# ---------------------------------------------------------------------------
# In-process login/route server with an in-process mux helper
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
    """Stand-in for ``computemcp-mux.py`` on the login node.

    Speaks the real gateway wire protocol with ``initiator=False`` and forwards
    each ``OPEN(job_id)`` to the loopback endpoint in ``relay_registry``.  The
    gateway writes that registry through ``route_registry_sink``.
    """

    def __init__(self, session: "_LoginSession") -> None:
        self.session = session
        self.state = session.state
        self.reader = session._mux_reader
        self.writer = _ChannelWriter(session._chan)
        self.mux = None
        self._serving: set[asyncio.Task] = set()

    async def run(self) -> None:
        self.mux = Multiplexer(self.reader, self.writer, initiator=False)
        await self.mux.start()
        self.state["mux_starts"] = self.state.get("mux_starts", 0) + 1
        try:
            while True:
                stream = await self.mux.accept()
                self.state["mux_streams"] = self.state.get("mux_streams", 0) + 1
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
                        asyncio.gather(*self._serving, return_exceptions=True),
                        2.0,
                    )

    async def _serve(self, stream) -> None:
        endpoint = (self.state.get("relay_registry") or {}).get(stream.job_id)
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


class _LoginSession(asyncssh.SSHServerSession):
    def __init__(self, state: dict) -> None:
        self._chan = None
        self.state = state
        self._command = ""
        self._is_mux = False
        self._mux_task: asyncio.Task | None = None
        self._mux_reader: _ChannelReader | None = None

    def connection_made(self, chan) -> None:
        self._chan = chan
        self.state["live_channels"] = self.state.get("live_channels", 0) + 1
        self.state["max_channels"] = max(
            self.state.get("max_channels", 0), self.state["live_channels"]
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
        if "computemcp-mux" in command:
            self._is_mux = True
            self.state["mux_execs"] = self.state.get("mux_execs", 0) + 1
        return True

    def session_started(self) -> None:
        if self._is_mux:
            self._mux_reader = _ChannelReader()
            self._mux_task = asyncio.create_task(
                _MuxHelperPeer(self).run()
            )
            return
        # Any other command is an unexpected extra outer channel; answer it so
        # the test can assert it never happens rather than hang.
        self.state["unexpected_commands"] = (
            self.state.get("unexpected_commands", 0) + 1
        )
        self._chan.write(b"unexpected\n")
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


class _LoginServer(asyncssh.SSHServer):
    def __init__(self, state: dict, *, password: str = "SECRET") -> None:
        self.state = state
        self.password = password

    def connection_made(self, conn) -> None:
        self.state["connections"] = self.state.get("connections", 0) + 1
        self.state["conn"] = conn

    def begin_auth(self, username: str) -> bool:
        return True

    def auth_completed(self) -> None:
        self.state["auths"] = self.state.get("auths", 0) + 1

    def password_auth_supported(self) -> bool:
        return True

    async def validate_password(self, username: str, password: str) -> bool:
        return password == self.password

    def connection_requested(self, dest_host, dest_port, orig_host, orig_port):
        # Every direct-tcpip channel would be a second outer channel.
        self.state["forwarded_channels"] = self.state.get(
            "forwarded_channels", 0
        ) + 1
        return False

    def session_requested(self):
        if self.state.get("live_channels", 0) >= 1:
            self.state["refused_channels"] = self.state.get(
                "refused_channels", 0
            ) + 1
            return False
        return _LoginSession(self.state)


class _Login:
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


async def _start_login(tmp_path) -> _Login:
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    host_path = os.path.join(str(tmp_path), "login_host_key")
    host_key.write_private_key(host_path)
    state: dict = {
        "commands": [],
        "connections": 0,
        "auths": 0,
        "live_channels": 0,
        "max_channels": 0,
        "relay_registry": {},
    }
    server = await asyncssh.listen(
        "127.0.0.1",
        0,
        server_host_keys=[host_path],
        server_factory=lambda: _LoginServer(state),
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


# ---------------------------------------------------------------------------
# In-process container endpoints (exec + PTY + SFTP)
# ---------------------------------------------------------------------------
class _ContainerServer(asyncssh.SSHServer):
    def __init__(self, state: dict) -> None:
        self.state = state

    def begin_auth(self, username: str) -> bool:
        return True

    def public_key_auth_supported(self) -> bool:
        return True

    async def validate_public_key(self, username, key) -> bool:
        return True


async def _start_container(tmp_path, host_key_path: str, label: str, files: dict):
    root = tmp_path / f"root-{label}"
    root.mkdir()
    for name, content in files.items():
        path = root / name.lstrip("/")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    state = {"label": label, "commands": []}

    async def process_factory(process):
        command = process.command
        if command is not None:
            state["commands"].append(command)
            process.stdout.write(f"{label}:{command}\n".encode())
            process.exit(0)
            return
        # Interactive shell (PTY): announce readiness, then echo input.
        process.stdout.write(b"ready\n")
        while True:
            data = await process.stdin.read(65536)
            if not data:
                break
            process.stdout.write(data)

    server = await asyncssh.listen(
        "127.0.0.1",
        0,
        server_host_keys=[host_key_path],
        server_factory=lambda: _ContainerServer(state),
        process_factory=process_factory,
        sftp_factory=lambda chan, r=root: asyncssh.SFTPServer(
            chan, chroot=str(r).encode()
        ),
        encoding=None,
    )
    return server, state, root


# ---------------------------------------------------------------------------
# Gateway harness
# ---------------------------------------------------------------------------
class FakeProvisioner:
    """Minimal stateful scheduler for monitor/cancel paths."""

    def __init__(self) -> None:
        self.states: dict[str, str] = {}
        self.cancelled: list[str] = []

    def _id(self, job) -> str:
        return job.slurm_job_id if isinstance(job, jobs.JobRecord) else str(job)

    async def observe(self, job):
        return self.states.get(self._id(job), "RUNNING")

    async def cancel(self, job):
        slurm_id = self._id(job)
        self.states[slurm_id] = "CANCELLED"
        self.cancelled.append(slurm_id)

    async def attach(self, record):
        return None

    async def submit(self, record):
        raise AssertionError("the wiring tests inject ready records")

    async def detach(self, record):
        return None

    async def find_by_token(self, token):
        return None


def _target(login: _Login, container_pin: str, client_key: str, deploy_dir: str):
    return TargetConfig(
        name="frontier",
        user="agent",
        container_user="ubuntu",
        transport=TransportConfig(
            kind="tunnel",
            ssh_targets=("login",),
            remote_host="127.0.0.1",
            remote_port=1,
        ),
        route_host_key_sha256=login.pin,
        host_key_sha256=container_pin,
        client_key=client_key,
        interactive_auth=True,
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(options={"ntasks-per-node": 1}),
            srun=SlurmStageConfig(),
        ),
        bundle=BundleConfig(source="computemcp-slurm", deploy_dir=deploy_dir),
    )


def _config(target: TargetConfig) -> GatewayConfig:
    clients = {
        "agent": ClientConfig(
            client_id="agent",
            token_sha256=hash_token("agent-token"),
            targets=("frontier",),
        ),
        "other": ClientConfig(
            client_id="other",
            token_sha256=hash_token("other-token"),
            targets=("frontier",),
        ),
        "admin": ClientConfig(
            client_id="admin",
            token_sha256=hash_token("admin-token"),
            allow_all=True,
        ),
    }
    return GatewayConfig(
        server=ServerConfig(),
        ssh=SSHConfig(),
        sessions=SessionConfig(),
        targets={"frontier": target},
        clients=clients,
    )


def _seed(gw: Gateway, label, endpoint, *, kind="private", owner="agent"):
    record = jobs.JobRecord(
        job_id=jobs.new_job_id(),
        request_id=f"req-{label}",
        owner_client_id=owner,
        target="frontier",
        kind=kind,
        state=jobs.READY,
        slurm_job_id=label,
        node="cn1",
        relay_endpoint=endpoint,
        generation=1,
    )
    gw.job_store.save_record(record)
    return record


def _registry_sink(login: _Login):
    def sink(_target_name: str, mapping: dict[str, str]) -> None:
        converted = {}
        for job_id, endpoint in mapping.items():
            host, _, port = endpoint.rpartition(":")
            converted[job_id] = (host, int(port))
        login.state["relay_registry"] = converted

    return sink


@contextlib.asynccontextmanager
async def _wired(tmp_path, monkeypatch, *, b_owner="agent", shared=True):
    """Two private jobs (A, B) plus an optional shared job (S)."""
    login = await _start_login(tmp_path)
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    host_key_path = str(tmp_path / "container_host_key")
    host_key.write_private_key(host_key_path)
    container_pin = host_key.get_fingerprint("sha256")
    client_key = asyncssh.generate_private_key("ssh-ed25519")
    client_key_path = str(tmp_path / "client_key")
    client_key.write_private_key(client_key_path)

    labels = ["A", "B"] + (["S"] if shared else [])
    containers = {}
    for label in labels:
        files = {
            f"/{label}.txt": label.encode(),
            "/shared.txt": b"shared" if label == "S" else label.encode(),
        }
        server, state, root = await _start_container(
            tmp_path, host_key_path, label, files
        )
        containers[label] = SimpleNamespace(server=server, state=state, root=root)

    _route_alias(monkeypatch, login)
    target = _target(login, container_pin, client_key_path, str(tmp_path / "cmcp"))
    gw = Gateway(_config(target), state_root=tmp_path / "state", job_poll_interval=0.05)
    gw._ensure_jobs()
    gw.provisioner = FakeProvisioner()
    gw.route_registry_sink = _registry_sink(login)

    records = {
        "A": _seed(gw, "A", f"127.0.0.1:{containers['A'].server.get_port()}"),
        "B": _seed(
            gw,
            "B",
            f"127.0.0.1:{containers['B'].server.get_port()}",
            owner=b_owner,
        ),
    }
    if shared:
        records["S"] = _seed(
            gw,
            "S",
            f"127.0.0.1:{containers['S'].server.get_port()}",
            kind="shared",
        )
    for record in records.values():
        gw.provisioner.states[record.slurm_job_id] = "RUNNING"

    await asyncio.wait_for(gw.open_target("frontier", factor="SECRET"), CONNECT_TIMEOUT)
    client = TestClient(TestServer(gw.create_app()))
    await client.start_server()
    try:
        yield SimpleNamespace(
            gw=gw,
            client=client,
            login=login,
            containers=containers,
            records=records,
            target=target,
        )
    finally:
        with contextlib.suppress(Exception):
            await client.close()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(gw.stop(), 10)
        for container in containers.values():
            container.server.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(container.server.wait_closed(), 5)
        with contextlib.suppress(Exception):
            await login.close()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# One outer channel, two jobs, concurrent exec + transfer
# ---------------------------------------------------------------------------
async def test_two_jobs_concurrent_exec_and_transfer_single_channel(
    tmp_path, monkeypatch
):
    async with _wired(tmp_path, monkeypatch) as h:
        a, b = h.records["A"].job_id, h.records["B"].job_id
        exec_a, exec_b, upload_a, read_b = await asyncio.gather(
            h.client.post(
                "/v1/exec",
                headers=_auth("agent-token"),
                json={"target": "frontier", "job_id": a, "command": "who"},
            ),
            h.client.post(
                "/v1/exec",
                headers=_auth("agent-token"),
                json={"target": "frontier", "job_id": b, "command": "who"},
            ),
            h.client.put(
                "/v1/files/upload",
                headers=_auth("agent-token"),
                params={"target": "frontier", "job_id": a, "path": "/up.txt"},
                data=b"payload-a",
            ),
            h.client.get(
                "/v1/files/read",
                headers=_auth("agent-token"),
                params={"target": "frontier", "job_id": b, "path": "/B.txt"},
            ),
        )
        assert exec_a.status == 200, await exec_a.text()
        assert exec_b.status == 200, await exec_b.text()
        body_a = await exec_a.json()
        body_b = await exec_b.json()
        assert body_a["stdout"] == "A:who\n"
        assert body_b["stdout"] == "B:who\n"
        assert upload_a.status == 200, await upload_a.text()
        assert (await upload_a.json())["written"] == len(b"payload-a")
        assert read_b.status == 200, await read_b.text()
        assert (await read_b.json())["content"] == "B"

        # Each job's endpoint saw only its own traffic.
        assert h.containers["A"].root.joinpath("up.txt").read_bytes() == b"payload-a"
        assert h.containers["B"].state["commands"] == ["who"]
        assert h.containers["A"].state["commands"] == ["who"]

        # Exactly one authenticated outer channel; no extra sessions or forwards.
        assert h.login.state["auths"] == 1
        assert h.login.state["connections"] == 1
        assert h.login.state["max_channels"] == 1
        assert h.login.state["mux_execs"] == 1
        assert h.login.state.get("unexpected_commands", 0) == 0
        assert h.login.state.get("forwarded_channels", 0) == 0
        assert h.login.state.get("refused_channels", 0) == 0


async def test_exec_uses_job_connection_not_target_cache(tmp_path, monkeypatch):
    async with _wired(tmp_path, monkeypatch) as h:
        a = h.records["A"].job_id
        resp = await h.client.post(
            "/v1/exec",
            headers=_auth("agent-token"),
            json={"target": "frontier", "job_id": a, "command": "uname"},
        )
        assert resp.status == 200, await resp.text()
        # The job-keyed cache is used; the legacy target-keyed cache stays empty.
        assert h.gw.backend.known_targets() == []
        assert (a, 1) in h.gw.backend.known_jobs()
        assert h.containers["A"].state["commands"] == ["uname"]
        assert h.containers["B"].state["commands"] == []


# ---------------------------------------------------------------------------
# Stopping B closes only B; A's transfer and PTY survive
# ---------------------------------------------------------------------------
async def test_job_stop_is_isolated_between_jobs(tmp_path, monkeypatch):
    async with _wired(tmp_path, monkeypatch) as h:
        a, b = h.records["A"].job_id, h.records["B"].job_id
        sess_a = await h.client.post(
            "/v1/sessions",
            headers=_auth("agent-token"),
            json={"target": "frontier", "job_id": a},
        )
        sess_b = await h.client.post(
            "/v1/sessions",
            headers=_auth("agent-token"),
            json={"target": "frontier", "job_id": b},
        )
        assert sess_a.status == 200, await sess_a.text()
        assert sess_b.status == 200, await sess_b.text()
        sid_a = (await sess_a.json())["session_id"]
        sid_b = (await sess_b.json())["session_id"]

        # Touch both jobs' file state so close_job has something to retire.
        for job_id in (a, b):
            resp = await h.client.get(
                "/v1/files/stat",
                headers=_auth("agent-token"),
                params={"target": "frontier", "job_id": job_id, "path": "/shared.txt"},
            )
            assert resp.status == 200, await resp.text()
        assert set(h.gw.files._states) == {a, b}

        conn_a = await h.gw.backend.job_connection(
            h.target, a, "127.0.0.1", h.containers["A"].server.get_port(),
            generation=1, transport=h.gw.runtimes["frontier"].tunnel.mux,
        )

        stopped = await h.client.post(
            f"/v1/jobs/{b}/cancel", headers=_auth("agent-token")
        )
        assert stopped.status == 200, await stopped.text()

        # B's PTY, file state and cached connection are gone.
        assert sid_b not in h.gw.sessions._sessions
        assert b not in h.gw.files._states
        assert (b, 1) not in h.gw.backend.known_jobs()

        # A's PTY, file state, cached connection and file access are untouched.
        assert sid_a in h.gw.sessions._sessions
        assert a in h.gw.files._states
        assert conn_a.is_closed() is False
        read_a = await h.client.get(
            "/v1/files/read",
            headers=_auth("agent-token"),
            params={"target": "frontier", "job_id": a, "path": "/A.txt"},
        )
        assert read_a.status == 200, await read_a.text()
        assert (await read_a.json())["content"] == "A"

        # Stopping A now closes the remaining session.
        await h.client.post(f"/v1/jobs/{a}/cancel", headers=_auth("agent-token"))
        assert sid_a not in h.gw.sessions._sessions


# ---------------------------------------------------------------------------
# File route authorization: foreign and unknown ids are identical
# ---------------------------------------------------------------------------
async def test_file_routes_reject_foreign_and_unknown_identically(
    tmp_path, monkeypatch
):
    async with _wired(tmp_path, monkeypatch, b_owner="other") as h:
        foreign = h.records["B"].job_id
        unknown = "job-" + "0" * 32
        responses = []
        for job_id in (foreign, unknown):
            responses.append(
                await h.client.get(
                    "/v1/files/stat",
                    headers=_auth("agent-token"),
                    params={
                        "target": "frontier",
                        "job_id": job_id,
                        "path": "/shared.txt",
                    },
                )
            )
        assert [r.status for r in responses] == [404, 404]
        texts = [await r.text() for r in responses]
        assert texts[0] == texts[1] == JOB_ACCESS_DENIED_MESSAGE
        # No resolver/network work happened for either rejected id.
        assert h.gw.files._states == {}
        assert h.gw.backend.known_jobs() == []


async def test_omitted_job_id_selects_shared_only(tmp_path, monkeypatch):
    async with _wired(tmp_path, monkeypatch) as h:
        # A and S both have /shared.txt with different contents.
        resp = await h.client.get(
            "/v1/files/read",
            headers=_auth("agent-token"),
            params={"target": "frontier", "path": "/shared.txt"},
        )
        assert resp.status == 200, await resp.text()
        assert (await resp.json())["content"] == "shared"
        # The shared job's connection was used, not a private job's.
        assert (h.records["S"].job_id, 1) in h.gw.backend.known_jobs()
        assert (h.records["A"].job_id, 1) not in h.gw.backend.known_jobs()


# ---------------------------------------------------------------------------
# Direct Docker target keeps the legacy path and never builds job services
# ---------------------------------------------------------------------------
async def test_docker_legacy_path_is_unchanged(tmp_path, monkeypatch):
    target = TargetConfig(
        name="hal",
        user="agent",
        transport=TransportConfig(
            kind="direct", remote_host="127.0.0.1", remote_port=9
        ),
        host_key_sha256=FINGERPRINT,
    )
    cfg = GatewayConfig(
        server=ServerConfig(),
        ssh=SSHConfig(),
        sessions=SessionConfig(),
        targets={"hal": target},
        clients={
            "admin": ClientConfig(
                client_id="admin",
                token_sha256=hash_token("admin-token"),
                allow_all=True,
            )
        },
    )
    gw = Gateway(cfg)
    assert gw.jobs is None

    class _Result:
        exit_status = 0
        stdout = b"legacy\n"
        stderr = b""

    async def fake_run(conn, command, cwd=None, timeout=None, env=None, stdin=None):
        return _Result()

    async def fake_provider(name):
        return gw.config.targets[name], object()

    class FakeSFTP:
        def __init__(self):
            self.calls = []

        def exit(self):
            pass

        async def chmod(self, path, mode):
            self.calls.append((path, mode))

    holder = {}

    class FakeSftpCtx:
        def __init__(self, conn):
            self._sftp = FakeSFTP()
            holder["sftp"] = self._sftp

        async def __aenter__(self):
            return self._sftp

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(gw.backend, "run", fake_run)
    monkeypatch.setattr(gw, "_connection_provider", fake_provider)
    monkeypatch.setattr(gw.backend, "sftp_client", lambda conn: FakeSftpCtx(conn))

    client = TestClient(TestServer(gw.create_app()))
    await client.start_server()
    try:
        resp = await client.post(
            "/v1/exec",
            headers=_auth("admin-token"),
            json={"target": "hal", "command": "hostname"},
        )
        assert resp.status == 200, await resp.text()
        assert (await resp.json())["stdout"] == "legacy\n"

        # A legacy target keeps its historical behaviour: an unknown job_id is
        # not threaded into any job-scoped service (the old endpoint ignored it).
        ignored = await client.post(
            "/v1/exec",
            headers=_auth("admin-token"),
            json={"target": "hal", "command": "x", "job_id": "job-" + "0" * 32},
        )
        assert ignored.status == 200

        resp = await client.post(
            "/v1/files/chmod",
            headers=_auth("admin-token"),
            params={"target": "hal"},
            json={"path": "/work/x.sh", "mode": "755"},
        )
        assert resp.status == 200, await resp.text()
        assert holder["sftp"].calls == [("/work/x.sh", 0o755)]
        # The job-scoped file service was never touched.
        assert gw.files.job_count() == 0
        assert gw.jobs is None
    finally:
        await client.close()

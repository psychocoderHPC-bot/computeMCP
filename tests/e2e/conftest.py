# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Shared in-process end-to-end harness for the multi-job validation suite.

This module builds the highest-fidelity stack that runs without a real cluster:

* a real :class:`compute_mcp.gateway.Gateway` (with its real HTTP app, job
  registry, quota manager, PTY/SFTP services and lifecycle),
* the shipped :class:`compute_mcp.gateway.SlurmProvisioner` pointed at a
  stateful fake Slurm queue (``tests/fakes/fake_slurm.py``),
* an in-process AsyncSSH *login* server whose exec channel runs the scheduler
  commands, and an in-process AsyncSSH *container* server with exec, PTY and
  SFTP support.

Scheduler calls really travel over SSH (login), container exec/file/PTY calls
really travel over SSH (container), and every job gets a distinct container
endpoint published through the normal attach/readiness path.  Nothing here
reaches a network host outside loopback.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

import asyncssh
import pytest
from aiohttp.test_utils import TestClient, TestServer

# Make ``tests/fakes`` importable exactly like the top-level tests do.
_TESTS_DIR = Path(__file__).resolve().parents[1]
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))

from fakes.conftest import fake_slurm_env  # noqa: E402,F401  (re-exported)

from compute_mcp.auth import hash_token  # noqa: E402
from compute_mcp.config import (  # noqa: E402
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
from compute_mcp.gateway import Gateway, SlurmProvisioner  # noqa: E402

FINGERPRINT = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"

A_TOKEN = "agent-a-token"
B_TOKEN = "agent-b-token"
ADMIN_TOKEN = "admin-token"


def auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# AsyncSSH login node: execs a command with the fake Slurm environment
# ---------------------------------------------------------------------------
class _LoginSession(asyncssh.SSHServerSession):
    def __init__(self, env: dict[str, str]) -> None:
        self._env = env
        self._command: str | None = None

    def exec_requested(self, command: str) -> bool:
        self._command = command
        return True

    def connection_made(self, chan) -> None:
        self._chan = chan

    def session_started(self) -> None:
        assert self._chan is not None
        if not self._command:
            self._chan.exit(1)
            return
        argv = shlex.split(self._command)
        proc = subprocess.run(
            argv,
            env=self._env,
            cwd=None,
            capture_output=True,
        )
        if proc.stdout:
            self._chan.write(proc.stdout)
        if proc.stderr:
            self._chan.write_stderr(proc.stderr)
        self._chan.exit(proc.returncode)


class _LoginServer(asyncssh.SSHServer):
    def __init__(self, env: dict[str, str]) -> None:
        self._env = env
        self.connections = 0
        self.authentications = 0

    def connection_made(self, conn) -> None:
        self.connections += 1

    def public_key_auth_supported(self) -> bool:
        return True

    async def validate_public_key(self, username, key) -> bool:
        self.authentications += 1
        return True

    def session_requested(self):
        return _LoginSession(self._env)


# ---------------------------------------------------------------------------
# AsyncSSH container: exec, PTY shell and SFTP over a chroot
# ---------------------------------------------------------------------------
class _ContainerSFTPFactory(asyncssh.SFTPServerFactory):
    def __init__(self, root: str) -> None:
        self._root = root

    def __call__(self, chan):
        return asyncssh.SFTPServer(chan, chroot=self._root)


class _ContainerProcess:
    """Serve exec, PTY shell and echo over one AsyncSSH server process.

    ``process_factory`` is used (instead of ``session_requested``) because
    AsyncSSH's SFTP subsystem factory and a custom site session handler are
    mutually exclusive: when ``sftp_factory`` is configured, incoming sessions
    go through ``SSHServerProcess`` and the server's ``session_requested`` is
    never consulted.
    """

    def __init__(self, user: str) -> None:
        self._user = user

    def __call__(self, process) -> None:
        asyncio.create_task(self._run(process))
        return None

    async def _run(self, process) -> None:
        command = process.command
        if command is None:
            # PTY shell: emit a prompt, then echo whatever is typed.
            process.stdout.write(b"container$ ")
            await process.stdout.drain()
            while True:
                data = await process.stdin.read(4096)
                if not data:
                    break
                process.stdout.write(data)
                await process.stdout.drain()
            process.exit(0)
            return
        text = command.decode() if isinstance(command, bytes) else command
        text = text.strip()
        if text.startswith("whoami"):
            out = (self._user + "\n").encode()
        elif text in ("true", ""):
            out = b""
        else:
            out = b"ok\n"
        process.stdout.write(out)
        await process.stdout.drain()
        process.exit(0)


class _ContainerServer(asyncssh.SSHServer):
    def __init__(self, user: str) -> None:
        self._user = user

    def public_key_auth_supported(self) -> bool:
        return True

    async def validate_public_key(self, username, key) -> bool:
        return username == self._user


class SSHHarness:
    """Two loopback SSH servers plus credential files for one test."""

    def __init__(self, root: Path, fake_env) -> None:
        self.root = root
        self.fake_env = fake_env
        self.login_server = None
        self.container_server = None
        self.login_server_obj: _LoginServer | None = None
        self.container_server_obj: _ContainerServer | None = None
        self.login_port: int | None = None
        self.container_port: int | None = None
        self.login_client_key: str | None = None
        self.container_client_key: str | None = None
        self.container_pin: str | None = None
        self.container_root: str | None = None
        # Populated by the provisioner so tests can inspect real login auths.
        self.login_connections: list[Any] = []

    async def start(self) -> "SSHHarness":
        self.root.mkdir(parents=True, exist_ok=True)
        # --- login node -------------------------------------------------
        login_host_key = asyncssh.generate_private_key("ssh-ed25519")
        login_client_key = asyncssh.generate_private_key("ssh-ed25519")
        login_host_path = self.root / "login_host_key"
        login_client_path = self.root / "login_client_key"
        login_host_key.write_private_key(str(login_host_path))
        login_client_key.write_private_key(str(login_client_path))
        self.login_client_key = str(login_client_path)
        self.login_server_obj = _LoginServer(self.fake_env.environment())
        self.login_server = await asyncssh.listen(
            "127.0.0.1",
            0,
            server_host_keys=[str(login_host_path)],
            server_factory=lambda: self.login_server_obj,
            encoding=None,
        )
        self.login_port = self.login_server.get_port()

        # --- container --------------------------------------------------
        container_host_key = asyncssh.generate_private_key("ssh-ed25519")
        container_client_key = asyncssh.generate_private_key("ssh-ed25519")
        container_host_path = self.root / "container_host_key"
        container_client_path = self.root / "container_client_key"
        container_host_key.write_private_key(str(container_host_path))
        container_client_key.write_private_key(str(container_client_path))
        self.container_client_key = str(container_client_path)
        self.container_pin = container_host_key.get_fingerprint("sha256")
        self.container_root = str(self.root / "containerfs")
        os.makedirs(self.container_root, exist_ok=True)
        self.container_server_obj = _ContainerServer("agent")
        self.container_server = await asyncssh.listen(
            "127.0.0.1",
            0,
            server_host_keys=[str(container_host_path)],
            server_factory=lambda: self.container_server_obj,
            process_factory=_ContainerProcess("agent"),
            sftp_factory=_ContainerSFTPFactory(self.container_root),
            line_editor=False,
            encoding=None,
        )
        self.container_port = self.container_server.get_port()
        return self

    async def stop(self) -> None:
        for server in (self.login_server, self.container_server):
            if server is not None:
                server.close()
                with contextlib.suppress(Exception):
                    await server.wait_closed()

    # -- helpers -----------------------------------------------------------
    def mark_running(self, slurm_id: str) -> None:
        result = self.fake_env.run("slurmctl", "set-state", slurm_id, "RUNNING")
        assert result.returncode == 0, result.stderr

    def mark_terminal(self, slurm_id: str, state: str = "COMPLETED") -> None:
        result = self.fake_env.run("slurmctl", "set-state", slurm_id, state)
        assert result.returncode == 0, result.stderr

    def jobs(self) -> dict[str, dict]:
        state = self.fake_env.read_state()
        return dict((state or {}).get("jobs") or {})

    def sbatch_count(self) -> int:
        return sum(1 for entry in self.fake_env.read_log()
                   if entry.get("command") == "sbatch")

    def scancel_count(self) -> int:
        return sum(1 for entry in self.fake_env.read_log()
                   if entry.get("command") == "scancel")

    def find_by_comment(self, token: str) -> str | None:
        for job_id, job in self.jobs().items():
            if job.get("comment") == token:
                return str(job_id)
        return None


# ---------------------------------------------------------------------------
# Real SlurmProvisioner wired to the in-process login node
# ---------------------------------------------------------------------------
class E2EProvisioner:
    """Real scheduler adapter over the login SSH channel.

    ``submit``/``observe``/``cancel`` delegate to the shipped
    :class:`SlurmProvisioner`; only ``attach`` is extended to publish the
    per-job container endpoint the fake scheduler cannot know.  ``find_by_token``
    reads the fake cluster's stored ``--comment`` (a stand-in for a site where
    the scheduler exposes the job comment, e.g. ``sacct`` / ``squeue %k``).
    """

    def __init__(self, harness: SSHHarness) -> None:
        self.harness = harness
        self._conn: Any = None
        self.submit_calls = 0
        self.cancel_calls = 0
        self.attach_calls = 0
        self._slurm = SlurmProvisioner(run=self._run)

    async def _login(self):
        if self._conn is None or self._conn.is_closed():
            self._conn = await asyncssh.connect(
                "127.0.0.1",
                self.harness.login_port,
                username="login",
                client_keys=[self.harness.login_client_key],
                known_hosts=None,
            )
            self.harness.login_connections.append(self._conn)
        return self._conn

    async def _run(self, argv):
        conn = await self._login()
        command = " ".join(shlex.quote(str(part)) for part in argv)
        result = await conn.run(command, check=False)
        return (
            result.exit_status,
            result.stdout or "",
            result.stderr or "",
        )

    async def submit(self, record):
        self.submit_calls += 1
        return await self._slurm.submit(record)

    async def observe(self, job):
        return await self._slurm.observe(job)

    async def cancel(self, job):
        self.cancel_calls += 1
        return await self._slurm.cancel(job)

    async def detach(self, record):
        # Detach must never signal the scheduler.
        return None

    async def find_by_token(self, token):
        return self.harness.find_by_comment(token)

    async def attach(self, record):
        self.attach_calls += 1
        jobs = self.harness.jobs()
        job = jobs.get(str(record.slurm_job_id))
        if job is None or job.get("state") != "RUNNING":
            return None
        return {
            "node": "cn01",
            "relay_endpoint": "127.0.0.1:4100",
            "container_endpoint": f"127.0.0.1:{self.harness.container_port}",
            "generation": 1,
        }

    async def close(self):
        if self._conn is not None:
            self._conn.close()
            with contextlib.suppress(Exception):
                await self._conn.wait_closed()
            self._conn = None


# ---------------------------------------------------------------------------
# Gateway factory
# ---------------------------------------------------------------------------
def make_target(harness: SSHHarness, name: str = "frontier", **overrides) -> TargetConfig:
    values = dict(
        name=name,
        user="login",
        container_user="agent",
        transport=TransportConfig(
            kind="tunnel", ssh_targets=(name,)
        ),
        client_key=harness.container_client_key,
        host_key_sha256=harness.container_pin,
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(options={"ntasks-per-node": 1}),
            srun=SlurmStageConfig(),
        ),
        bundle=BundleConfig(source="computemcp-slurm", deploy_dir="/tmp/cmcp-e2e"),
    )
    values.update(overrides)
    return TargetConfig(**values)


def make_config(
    target: TargetConfig,
    *,
    job_limits: dict[str, dict[str, int]] | None = None,
) -> GatewayConfig:
    limits = dict(job_limits or {})
    clients = {
        "agent-a": ClientConfig(
            client_id="agent-a",
            token_sha256=hash_token(A_TOKEN),
            targets=(target.name,),
            job_limits=dict(limits.get("agent-a", {})),
        ),
        "agent-b": ClientConfig(
            client_id="agent-b",
            token_sha256=hash_token(B_TOKEN),
            targets=(target.name,),
            job_limits=dict(limits.get("agent-b", {})),
        ),
        "admin": ClientConfig(
            client_id="admin", token_sha256=hash_token(ADMIN_TOKEN), allow_all=True
        ),
    }
    return GatewayConfig(
        server=ServerConfig(),
        ssh=SSHConfig(),
        sessions=SessionConfig(),
        targets={target.name: target},
        clients=clients,
    )


class _RouteTunnel:
    """Tunnel-shaped route stand-in with no mux.

    The real login hop is exercised by the provisioner's SSH exec connection;
    this object only satisfies the gateway's route bookkeeping and forces a
    direct, per-job container dial (``mux is None``).
    """

    def __init__(self, target: TargetConfig, route: str) -> None:
        self.target = target
        self.route = route
        self.local_port = None
        self.provisioned_endpoint = None
        self.connection = object()
        self.mux = None
        self._alive = True

    def is_alive(self) -> bool:
        return self._alive

    async def stop(self, *, close_connection: bool = True) -> None:
        self._alive = False


async def _route_opener(target, route, factor=None, generation=0):
    return _RouteTunnel(target, route)


def make_gateway(
    state_root: Path,
    target: TargetConfig,
    provisioner: E2EProvisioner,
    *,
    job_limits: dict[str, dict[str, int]] | None = None,
) -> Gateway:
    gateway = Gateway(
        make_config(target, job_limits=job_limits),
        state_root=state_root,
        provisioner=provisioner,
        job_poll_interval=0.02,
    )
    gateway.route_opener = _route_opener
    return gateway


@contextlib.asynccontextmanager
async def running_client(gateway: Gateway):
    client = TestClient(TestServer(gateway.create_app()))
    await client.start_server()
    try:
        yield client
    finally:
        await client.close()


async def start_harness(root: Path, fake_env) -> SSHHarness:
    return await SSHHarness(root, fake_env).start()


@pytest.fixture
async def e2e_harness(tmp_path, fake_slurm_env):
    harness = await SSHHarness(tmp_path / "ssh", fake_slurm_env).start()
    try:
        yield harness
    finally:
        await harness.stop()


@pytest.fixture
async def e2e_gateway_factory(e2e_harness, tmp_path):
    """Build gateways on one durable state root; closes provisioners at end."""
    provisioners: list[E2EProvisioner] = []

    def build(
        *,
        state_root: Path | None = None,
        target: TargetConfig | None = None,
        job_limits: dict[str, dict[str, int]] | None = None,
    ) -> tuple[Gateway, E2EProvisioner]:
        prov = E2EProvisioner(e2e_harness)
        provisioners.append(prov)
        gw = make_gateway(
            state_root or (tmp_path / "state"),
            target or make_target(e2e_harness),
            prov,
            job_limits=job_limits,
        )
        return gw, prov


    try:
        yield build
    finally:
        for prov in provisioners:
            await prov.close()

# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Gateway route/job split, shared/private jobs, quota and detach (T6a).

The durable scheduler is injected: :class:`FakeProvisioner` is a stateful Slurm
stand-in (submit/observe/attach/cancel/detach), so these tests never touch a
real cluster.  One focused pair also drives the real ``SlurmProvisioner`` argv
through the stateful fake Slurm harness.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path

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
    SSHConfig,
    SlurmConfig,
    SlurmStageConfig,
    TargetConfig,
    TransportConfig,
)
from compute_mcp.gateway import (
    JOB_NOT_FOUND_TEXT,
    Gateway,
    SlurmProvisioner,
)

_TEST_DIR = Path(__file__).resolve().parent
if str(_TEST_DIR) not in sys.path:
    sys.path.insert(0, str(_TEST_DIR))

from fakes.conftest import fake_slurm_env  # noqa: E402,F401

FINGERPRINT = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"


# ---------------------------------------------------------------------------
# Fake scheduler + route
# ---------------------------------------------------------------------------
class FakeProvisioner:
    """Stateful Slurm stand-in with counters for every boundary operation."""

    def __init__(self) -> None:
        self.submit_calls = 0
        self.query_calls = 0
        self.attach_calls = 0
        self.cancel_calls = 0
        self.detach_calls = 0
        self._next = 0
        self.states: dict[str, str] = {}
        self.tokens: dict[str, str] = {}
        self.ready_payload: dict[str, dict] = {}
        self.submit_error: Exception | None = None

    async def submit(self, record):
        self.submit_calls += 1
        if self.submit_error is not None:
            raise self.submit_error
        self._next += 1
        slurm_id = str(1000 + self._next)
        self.states[slurm_id] = "PENDING"
        self.tokens[record.correlation_token] = slurm_id
        return f"{slurm_id};testcluster"

    async def observe(self, job):
        self.query_calls += 1
        slurm_id = job.slurm_job_id if isinstance(job, jobs.JobRecord) else str(job)
        return self.states.get(slurm_id)

    async def attach(self, record):
        self.attach_calls += 1
        slurm_id = record.slurm_job_id
        if self.states.get(slurm_id) == "RUNNING":
            return self.ready_payload.get(slurm_id)
        return None

    async def cancel(self, job):
        self.cancel_calls += 1
        slurm_id = job.slurm_job_id if isinstance(job, jobs.JobRecord) else str(job)
        if slurm_id:
            self.states[slurm_id] = "CANCELLED"

    async def detach(self, record):
        self.detach_calls += 1

    async def find_by_token(self, token):
        return self.tokens.get(token)

    def set_state(self, slurm_id: str, state: str) -> None:
        self.states[slurm_id] = state


class _FakeTunnel:
    def __init__(self, target, route):
        self.target = target
        self.route = route
        self.connection = object()
        self.local_port = None
        self.provisioned_endpoint = None
        self._alive = True

    def is_alive(self):
        return self._alive

    async def stop(self, *, close_connection=True):
        self._alive = False


async def _offline_route_opener(target, route, factor):
    return _FakeTunnel(target, route)


def _target(name="frontier", *, auto_connect=False):
    return TargetConfig(
        name=name,
        user="agent",
        transport=TransportConfig(kind="tunnel", ssh_targets=(name,)),
        host_key_sha256=FINGERPRINT,
        client_key="/tmp/does-not-exist-key",
        auto_connect=auto_connect,
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(options={"ntasks-per-node": 1}),
            srun=SlurmStageConfig(),
        ),
        bundle=BundleConfig(source="computemcp-slurm", deploy_dir="/tmp/cmcp"),
    )


def make_gateway(
    tmp_path,
    *,
    provisioner=None,
    job_limits=None,
    targets=None,
):
    targets = targets or {"frontier": _target()}
    limit_map = dict(job_limits or {})
    clients = {
        "agent": ClientConfig(
            client_id="agent",
            token_sha256=hash_token("agent-token"),
            targets=tuple(targets),
            job_limits=dict(limit_map),
        ),
        "other": ClientConfig(
            client_id="other",
            token_sha256=hash_token("other-token"),
            targets=tuple(targets),
            job_limits=dict(limit_map),
        ),
        "admin": ClientConfig(
            client_id="admin",
            token_sha256=hash_token("admin-token"),
            allow_all=True,
        ),
    }
    cfg = GatewayConfig(
        server=ServerConfig(),
        ssh=SSHConfig(),
        sessions=SessionConfig(),
        targets=targets,
        clients=clients,
    )
    gw = Gateway(
        cfg,
        state_root=tmp_path / "state",
        provisioner=provisioner,
        job_poll_interval=0.02,
    )
    gw.route_opener = _offline_route_opener
    return gw


def auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def make_client(gw: Gateway) -> TestClient:
    client = TestClient(TestServer(gw.create_app()))
    await client.start_server()
    return client


def agent(gw: Gateway):
    return gw.auth.authenticate_bearer("Bearer agent-token")


# ---------------------------------------------------------------------------
# (a) open / auto_connect never submit
# ---------------------------------------------------------------------------
async def test_open_and_auto_connect_submit_nothing(tmp_path):
    provisioner = FakeProvisioner()
    gw = make_gateway(tmp_path, provisioner=provisioner)
    opened = await gw.open_target("frontier")
    assert opened["state"] == "connected"
    assert gw._active_shared_record("frontier") is None
    await gw.poll_jobs_once()
    assert provisioner.submit_calls == 0
    await gw.stop()
    assert provisioner.cancel_calls == 0

    # auto_connect is open-only too.
    gw2 = make_gateway(
        tmp_path / "auto",
        provisioner=provisioner,
        targets={"frontier": _target(auto_connect=True)},
    )
    await gw2.start()
    await asyncio.sleep(0.05)
    assert gw2._active_shared_record("frontier") is None
    assert provisioner.submit_calls == 0
    await gw2.stop()
    assert provisioner.cancel_calls == 0


# ---------------------------------------------------------------------------
# (b) shared reuse and settings conflict
# ---------------------------------------------------------------------------
async def test_shared_reuse_and_conflict(tmp_path):
    provisioner = FakeProvisioner()
    gw = make_gateway(tmp_path, provisioner=provisioner)
    client = await make_client(gw)
    try:
        first = await client.post(
            "/v1/targets/frontier/shared", headers=auth("agent-token")
        )
        assert first.status == 200, await first.text()
        body1 = await first.json()
        assert body1["job"]["state"] == "pending"

        second = await client.post(
            "/v1/targets/frontier/shared", headers=auth("agent-token")
        )
        body2 = await second.json()
        assert body2["job_id"] == body1["job_id"]
        assert provisioner.submit_calls == 1

        # A different client converges on the same single shared job.
        third = await client.post(
            "/v1/targets/frontier/shared", headers=auth("other-token")
        )
        assert (await third.json())["job_id"] == body1["job_id"]
        assert provisioner.submit_calls == 1

        # Conflicting settings return the existing id and an error; no replace.
        conflicted = await client.post(
            "/v1/targets/frontier/shared",
            headers=auth("agent-token"),
            json={"set": {"gpus-per-node": 2}},
        )
        body3 = await conflicted.json()
        assert body3["job_id"] == body1["job_id"]
        assert body3["settings_conflict"] is True
        assert "error" in body3
        assert provisioner.submit_calls == 1

        # Only the operator may stop a shared job.
        denied = await client.post(
            f"/v1/jobs/{body1['job_id']}/cancel", headers=auth("agent-token")
        )
        assert denied.status == 403
        allowed = await client.post(
            f"/v1/jobs/{body1['job_id']}/cancel", headers=auth("admin-token")
        )
        assert allowed.status == 200, await allowed.text()
        assert provisioner.cancel_calls == 1
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# target-connect == open + shared, may return pending
# ---------------------------------------------------------------------------
async def test_connect_is_open_plus_shared(tmp_path):
    provisioner = FakeProvisioner()
    gw = make_gateway(tmp_path, provisioner=provisioner)
    client = await make_client(gw)
    try:
        first = await client.post(
            "/v1/targets/frontier/connect", headers=auth("agent-token")
        )
        assert first.status == 200, await first.text()
        body1 = await first.json()
        assert body1["state"] == "connected"
        assert body1["job_state"] == "pending"

        second = await client.post(
            "/v1/targets/frontier/connect", headers=auth("agent-token")
        )
        body2 = await second.json()
        assert body2["job_id"] == body1["job_id"]
        assert provisioner.submit_calls == 1
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# (c) quota last-slot race, 0 disables, default 1
# ---------------------------------------------------------------------------
async def test_quota_last_slot_race(tmp_path):
    provisioner = FakeProvisioner()
    gw = make_gateway(
        tmp_path, provisioner=provisioner, job_limits={"frontier": 1}
    )
    who = agent(gw)
    results = await asyncio.gather(
        gw.target_sbatch("frontier", who, request_id="race-1"),
        gw.target_sbatch("frontier", who, request_id="race-2"),
        return_exceptions=True,
    )
    winners = [item for item in results if isinstance(item, dict)]
    losers = [item for item in results if isinstance(item, jobs.QuotaExceeded)]
    assert len(winners) == 1
    assert len(losers) == 1
    assert provisioner.submit_calls == 1


async def test_quota_zero_disables_and_default_is_one(tmp_path):
    provisioner = FakeProvisioner()
    disabled = make_gateway(
        tmp_path / "zero", provisioner=provisioner, job_limits={"frontier": 0}
    )
    with pytest.raises(jobs.QuotaExceeded):
        await disabled.target_sbatch("frontier", agent(disabled))
    assert provisioner.submit_calls == 0

    default = make_gateway(tmp_path / "default", provisioner=provisioner)
    who = agent(default)
    await default.target_sbatch("frontier", who, request_id="one")
    with pytest.raises(jobs.QuotaExceeded):
        await default.target_sbatch("frontier", who, request_id="two")
    assert provisioner.submit_calls == 1


# ---------------------------------------------------------------------------
# (d) pending -> ready via the fake scheduler
# ---------------------------------------------------------------------------
async def test_pending_to_ready_via_fake_scheduler(tmp_path):
    provisioner = FakeProvisioner()
    gw = make_gateway(tmp_path, provisioner=provisioner)
    record = await gw.target_sbatch("frontier", agent(gw))
    assert record["state"] == "pending"
    slurm_id = record["slurm_job_id"]
    provisioner.set_state(slurm_id, "RUNNING")
    provisioner.ready_payload[slurm_id] = {
        "node": "cn17",
        "relay_endpoint": "127.0.0.1:4100",
        "generation": 2,
    }
    await gw.poll_jobs_once()
    updated = gw.job_store.load_record(record["job_id"])
    assert updated.state == "ready"
    assert updated.node == "cn17"
    assert updated.relay_endpoint == "127.0.0.1:4100"
    assert updated.generation == 2
    assert provisioner.attach_calls == 1


# ---------------------------------------------------------------------------
# (e) cancel pending; a late ready must not resurrect it
# ---------------------------------------------------------------------------
async def test_cancel_pending_and_late_ready(tmp_path):
    provisioner = FakeProvisioner()
    gw = make_gateway(tmp_path, provisioner=provisioner)
    who = agent(gw)
    record = await gw.target_sbatch("frontier", who)
    slurm_id = record["slurm_job_id"]
    cancelled = await gw.cancel_job(who, record["job_id"])
    assert cancelled["state"] == "stopping"
    assert provisioner.cancel_calls == 1

    # Slurm publishes RUNNING/ready after the cancel; the record must stay
    # stopping and never become ready.
    provisioner.set_state(slurm_id, "RUNNING")
    provisioner.ready_payload[slurm_id] = {
        "node": "cn17",
        "relay_endpoint": "127.0.0.1:4100",
    }
    await gw.poll_jobs_once()
    assert gw.job_store.load_record(record["job_id"]).state == "stopping"
    assert provisioner.attach_calls == 0

    provisioner.set_state(slurm_id, "CANCELLED")
    await gw.poll_jobs_once()
    assert gw.job_store.load_record(record["job_id"]).state == "terminal"
    assert provisioner.cancel_calls == 1


# ---------------------------------------------------------------------------
# (f) outage: shutdown detaches, restart restores ids and quota
# ---------------------------------------------------------------------------
async def test_outage_detach_and_restart_restore(tmp_path):
    provisioner = FakeProvisioner()
    gw = make_gateway(
        tmp_path, provisioner=provisioner, job_limits={"frontier": 2}
    )
    who = agent(gw)
    pending = await gw.target_sbatch("frontier", who, request_id="p1")
    running = await gw.target_sbatch("frontier", who, request_id="p2")
    provisioner.set_state(running["slurm_job_id"], "RUNNING")
    await gw.poll_jobs_once()
    assert gw.quota.counted("agent", "frontier") == 2

    await gw.stop()
    assert provisioner.cancel_calls == 0
    # Records survive the shutdown with their native ids.
    assert (
        gw.job_store.load_record(pending["job_id"]).slurm_job_id
        == pending["slurm_job_id"]
    )
    assert (
        gw.job_store.load_record(running["job_id"]).slurm_job_id
        == running["slurm_job_id"]
    )

    # Restart against the same state root: reconcile, never resubmit.
    restarted = make_gateway(
        tmp_path, provisioner=provisioner, job_limits={"frontier": 2}
    )
    await restarted.open_target("frontier")
    assert restarted.job_store.load_record(pending["job_id"]).slurm_job_id == (
        pending["slurm_job_id"]
    )
    assert restarted.quota.counted("agent", "frontier") == 2
    assert provisioner.submit_calls == 2
    await restarted.stop()
    assert provisioner.cancel_calls == 0


# ---------------------------------------------------------------------------
# (g) two clients: visibility and identical foreign/unknown errors
# ---------------------------------------------------------------------------
async def test_two_clients_visibility_and_auth(tmp_path):
    provisioner = FakeProvisioner()
    gw = make_gateway(
        tmp_path, provisioner=provisioner, job_limits={"frontier": 2}
    )
    client = await make_client(gw)
    try:
        a_private = await client.post(
            "/v1/targets/frontier/sbatch", headers=auth("agent-token")
        )
        a = await a_private.json()
        shared = await client.post(
            "/v1/targets/frontier/shared", headers=auth("agent-token")
        )
        s = await shared.json()
        b_private = await client.post(
            "/v1/targets/frontier/sbatch", headers=auth("other-token")
        )
        b = await b_private.json()

        listing_a = await client.get(
            "/v1/jobs", headers=auth("agent-token"), params={"target": "frontier"}
        )
        ids_a = {entry["job_id"] for entry in (await listing_a.json())["jobs"]}
        assert ids_a == {a["job_id"], s["job_id"]}

        listing_b = await client.get(
            "/v1/jobs", headers=auth("other-token"), params={"target": "frontier"}
        )
        ids_b = {entry["job_id"] for entry in (await listing_b.json())["jobs"]}
        assert ids_b == {b["job_id"], s["job_id"]}

        listing_admin = await client.get(
            "/v1/jobs", headers=auth("admin-token"), params={"target": "frontier"}
        )
        ids_admin = {
            entry["job_id"] for entry in (await listing_admin.json())["jobs"]
        }
        assert ids_admin == {a["job_id"], b["job_id"], s["job_id"]}

        # Foreign and unknown ids are indistinguishable.
        foreign = await client.get(
            f"/v1/jobs/{b['job_id']}", headers=auth("agent-token")
        )
        unknown = await client.get(
            f"/v1/jobs/{'job-' + '0' * 32}", headers=auth("agent-token")
        )
        assert foreign.status == unknown.status == 404
        assert (await foreign.text()) == (await unknown.text()) == JOB_NOT_FOUND_TEXT
        foreign_cancel = await client.post(
            f"/v1/jobs/{b['job_id']}/cancel", headers=auth("agent-token")
        )
        unknown_cancel = await client.post(
            f"/v1/jobs/{'job-' + '0' * 32}/cancel", headers=auth("agent-token")
        )
        assert foreign_cancel.status == unknown_cancel.status == 404
        assert (await foreign_cancel.text()) == JOB_NOT_FOUND_TEXT

        # An omitted id selects the shared job, never the caller's private one.
        resolved = gw.resolve_job(agent(gw), "frontier")
        assert resolved.job_id == s["job_id"]

        # A stale shared id never selects its replacement.
        await client.post(
            f"/v1/jobs/{s['job_id']}/cancel", headers=auth("admin-token")
        )
        provisioner.set_state(gw.job_store.load_record(s["job_id"]).slurm_job_id,
                              "CANCELLED")
        await gw.poll_jobs_once()
        replacement = await client.post(
            "/v1/targets/frontier/shared", headers=auth("agent-token")
        )
        new_shared = await replacement.json()
        assert new_shared["job_id"] != s["job_id"]
        stale = gw.resolve_job(agent(gw), "frontier", s["job_id"])
        assert stale.job_id == s["job_id"]
        assert gw.resolve_job(agent(gw), "frontier").job_id == new_shared["job_id"]
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# (h) non-Slurm direct-Docker keeps the legacy connect path
# ---------------------------------------------------------------------------
async def test_non_multijob_direct_regression(tmp_path, monkeypatch):
    target = TargetConfig(
        name="hal",
        user="agent",
        transport=TransportConfig(kind="direct", remote_host="127.0.0.1", remote_port=9),
        host_key_sha256=FINGERPRINT,
    )
    gw = make_gateway(
        tmp_path, provisioner=FakeProvisioner(), targets={"hal": target}
    )

    class _DirectTunnel:
        route = None
        local_port = 9
        provisioned_endpoint = None

        def is_alive(self):
            return True

        async def stop(self):
            return None

    async def fake_connect(target, on_route=None, factor=None, **kwargs):
        return _DirectTunnel()

    monkeypatch.setattr(gw.tunnels, "connect", fake_connect)
    result = await gw.connect_target("hal")
    assert result["state"] == "connected"
    # The non-Slurm path never creates the durable job registry.
    assert gw.jobs is None
    await gw.stop()


# ---------------------------------------------------------------------------
# Real adapter: exact scheduler argv and token reconciliation
# ---------------------------------------------------------------------------
async def test_slurm_provisioner_uses_parsable_comment_and_token_lookup():
    calls = []

    async def run(argv):
        calls.append(list(argv))
        if argv[0] == "sbatch":
            return (0, "413872;sitecluster", "")
        if argv[0] == "squeue":
            if "%T" in " ".join(argv):
                return (0, "PENDING\n", "")
            return (0, "413872 cmcp-token-1\n", "")
        return (0, "", "")

    provisioner = SlurmProvisioner(run=run)
    record = jobs.JobRecord(
        job_id=jobs.new_job_id(),
        request_id="r",
        owner_client_id="agent",
        target="frontier",
        kind="private",
        correlation_token="cmcp-token-1",
    )
    submitted = await provisioner.submit(record)
    assert submitted == "413872;sitecluster"
    assert "--parsable" in calls[0]
    assert f"--comment={record.correlation_token}" in calls[0]

    assert await provisioner.observe("413872") == "PENDING"
    assert await provisioner.find_by_token(record.correlation_token) == "413872"

    await provisioner.cancel("413872")
    assert calls[-1] == ["scancel", "413872"]
    before = len(calls)
    await provisioner.detach(record)
    assert len(calls) == before  # detach never signals the scheduler


async def test_slurm_provisioner_against_fake_slurm(fake_slurm_env):
    env = fake_slurm_env

    async def run(argv):
        proc = await asyncio.to_thread(
            subprocess.run,
            argv,
            env=env.environment(),
            capture_output=True,
            text=True,
        )
        return proc.returncode, proc.stdout, proc.stderr

    provisioner = SlurmProvisioner(run=run)
    record = jobs.JobRecord(
        job_id=jobs.new_job_id(),
        request_id="r",
        owner_client_id="agent",
        target="frontier",
        kind="private",
        parameters={"wrap": "true"},
    )
    submitted = await provisioner.submit(record)
    slurm_id, cluster = submitted.split(";")
    assert cluster == env.cluster
    assert await provisioner.observe(slurm_id) == "PENDING"

    sbatch_entries = [
        entry for entry in env.read_log() if entry["command"] == "sbatch"
    ]
    assert len(sbatch_entries) == 1
    argv = sbatch_entries[0]["argv"]
    assert "--parsable" in argv
    assert f"--comment={record.correlation_token}" in argv

    await provisioner.cancel(slurm_id)
    assert "scancel" in env.commands_logged()

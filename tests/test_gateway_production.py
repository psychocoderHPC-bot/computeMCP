# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Production gateway construction, bundle stages and job-selection wiring.

These tests exercise the paths the independent audit flagged: a durable state
root, the real T8 bundle-stage adapter, the cross-process shared-job lock, the
approval quota round-trip, 2FA forwarding, preview fields and clean-home
validation.  A fake stage runner replaces SSH/cluster execution; every other
piece is the production object graph built by ``build_production_gateway``.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp.test_utils import TestClient, TestServer

from compute_mcp import jobs
from compute_mcp.auth import hash_token
from compute_mcp.config import (
    BundleConfig,
    ClientConfig,
    ConfigError,
    GatewayConfig,
    ServerConfig,
    SessionConfig,
    SlurmConfig,
    SlurmStageConfig,
    SSHConfig,
    TargetConfig,
    TransportConfig,
    load_config,
)
from compute_mcp.gateway import (
    BundleStageProvisioner,
    Gateway,
    build_production_gateway,
)

FINGERPRINT = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
class FakeProvisioner:
    """Minimal stateful scheduler stand-in with boundary counters."""

    def __init__(self) -> None:
        self.submit_calls = 0
        self.attach_calls = 0
        self.cancel_calls = 0
        self.detach_calls = 0
        self._next = 100

    async def submit(self, record):
        self.submit_calls += 1
        self._next += 1
        return f"{self._next};site"

    async def observe(self, job):
        return "RUNNING"

    async def attach(self, record):
        self.attach_calls += 1
        return {"node": "cn01", "relay_endpoint": "127.0.0.1:4000"}

    async def cancel(self, job):
        self.cancel_calls += 1

    async def detach(self, record):
        self.detach_calls += 1


class _FakeTunnel:
    def __init__(self, target, route):
        self.target = target
        self.route = route
        self.connection = object()
        self.local_port = None
        self.provisioned_endpoint = None
        self.mux = None
        self._alive = True

    def is_alive(self):
        return self._alive

    async def stop(self, *, close_connection=True):
        self._alive = False


async def _offline_route_opener(target, route, factor):
    return _FakeTunnel(target, route)


def _target(name="frontier", *, clean_home=None, home_base=None):
    return TargetConfig(
        name=name,
        user="agent",
        transport=TransportConfig(kind="tunnel", ssh_targets=(name,)),
        host_key_sha256=FINGERPRINT,
        client_key="/tmp/does-not-exist-key",
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(options={"ntasks-per-node": 1}),
            srun=SlurmStageConfig(),
        ),
        bundle=BundleConfig(source="computemcp-slurm", deploy_dir="/tmp/cmcp"),
        clean_home=clean_home,
        home_base=home_base,
    )


def _config(targets, *, job_limits=None):
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
            client_id="admin", token_sha256=hash_token("admin-token"), allow_all=True
        ),
    }
    return GatewayConfig(
        server=ServerConfig(),
        ssh=SSHConfig(),
        sessions=SessionConfig(),
        targets=targets,
        clients=clients,
    )


def make_gateway(tmp_path, *, provisioner=None, targets=None, job_limits=None):
    targets = targets or {"frontier": _target()}
    gw = Gateway(
        _config(targets, job_limits=job_limits),
        state_root=tmp_path / "state",
        provisioner=provisioner or FakeProvisioner(),
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


class StageRunner:
    """Records stage commands/env and returns canned helper output."""

    def __init__(self, outputs):
        self.outputs = outputs
        self.calls = []

    async def __call__(self, connection, command, *, env=None, timeout=None):
        self.calls.append({"command": command, "env": dict(env or {})})
        action = command.rsplit(" ", 1)[-1]
        return (0, self.outputs.get(action, ""), "")


# ---------------------------------------------------------------------------
# A. production constructor: durable root + stages + mux opener
# ---------------------------------------------------------------------------
async def test_production_constructor_sets_wiring_and_submits(tmp_path, monkeypatch):
    target = _target()
    runner = StageRunner(
        {
            "submit": (
                "JOB_ID job-x\nSLURM_JOB_ID 4242\nCLUSTER site\n"
                "CORRELATION_TOKEN cmcp-token\n"
                "IMAGE_DIGEST sha256:base123\nSTATE pending\n"
            )
        }
    )
    opened = {}

    async def fake_open_job_route(target, route, **kwargs):
        opened["target"] = target.name
        opened["helper_argv"] = kwargs.get("helper_argv")
        opened["provision"] = kwargs.get("provision")
        return _FakeTunnel(target, route)

    gw = build_production_gateway(
        _config({"frontier": target}),
        state_root=tmp_path / "durable",
        stage_runner=runner,
        stage_deploy_dir="/tmp/cmcp",
        stage_connection=object(),
    )
    monkeypatch.setattr(gw.tunnels, "open_job_route", fake_open_job_route)

    assert gw.state_root == tmp_path / "durable"
    assert gw.provisioner_factory is not None
    assert gw.route_opener is not None

    record = await gw.target_sbatch("frontier", agent(gw))
    assert record["state"] == "pending"
    assert record["slurm_job_id"] == "4242"
    assert record["base_digest"] == "sha256:base123"
    # The production route opener used the T5 mux helper and never provisioned.
    assert opened["target"] == "frontier"
    assert opened["provision"] is False
    assert any("computemcp-mux.py" in part for part in opened["helper_argv"])
    assert runner.calls


# ---------------------------------------------------------------------------
# B. stage env contract and base_digest read-back
# ---------------------------------------------------------------------------
async def test_bundle_stage_env_contract_and_digest(tmp_path):
    target = _target(clean_home="benchmarks/run-42")
    gw = Gateway(
        _config({"frontier": target}),
        state_root=tmp_path / "state",
        provisioner=FakeProvisioner(),
    )
    runner = StageRunner(
        {
            "submit": (
                "SLURM_JOB_ID 77\nCLUSTER site\nCORRELATION_TOKEN token-77\n"
                "IMAGE_DIGEST sha256:abc\nSTATE pending\n"
            )
        }
    )
    adapter = BundleStageProvisioner(
        gw,
        target,
        run=runner,
        deploy_dir="/tmp/cmcp",
        connection=object(),
    )
    record = jobs.JobRecord(
        job_id=jobs.new_job_id(),
        request_id="r",
        owner_client_id="agent",
        target="frontier",
        kind="private",
        root_mode="clean",
        home="benchmarks/run-42",
        correlation_token="token-77",
    )
    submitted = await adapter.submit(record)
    assert submitted == "77;site"
    assert record.base_digest == "sha256:abc"

    call = runner.calls[0]
    assert call["command"] == "bash /tmp/cmcp/computemcp-provision.sh submit"
    env = call["env"]
    for key in (
        "COMPUTEMCP_JOB_ID",
        "COMPUTEMCP_JOB_KIND",
        "COMPUTEMCP_CLIENT_ID",
        "COMPUTEMCP_ROOT_MODE",
        "COMPUTEMCP_CLEAN_HOME",
        "COMPUTEMCP_HOME_BASE",
        "COMPUTEMCP_CORRELATION_TOKEN",
        "COMPUTEMCP_STATE_DIR",
        "COMPUTEMCP_SBATCH_ARGS",
        "COMPUTEMCP_SRUN_ARGS",
    ):
        assert key in env, key
    assert env["COMPUTEMCP_JOB_ID"] == record.job_id
    assert env["COMPUTEMCP_JOB_KIND"] == "private"
    assert env["COMPUTEMCP_CLIENT_ID"] == "agent"
    assert env["COMPUTEMCP_ROOT_MODE"] == "clean"
    assert env["COMPUTEMCP_CLEAN_HOME"] == "benchmarks/run-42"
    assert env["COMPUTEMCP_CORRELATION_TOKEN"] == "token-77"
    # The helper turns this token into sbatch --comment.
    script = (
        Path("src/compute_mcp/bundles/computemcp-slurm/computemcp-provision.sh")
        .read_text()
    )
    assert 'HELPER_ARGS+=(--comment="$CORRELATION_TOKEN")' in script

    runner.outputs["observe"] = "STATE terminal\nSLURM_JOB_ID 77\n"
    assert await adapter.observe(record) == "COMPLETED"
    runner.outputs["attach"] = "ENDPOINT 127.0.0.1:4100\nNODE cn02\n"
    attached = await adapter.attach(record)
    assert attached["relay_endpoint"] == "127.0.0.1:4100"
    assert attached["node"] == "cn02"


# ---------------------------------------------------------------------------
# C. two gateways -> exactly one shared job
# ---------------------------------------------------------------------------
async def test_two_gateways_create_one_shared_job(tmp_path):
    provisioner = FakeProvisioner()
    target = _target()
    cfg = _config({"frontier": target})
    first = Gateway(cfg, state_root=tmp_path / "shared", provisioner=provisioner)
    second = Gateway(cfg, state_root=tmp_path / "shared", provisioner=provisioner)
    first.route_opener = _offline_route_opener
    second.route_opener = _offline_route_opener

    one, two = await asyncio.gather(
        first.target_shared("frontier", agent(first)),
        second.target_shared("frontier", agent(second)),
    )
    assert provisioner.submit_calls == 1
    assert one["job_id"] == two["job_id"]
    assert len([r for r in first.job_store.scan() if r.kind == "shared"]) == 1


# ---------------------------------------------------------------------------
# E. approve job_limits round-trip over HTTP
# ---------------------------------------------------------------------------
async def test_approve_job_limits_round_trip(tmp_path):
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text(
        """
        [server]
        allow_enrollment = true

        [clients.admin]
        token = "admin-token"
        targets = ["*"]

        [targets.hal]
        transport = "direct"
        remote_host = "127.0.0.1"
        remote_port = 9
        user = "agent"
        host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
        """
    )
    gw = Gateway(load_config(cfg_path))
    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/enroll",
            json={"client_id": "newci", "targets": ["hal"], "label": "new"},
        )
        assert resp.status == 200, await resp.text()
        rid = (await resp.json())["request_id"]

        resp = await client.post(
            f"/v1/enroll-requests/{rid}/approve",
            headers=auth("admin-token"),
            json={"job_limits": {"hal": 2}},
        )
        assert resp.status == 200, await resp.text()
        assert (await resp.json())["job_limits"] == {"hal": 2}

        assert gw.config.clients["newci"].job_limits == {"hal": 2}
        assert 'job_limits = { "hal" = 2 }' in cfg_path.read_text()

        resp = await client.get("/v1/clients/newci", headers=auth("admin-token"))
        assert (await resp.json())["job_limits"] == {"hal": 2}

        # A fresh load sees the persisted quota.
        reloaded = load_config(cfg_path)
        assert reloaded.clients["newci"].job_limits == {"hal": 2}
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# F. sbatch forwards the second factor over HTTP
# ---------------------------------------------------------------------------
async def test_sbatch_forwards_factor(tmp_path):
    gw = make_gateway(tmp_path)
    seen = {}

    async def fake_open_target(name, factor=None, **kwargs):
        seen["factor"] = factor
        return gw.public_status(name)

    gw.open_target = fake_open_target
    client = await make_client(gw)
    try:
        resp = await client.post(
            "/v1/targets/frontier/sbatch",
            headers=auth("agent-token"),
            json={"factor": "otp-1234"},
        )
        assert resp.status == 200, await resp.text()
        assert seen["factor"] == "otp-1234"
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# G. preview exposes the job-selection fields
# ---------------------------------------------------------------------------
async def test_preview_fields_present(tmp_path):
    gw = make_gateway(tmp_path)
    client = await make_client(gw)
    try:
        shared = await client.post(
            "/v1/targets/frontier/shared", headers=auth("agent-token")
        )
        assert shared.status == 200, await shared.text()
        shared_id = (await shared.json())["job_id"]

        resp = await client.post(
            "/v1/targets/frontier/preview", headers=auth("agent-token")
        )
        assert resp.status == 200, await resp.text()
        body = await resp.json()
        for key in (
            "root_mode",
            "home",
            "clean_home",
            "shared_reuse",
            "settings_conflict",
            "quota_available",
        ):
            assert key in body, key
        assert body["root_mode"] == "fakeroot"
        assert body["shared_reuse"] == shared_id
        assert body["settings_conflict"] is False
        assert body["quota_available"] is True
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# H/I. clean_home default, symlink escape and shared clean_home
# ---------------------------------------------------------------------------
async def test_clean_home_default_and_symlink_escape(tmp_path):
    home_base = tmp_path / "homes"
    target = _target(clean_home="benchmarks/run-42", home_base=str(home_base))
    gw = make_gateway(tmp_path, targets={"frontier": target})

    record = await gw.target_sbatch("frontier", agent(gw))
    assert record["home"] == "benchmarks/run-42"

    private = home_base / "agents" / "agent"
    private.mkdir(parents=True)
    (private / "escape").symlink_to(tmp_path)
    with pytest.raises(ConfigError, match="refusing a symlink in the home path"):
        gw._resolve_clean_home(
            "escape/run", target=target, client_id="agent"
        )
    with pytest.raises(ConfigError, match="must not contain"):
        gw._resolve_clean_home("../outside", target=target, client_id="agent")

    # target-shared accepts a clean_home and records it for conflict checks.
    shared = await gw.target_shared(
        "frontier", agent(gw), clean_home="benchmarks/shared-run"
    )
    assert shared["job"]["home"] == "benchmarks/shared-run"


# ---------------------------------------------------------------------------
# D. multijob recovery is route-open only (no submit, no scancel)
# ---------------------------------------------------------------------------
async def test_multijob_recovery_is_open_only(tmp_path, monkeypatch):
    provisioner = FakeProvisioner()
    gw = make_gateway(tmp_path, provisioner=provisioner)
    opened = []

    async def fake_open_route(target, route, factor):
        opened.append((target.name, route))
        return _FakeTunnel(target, route)

    async def forbidden_connect(*args, **kwargs):
        raise AssertionError("legacy tunnel.connect must not run for multijob")

    gw.route_opener = fake_open_route
    monkeypatch.setattr(gw.tunnels, "connect", forbidden_connect)

    runtime = await gw.ensure_connected("frontier")
    assert runtime.state == "connected"
    assert opened == [("frontier", "frontier")]
    assert provisioner.submit_calls == 0
    assert provisioner.cancel_calls == 0

    # A closed route is re-opened, still without provisioning or signalling.
    gw.runtimes["frontier"].tunnel._alive = False
    runtime = await gw.ensure_connected("frontier")
    assert runtime.state == "connected"
    assert len(opened) == 2
    assert provisioner.submit_calls == 0
    assert provisioner.cancel_calls == 0
    assert gw.quota.counted("agent", "frontier") == 0


# ---------------------------------------------------------------------------
# J. shutdown never scancels a job
# ---------------------------------------------------------------------------
async def test_shutdown_never_scancels(tmp_path):
    provisioner = FakeProvisioner()
    gw = make_gateway(tmp_path, provisioner=provisioner, job_limits={"frontier": 2})
    record = await gw.target_sbatch("frontier", agent(gw))
    await gw.target_shared("frontier", agent(gw))

    await gw.stop()
    assert provisioner.cancel_calls == 0
    assert gw.job_store.load_record(record["job_id"]).slurm_job_id == (
        record["slurm_job_id"]
    )
    assert any(r.kind == "shared" for r in gw.job_store.scan())


# ---------------------------------------------------------------------------
# K. token-based reconciliation of an accepted-but-unrecorded submission
# ---------------------------------------------------------------------------
class TokenRunner:
    """Stage runner whose observe-with-token result is programmable."""

    def __init__(self, observe):
        self.observe = observe
        self.calls = []

    async def __call__(self, connection, command, *, env=None, timeout=None):
        action = command.rsplit(" ", 1)[-1]
        self.calls.append(
            {
                "action": action,
                "command": command,
                "env": dict(env or {}),
                "timeout": timeout,
            }
        )
        if action != "observe":
            return (0, "", "")
        result = self.observe
        if isinstance(result, BaseException):
            raise result
        if callable(result):
            return result(dict(env or {}))
        return (0, result, "")


class TokenCrashProvisioner(FakeProvisioner):
    """Accepts the allocation, then loses the reply before the id is persisted."""

    def __init__(self, *, recovered="55555", miss_times=0):
        super().__init__()
        self.recovered = recovered
        self.miss_times = miss_times
        self.token_calls = 0
        self.submitted_tokens = []

    async def submit(self, record):
        self.submit_calls += 1
        self.submitted_tokens.append(record.correlation_token)
        raise RuntimeError("connection reset after sbatch accepted")

    async def observe(self, job):
        return "PENDING"

    async def find_by_token(self, token):
        self.token_calls += 1
        if self.token_calls <= self.miss_times:
            return None
        if token in self.submitted_tokens:
            return self.recovered
        return None


class CountingProvisioner(FakeProvisioner):
    def __init__(self):
        super().__init__()
        self.token_calls = 0

    async def find_by_token(self, token):
        self.token_calls += 1
        return None


async def test_bundle_find_by_token_observe_contract(tmp_path):
    target = _target()
    gw = Gateway(
        _config({"frontier": target}),
        state_root=tmp_path / "state",
        provisioner=FakeProvisioner(),
    )
    runner = TokenRunner("SLURM_JOB_ID 4242\nSTATE pending\n")
    adapter = BundleStageProvisioner(
        gw,
        target,
        run=runner,
        deploy_dir="/tmp/cmcp",
        connection=object(),
    )

    assert await adapter.find_by_token("cmcp-token-1") == "4242"
    call = runner.calls[-1]
    assert call["command"] == "bash /tmp/cmcp/computemcp-provision.sh observe"
    assert call["env"]["COMPUTEMCP_CORRELATION_TOKEN"] == "cmcp-token-1"
    # The lookup is keyed by token, not by a job/slurm id.
    assert "COMPUTEMCP_JOB_ID" not in call["env"]
    assert call["timeout"] is not None and call["timeout"] <= 30

    runner.observe = ""
    assert await adapter.find_by_token("cmcp-token-1") is None

    runner.observe = "SLURM_JOB_ID not-a-number\n"
    assert await adapter.find_by_token("cmcp-token-1") is None

    runner.observe = RuntimeError("route closed")
    assert await adapter.find_by_token("cmcp-token-1") is None
    assert await adapter.find_by_token("   ") is None


async def test_ambiguous_submit_adopts_token_id_once(tmp_path):
    provisioner = TokenCrashProvisioner(recovered="55555")
    gw = make_gateway(tmp_path, provisioner=provisioner)

    record = await gw.target_sbatch("frontier", agent(gw))

    assert provisioner.submit_calls == 1
    assert provisioner.token_calls == 1
    assert record["slurm_job_id"] == "55555"
    assert record["state"] == jobs.PENDING
    stored = gw.job_store.load_record(record["job_id"])
    assert stored.slurm_job_id == "55555"
    assert stored.state == jobs.PENDING
    assert gw.job_store.load_submission(record["job_id"])["status"] == "accepted"
    assert gw.quota.counted("agent", "frontier") == 1


async def test_missing_token_keeps_submission_unknown_and_quota(tmp_path):
    provisioner = TokenCrashProvisioner(miss_times=10)
    gw = make_gateway(tmp_path, provisioner=provisioner)

    with pytest.raises(jobs.SubmissionUncertain):
        await gw.target_sbatch("frontier", agent(gw))

    assert provisioner.submit_calls == 1
    assert provisioner.token_calls == 1
    records = gw.job_store.scan()
    assert len(records) == 1
    record = records[0]
    assert record.state == jobs.SUBMISSION_UNKNOWN
    assert record.slurm_job_id is None
    assert gw.job_store.load_submission(record.job_id)["status"] == "unknown"
    # The reserved slot is held: nothing was released and nothing resubmitted.
    assert gw.quota.counted("agent", "frontier") == 1


async def test_reconcile_adopts_token_after_submission_unknown(tmp_path):
    provisioner = TokenCrashProvisioner(recovered="66666", miss_times=1)
    gw = make_gateway(tmp_path, provisioner=provisioner)

    with pytest.raises(jobs.SubmissionUncertain):
        await gw.target_sbatch("frontier", agent(gw))

    assert gw.job_store.scan()[0].state == jobs.SUBMISSION_UNKNOWN
    assert provisioner.submit_calls == 1

    report = await gw.jobs.reconcile_all()

    recovered = gw.job_store.scan()[0]
    assert recovered.slurm_job_id == "66666"
    assert recovered.state == jobs.PENDING
    assert report.recovered == 1
    assert provisioner.submit_calls == 1  # never a second sbatch
    assert gw.quota.counted("agent", "frontier") == 1


async def test_regular_submit_never_consults_token(tmp_path):
    provisioner = CountingProvisioner()
    gw = make_gateway(tmp_path, provisioner=provisioner)

    record = await gw.target_sbatch("frontier", agent(gw))

    assert record["state"] == jobs.PENDING
    assert record["slurm_job_id"] is not None
    assert provisioner.submit_calls == 1
    assert provisioner.token_calls == 0

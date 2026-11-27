# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Crash-window and restart-reconciliation tests for the durable job registry.

Each test drives the manager into a specific crash window, throws away the
in-memory manager, and starts a new one against the same state root.  The
scheduler is a stateful fake so the tests can prove that recovery never issues
a second ``sbatch`` and never calls ``scancel`` during teardown.
"""

from __future__ import annotations

import pytest

from compute_mcp import jobs
from compute_mcp.jobs import (
    JobManager,
    JobRecord,
    JobRequest,
    JobStore,
    QuotaManager,
    QuotaExceeded,
    SubmissionUncertain,
)


class SimulatedCrash(RuntimeError):
    """Process death at a named persistence boundary."""


class CrashAt:
    def __init__(self, point: str) -> None:
        self.point = point

    def __call__(self, phase: str, record: JobRecord) -> None:
        if phase == self.point:
            raise SimulatedCrash(phase)


class FakeScheduler:
    def __init__(self) -> None:
        self._next = 500
        self.jobs: dict[str, str] = {}
        self.tokens: dict[str, str] = {}
        self.states: dict[str, str] = {}
        self.submit_calls = 0
        self.cancel_calls = 0
        self.query_calls = 0
        self.accept_then_raise: BaseException | None = None

    def submit(self, record: JobRecord) -> str:
        self.submit_calls += 1
        self._next += 1
        slurm_id = str(self._next)
        self.jobs[slurm_id] = record.correlation_token
        self.tokens[record.correlation_token] = slurm_id
        self.states[slurm_id] = "PENDING"
        if self.accept_then_raise is not None:
            raise self.accept_then_raise
        return slurm_id

    def query(self, slurm_job_id: str) -> str | None:
        self.query_calls += 1
        return self.states.get(slurm_job_id)

    def cancel(self, slurm_job_id: str) -> None:
        self.cancel_calls += 1
        self.states[slurm_job_id] = "CANCELLED"

    def find_by_token(self, token: str) -> str | None:
        return self.tokens.get(token)

    def set_state(self, slurm_job_id: str, state: str) -> None:
        self.states[slurm_job_id] = state


class NoTokenLookupScheduler(FakeScheduler):
    find_by_token = None  # type: ignore[assignment]


def build(tmp_path, scheduler=None, *, phase_hook=None, limits=None):
    scheduler = scheduler or FakeScheduler()
    store = JobStore(tmp_path / "state")
    quota = QuotaManager(
        limits or {},
        snapshot=store.scan,
        guard=store.quota_lock,
    )
    manager = JobManager(store, scheduler, quota, phase_hook=phase_hook)
    return manager, scheduler, store, quota


def request(request_id="req", *, kind="private", **params):
    return JobRequest(
        client_id="agent",
        target="hal",
        request_id=request_id,
        kind=kind,
        parameters=dict(params),
    )


@pytest.mark.asyncio
async def test_crash_before_submit_keeps_reservation_and_never_submits(tmp_path):
    scheduler = FakeScheduler()
    manager, scheduler, store, quota = build(
        tmp_path, scheduler, phase_hook=CrashAt("before_submit")
    )
    with pytest.raises(SimulatedCrash):
        await manager.submit(request("pre-submit"))

    assert scheduler.submit_calls == 0
    record = store.find_by_request_id("pre-submit")
    assert record.state == jobs.SUBMITTING
    assert record.slurm_job_id is None
    assert store.load_submission(record.job_id)["status"] == "intent"
    assert quota.counted("agent", "hal") == 1

    restarted, scheduler, store, quota = build(tmp_path, scheduler)
    report = await restarted.reconcile_all()
    recovered = store.load_record(record.job_id)
    assert recovered.state == jobs.SUBMISSION_UNKNOWN
    assert report.uncertain == 1
    assert scheduler.submit_calls == 0
    assert quota.counted("agent", "hal") == 1

    with pytest.raises(QuotaExceeded):
        await restarted.submit(request("another"))
    assert scheduler.submit_calls == 0


@pytest.mark.asyncio
async def test_crash_after_accept_before_persist_recovers_by_token(tmp_path):
    scheduler = FakeScheduler()
    manager, scheduler, store, quota = build(
        tmp_path, scheduler, phase_hook=CrashAt("after_submit_accept")
    )
    with pytest.raises(SimulatedCrash):
        await manager.submit(request("accepted-lost"))

    assert scheduler.submit_calls == 1
    assert scheduler.tokens
    on_disk = store.find_by_request_id("accepted-lost")
    assert on_disk.slurm_job_id is None
    assert quota.counted("agent", "hal") == 1

    restarted, scheduler, store, quota = build(tmp_path, scheduler)
    report = await restarted.reconcile_all()
    recovered = store.load_record(on_disk.job_id)
    slurm_id = next(iter(scheduler.tokens.values()))
    assert recovered.slurm_job_id == slurm_id
    assert recovered.state == jobs.PENDING
    assert report.recovered == 1
    assert scheduler.submit_calls == 1
    assert quota.counted("agent", "hal") == 1


@pytest.mark.asyncio
async def test_crash_after_persist_before_ready_reconciles_by_id(tmp_path):
    scheduler = FakeScheduler()
    manager, scheduler, store, _ = build(
        tmp_path, scheduler, phase_hook=CrashAt("after_submit_persist")
    )
    with pytest.raises(SimulatedCrash):
        await manager.submit(request("persisted"))

    record = store.find_by_request_id("persisted")
    assert record.state == jobs.PENDING
    assert record.slurm_job_id is not None
    scheduler.set_state(record.slurm_job_id, "RUNNING")

    restarted, scheduler, store, quota = build(tmp_path, scheduler)
    report = await restarted.reconcile_all()
    recovered = store.load_record(record.job_id)
    assert recovered.state == jobs.STARTING
    assert recovered.scheduler_state == "RUNNING"
    assert report.active == 1
    assert scheduler.submit_calls == 1
    assert quota.counted("agent", "hal") == 1


@pytest.mark.asyncio
async def test_crash_after_ready_before_response_restores_endpoints(tmp_path):
    scheduler = FakeScheduler()

    class ReadyCrash(CrashAt):
        def __call__(self, phase, record):
            if phase == "after_ready_publish":
                raise SimulatedCrash(phase)

    manager, scheduler, store, _ = build(tmp_path, scheduler, phase_hook=ReadyCrash("x"))
    record = await manager.submit(request("ready-crash"))
    with pytest.raises(SimulatedCrash):
        await manager.mark_ready(
            record.job_id,
            node="node-17",
            container_endpoint="10.0.0.7:2222",
            relay_endpoint="127.0.0.1:31055",
            relay_pid=4242,
            generation=4,
        )

    restarted, scheduler, store, _ = build(tmp_path, scheduler)
    loaded = store.load_record(record.job_id)
    assert loaded.state == jobs.READY
    assert loaded.node == "node-17"
    assert loaded.container_endpoint == "10.0.0.7:2222"
    assert loaded.relay_endpoint == "127.0.0.1:31055"
    assert loaded.relay_host == "127.0.0.1"
    assert loaded.relay_port == 31055
    assert loaded.relay_pid == 4242
    assert loaded.generation == 4
    assert store.load_ready(record.job_id)["node"] == "node-17"

    # A retry of the same request returns the ready job; it does not resubmit.
    retried = await restarted.submit(request("ready-crash"))
    assert retried.job_id == record.job_id
    assert retried.state == jobs.READY
    assert scheduler.submit_calls == 1

    await restarted.reconcile_all()
    assert store.load_record(record.job_id).state == jobs.READY


@pytest.mark.asyncio
async def test_ambiguous_submit_marks_unknown_and_never_resubmits(tmp_path):
    scheduler = FakeScheduler()
    scheduler.accept_then_raise = RuntimeError("lost scheduler acknowledgement")
    manager, scheduler, store, quota = build(tmp_path, scheduler)
    with pytest.raises(SubmissionUncertain):
        await manager.submit(request("ambiguous"))

    record = store.find_by_request_id("ambiguous")
    assert record.state == jobs.SUBMISSION_UNKNOWN
    assert record.slurm_job_id is None
    assert store.load_submission(record.job_id)["status"] == "unknown"
    assert scheduler.submit_calls == 1
    assert quota.counted("agent", "hal") == 1

    restarted, scheduler, store, quota = build(tmp_path, scheduler)
    report = await restarted.reconcile_all()
    recovered = store.load_record(record.job_id)
    assert recovered.state == jobs.PENDING
    assert recovered.slurm_job_id is not None
    assert report.recovered == 1
    assert scheduler.submit_calls == 1
    assert scheduler.cancel_calls == 0
    assert quota.counted("agent", "hal") == 1


@pytest.mark.asyncio
async def test_reconcile_confirmed_terminal_releases_quota(tmp_path):
    scheduler = FakeScheduler()
    manager, scheduler, store, quota = build(tmp_path, scheduler)
    record = await manager.submit(request("terminal"))

    scheduler.set_state(record.slurm_job_id, "COMPLETED")
    restarted, scheduler, store, quota = build(tmp_path, scheduler)
    report = await restarted.reconcile_all()

    recovered = store.load_record(record.job_id)
    assert recovered.state == jobs.TERMINAL
    assert recovered.terminal_reason == "scheduler_completed"
    assert store.load_terminal(record.job_id)["reason"] == "scheduler_completed"
    assert report.terminal == 1
    assert quota.counted("agent", "hal") == 0


@pytest.mark.asyncio
async def test_missing_scheduler_entry_stays_uncertain_and_holds_slot(tmp_path):
    scheduler = FakeScheduler()
    manager, scheduler, store, quota = build(tmp_path, scheduler)
    record = await manager.submit(request("missing"))
    scheduler.states.clear()

    restarted, scheduler, store, quota = build(tmp_path, scheduler)
    report = await restarted.reconcile_all()
    recovered = store.load_record(record.job_id)
    assert recovered.state == jobs.PENDING  # missing squeue proves nothing
    assert report.uncertain == 1
    assert quota.counted("agent", "hal") == 1
    assert scheduler.cancel_calls == 0


@pytest.mark.asyncio
async def test_reconcile_recovers_id_from_scheduler_file_window(tmp_path):
    store = JobStore(tmp_path / "state")
    record = JobRecord(
        job_id=jobs.new_job_id(),
        request_id="window",
        owner_client_id="agent",
        target="hal",
        kind="private",
    )
    store.save_record(record)
    store.save_scheduler(
        record.job_id,
        {"slurm_job_id": "777", "cluster": "site", "scheduler_state": None},
    )
    scheduler = FakeScheduler()
    scheduler.states["777"] = "RUNNING"

    restarted, scheduler, store, _ = build(tmp_path, scheduler)
    report = await restarted.reconcile_all()
    recovered = store.load_record(record.job_id)
    assert recovered.slurm_job_id == "777"
    assert recovered.state == jobs.STARTING
    assert report.recovered == 1
    assert scheduler.submit_calls == 0


@pytest.mark.asyncio
async def test_restart_restores_quota_for_private_and_shared(tmp_path):
    scheduler = FakeScheduler()
    manager, scheduler, store, quota = build(tmp_path, scheduler)
    private = await manager.submit(request("p", kind="private"))
    shared = await manager.submit(request("s", kind="shared"))

    restarted, scheduler, store, quota = build(tmp_path, scheduler)
    await restarted.reconcile_all()
    assert quota.counted("agent", "hal") == 1
    with pytest.raises(QuotaExceeded):
        await restarted.submit(request("p2", kind="private"))
    # Shared submissions remain free.
    again = await restarted.submit(request("s2", kind="shared"))
    assert again.kind == "shared"
    assert {private.job_id, shared.job_id}.issubset(
        {item.job_id for item in store.scan()}
    )


@pytest.mark.asyncio
async def test_no_token_lookup_leaves_uncertain_record_and_slot(tmp_path):
    scheduler = NoTokenLookupScheduler()
    manager, scheduler, store, quota = build(
        tmp_path, scheduler, phase_hook=CrashAt("after_submit_accept")
    )
    with pytest.raises(SimulatedCrash):
        await manager.submit(request("no-lookup"))

    restarted, scheduler, store, quota = build(tmp_path, scheduler)
    report = await restarted.reconcile_all()
    record = store.find_by_request_id("no-lookup")
    assert record.state == jobs.SUBMISSION_UNKNOWN
    assert report.uncertain == 1
    assert scheduler.submit_calls == 1
    assert quota.counted("agent", "hal") == 1


@pytest.mark.asyncio
async def test_restart_never_cancels_or_resubmits(tmp_path):
    scheduler = FakeScheduler()
    manager, scheduler, store, quota = build(tmp_path, scheduler)
    record = await manager.submit(request("stable"))
    await manager.mark_ready(record.job_id, node="n", relay_endpoint="r:1")

    restarted, scheduler, store, quota = build(tmp_path, scheduler)
    await restarted.reconcile_all()
    await restarted.shutdown()
    assert scheduler.submit_calls == 1
    assert scheduler.cancel_calls == 0
    assert quota.counted("agent", "hal") == 1

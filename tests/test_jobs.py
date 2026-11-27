# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Unit tests for the durable job registry and quota core."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import time

import pytest

from compute_mcp import jobs
from compute_mcp.jobs import (
    JobManager,
    JobRecord,
    JobRequest,
    JobStore,
    QuotaManager,
    IllegalTransition,
    InvalidJobId,
    LockTimeout,
    QuotaExceeded,
    RequestIdConflict,
)


class FakeScheduler:
    """Minimal stateful scheduler; every call is counted."""

    def __init__(self) -> None:
        self._next = 1000
        self.jobs: dict[str, str] = {}
        self.tokens: dict[str, str] = {}
        self.states: dict[str, str] = {}
        self.submit_calls = 0
        self.query_calls = 0
        self.cancel_calls = 0
        self.reject = False

    def submit(self, record: JobRecord) -> str:
        self.submit_calls += 1
        if self.reject:
            raise jobs.SchedulerRejected("partition unavailable")
        self._next += 1
        slurm_id = str(self._next)
        self.jobs[slurm_id] = record.correlation_token
        self.tokens[record.correlation_token] = slurm_id
        self.states[slurm_id] = "PENDING"
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


def make_manager(tmp_path, scheduler=None, *, limit=None, default_limit=1, **kwargs):
    scheduler = scheduler or FakeScheduler()
    store = JobStore(tmp_path / "state")
    limits = {} if limit is None else {"agent": {"hal": limit}}
    quota = QuotaManager(
        limits,
        default_limit=default_limit,
        snapshot=store.scan,
        guard=store.quota_lock,
    )
    manager = JobManager(store, scheduler, quota, **kwargs)
    return manager, scheduler, store, quota


def request(request_id="req-1", *, client="agent", target="hal", kind="private", **params):
    return JobRequest(
        client_id=client,
        target=target,
        request_id=request_id,
        kind=kind,
        parameters=dict(params),
    )


# ---------------------------------------------------------------------------
# Identity and lifecycle
# ---------------------------------------------------------------------------


def test_job_id_is_opaque_unique_and_path_safe():
    ids = {jobs.new_job_id() for _ in range(64)}
    assert len(ids) == 64
    for job_id in ids:
        assert jobs.validate_job_id(job_id) == job_id
    assert jobs.new_correlation_token() != jobs.new_correlation_token()


def test_legal_transition_graph_and_illegal_jumps():
    record = JobRecord(
        job_id=jobs.new_job_id(),
        request_id="r",
        owner_client_id="agent",
        target="hal",
        kind="private",
    )
    jobs.transition(record, jobs.PENDING)
    jobs.transition(record, jobs.STARTING)
    jobs.transition(record, jobs.READY)
    jobs.transition(record, jobs.STOPPING)
    jobs.transition(record, jobs.TERMINAL, reason="done")
    assert record.state == jobs.TERMINAL
    assert record.terminal_reason == "done"
    # Terminal is absorbing.
    with pytest.raises(IllegalTransition):
        jobs.transition(record, jobs.READY)
    # Repeating the current state is an idempotent no-op.
    jobs.transition(record, jobs.TERMINAL, reason="again")
    assert record.terminal_reason == "again"

    fresh = JobRecord(
        job_id=jobs.new_job_id(),
        request_id="r2",
        owner_client_id="agent",
        target="hal",
        kind="private",
    )
    # submitting cannot jump straight to ready.
    with pytest.raises(IllegalTransition):
        jobs.transition(fresh, jobs.READY)
    jobs.transition(fresh, jobs.SUBMISSION_UNKNOWN)
    jobs.transition(fresh, jobs.PENDING)
    jobs.transition(fresh, jobs.TERMINAL)


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "job-xyz",
        "job-../../etc/passwd",
        "../job-0123456789abcdef0123456789abcdef",
        "/tmp/job-0123456789abcdef0123456789abcdef",
        "job-0123456789abcdef0123456789abcde\x00",
        "job-0123456789abcdef0123456789ABCDEF",
    ],
)
def test_store_rejects_bad_and_traversal_job_ids(tmp_path, bad):
    store = JobStore(tmp_path / "state")
    with pytest.raises(InvalidJobId):
        store.load_record(bad)
    with pytest.raises(InvalidJobId):
        store.job_dir(bad)


# ---------------------------------------------------------------------------
# Durable store
# ---------------------------------------------------------------------------


def test_store_roundtrip_all_files_and_no_temp_leftovers(tmp_path):
    store = JobStore(tmp_path / "state")
    record = JobRecord(
        job_id=jobs.new_job_id(),
        request_id="req",
        owner_client_id="agent",
        target="hal",
        kind="private",
        parameters={"nodes": 1},
        root_mode="clean",
        base_digest="sha256:abc",
        home="/scratch/agent",
    )
    store.save_record(record)
    store.save_submission(record.job_id, {"status": "intent"})
    store.save_scheduler(record.job_id, {"slurm_job_id": "42"})
    store.write_settings(record.job_id, {"B": "2", "A": "1"})
    store.save_ready(record.job_id, {"node": "n1"})
    store.save_relay(record.job_id, {"endpoint": "r:1"})
    store.save_terminal(record.job_id, {"reason": "done"})

    loaded = store.load_record(record.job_id)
    assert loaded.to_dict() == record.to_dict()
    assert store.load_submission(record.job_id)["status"] == "intent"
    assert store.load_scheduler(record.job_id)["slurm_job_id"] == "42"
    assert store.read_settings(record.job_id) == "A=1\nB=2\n"
    assert store.load_ready(record.job_id)["node"] == "n1"
    assert store.load_relay(record.job_id)["endpoint"] == "r:1"
    assert store.load_terminal(record.job_id)["reason"] == "done"

    assert [item.job_id for item in store.scan()] == [record.job_id]
    leftovers = [
        path
        for path in store.job_dir(record.job_id).iterdir()
        if path.name.startswith(".") and ".tmp-" in path.name
    ]
    assert leftovers == []


def test_store_scan_skips_foreign_and_corrupt_entries(tmp_path):
    store = JobStore(tmp_path / "state")
    good = JobRecord(
        job_id=jobs.new_job_id(),
        request_id="ok",
        owner_client_id="agent",
        target="hal",
        kind="private",
    )
    store.save_record(good)
    (store.jobs_dir / "not-a-job").mkdir()
    corrupt_id = jobs.new_job_id()
    (store.jobs_dir / corrupt_id).mkdir()
    (store.jobs_dir / corrupt_id / "job.json").write_text("{not json", encoding="utf-8")

    scanned = store.scan()
    assert [item.job_id for item in scanned] == [good.job_id]
    assert any(name == corrupt_id for name, _ in store.last_scan_errors)


# ---------------------------------------------------------------------------
# Locks
# ---------------------------------------------------------------------------


def _lock_payload(*, pid=None, lease_until=None):
    return {
        "host": socket.gethostname(),
        "boot_id": "",
        "pid": os.getpid() if pid is None else pid,
        "start_ticks": None,
        "lease_until": time.time() + 60 if lease_until is None else lease_until,
        "created_at": time.time() - 100,
        "token": "test-token",
    }


def test_lock_live_and_contention(tmp_path):
    store = JobStore(tmp_path / "state")
    job_id = jobs.new_job_id()
    lock = store.acquire_lock(job_id, timeout=0.5)
    try:
        assert store.lock_is_stale(job_id) is False
        with pytest.raises(LockTimeout):
            store.acquire_lock(job_id, timeout=0.05, poll_interval=0.005)
    finally:
        lock.release()
    assert store.read_lock(job_id) is None


def test_lock_stale_by_dead_pid_and_expired_lease(tmp_path):
    store = JobStore(tmp_path / "state")
    job_id = jobs.new_job_id()
    store.ensure_job_dir(job_id)
    lock_path = store.lock_path(job_id)

    lock_path.write_text(json.dumps(_lock_payload(pid=2_000_000_000)), encoding="utf-8")
    assert store.lock_is_stale(job_id) is True
    acquired = store.acquire_lock(job_id, timeout=0.5)
    acquired.release()

    lock_path.write_text(
        json.dumps(_lock_payload(lease_until=time.time() - 1)), encoding="utf-8"
    )
    assert store.lock_is_stale(job_id) is True
    acquired = store.acquire_lock(job_id, timeout=0.5)
    acquired.release()


def test_lock_renew_keeps_ownership(tmp_path):
    store = JobStore(tmp_path / "state")
    job_id = jobs.new_job_id()
    lock = store.acquire_lock(job_id, timeout=0.5, lease_seconds=0.05)
    try:
        time.sleep(0.08)
        assert store.lock_is_stale(job_id) is True
        lock.renew(lease_seconds=60)
        assert store.lock_is_stale(job_id) is False
    finally:
        lock.release()


# ---------------------------------------------------------------------------
# Submission and idempotency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_request_id_returns_existing_job_without_resubmitting(tmp_path):
    manager, scheduler, store, quota = make_manager(tmp_path)
    first = await manager.submit(request("same", nodes=1))
    second = await manager.submit(request("same", nodes=1))
    assert second.job_id == first.job_id
    assert scheduler.submit_calls == 1
    assert quota.counted("agent", "hal") == 1
    assert store.load_submission(first.job_id)["status"] == "accepted"


@pytest.mark.asyncio
async def test_request_id_reuse_with_different_parameters_conflicts(tmp_path):
    manager, *_ = make_manager(tmp_path)
    await manager.submit(request("same", nodes=1))
    with pytest.raises(RequestIdConflict):
        await manager.submit(request("same", nodes=2))


@pytest.mark.asyncio
async def test_definitive_scheduler_rejection_releases_quota(tmp_path):
    manager, scheduler, store, quota = make_manager(tmp_path)
    scheduler.reject = True
    with pytest.raises(jobs.SubmissionRejected):
        await manager.submit(request("rejected"))
    record = store.find_by_request_id("rejected")
    assert record.state == jobs.TERMINAL
    assert quota.counted("agent", "hal") == 0
    # The slot is free again.
    scheduler.reject = False
    accepted = await manager.submit(request("after-reject"))
    assert accepted.state == jobs.PENDING


@pytest.mark.asyncio
async def test_shared_jobs_do_not_consume_private_quota(tmp_path):
    manager, scheduler, store, quota = make_manager(tmp_path, limit=1)
    private = await manager.submit(request("private", kind="private"))
    shared_one = await manager.submit(request("shared-1", kind="shared"))
    shared_two = await manager.submit(request("shared-2", kind="shared"))
    assert {private.kind, shared_one.kind, shared_two.kind} == {"private", "shared"}
    assert quota.counted("agent", "hal") == 1
    with pytest.raises(QuotaExceeded):
        await manager.submit(request("private-2", kind="private"))


@pytest.mark.asyncio
async def test_settings_env_written_before_submission(tmp_path):
    manager, scheduler, store, _ = make_manager(tmp_path)
    record = await manager.submit(
        JobRequest(
            client_id="agent",
            target="hal",
            request_id="settings",
            settings={"ROOT_MODE": "clean", "IMAGE": "base.sif"},
        )
    )
    assert store.read_settings(record.job_id) == "IMAGE=base.sif\nROOT_MODE=clean\n"


# ---------------------------------------------------------------------------
# Quota basics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_quota_zero_disables_and_default_is_one():
    zero = QuotaManager({"agent": {"hal": 0}})
    with pytest.raises(QuotaExceeded):
        await zero.reserve("agent", "hal", "private", jobs.new_job_id())

    default = QuotaManager()
    await default.reserve("agent", "hal", "private", jobs.new_job_id())
    with pytest.raises(QuotaExceeded):
        await default.reserve("agent", "hal", "private", jobs.new_job_id())

    per_target = QuotaManager({"agent": {"hal": 1, "juwels": 0}})
    await per_target.reserve("agent", "hal", "private", jobs.new_job_id())
    with pytest.raises(QuotaExceeded):
        await per_target.reserve("agent", "juwels", "private", jobs.new_job_id())


# ---------------------------------------------------------------------------
# Cancellation and detach
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_calls_cancel_and_confirmed_terminal_frees_quota(tmp_path):
    manager, scheduler, store, quota = make_manager(tmp_path)
    record = await manager.submit(request("stop-me"))
    await manager.mark_ready(
        record.job_id,
        node="n1",
        container_endpoint="c:2222",
        relay_endpoint="relay:31000",
        generation=2,
    )
    assert quota.counted("agent", "hal") == 1

    stopped = await manager.stop(record.job_id)
    assert stopped.state == jobs.STOPPING
    assert scheduler.cancel_calls == 1
    # Cancelling still holds the slot until terminal is confirmed.
    assert quota.counted("agent", "hal") == 1

    scheduler.set_state(record.slurm_job_id, "CANCELLED")
    terminal = await manager.poll(record.job_id)
    assert terminal.state == jobs.TERMINAL
    assert terminal.terminal_reason == "scheduler_cancelled"
    assert store.load_terminal(record.job_id)["reason"] == "scheduler_cancelled"
    assert quota.counted("agent", "hal") == 0


@pytest.mark.asyncio
async def test_shutdown_detaches_and_never_cancels(tmp_path):
    manager, scheduler, store, _ = make_manager(tmp_path)
    record = await manager.submit(request("keep-alive"))
    await manager.mark_ready(record.job_id, node="n1", relay_endpoint="r:1")

    detached = await manager.shutdown()
    assert [item.job_id for item in detached] == [record.job_id]
    assert scheduler.cancel_calls == 0
    assert store.load_record(record.job_id).state == jobs.READY

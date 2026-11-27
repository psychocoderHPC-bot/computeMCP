# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Adversarial regression tests for the four T2b audit findings.

Each test is written against the public core API only (no gateway, no
``tests/fakes``) and carries its own minimal scheduler so it stays runnable on
its own.  The four themes are:

1. cross-client request-id idempotency must never leak another client's job;
2. ``QuotaManager.set_limits`` replaces the override map instead of merging;
3. shared-job creation is serialised by a cross-process, per-target lock;
4. quota slots are held across every non-terminal state and only a definitive
   outcome frees them (route loss, restart and an unknown scheduler reply do
   not).
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time

import pytest

from compute_mcp import jobs
from compute_mcp.jobs import (
    JobManager,
    JobRecord,
    JobRequest,
    JobStore,
    LockTimeout,
    QuotaExceeded,
    QuotaManager,
    RequestIdConflict,
)


class FakeScheduler:
    """Thread-safe, minimal stateful scheduler shared by the lock test."""

    def __init__(self) -> None:
        self._next = 1000
        self._guard = threading.Lock()
        self.tokens: dict[str, str] = {}
        self.states: dict[str, str] = {}
        self.submit_calls = 0
        self.cancel_calls = 0

    def submit(self, record: JobRecord) -> str:
        with self._guard:
            self.submit_calls += 1
            self._next += 1
            slurm_id = str(self._next)
            self.tokens[record.correlation_token] = slurm_id
            self.states[slurm_id] = "PENDING"
            return slurm_id

    def query(self, slurm_job_id: str) -> str | None:
        with self._guard:
            return self.states.get(slurm_job_id)

    def cancel(self, slurm_job_id: str) -> None:
        with self._guard:
            self.cancel_calls += 1
            self.states[slurm_job_id] = "CANCELLED"

    def find_by_token(self, token: str) -> str | None:
        with self._guard:
            return self.tokens.get(token)


def make_manager(root, scheduler=None, *, limits=None, default_limit=1):
    scheduler = scheduler or FakeScheduler()
    store = JobStore(root)
    quota = QuotaManager(
        limits or {},
        default_limit=default_limit,
        snapshot=store.scan,
        guard=store.quota_lock,
    )
    return JobManager(store, scheduler, quota), scheduler, store, quota


def request(
    request_id="req-1",
    *,
    client="agent",
    target="hal",
    kind="private",
    **params,
):
    return JobRequest(
        client_id=client,
        target=target,
        request_id=request_id,
        kind=kind,
        parameters=dict(params),
    )


# ---------------------------------------------------------------------------
# 1. Cross-client idempotency leak
# ---------------------------------------------------------------------------


def test_request_signature_binds_the_owner():
    """The owner is part of both signatures, so a foreign replay cannot match."""
    base = dict(request_id="rid", target="hal", kind="private")
    first = JobRecord(job_id=jobs.new_job_id(), owner_client_id="client-a", **base)
    second = JobRecord(job_id=jobs.new_job_id(), owner_client_id="client-b", **base)
    assert jobs._record_signature(first) != jobs._record_signature(second)
    assert jobs._request_signature_of(
        request("rid", client="client-a")
    ) != jobs._request_signature_of(request("rid", client="client-b"))


@pytest.mark.asyncio
async def test_replayed_request_id_never_returns_another_clients_job(tmp_path):
    manager, scheduler, store, quota = make_manager(tmp_path / "state")
    first = await manager.submit(request("shared-key", client="client-a", nodes=1))
    assert first.owner_client_id == "client-a"

    # Client B replays the exact same idempotency key and parameters.  It must
    # behave like an unknown id for B: a fresh, owner-scoped job, never A's.
    second = await manager.submit(request("shared-key", client="client-b", nodes=1))
    assert second.job_id != first.job_id
    assert second.owner_client_id == "client-b"
    assert scheduler.submit_calls == 2
    assert len(store.scan()) == 2

    # The store-level lookup is exact per owner, and invisible to a third
    # client that never used the key.
    assert store.find_by_request_id("shared-key", "client-a").job_id == first.job_id
    assert store.find_by_request_id("shared-key", "client-b").job_id == second.job_id
    assert store.find_by_request_id("shared-key", "client-c") is None
    # Both clients keep one counted slot in their own bucket.
    assert quota.counted("client-a", "hal") == 1
    assert quota.counted("client-b", "hal") == 1


@pytest.mark.asyncio
async def test_same_owner_idempotency_and_conflict_are_preserved(tmp_path):
    manager, scheduler, store, _ = make_manager(tmp_path / "state")
    first = await manager.submit(request("same", client="agent", nodes=1))
    again = await manager.submit(request("same", client="agent", nodes=1))
    assert again.job_id == first.job_id
    assert scheduler.submit_calls == 1
    with pytest.raises(RequestIdConflict):
        await manager.submit(request("same", client="agent", nodes=2))


# ---------------------------------------------------------------------------
# 2. set_limits replaces the override map exactly
# ---------------------------------------------------------------------------


def test_set_limits_replaces_removed_overrides_with_the_default(tmp_path):
    quota = QuotaManager(
        {"agent": {"hal": 2, "juwels": 3}, "gone": {"hal": 5}}, default_limit=1
    )
    quota.set_limits({"agent": {"hal": 0}})
    assert quota.limit_for("agent", "hal") == 0
    # Removed target and removed client both fall back to the default.
    assert quota.limit_for("agent", "juwels") == 1
    assert quota.limit_for("agent", "missing") == 1
    assert quota.limit_for("gone", "hal") == 1

    quota.set_limits({})
    assert quota.limit_for("agent", "hal") == 1


@pytest.mark.asyncio
async def test_set_limits_zero_disables_and_restoring_allows_one(tmp_path):
    quota = QuotaManager({}, default_limit=1)
    quota.set_limits({"agent": {"hal": 0}})
    with pytest.raises(QuotaExceeded):
        await quota.reserve("agent", "hal", "private", jobs.new_job_id())

    quota.set_limits({})
    await quota.reserve("agent", "hal", "private", jobs.new_job_id())
    assert quota.counted("agent", "hal") == 1


def test_set_limits_validation_is_atomic(tmp_path):
    quota = QuotaManager({"agent": {"hal": 1}})
    with pytest.raises(ValueError):
        quota.set_limits({"agent": {"hal": -1}})
    assert quota.limit_for("agent", "hal") == 1
    with pytest.raises(ValueError):
        quota.set_limits({"agent": {"hal": True}})
    assert quota.limit_for("agent", "hal") == 1


# ---------------------------------------------------------------------------
# 3. Durable shared-job uniqueness across processes
# ---------------------------------------------------------------------------


def test_shared_lock_is_exclusive_per_target_across_stores(tmp_path):
    store = JobStore(tmp_path / "state")
    other = JobStore(tmp_path / "state")
    lock = store.shared_lock("hal", "client-a", timeout=0.5)
    try:
        # A second process (another JobStore) contends even with another owner.
        with pytest.raises(LockTimeout):
            other.shared_lock("hal", "client-b", timeout=0.05, poll_interval=0.005)
        # A different target is independent.
        with other.shared_lock("juwels", "client-b", timeout=0.5):
            pass
    finally:
        lock.release()

    path = store.shared_lock_path("hal")
    with other.shared_lock("hal", "client-b", timeout=0.5):
        assert path.exists()
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["identity"] == "shared:hal:client-b"
    assert not path.exists()


def test_shared_lock_recovers_a_dead_holder(tmp_path):
    store = JobStore(tmp_path / "state")
    path = store.shared_lock_path("hal")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "host": socket.gethostname(),
                "boot_id": "",
                "pid": 2_000_000_000,  # never a live pid
                "start_ticks": None,
                "lease_until": time.time() + 60,
                "created_at": time.time() - 100,
                "token": "dead",
                "identity": "shared:hal:old",
            }
        ),
        encoding="utf-8",
    )
    with store.shared_lock("hal", "new-owner", timeout=1.0):
        pass
    assert not path.exists()


def _create_shared_if_absent(root, scheduler, request_id, started, results):
    """One gateway process: get-or-create a shared job under the target lock."""
    manager, _, store, _ = make_manager(root, scheduler)
    started.wait(timeout=10)
    with store.shared_lock("hal", "@shared", timeout=15):
        existing = [
            record
            for record in store.scan()
            if record.target == "hal"
            and record.kind == "shared"
            and record.state != jobs.TERMINAL
        ]
        if existing:
            results.append(existing[0].job_id)
            return
        record = asyncio.run(manager.submit(request(request_id, kind="shared")))
        results.append(record.job_id)


def test_two_manager_instances_create_only_one_shared_job(tmp_path):
    root = tmp_path / "state"
    scheduler = FakeScheduler()
    started = threading.Barrier(2, timeout=10)
    results: list[str] = []
    threads = [
        threading.Thread(
            target=_create_shared_if_absent,
            args=(root, scheduler, f"shared-{index}", started, results),
        )
        for index in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not any(thread.is_alive() for thread in threads)
    assert len(results) == 2
    # Both "processes" converge on the same durable record; only one submitted.
    assert len(set(results)) == 1
    assert scheduler.submit_calls == 1
    shared = [r for r in JobStore(root).scan() if r.kind == "shared"]
    assert [record.job_id for record in shared] == [results[0]]
    assert not JobStore(root).shared_lock_path("hal").exists()


# ---------------------------------------------------------------------------
# 4. Quota counting / release invariants
# ---------------------------------------------------------------------------


COUNTED_STATES = [
    (None, "reserved"),  # None keeps the initial reservation
    (jobs.SUBMISSION_UNKNOWN, "uncertain"),
    (jobs.PENDING, "pending"),
    (jobs.STARTING, "starting"),
    (jobs.READY, "ready"),
    (jobs.STOPPING, "cancelling"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("state,category", COUNTED_STATES)
async def test_counted_state_holds_the_slot_until_a_definitive_release(
    state, category
):
    quota = QuotaManager({"agent": {"hal": 2}})
    job_id = jobs.new_job_id()
    assert await quota.reserve("agent", "hal", "private", job_id) == "reserved"
    if state is not None:
        assert await quota.mark(job_id, state) == category
    assert quota.category_of(job_id) == category
    assert quota.counted("agent", "hal") == 1

    # A non-definitive terminal observation retains the previous category.
    await quota.mark(job_id, jobs.TERMINAL, definitive=False)
    assert quota.category_of(job_id) == category
    assert quota.counted("agent", "hal") == 1

    assert await quota.release(job_id, definitive=False) is False
    assert quota.counted("agent", "hal") == 1

    assert await quota.release(job_id, definitive=True) is True
    assert quota.counted("agent", "hal") == 0
    assert quota.category_of(job_id) is None


@pytest.mark.asyncio
async def test_route_loss_restart_and_unknown_reply_never_release(tmp_path):
    root = tmp_path / "state"
    scheduler = FakeScheduler()
    manager, scheduler, store, quota = make_manager(root, scheduler)
    record = await manager.submit(request("keep", client="agent", nodes=1))
    await manager.mark_ready(
        record.job_id,
        node="n1",
        container_endpoint="c:2222",
        relay_endpoint="relay:31000",
    )
    assert quota.counted("agent", "hal") == 1

    # Route loss / detach must neither scancel nor free the slot.
    await manager.shutdown()
    await manager.detach_all()
    assert scheduler.cancel_calls == 0
    assert quota.counted("agent", "hal") == 1

    # A fresh process rebuilds usage from disk and still counts the job.
    restarted, _, _, restarted_quota = make_manager(root, scheduler)
    report = await restarted.ensure_reconciled()
    assert report.total == 1
    assert restarted_quota.counted("agent", "hal") == 1

    # The scheduler forgets the job (unknown reply): still no release.
    scheduler.states.pop(record.slurm_job_id, None)
    observed = await restarted.poll(record.job_id)
    assert observed.state != jobs.TERMINAL
    assert restarted_quota.counted("agent", "hal") == 1

    # The held slot still blocks a second private job for the same client.
    with pytest.raises(QuotaExceeded):
        await restarted.submit(request("other", client="agent", nodes=1))


@pytest.mark.asyncio
async def test_cancelling_holds_the_slot_until_terminal_is_confirmed(tmp_path):
    scheduler = FakeScheduler()
    manager, scheduler, store, quota = make_manager(tmp_path / "state", scheduler)
    record = await manager.submit(request("cancel-me", client="agent"))
    await manager.stop(record.job_id)
    assert quota.category_of(record.job_id) == "cancelling"
    assert quota.counted("agent", "hal") == 1

    # Unknown scheduler state after scancel must not free the slot.
    scheduler.states.pop(record.slurm_job_id, None)
    await manager.poll(record.job_id)
    assert quota.counted("agent", "hal") == 1

    # Only a confirmed terminal state does.
    scheduler.states[record.slurm_job_id] = "CANCELLED"
    terminal = await manager.poll(record.job_id)
    assert terminal.state == jobs.TERMINAL
    assert quota.counted("agent", "hal") == 0

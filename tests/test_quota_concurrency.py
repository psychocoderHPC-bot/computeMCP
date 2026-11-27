# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Concurrency tests for quota reservation.

The last-slot race is driven through the real :class:`JobManager` with a fake
scheduler whose ``submit`` parks on a two-party barrier.  If the quota logic
let a second submission through, the barrier would see three parties and break,
so a single submission is the observable proof that reservation happens before
``sbatch``.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from compute_mcp import jobs
from compute_mcp.jobs import (
    JobManager,
    JobRecord,
    JobRequest,
    JobStore,
    QuotaManager,
    QuotaExceeded,
)


class BarrierScheduler:
    def __init__(self) -> None:
        self.barrier = threading.Barrier(2, timeout=10)
        self.submit_calls = 0
        self.cancel_calls = 0
        self.ids: list[str] = []

    def submit(self, record: JobRecord) -> str:
        self.submit_calls += 1
        self.barrier.wait()
        slurm_id = str(9000 + self.submit_calls)
        self.ids.append(slurm_id)
        return slurm_id

    def query(self, slurm_job_id: str) -> str | None:
        return "PENDING"

    def cancel(self, slurm_job_id: str) -> None:
        self.cancel_calls += 1


def make_manager(tmp_path, scheduler, limits=None, *, default_limit=1):
    store = JobStore(tmp_path / "state")
    quota = QuotaManager(
        limits or {},
        default_limit=default_limit,
        snapshot=store.scan,
        guard=store.quota_lock,
    )
    return JobManager(store, scheduler, quota), store, quota


def request(index: int, *, target="hal", kind="private"):
    return JobRequest(
        client_id="agent",
        target=target,
        request_id=f"race-{index}",
        kind=kind,
    )


@pytest.mark.asyncio
async def test_last_slot_race_exactly_one_submission(tmp_path):
    scheduler = BarrierScheduler()
    manager, store, quota = make_manager(
        tmp_path, scheduler, {"agent": {"hal": 1}}
    )
    tasks = [
        asyncio.create_task(manager.submit(request(index))) for index in range(8)
    ]
    # The single accepted submission waits at the barrier; pair with it.  All
    # other tasks run while it is parked and must be rejected by quota.
    await asyncio.to_thread(scheduler.barrier.wait)
    results = await asyncio.gather(*tasks, return_exceptions=True)

    accepted = [item for item in results if isinstance(item, JobRecord)]
    rejected = [item for item in results if isinstance(item, QuotaExceeded)]
    assert len(accepted) == 1
    assert len(rejected) == 7
    assert scheduler.submit_calls == 1
    assert quota.counted("agent", "hal") == 1
    assert len(store.scan()) == 1


@pytest.mark.asyncio
async def test_concurrent_reserve_never_exceeds_limit():
    quota = QuotaManager({"agent": {"hal": 3}})
    job_ids = [jobs.new_job_id() for _ in range(12)]
    results = await asyncio.gather(
        *(
            quota.reserve("agent", "hal", "private", job_id)
            for job_id in job_ids
        ),
        return_exceptions=True,
    )
    accepted = [item for item in results if isinstance(item, str)]
    rejected = [item for item in results if isinstance(item, QuotaExceeded)]
    assert len(accepted) == 3
    assert len(rejected) == 9
    assert quota.counted("agent", "hal") == 3


@pytest.mark.asyncio
async def test_client_loss_never_frees_a_slot(tmp_path):
    scheduler = BarrierScheduler()
    manager, store, quota = make_manager(
        tmp_path, scheduler, {"agent": {"hal": 1}}
    )
    # Do not use the barrier here: submit without the paired wait.
    scheduler.barrier = threading.Barrier(1)
    record = await manager.submit(request(0))
    assert quota.counted("agent", "hal") == 1

    # A disconnect/lost route is not a definitive outcome.
    assert await quota.release(record.job_id, definitive=False) is False
    assert quota.counted("agent", "hal") == 1
    with pytest.raises(QuotaExceeded):
        await manager.submit(request(1))


@pytest.mark.asyncio
async def test_terminal_only_frees_when_definitive():
    quota = QuotaManager({"agent": {"hal": 1}})
    job_id = jobs.new_job_id()
    await quota.reserve("agent", "hal", "private", job_id)

    await quota.mark(job_id, jobs.TERMINAL, definitive=False)
    assert quota.counted("agent", "hal") == 1

    assert await quota.release(job_id, definitive=False) is False
    assert quota.counted("agent", "hal") == 1

    assert await quota.release(job_id, definitive=True) is True
    assert quota.counted("agent", "hal") == 0
    assert await quota.release(job_id, definitive=True) is False


@pytest.mark.asyncio
async def test_restore_counts_active_states_and_skips_terminal_and_shared():
    states = [
        jobs.SUBMITTING,
        jobs.SUBMISSION_UNKNOWN,
        jobs.PENDING,
        jobs.STARTING,
        jobs.READY,
        jobs.STOPPING,
        jobs.TERMINAL,
    ]
    records = [
        JobRecord(
            job_id=jobs.new_job_id(),
            request_id=f"r-{state}",
            owner_client_id="agent",
            target="hal",
            kind="private",
            state=state,
        )
        for state in states
    ]
    records.append(
        JobRecord(
            job_id=jobs.new_job_id(),
            request_id="shared",
            owner_client_id="agent",
            target="hal",
            kind="shared",
            state=jobs.READY,
        )
    )
    quota = QuotaManager({"agent": {"hal": 10}})
    await quota.restore(records)
    # Six active private states; terminal and shared are not counted.
    assert quota.counted("agent", "hal") == 6


@pytest.mark.asyncio
async def test_per_target_limits_are_independent():
    quota = QuotaManager({"agent": {"hal": 1, "juwels": 2}})
    await quota.reserve("agent", "hal", "private", jobs.new_job_id())
    with pytest.raises(QuotaExceeded):
        await quota.reserve("agent", "hal", "private", jobs.new_job_id())
    await quota.reserve("agent", "juwels", "private", jobs.new_job_id())
    await quota.reserve("agent", "juwels", "private", jobs.new_job_id())
    with pytest.raises(QuotaExceeded):
        await quota.reserve("agent", "juwels", "private", jobs.new_job_id())
    assert quota.counted("agent", "hal") == 1
    assert quota.counted("agent", "juwels") == 2


@pytest.mark.asyncio
async def test_two_managers_share_the_durable_last_slot(tmp_path):
    """A second QuotaManager over the same records cannot claim the last slot."""
    store = JobStore(tmp_path / "state")
    first = QuotaManager(
        {"agent": {"hal": 1}}, snapshot=store.scan, guard=store.quota_lock
    )
    second = QuotaManager(
        {"agent": {"hal": 1}}, snapshot=store.scan, guard=store.quota_lock
    )
    record = JobRecord(
        job_id=jobs.new_job_id(),
        request_id="durable",
        owner_client_id="agent",
        target="hal",
        kind="private",
    )
    await first.reserve(
        "agent", "hal", "private", record.job_id, persist=lambda: store.save_record(record)
    )
    with pytest.raises(QuotaExceeded):
        await second.reserve("agent", "hal", "private", jobs.new_job_id())
    assert second.counted("agent", "hal") == 1

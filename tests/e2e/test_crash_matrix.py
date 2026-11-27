# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Gateway-level crash/retry matrix (the four submission windows).

The JobManager unit tests already inject the four phase hooks.  This module
drives the same four windows through the *real* gateway and a real scheduler
(fake Slurm reached over in-process SSH), then restarts a fresh gateway on the
same durable state root:

* before_submit          -> no ``sbatch``; slot held; record becomes uncertain;
* after_submit_accept    -> one ``sbatch``; the id is recovered from the job
                            comment; never resubmitted;
* after_submit_persist   -> one ``sbatch``; the persisted id is reconciled;
* after_ready_publish    -> one ``sbatch``; endpoints/relay survive restart.

Every window asserts no resubmission and that the quota reservation is held.
"""

from __future__ import annotations

import pytest

from compute_mcp import jobs as jobcore

from .conftest import A_TOKEN

pytestmark = pytest.mark.asyncio


class Crash(RuntimeError):
    pass


async def _submit(gateway, token):
    client = gateway.auth.authenticate_bearer(f"Bearer {token}")
    return await gateway.target_sbatch("frontier", client, request_id="crash-1")


def _install_hook(gateway, phase: str) -> None:
    gateway._ensure_jobs()

    def hook(name, record):
        if name == phase:
            raise Crash(name)

    gateway.jobs._phase_hook = hook


@pytest.mark.parametrize(
    "phase",
    ["before_submit", "after_submit_accept", "after_submit_persist"],
)
async def test_submission_window_recovers_without_resubmit(
    phase, e2e_harness, e2e_gateway_factory, tmp_path
):
    harness = e2e_harness
    limits = {"agent-a": {"frontier": 1}}
    state_root = tmp_path / "state"
    gateway, _prov = e2e_gateway_factory(
        state_root=state_root, job_limits=limits
    )
    await gateway.open_target("frontier")
    _install_hook(gateway, phase)

    with pytest.raises(Crash):
        await _submit(gateway, A_TOKEN)

    # Durable intent exists; no response was delivered.
    records = gateway.job_store.scan()
    assert len(records) == 1
    crashed = records[0]
    assert crashed.state in (
        jobcore.SUBMITTING, jobcore.SUBMISSION_UNKNOWN, jobcore.PENDING
    )
    await gateway.stop()
    assert harness.scancel_count() == 0

    restarted, _prov2 = e2e_gateway_factory(
        state_root=state_root, job_limits=limits
    )
    await restarted.open_target("frontier")

    recovered = restarted.job_store.load_record(crashed.job_id)
    queue_jobs = harness.jobs()
    if phase == "before_submit":
        assert harness.sbatch_count() == 0
        assert recovered.slurm_job_id is None
        assert recovered.state == jobcore.SUBMISSION_UNKNOWN
    elif phase == "after_submit_accept":
        # The comment-carrying submission is found; the id is adopted once.
        assert harness.sbatch_count() == 1
        assert len(queue_jobs) == 1
        only_id = next(iter(queue_jobs))
        assert recovered.slurm_job_id == only_id
        assert recovered.state == jobcore.PENDING
    else:  # after_submit_persist
        assert harness.sbatch_count() == 1
        assert len(queue_jobs) == 1
        only_id = next(iter(queue_jobs))
        assert recovered.slurm_job_id == only_id
        assert recovered.state == jobcore.PENDING

    # No blind resubmit, and the slot stays reserved.
    assert restarted_quota(restarted) == 1
    assert harness.sbatch_count() <= 1

    await restarted.stop()
    assert harness.scancel_count() == 0


async def test_ready_window_restores_endpoints_without_resubmit(
    e2e_harness, e2e_gateway_factory, tmp_path
):
    harness = e2e_harness
    limits = {"agent-a": {"frontier": 1}}
    state_root = tmp_path / "state"
    gateway, _prov = e2e_gateway_factory(
        state_root=state_root, job_limits=limits
    )
    await gateway.open_target("frontier")
    record = await _submit(gateway, A_TOKEN)
    harness.mark_running(record["slurm_job_id"])

    _install_hook(gateway, "after_ready_publish")
    with pytest.raises(Crash):
        await gateway.poll_jobs_once()

    crashed = gateway.job_store.load_record(record["job_id"])
    ready_payload = gateway.job_store.load_ready(record["job_id"])
    assert ready_payload is not None
    relay = crashed.relay_endpoint
    assert relay
    await gateway.stop()
    assert harness.scancel_count() == 0

    restarted, _prov2 = e2e_gateway_factory(
        state_root=state_root, job_limits=limits
    )
    await restarted.open_target("frontier")
    recovered = restarted.job_store.load_record(record["job_id"])
    assert recovered.state == jobcore.READY
    assert recovered.slurm_job_id == record["slurm_job_id"]
    assert recovered.relay_endpoint == relay
    assert recovered.container_endpoint
    assert harness.sbatch_count() == 1
    assert restarted_quota(restarted) == 1

    await restarted.stop()
    assert harness.scancel_count() == 0


def restarted_quota(gateway) -> int:
    return gateway.quota.counted("agent-a", "frontier")

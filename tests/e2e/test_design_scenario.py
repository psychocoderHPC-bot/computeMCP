# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Design §1 acceptance scenario on the real gateway stack.

Two clients, two private jobs for A plus one shared job, real HTTP endpoints,
real SSH (in-process login + container), real durable state.  The scenario
follows the design document:

1. opening submits nothing;
2. A submits two private jobs (one runs, one stays pending), B starts the
   shared job;
3. A transfers a file into its running job while B holds a PTY on the shared
   job;
4. the whole gateway stops: zero ``scancel``;
5. the pending job starts during the outage, a new gateway on the same state
   root restores ids, quota and the relay without another ``sbatch``;
6. B sees only the shared job and its own.
"""

from __future__ import annotations

import asyncio

import pytest

from tests.e2e.conftest import A_TOKEN, B_TOKEN, auth, running_client

pytestmark = pytest.mark.asyncio


async def _submit(client, token: str, request_id: str):
    response = await client.post(
        "/v1/targets/frontier/sbatch",
        headers=auth(token),
        json={"request_id": request_id},
    )
    assert response.status == 200, await response.text()
    return await response.json()


async def _listing(client, token: str) -> set[str]:
    response = await client.get(
        "/v1/jobs", headers=auth(token), params={"target": "frontier"}
    )
    assert response.status == 200, await response.text()
    return {entry["job_id"] for entry in (await response.json())["jobs"]}


async def _make_ready(gateway, harness, record):
    harness.mark_running(record["slurm_job_id"])
    await gateway.poll_jobs_once()
    stored = gateway.job_store.load_record(record["job_id"])
    assert stored.state == "ready", stored
    return stored


async def test_design_acceptance_scenario(e2e_harness, e2e_gateway_factory, tmp_path):
    harness = e2e_harness
    limits = {"agent-a": {"frontier": 2}, "agent-b": {"frontier": 2}}
    state_root = tmp_path / "state"
    gateway, provisioner = e2e_gateway_factory(
        state_root=state_root, job_limits=limits
    )

    try:
        # 1. Opening the route submits nothing.
        opened = await gateway.open_target("frontier")
        assert opened["state"] == "connected"
        assert provisioner.submit_calls == 0
        assert harness.sbatch_count() == 0
        assert gateway._active_shared_record("frontier") is None

        async with running_client(gateway) as client:
            # 2. A submits two private jobs; B starts the shared job.
            p1 = await _submit(client, A_TOKEN, "a-1")
            p2 = await _submit(client, A_TOKEN, "a-2")
            assert p1["state"] == p2["state"] == "pending"
            assert p1["slurm_job_id"] != p2["slurm_job_id"]
            assert gateway.quota.counted("agent-a", "frontier") == 2

            # A third private submit meets the quota; it must not reach Slurm.
            denied = await client.post(
                "/v1/targets/frontier/sbatch",
                headers=auth(A_TOKEN),
                json={"request_id": "a-3"},
            )
            assert denied.status == 429, await denied.text()
            assert harness.sbatch_count() == 2

            paused = await client.post(
                "/v1/targets/frontier/shared", headers=auth(B_TOKEN)
            )
            assert paused.status == 200, await paused.text()
            shared = (await paused.json())["job"]
            assert shared["kind"] == "shared"
            # The shared job consumes no personal quota.
            assert gateway.quota.counted("agent-a", "frontier") == 2
            assert gateway.quota.counted("agent-b", "frontier") == 0

            # One private job starts; the other remains pending.
            p1_ready = await _make_ready(gateway, harness, p1)
            shared_ready = await _make_ready(gateway, harness, shared)
            assert p1_ready.state == "ready"
            assert shared_ready.state == "ready"
            assert gateway.job_store.load_record(p2["job_id"]).state == "pending"

            # 3. B holds a PTY on the shared job while A transfers into p1.
            session = await client.post(
                "/v1/sessions",
                headers=auth(B_TOKEN),
                json={"target": "frontier", "job_id": shared["job_id"]},
            )
            assert session.status == 200, await session.text()
            session_id = (await session.json())["session_id"]

            payload = b"design-section-1-payload\n" * 4096
            upload = await client.put(
                "/v1/files/upload",
                headers=auth(A_TOKEN),
                params={
                    "target": "frontier",
                    "job_id": p1["job_id"],
                    "path": "/from-a.bin",
                },
                data=payload,
            )
            assert upload.status == 200, await upload.text()
            assert (await upload.json())["written"] == len(payload)

            read = await client.get(
                "/v1/files/read",
                headers=auth(A_TOKEN),
                params={
                    "target": "frontier",
                    "job_id": p1["job_id"],
                    "path": "/from-a.bin",
                },
            )
            assert read.status == 200, await read.text()
            assert (await read.json())["content"].encode() == payload

            # The shared PTY survived the unrelated transfer and echoes input.
            still = await client.get(
                f"/v1/sessions/{session_id}", headers=auth(B_TOKEN)
            )
            assert still.status == 200, await still.text()
            wrote = await client.post(
                f"/v1/sessions/{session_id}/write",
                headers=auth(B_TOKEN),
                json={"data": "still-here\n"},
            )
            assert wrote.status == 200, await wrote.text()
            await asyncio.sleep(0.1)
            echoed = await client.post(
                f"/v1/sessions/{session_id}/read",
                headers=auth(B_TOKEN),
                json={"max_bytes": 65536},
            )
            assert echoed.status == 200, await echoed.text()
            assert "still-here" in (await echoed.json())["data"]

            # Pending p2 has no container: exec is a clean conflict, not a dial.
            pending_exec = await client.post(
                "/v1/exec",
                headers=auth(A_TOKEN),
                json={"target": "frontier", "job_id": p2["job_id"],
                      "command": "true"},
            )
            assert pending_exec.status == 409, await pending_exec.text()

            # Exec on the ready private job works over the real container SSH.
            exec_ok = await client.post(
                "/v1/exec",
                headers=auth(A_TOKEN),
                json={"target": "frontier", "job_id": p1["job_id"],
                      "command": "whoami"},
            )
            assert exec_ok.status == 200, await exec_ok.text()
            assert (await exec_ok.json())["stdout"].strip() == "agent"

            # Visibility: A sees its two private jobs plus shared; B sees shared.
            assert await _listing(client, A_TOKEN) == {
                p1["job_id"], p2["job_id"], shared["job_id"]
            }
            assert await _listing(client, B_TOKEN) == {shared["job_id"]}

        # 4. Whole-gateway shutdown: detach only, zero scancel.
        p1_relay = gateway.job_store.load_record(p1["job_id"]).relay_endpoint
        assert p1_relay
        await gateway.stop()
        assert provisioner.cancel_calls == 0
        assert harness.scancel_count() == 0
        assert gateway.job_store.load_record(p1["job_id"]).slurm_job_id == (
            p1["slurm_job_id"]
        )

        # 5. The pending job starts during the outage; a new gateway reconciles.
        harness.mark_running(p2["slurm_job_id"])
        restarted, restarted_prov = e2e_gateway_factory(
            state_root=state_root, job_limits=limits
        )
        opened_again = await restarted.open_target("frontier")
        assert opened_again["state"] == "connected"
        await restarted.poll_jobs_once()

        assert restarted.job_store.load_record(p1["job_id"]).slurm_job_id == (
            p1["slurm_job_id"]
        )
        assert restarted.job_store.load_record(p2["job_id"]).slurm_job_id == (
            p2["slurm_job_id"]
        )
        assert restarted.job_store.load_record(shared["job_id"]).slurm_job_id == (
            shared["slurm_job_id"]
        )
        # Relay information survives the restart.
        assert restarted.job_store.load_record(p1["job_id"]).relay_endpoint == p1_relay
        # Quota is rebuilt from durable records; shared costs nobody.
        assert restarted.quota.counted("agent-a", "frontier") == 2
        assert restarted.quota.counted("agent-b", "frontier") == 0
        # No duplicate submission: the original three sbatch calls are all.
        assert restarted_prov.submit_calls == 0
        assert harness.sbatch_count() == 3
        # The job that started during the outage is now usable.
        assert restarted.job_store.load_record(p2["job_id"]).state == "ready"

        # 6. B sees the shared job and now its own job.
        async with running_client(restarted) as client:
            b_own = await _submit(client, B_TOKEN, "b-1")
            assert await _listing(client, B_TOKEN) == {
                shared["job_id"], b_own["job_id"]
            }
            assert await _listing(client, A_TOKEN) == {
                p1["job_id"], p2["job_id"], shared["job_id"]
            }

        await restarted.stop()
        assert restarted_prov.cancel_calls == 0
        assert harness.scancel_count() == 0

        # Real login authentications: one reused connection per gateway, none
        # per scheduler command.
        assert len(harness.login_connections) == 2
        assert harness.login_server_obj.connections == 2
    finally:
        try:
            await gateway.stop()
        except Exception:
            pass

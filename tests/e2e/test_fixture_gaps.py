# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Fixture-matrix gaps: non-Slurm open, job authorization, SFTP/PTY, clean root.

Each test fills one audit gap that the existing suite did not cover with the
real gateway stack:

* #3  a non-Slurm direct target: ``target-open`` starts no container/job and a
      private submit returns an actionable 400;
* #4  job-id authorization on exec, every file operation and PTY creation:
      a foreign id and an unknown id are indistinguishable;
* #10 a 100 MiB SFTP transfer verifies by SHA-256 while a PTY on another job
      stays responsive;
* #14 two clean jobs get distinct writable runtime directories, the base digest
      is recorded, and fakeroot vs clean settings stay isolated.

The alternate-login relay rebuild (#8) and cross-node shared-root locks (#12)
cannot be reproduced on one loopback host and are recorded as unverified in the
task report.
"""

from __future__ import annotations

import hashlib

import pytest
from aiohttp.test_utils import TestClient, TestServer

from compute_mcp import jobs as jobcore
from compute_mcp.auth import hash_token
from compute_mcp.config import (
    ClientConfig,
    GatewayConfig,
    ServerConfig,
    SessionConfig,
    SSHConfig,
    TargetConfig,
    TransportConfig,
)
from compute_mcp.gateway import (
    JOB_ACCESS_DENIED_MESSAGE,
    JOB_NOT_FOUND_TEXT,
    Gateway,
    build_production_gateway,
)

from .conftest import (
    A_TOKEN,
    B_TOKEN,
    FINGERPRINT,
    auth,
    make_target,
    running_client,
)

pytestmark = pytest.mark.asyncio

UNKNOWN = "job-" + "0" * 32


async def _submit(client, token: str, request_id: str):
    response = await client.post(
        "/v1/targets/frontier/sbatch",
        headers=auth(token),
        json={"request_id": request_id},
    )
    assert response.status == 200, await response.text()
    return await response.json()


async def _ready(gateway, harness, record):
    harness.mark_running(record["slurm_job_id"])
    await gateway.poll_jobs_once()
    return gateway.job_store.load_record(record["job_id"])


# ---------------------------------------------------------------------------
# #3 non-Slurm direct target
# ---------------------------------------------------------------------------
async def test_non_slurm_open_starts_nothing_and_sbatch_is_actionable(tmp_path):
    target = TargetConfig(
        name="hal",
        user="agent",
        transport=TransportConfig(kind="direct", remote_host="127.0.0.1",
                                  remote_port=9),
        host_key_sha256=FINGERPRINT,
    )
    cfg = GatewayConfig(
        server=ServerConfig(),
        ssh=SSHConfig(),
        sessions=SessionConfig(),
        targets={"hal": target},
        clients={
            "agent-a": ClientConfig(
                client_id="agent-a",
                token_sha256=hash_token(A_TOKEN),
                targets=("hal",),
            ),
            "admin": ClientConfig(
                client_id="admin", token_sha256=hash_token("admin-token"),
                allow_all=True,
            ),
        },
    )
    gateway = Gateway(cfg, state_root=tmp_path / "state")

    connects: list[str] = []

    async def forbidden_connect(*_args, **_kwargs):
        connects.append("connect")
        raise AssertionError("target-open must not start a container")

    gateway.tunnels.connect = forbidden_connect  # type: ignore[assignment]

    opened = await gateway.open_target("hal")
    assert connects == []
    assert gateway.jobs is None  # no durable job registry was created
    assert gateway.runtimes["hal"].state != "connected"

    client = TestClient(TestServer(gateway.create_app()))
    await client.start_server()
    try:
        sbatch = await client.post(
            "/v1/targets/hal/sbatch", headers=auth(A_TOKEN), json={}
        )
        assert sbatch.status == 400, await sbatch.text()
        assert "private Slurm jobs are not available" in await sbatch.text()
        shared = await client.post(
            "/v1/targets/hal/shared", headers=auth(A_TOKEN), json={}
        )
        assert shared.status == 400, await shared.text()
        assert "does not use Slurm jobs" in await shared.text()
        assert connects == []
        assert gateway.jobs is None
    finally:
        await client.close()
        await gateway.stop()


# ---------------------------------------------------------------------------
# #4 job-id authorization: foreign == unknown on every job-scoped route
# ---------------------------------------------------------------------------
async def test_job_id_authorization_foreign_equals_unknown(
    e2e_harness, e2e_gateway_factory, tmp_path
):
    harness = e2e_harness
    limits = {"agent-a": {"frontier": 1}, "agent-b": {"frontier": 1}}
    gateway, _prov = e2e_gateway_factory(
        state_root=tmp_path / "state", job_limits=limits
    )
    await gateway.open_target("frontier")
    async with running_client(gateway) as client:
        a_job = await _submit(client, A_TOKEN, "a-1")
        b_job = await _submit(client, B_TOKEN, "b-1")
        await _ready(gateway, harness, a_job)
        await _ready(gateway, harness, b_job)

        def request(method: str, path: str, *, params=None, json=None, data=None):
            async def call(job_id: str):
                query = dict(params or {})
                query.update({"target": "frontier", "job_id": job_id})
                return await client.request(
                    method, path, headers=auth(B_TOKEN),
                    params=query, json=json, data=data,
                )
            return call

        async def job_get(job_id: str):
            return await client.get(f"/v1/jobs/{job_id}", headers=auth(B_TOKEN))

        async def job_cancel(job_id: str):
            return await client.post(
                f"/v1/jobs/{job_id}/cancel", headers=auth(B_TOKEN)
            )

        async def session_create(job_id: str):
            return await client.post(
                "/v1/sessions", headers=auth(B_TOKEN),
                json={"target": "frontier", "job_id": job_id},
            )

        routes = {
            "exec": lambda jid: client.post(
                "/v1/exec", headers=auth(B_TOKEN),
                json={"target": "frontier", "job_id": jid, "command": "true"},
            ),
            "file-stat": request(
                "GET", "/v1/files/stat", params={"path": "/x"}),
            "file-list": request(
                "GET", "/v1/files/list", params={"path": "/"}),
            "file-read": request(
                "GET", "/v1/files/read", params={"path": "/x"}),
            "file-write": request(
                "PUT", "/v1/files/write", json={"path": "/x", "content": "y"}),
            "file-mkdir": request(
                "POST", "/v1/files/mkdir", json={"path": "/d"}),
            "file-remove": request(
                "POST", "/v1/files/remove", json={"path": "/x"}),
            "file-rename": request(
                "POST", "/v1/files/rename",
                json={"source": "/x", "destination": "/y"}),
            "file-chmod": request(
                "POST", "/v1/files/chmod", json={"path": "/x", "mode": 420}),
            "file-upload": request(
                "PUT", "/v1/files/upload", params={"path": "/x"}, data=b"z"),
            "pty": session_create,
            "job-get": job_get,
            "job-cancel": job_cancel,
        }

        for name, call in routes.items():
            foreign = await call(a_job["job_id"])
            unknown = await call(UNKNOWN)
            assert foreign.status == unknown.status == 404, (
                f"{name}: foreign {foreign.status}, unknown {unknown.status}"
            )
            foreign_text = await foreign.text()
            unknown_text = await unknown.text()
            assert foreign_text == unknown_text, name
            assert foreign_text in (
                JOB_ACCESS_DENIED_MESSAGE, JOB_NOT_FOUND_TEXT
            ), (name, foreign_text)

        # B can still use its own job (authorization is not a blanket deny).
        own = await client.post(
            "/v1/exec", headers=auth(B_TOKEN),
            json={"target": "frontier", "job_id": b_job["job_id"],
                  "command": "whoami"},
        )
        assert own.status == 200, await own.text()
        assert (await own.json())["stdout"].strip() == "agent"


# ---------------------------------------------------------------------------
# #10 large SFTP transfer while another job's PTY is live
# ---------------------------------------------------------------------------
async def test_large_transfer_hashes_and_pty_survives(
    e2e_harness, e2e_gateway_factory, tmp_path
):
    harness = e2e_harness
    limits = {"agent-a": {"frontier": 2}}
    gateway, _prov = e2e_gateway_factory(
        state_root=tmp_path / "state", job_limits=limits
    )
    await gateway.open_target("frontier")
    async with running_client(gateway) as client:
        hull = await _submit(client, A_TOKEN, "hull")
        shell = await _submit(client, A_TOKEN, "shell")
        await _ready(gateway, harness, hull)
        await _ready(gateway, harness, shell)

        session = await client.post(
            "/v1/sessions", headers=auth(A_TOKEN),
            json={"target": "frontier", "job_id": shell["job_id"]},
        )
        assert session.status == 200, await session.text()
        session_id = (await session.json())["session_id"]

        size = 100 * 1024 * 1024  # >= 100 MiB
        chunk = bytes(range(256)) * 4096
        payload = chunk * (size // len(chunk))
        payload += b"\0" * (size - len(payload))
        digest = hashlib.sha256(payload).hexdigest()

        upload = client.put(
            "/v1/files/upload",
            headers=auth(A_TOKEN),
            params={
                "target": "frontier",
                "job_id": hull["job_id"],
                "path": "/big.bin",
            },
            data=payload,
        )
        upload_task = __import__("asyncio").create_task(upload)
        # The PTY on the other job stays alive while the transfer runs.
        alive = await client.get(
            f"/v1/sessions/{session_id}", headers=auth(A_TOKEN)
        )
        assert alive.status == 200, await alive.text()
        wrote = await client.post(
            f"/v1/sessions/{session_id}/write",
            headers=auth(A_TOKEN), json={"data": "ping\n"},
        )
        assert wrote.status == 200, await wrote.text()

        response = await upload_task
        assert response.status == 200, await response.text()
        body = await response.json()
        assert body["written"] == len(payload)
        assert body["complete"] is True

        downloaded = await client.get(
            "/v1/files/read",
            headers=auth(A_TOKEN),
            params={
                "target": "frontier",
                "job_id": hull["job_id"],
                "path": "/big.bin",
                "encoding": "stream",
            },
        )
        assert downloaded.status == 200
        data = await downloaded.read()
        assert len(data) == len(payload)
        assert hashlib.sha256(data).hexdigest() == digest

        # Still alive after the transfer.
        assert (
            await client.get(f"/v1/sessions/{session_id}", headers=auth(A_TOKEN))
        ).status == 200


# ---------------------------------------------------------------------------
# #14 clean vs fakeroot isolation and recorded digest
# ---------------------------------------------------------------------------
async def test_clean_jobs_have_isolated_runtime_and_digest(tmp_path):
    from tests.e2e.conftest import _RouteTunnel  # local import keeps conftest lean

    digest = "a" * 64
    calls: list[dict] = []

    async def runner(connection, command, *, env=None, timeout=None):
        env = dict(env or {})
        calls.append({"command": command, "env": env})
        job_id = env.get("COMPUTEMCP_JOB_ID", "")
        if command.endswith("submit"):
            output = f"SLURM_JOB_ID 900\nSTATE pending\n"
            if env.get("COMPUTEMCP_ROOT_MODE") == "clean":
                output += f"IMAGE_DIGEST {digest}\n"
            return (0, output, "")
        return (0, "", "")

    home_base = tmp_path / "homes"
    home_base.mkdir()
    target = _stage_target(tmp_path, home_base)

    gateway = build_production_gateway(
        _stage_config(target),
        state_root=tmp_path / "state",
        stage_runner=runner,
        stage_deploy_dir="/tmp/cmcp-e2e",
        stage_connection=object(),
    )

    async def route_opener(target, route, factor=None, generation=0):
        return _RouteTunnel(target, route)

    gateway.route_opener = route_opener
    client = gateway.auth.authenticate_bearer(f"Bearer {A_TOKEN}")

    await gateway.open_target("frontier")
    fakeroot = await gateway.target_sbatch("frontier", client, request_id="fakeroot")
    clean = await gateway.target_sbatch(
        "frontier", client, request_id="clean",
        root_mode="clean", clean_home="bench/one",
    )

    assert fakeroot["root_mode"] == "fakeroot"
    assert clean["root_mode"] == "clean"
    assert clean["home"] == "bench/one"
    assert fakeroot["home"] is None
    assert clean["base_digest"] == digest
    assert fakeroot["base_digest"] is None

    store = gateway.job_store
    fakeroot_dir = store.job_dir(fakeroot["job_id"])
    clean_dir = store.job_dir(clean["job_id"])
    assert fakeroot_dir != clean_dir
    assert fakeroot_dir.parent == clean_dir.parent == gateway.state_root / "jobs"
    # Neither job can write into the other's runtime directory through a
    # shared parent component.
    assert fakeroot_dir not in clean_dir.parents
    assert clean_dir not in fakeroot_dir.parents

    clean_env = next(
        call["env"] for call in calls
        if call["command"].endswith("submit")
        and call["env"].get("COMPUTEMCP_JOB_ID") == clean["job_id"]
    )
    fakeroot_env = next(
        call["env"] for call in calls
        if call["command"].endswith("submit")
        and call["env"].get("COMPUTEMCP_JOB_ID") == fakeroot["job_id"]
    )
    assert clean_env["COMPUTEMCP_ROOT_MODE"] == "clean"
    assert clean_env["COMPUTEMCP_CLEAN_HOME"] == "bench/one"
    assert clean_env["COMPUTEMCP_STATE_DIR"] == str(clean_dir)
    assert fakeroot_env["COMPUTEMCP_ROOT_MODE"] == "fakeroot"
    assert fakeroot_env["COMPUTEMCP_CLEAN_HOME"] == ""
    assert fakeroot_env["COMPUTEMCP_STATE_DIR"] == str(fakeroot_dir)
    await gateway.stop()


def _stage_target(tmp_path, home_base) -> TargetConfig:
    from compute_mcp.config import (
        BundleConfig,
        SlurmConfig,
        SlurmStageConfig,
    )

    return TargetConfig(
        name="frontier",
        user="login",
        transport=TransportConfig(kind="tunnel", ssh_targets=("frontier",)),
        host_key_sha256=FINGERPRINT,
        client_key="/tmp/does-not-exist-key",
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(options={"ntasks-per-node": 1}),
            srun=SlurmStageConfig(),
        ),
        bundle=BundleConfig(source="computemcp-slurm", deploy_dir="/tmp/cmcp-e2e"),
        home_base=str(home_base),
    )


def _stage_config(target: TargetConfig) -> GatewayConfig:
    return GatewayConfig(
        server=ServerConfig(),
        ssh=SSHConfig(),
        sessions=SessionConfig(),
        targets={target.name: target},
        clients={
            "agent-a": ClientConfig(
                client_id="agent-a",
                token_sha256=hash_token(A_TOKEN),
                targets=(target.name,),
                job_limits={target.name: 2},
            ),
            "admin": ClientConfig(
                client_id="admin", token_sha256=hash_token("admin-token"),
                allow_all=True,
            ),
        },
    )

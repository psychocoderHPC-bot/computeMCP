# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Tests for the stateful fake Slurm harness in ``tests/fakes``.

The harness is the scheduler used by the multi-job design tests (design §10
fixtures 2, 5, 6, 7, 8).  These tests pin its contract:

* no command means an empty log (open/no-submit submits nothing);
* N concurrent submits for the last slot admit exactly one job;
* PENDING -> RUNNING -> COMPLETED is observable through ``squeue``;
* ``scancel`` marks a job CANCELLED;
* the queue survives separate processes (durable ``queue.json``);
* a shutdown/inspection path emits zero ``scancel``;
* ``sbatch`` parses argv/params and records script path/body/comment.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

# ``tests`` has no __init__.py; make ``fakes`` importable no matter how pytest
# is invoked, then re-export the fixtures into this module's namespace.
_TEST_DIR = Path(__file__).resolve().parent
if str(_TEST_DIR) not in sys.path:
    sys.path.insert(0, str(_TEST_DIR))

from fakes.conftest import fake_slurm_ctl, fake_slurm_env  # noqa: E402,F401


def _script(tmp_path: Path, name: str = "job.sh", body: str = "#!/bin/bash\necho job\n"):
    path = tmp_path / name
    path.write_text(body, encoding="utf-8")
    return path


def _split_parsable(stdout: str):
    job_id, cluster = stdout.strip().split(";", 1)
    return int(job_id), cluster


def test_open_without_submit_leaves_log_empty(fake_slurm_env):
    """Fixture 2: opening a target must submit nothing and log nothing."""
    env = fake_slurm_env
    assert not env.log_path.exists()
    assert env.read_log() == []
    assert env.read_state() is None


def test_concurrent_last_slot_admits_exactly_one(fake_slurm_env, tmp_path):
    """Fixture 5: N barrier-synchronised submits race for the last slot."""
    env = fake_slurm_env
    script = _script(tmp_path)
    workers = 8
    barrier = tmp_path / "barrier"
    extra = {
        "FAKE_SLURM_MAX_ACTIVE": "1",
        "FAKE_SLURM_BARRIER_DIR": str(barrier),
        "FAKE_SLURM_BARRIER_N": str(workers),
        "FAKE_SLURM_BARRIER_TIMEOUT": "20",
    }
    child_env = env.environment(extra)
    procs = [
        subprocess.Popen(
            ["sbatch", "--parsable", str(script)],
            env=child_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(workers)
    ]
    results = []
    for proc in procs:
        stdout, stderr = proc.communicate(timeout=60)
        results.append((proc.returncode, stdout, stderr))

    accepted = [item for item in results if item[0] == 0]
    rejected = [item for item in results if item[0] != 0]
    assert len(accepted) == 1, results
    assert len(rejected) == workers - 1
    for code, _stdout, stderr in rejected:
        assert code == 1
        assert "queue is full" in stderr

    state = env.read_state()
    assert state is not None
    assert len(state["jobs"]) == 1
    (job,) = state["jobs"].values()
    assert job["state"] == "PENDING"
    assert state["next_id"] == job["id"] + 1

    sbatch_entries = [
        entry for entry in env.read_log() if entry["command"] == "sbatch"
    ]
    assert len(sbatch_entries) == workers
    assert len([e for e in sbatch_entries if e["result"] == "ok"]) == 1
    assert len([e for e in sbatch_entries if e["result"] == "rejected"]) == workers - 1


def test_pending_to_ready_and_squeue_format(fake_slurm_env, fake_slurm_ctl, tmp_path):
    """Fixture 6: a queued job is visible as PENDING, then becomes RUNNING."""
    env = fake_slurm_env
    script = _script(tmp_path, "demo.sh", "#!/bin/bash\necho demo\n")
    proc = env.submit(script, "--job-name", "demo")
    assert proc.returncode == 0, proc.stderr
    job_id, cluster = _split_parsable(proc.stdout)
    assert cluster == "testcluster"

    queued = env.run("squeue", "--noheader", "--format", "%i|%T|%j")
    assert queued.returncode == 0
    assert f"{job_id}|PENDING|demo" in queued.stdout

    assert fake_slurm_ctl("start-job", str(job_id)).returncode == 0
    running = env.run("squeue", "--noheader", "--format", "%i|%T|%j")
    assert f"{job_id}|RUNNING|demo" in running.stdout

    assert fake_slurm_ctl("set-state", str(job_id), "COMPLETED").returncode == 0
    # Terminal jobs leave the default queue view.
    assert env.run("squeue", "--noheader").stdout.strip() == ""
    finished = env.run("squeue", "--noheader", "--states", "COMPLETED", "--format", "%i|%T")
    assert f"{job_id}|COMPLETED" in finished.stdout


def test_squeue_filters_user_and_cluster(fake_slurm_env, tmp_path):
    """squeue honours --user and --clusters."""
    env = fake_slurm_env
    script = _script(tmp_path)

    alice = env.submit(script, "--job-name", "a", extra_env={"FAKE_SLURM_USER": "alice"})
    bob = env.submit(
        script,
        "--job-name",
        "b",
        "--clusters=other",
        extra_env={"FAKE_SLURM_USER": "bob"},
    )
    assert alice.returncode == 0, alice.stderr
    assert bob.returncode == 0, bob.stderr
    alice_id, _ = _split_parsable(alice.stdout)
    bob_id, bob_cluster = _split_parsable(bob.stdout)
    assert bob_cluster == "other"

    mine = env.run("squeue", "--noheader", "--user", "alice", "--format", "%i|%j")
    assert f"{alice_id}|a" in mine.stdout
    assert f"{bob_id}|b" not in mine.stdout

    only_other = env.run(
        "squeue", "--noheader", "--clusters", "other", "--format", "%i|%j"
    )
    assert f"{bob_id}|b" in only_other.stdout
    assert f"{alice_id}|a" not in only_other.stdout


def test_cancel_marks_job_cancelled(fake_slurm_env, fake_slurm_ctl, tmp_path):
    """Fixture 6: scancel moves one pending job to CANCELLED, leaving peers."""
    env = fake_slurm_env
    first = env.submit(_script(tmp_path))
    second = env.submit(_script(tmp_path))
    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    job_id, _ = _split_parsable(first.stdout)
    other_id, _ = _split_parsable(second.stdout)

    cancel = env.run("scancel", str(job_id))
    assert cancel.returncode == 0, cancel.stderr

    state = env.read_state()
    assert state["jobs"][str(job_id)]["state"] == "CANCELLED"
    assert state["jobs"][str(other_id)]["state"] == "PENDING"
    visible = env.run("squeue", "--noheader", "--format", "%i")
    assert str(other_id) in visible.stdout
    assert str(job_id) not in visible.stdout

    listed = json.loads(fake_slurm_ctl("list").stdout)
    assert listed["jobs"][str(job_id)]["state"] == "CANCELLED"

    scancel_entries = [e for e in env.read_log() if e["command"] == "scancel"]
    assert len(scancel_entries) == 1
    assert scancel_entries[0]["cancelled"] == [job_id]


def test_state_survives_process_restart(fake_slurm_env, fake_slurm_ctl, tmp_path):
    """Fixture 7/8: queue.json is durable and job ids stay monotonic."""
    env = fake_slurm_env
    script = _script(tmp_path)

    first = env.submit(script)
    assert first.returncode == 0, first.stderr
    first_id, _ = _split_parsable(first.stdout)
    assert env.queue_path.exists()

    # Every command is a separate process: this is a cold read of the state.
    listed = json.loads(fake_slurm_ctl("list").stdout)
    assert str(first_id) in listed["jobs"]
    assert listed["next_id"] == first_id + 1

    second = env.submit(script)
    assert second.returncode == 0, second.stderr
    second_id, _ = _split_parsable(second.stdout)
    assert second_id == first_id + 1

    fresh = env.run("squeue", "--noheader", "--format", "%i|%T")
    assert f"{first_id}|PENDING" in fresh.stdout
    assert f"{second_id}|PENDING" in fresh.stdout

    state = env.read_state()
    assert set(state["jobs"]) == {str(first_id), str(second_id)}
    assert state["next_id"] == second_id + 1


def test_shutdown_path_records_zero_scancel(fake_slurm_env, fake_slurm_ctl, tmp_path):
    """Fixture 8: inspecting/returning during shutdown must not cancel jobs."""
    env = fake_slurm_env
    script = _script(tmp_path)
    ids = []
    for index in range(2):
        proc = env.submit(script, "--job-name", f"job{index}")
        assert proc.returncode == 0, proc.stderr
        ids.append(_split_parsable(proc.stdout)[0])

    # Emulate the gateway shutdown path: read state, do not submit or cancel.
    assert fake_slurm_ctl("list").returncode == 0
    assert env.run("squeue", "--noheader").returncode == 0
    assert fake_slurm_ctl("get", str(ids[0])).returncode == 0

    entries = env.read_log()
    assert entries
    assert not [e for e in entries if e["command"] == "scancel"]
    state = env.read_state()
    for job_id in ids:
        assert state["jobs"][str(job_id)]["state"] == "PENDING"


def test_sbatch_argv_and_param_parsing(fake_slurm_env, tmp_path):
    """Fixture 2: sbatch records argv, script body and parsed parameters."""
    env = fake_slurm_env
    body = "#!/bin/bash\necho parsed\n"
    script = _script(tmp_path, "parse.sh", body)
    args = [
        "--parsable",
        "--comment=corr-abc",
        "--job-name=demo",
        "--partition=debug",
        "--nodes=2",
        "--ntasks=4",
        "--cpus-per-task=2",
        "--mem=8G",
        "--time=01:00:00",
        "--gpus=1",
        "--output=out.log",
        str(script),
    ]
    proc = env.run("sbatch", *args)
    assert proc.returncode == 0, proc.stderr
    job_id, cluster = _split_parsable(proc.stdout)
    assert cluster == "testcluster"

    job = env.read_state()["jobs"][str(job_id)]
    assert job["state"] == "PENDING"
    assert job["script_path"] == str(script)
    assert job["script_body"] == body
    assert job["comment"] == "corr-abc"
    assert job["name"] == "demo"
    assert job["partition"] == "debug"
    expected = {
        "nodes": "2",
        "ntasks": "4",
        "cpus-per-task": "2",
        "mem": "8G",
        "time": "01:00:00",
        "gpus": "1",
        "output": "out.log",
    }
    for key, value in expected.items():
        assert job["params"][key] == value
    assert "--comment=corr-abc" in job["argv"]
    assert job["argv"][-1] == str(script)

    # Separate-value spelling and --wrap (no script path).
    wrapped = env.run(
        "sbatch",
        "--parsable",
        "--comment",
        "corr space",
        "--job-name",
        "wrapped",
        "--wrap",
        "echo hi",
    )
    assert wrapped.returncode == 0, wrapped.stderr
    wrap_id, _ = _split_parsable(wrapped.stdout)
    wrap_job = env.read_state()["jobs"][str(wrap_id)]
    assert wrap_job["comment"] == "corr space"
    assert wrap_job["name"] == "wrapped"
    assert wrap_job["script_path"] is None
    assert wrap_job["script_body"] == "echo hi"

    # Short options map to the long parameter names.
    short = env.run("sbatch", "--parsable", "-J", "shorty", "-p", "debug", str(script))
    assert short.returncode == 0, short.stderr
    short_id, _ = _split_parsable(short.stdout)
    short_job = env.read_state()["jobs"][str(short_id)]
    assert short_job["name"] == "shorty"
    assert short_job["partition"] == "debug"

    # Non-parsable superset stays Slurm-compatible.
    classic = env.submit(script, parsable=False)
    assert classic.returncode == 0, classic.stderr
    assert classic.stdout.startswith("Submitted batch job ")


def test_sinfo_reports_one_partition_and_reset_clears(fake_slurm_env, fake_slurm_ctl, tmp_path):
    """sinfo emits one partition line; slurmctl reset clears queue and log."""
    env = fake_slurm_env
    sinfo = env.run("sinfo", "--noheader")
    assert sinfo.returncode == 0
    lines = [line for line in sinfo.stdout.splitlines() if line.strip()]
    assert len(lines) == 1
    assert "idle" in lines[0]

    proc = env.submit(_script(tmp_path))
    assert proc.returncode == 0, proc.stderr
    assert env.read_state()["jobs"]

    reset = fake_slurm_ctl("reset")
    assert reset.returncode == 0
    state = env.read_state()
    assert state["jobs"] == {}
    assert state["next_id"] == 1
    reset_entries = [e for e in env.read_log() if e.get("subcommand") == "reset"]
    assert len(reset_entries) == 1


def test_scancel_unknown_job_errors(fake_slurm_env):
    """An unknown bare id is a scancel error, not a silent success."""
    env = fake_slurm_env
    proc = env.run("scancel", "999")
    assert proc.returncode == 1
    assert "Invalid job id" in proc.stderr

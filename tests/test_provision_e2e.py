# SPDX-FileCopyrightText: Ren\u00e9 Widera
#
# SPDX-License-Identifier: ISC
"""End-to-end provisioner flow with executable evidence.

Task 2 of the direct-provision integration pass.  This module shares the
hermetic stub cluster from test_provision_shell.py (its fixture writes the
stub ``sbatch``/``srun``/``docker`` executables and the loopback banner
server) but drives the REAL ``computemcp-provision.sh`` for both scheduling
modes in fresh ``tmp_path`` directories, asserting the observable side
effects: state files, the docker stub trace, and the reported endpoint.

Covered behaviour (evidence, not just "does not fail"):

- Slurm mode: ``sbatch`` is actually invoked, jobid is tracked in the state
  directory, the relay listens on the configured forward port and
  ``ENDPOINT 127.0.0.1:<forward>`` is reported.
- Direct mode: the container is built and started on the login node,
  Docker's ephemeral published host port is reported
  (``ENDPOINT 127.0.0.1:<published>``), a second provision reuses the
  running container with no rebuild/restart (the docker stub trace stays
  at one build / one create / one start), and ``stop`` works both with the
  full COMPUTEMCP_* environment and with only ``COMPUTEMCP_STATE_DIR``
  (the gateway-style stop call).
"""

from __future__ import annotations

import contextlib
import os
import subprocess
from pathlib import Path

import pytest

from test_provision_shell import (
    PROVISION,
    _Stubs,
    _base_env,
    _cleanup,
    _docker_counts,
    _free_port,
    _published_port,
    _wait_port,
    stubs_banner_path,
)

pytestmark = pytest.mark.skipif(
    subprocess.run(["bash", "-c", "true"], check=False).returncode != 0,
    reason="bash is required for the shell tests",
)


@pytest.fixture
def stubs(tmp_path: Path):
    import test_provision_shell as shell
    stubs_ = _Stubs(tmp_path)
    shell._stubs_instance = stubs_
    return stubs_


def test_slurm_mode_submits_sbatch_and_reports_relay_endpoint(tmp_path, stubs):
    """Slurm mode: one sbatch submission; the relay port is the endpoint."""
    container_port = _free_port()
    forward_port = _free_port()
    workdir = tmp_path / "slurm"
    workdir.mkdir()
    storage = workdir / "storage"
    state_dir = storage / "slurmflow" / "state"

    env = _base_env("slurmflow", storage, container_port)
    env["COMPUTEMCP_FORWARD_PORT"] = str(forward_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["SLURM_JOB_OUT"] = str(workdir / "slurm-job.out")
    env["SLURM_JOBID_FILE"] = str(state_dir / "jobid")
    env["PATH"] = stubs.slurm.shell_path

    result = subprocess.run(
        ["bash", str(PROVISION), "provision"],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        cwd=workdir,
    )
    job_pid_file = Path(env["SLURM_JOB_OUT"] + ".pid")

    def teardown():
        if job_pid_file.exists():
            with contextlib.suppress(OSError, ValueError):
                os.kill(int(job_pid_file.read_text().strip()), 15)
        _cleanup()

    try:
        assert result.returncode == 0, (
            f"provision exited {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        endpoint_line = next(
            line for line in result.stdout.splitlines()
            if line.strip().startswith("ENDPOINT")
        )
        assert (
            endpoint_line.strip() == f"ENDPOINT 127.0.0.1:{forward_port}"
        ), endpoint_line
        # The scheduler was actually invoked and the jobid persisted.
        assert "sbatch --parsable" in (workdir / "slurm.log").read_text()
        assert (state_dir / "jobid").read_text().strip() == "4242"
        assert (state_dir / "mode").read_text().strip() == "slurm"
        settings = state_dir / "srun-slurmflow.settings"
        assert settings.exists()
        assert "COMPUTEMCP_PROVISION_ENV" in settings.read_text()
        # The relay must really listen on the reported forward port.
        assert _wait_port(forward_port, timeout=15), (
            f"relay not listening on {forward_port}\nstderr:\n{result.stderr}"
        )
    finally:
        teardown()


def test_direct_mode_build_start_published_endpoint_idempotent(tmp_path, stubs):
    """Direct mode: one build/create/start, published endpoint, reuse on repeat."""
    container_port = _free_port()
    workdir = tmp_path / "direct"
    workdir.mkdir()
    storage = workdir / "storage"
    published_port = _published_port(container_port)

    env = _base_env("directflow", storage, container_port)
    env["DOCKER_STUB_PUBLISHED_PORT"] = str(published_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["PATH"] = stubs.direct.shell_path

    try:
        first = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
            cwd=workdir,
        )
        assert first.returncode == 0, (
            f"first provision exited {first.returncode}\n"
            f"stdout:\n{first.stdout}\nstderr:\n{first.stderr}"
        )
        # Docker publishes an ephemeral host port; that is what gets reported.
        assert (
            first.stdout.splitlines()[-1].strip()
            == f"ENDPOINT 127.0.0.1:{published_port}"
        ), first.stdout
        # Direct mode never touches the scheduler.
        slurm_log = workdir / "slurm.log"
        assert not slurm_log.exists() or not slurm_log.read_text().strip(), (
            f"direct mode invoked a Slurm tool:\n{slurm_log.read_text()}"
        )
        # First time: exactly one build, one create, one start.
        assert _docker_counts(workdir / "docker.log") == {
            "build": 1, "create": 1, "start": 1, "stop": 0,
        }, _docker_counts(workdir / "docker.log")
        assert _wait_port(published_port, timeout=15)

        # Idempotent: second run must NOT rebuild or restart.
        second = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
            cwd=workdir,
        )
        assert second.returncode == 0, second.stderr
        assert (
            second.stdout.splitlines()[-1].strip()
            == f"ENDPOINT 127.0.0.1:{published_port}"
        ), second.stdout
        assert _docker_counts(workdir / "docker.log") == {
            "build": 1, "create": 1, "start": 1, "stop": 0,
        }, _docker_counts(workdir / "docker.log")
        assert "Reusing running container" in second.stderr
        # The tracked mode and runtime are recorded for later env-less calls.
        assert (
            storage / "directflow" / "state" / "mode"
        ).read_text().strip() == "direct"
        assert (
            storage / "directflow" / "state" / "container.runtime"
        ).read_text().strip() == "docker"
    finally:
        _cleanup()


def test_direct_mode_stop_without_full_env(tmp_path, stubs):
    """stop with only COMPUTEMCP_STATE_DIR (the gateway-style call) works."""
    container_port = _free_port()
    workdir = tmp_path / "stopenv"
    workdir.mkdir()
    storage = workdir / "storage"
    state_dir = storage / "stopflow" / "state"
    published_port = _published_port(container_port)

    env = _base_env("stopflow", storage, container_port)
    env["DOCKER_STUB_PUBLISHED_PORT"] = str(published_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["PATH"] = stubs.direct.shell_path

    try:
        provision = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
            cwd=workdir,
        )
        assert provision.returncode == 0, provision.stderr
        assert (state_dir / "container.runtime").read_text().strip() == "docker"
        assert _wait_port(published_port, timeout=10)

        stop_no_env = {
            "PATH": stubs.direct.shell_path,
            "HOME": os.environ.get("HOME", str(workdir)),
            "COMPUTEMCP_STATE_DIR": str(state_dir),
            "BANNER_SERVER": str(stubs_banner_path()),
            "DOCKER_STUB_LOG": str(workdir / "docker-stop-noenv.log"),
            "DOCKER_STUB_STATE": str(workdir / "dockerstate"),
        }
        stop1 = subprocess.run(
            ["bash", str(PROVISION), "stop"],
            capture_output=True,
            text=True,
            timeout=60,
            env=stop_no_env,
            cwd=workdir,
        )
        # The regression guard: no "must be apptainer or docker" failure.
        assert "must be apptainer or docker" not in stop1.stderr, stop1.stderr
        assert stop1.returncode == 0, stop1.stderr
        stop_log = workdir / "docker-stop-noenv.log"
        assert stop_log.exists(), "docker stub was not invoked for stop"
        assert "docker stop computemcp-stopflow" in stop_log.read_text()
        assert not _wait_port(published_port, timeout=3)
    finally:
        _cleanup()


def test_direct_mode_stop_with_full_env(tmp_path, stubs):
    """stop with the full COMPUTEMCP_* environment works too."""
    container_port = _free_port()
    workdir = tmp_path / "stopfull"
    workdir.mkdir()
    storage = workdir / "storage"
    published_port = _published_port(container_port)

    env = _base_env("stopfull", storage, container_port)
    env["DOCKER_STUB_PUBLISHED_PORT"] = str(published_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["PATH"] = stubs.direct.shell_path

    try:
        provision = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
            cwd=workdir,
        )
        assert provision.returncode == 0, provision.stderr
        stop = subprocess.run(
            ["bash", str(PROVISION), "stop"],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
            cwd=workdir,
        )
        assert "must be apptainer or docker" not in stop.stderr, stop.stderr
        assert stop.returncode == 0, stop.stderr
        assert (
            (workdir / "docker.log").read_text()
            .count("docker stop computemcp-stopfull")
            == 1
        )
        assert not _wait_port(published_port, timeout=3)
    finally:
        _cleanup()

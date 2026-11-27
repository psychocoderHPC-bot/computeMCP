# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Environment-bootstrap gap (#16): stripped env, shell-stubbed python3, files.

The compute-node batch script must bootstrap from the per-job settings file and
must not depend on an interactive login environment.  It still uses ``python3``
for the SSH banner probe (design §6), so this test provides a *shell stub* named
``python3`` on ``PATH`` instead of the real interpreter and runs the real
``computemcp-job.sh`` with a stripped environment.  The real banner probe is
covered by ``tests/test_provision_shell.py``.

It also proves that all five bundle helpers ship in the installed package.
"""

from __future__ import annotations

import importlib.resources
import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

import pytest

BUNDLE_NAMES = (
    "computemcp-provision.sh",
    "computemcp-container.sh",
    "computemcp-job.sh",
    "computemcp-relay.py",
    "computemcp-mux.py",
)

SOURCE_BUNDLE = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "compute_mcp"
    / "bundles"
    / "computemcp-slurm"
)


def test_all_five_bundle_helpers_ship():
    shipped = importlib.resources.files("compute_mcp.bundles").joinpath(
        "computemcp-slurm"
    )
    for name in BUNDLE_NAMES:
        assert (SOURCE_BUNDLE / name).is_file(), f"source bundle missing {name}"
        assert (shipped / name).is_file(), f"packaged bundle missing {name}"


def test_job_sh_bootstrap_with_stripped_env_and_shell_python_stub(tmp_path):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    stub_bin = tmp_path / "bin"
    stub_bin.mkdir()
    state = tmp_path / "storage" / "system" / "state" / "jobs" / "job-test"
    state.mkdir(parents=True)
    home = tmp_path / "home"
    home.mkdir()

    # A shell stub stands in for the real interpreter.  It reports success for
    # the banner probe so the test exercises the shell bootstrap, not Python.
    python_stub = stub_bin / "python3"
    python_stub.write_text("#!/bin/sh\nexit 0\n")
    python_stub.chmod(0o755)

    start_marker = tmp_path / "container.started"
    stop_marker = tmp_path / "container.stopped"
    container_stub = bundle / "computemcp-container.sh"
    container_stub.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f'case "$1" in\n'
        f"  start) mkdir -p \"$COMPUTEMCP_STATE_DIR\"; "
        f"printf '%s\\n' 2222 > \"$COMPUTEMCP_STATE_DIR/container.port\"; "
        f"touch '{start_marker}' ;;\n"
        f"  stop) touch '{stop_marker}' ;;\n"
        f"esac\n"
    )
    container_stub.chmod(0o755)
    assert subprocess.run(
        ["bash", "-n", str(container_stub)], capture_output=True
    ).returncode == 0

    job_sh = SOURCE_BUNDLE / "computemcp-job.sh"
    assert subprocess.run(
        ["bash", "-n", str(job_sh)], capture_output=True
    ).returncode == 0

    env = {
        "PATH": f"{stub_bin}:/usr/local/bin:/usr/bin:/bin",
        "HOME": str(home),
        "SLURM_JOB_ID": "4242",
        "COMPUTEMCP_STATE_DIR": str(state),
        "COMPUTEMCP_BUNDLE_DIR": str(bundle),
        "COMPUTEMCP_CONTAINER_PORT": "0",
        "COMPUTEMCP_SSH_WAIT_SECONDS": "1",
    }
    # Prove the shell stub shadows any real interpreter.
    assert shutil.which("python3", path=env["PATH"]) == str(python_stub)

    proc = subprocess.Popen(
        ["bash", str(job_sh)],
        env=env,
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    ready = state / "ready"
    deadline = time.time() + 15
    try:
        while time.time() < deadline and not ready.exists():
            if proc.poll() is not None:
                break
            time.sleep(0.05)
        assert ready.exists(), "job.sh never published readiness"
        assert ready.read_text().strip(), "ready marker is empty"
        assert start_marker.exists(), "container start was not invoked"
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
        try:
            _out, err = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            _out, err = proc.communicate(timeout=5)
    assert proc.returncode == 0, f"job.sh exited {proc.returncode}: {err}"
    assert "Container ready" in err
    assert stop_marker.exists(), "the EXIT trap did not stop the container"

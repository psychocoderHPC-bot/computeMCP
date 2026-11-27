# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Shared fixtures for the stateful fake Slurm harness.

Import ``fake_slurm_env`` / ``fake_slurm_ctl`` into a test module (or live
under ``tests/fakes/``) to get a per-test isolated queue, command log and a
``PATH`` whose ``sbatch``/``squeue``/... resolve to ``fake_slurm.py``.

The fixtures never touch the repository's root ``conftest.py``; each test
gets its own ``tmp_path`` state directory, so parallel pytest workers cannot
observe each other's queue.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

FAKES_DIR = Path(__file__).resolve().parent
FAKE_SLURM = FAKES_DIR / "fake_slurm.py"
COMMANDS = ("sbatch", "squeue", "scancel", "srun", "sinfo", "slurmctl")


class FakeSlurm:
    """A per-test fake scheduler: state dir, log path, bin dir and runner."""

    def __init__(
        self,
        root: Path,
        state_dir: Path,
        log_path: Path,
        bin_dir: Path,
        cluster: str,
        base_path: str,
    ) -> None:
        self.root = root
        self.state_dir = state_dir
        self.log_path = log_path
        self.bin_dir = bin_dir
        self.cluster = cluster
        self.base_path = base_path
        self.module_path = FAKE_SLURM

    # -- environment --------------------------------------------------------

    def environment(self, extra=None):
        """Full subprocess environment with this fake cluster on PATH."""
        env = dict(os.environ)
        env["FAKE_SLURM_STATE"] = str(self.state_dir)
        env["FAKE_SLURM_LOG"] = str(self.log_path)
        env["FAKE_SLURM_CLUSTER"] = self.cluster
        env["PATH"] = str(self.bin_dir) + os.pathsep + self.base_path
        # Drop inherited knobs so a test never picks up a sibling's config.
        for name in (
            "FAKE_SLURM_MAX_ACTIVE",
            "FAKE_SLURM_MAX_JOBS",
            "FAKE_SLURM_BARRIER_DIR",
            "FAKE_SLURM_BARRIER_N",
            "FAKE_SLURM_BARRIER_TIMEOUT",
            "FAKE_SLURM_SUBMIT_DELAY",
            "FAKE_SLURM_AUTO_LAUNCH",
            "FAKE_SLURM_SRUN_EXEC",
            "FAKE_SLURM_USER",
            "FAKE_SLURM_PARTITION",
        ):
            env.pop(name, None)
        if extra:
            env.update({str(key): str(value) for key, value in extra.items()})
        return env

    # -- process helpers ----------------------------------------------------

    def run(
        self,
        command,
        *args,
        extra_env=None,
        timeout=30,
        check=False,
        input=None,
        cwd=None,
    ):
        """Run a fake command through ``PATH`` (proves argv[0] dispatch)."""
        argv = [str(command)]
        argv.extend(str(arg) for arg in args)
        return subprocess.run(
            argv,
            env=self.environment(extra_env),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=check,
            input=input,
            cwd=None if cwd is None else str(cwd),
        )

    def submit(self, script=None, *args, extra_env=None, parsable=True):
        """Convenience wrapper for ``sbatch``."""
        argv = ["--parsable"] if parsable else []
        argv.extend(str(arg) for arg in args)
        if script is not None:
            argv.append(str(script))
        return self.run("sbatch", *argv, extra_env=extra_env)

    # -- state / log readers ------------------------------------------------

    @property
    def queue_path(self) -> Path:
        return self.state_dir / "queue.json"

    def read_state(self):
        try:
            return json.loads(self.queue_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None

    def read_log(self):
        try:
            text = self.log_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return []
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    def commands_logged(self):
        return [entry.get("command") for entry in self.read_log()]


@pytest.fixture
def fake_slurm_env(tmp_path, monkeypatch):
    """Isolated fake Slurm cluster for one test."""
    root = tmp_path / "fake-slurm"
    bin_dir = root / "bin"
    state_dir = root / "state"
    log_path = root / "commands.jsonl"
    bin_dir.mkdir(parents=True)

    if not os.access(FAKE_SLURM, os.X_OK):
        os.chmod(FAKE_SLURM, 0o755)
    for name in COMMANDS:
        os.symlink(FAKE_SLURM, bin_dir / name)

    base_path = os.environ.get("PATH", "")
    env = FakeSlurm(
        root=root,
        state_dir=state_dir,
        log_path=log_path,
        bin_dir=bin_dir,
        cluster="testcluster",
        base_path=base_path,
    )
    monkeypatch.setenv("FAKE_SLURM_STATE", str(state_dir))
    monkeypatch.setenv("FAKE_SLURM_LOG", str(log_path))
    monkeypatch.setenv("FAKE_SLURM_CLUSTER", "testcluster")
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + base_path)
    return env


@pytest.fixture
def fake_slurm_ctl(fake_slurm_env):
    """Callable wrapper around the ``slurmctl`` control command."""

    def ctl(*args, **kwargs):
        return fake_slurm_env.run("slurmctl", *args, **kwargs)

    ctl.env = fake_slurm_env
    return ctl

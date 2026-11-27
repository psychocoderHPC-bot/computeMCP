#!/usr/bin/env python3
# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Concurrent private-submit stress against the stateful fake Slurm queue.

Run this on a real development container to measure the gateway's durable
quota/reservation path under load, independent of SSH and HTTP:

    PYTHONPATH=src python tests/e2e/stress_submit.py --n 200 --quota 5

It builds an isolated fake Slurm cluster (``tests/fakes/fake_slurm.py``),
creates a real :class:`compute_mcp.jobs.JobManager`, and fires ``N`` concurrent
unique private submissions at a small quota.  The invariants are: exactly
``quota`` submissions reach ``sbatch``, every other request is rejected with
``QuotaExceeded``, ids are unique and nothing hangs.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from compute_mcp import jobs as jobcore  # noqa: E402


def build_fake_slurm(root: Path) -> dict[str, str]:
    fakes = REPO / "tests" / "fakes"
    script = fakes / "fake_slurm.py"
    if not os.access(script, os.X_OK):
        os.chmod(script, 0o755)
    bin_dir = root / "bin"
    bin_dir.mkdir(parents=True)
    for name in ("sbatch", "squeue", "scancel", "srun", "sinfo", "slurmctl"):
        os.symlink(script, bin_dir / name)
    env = dict(os.environ)
    env.update(
        {
            "FAKE_SLURM_STATE": str(root / "state"),
            "FAKE_SLURM_LOG": str(root / "commands.jsonl"),
            "FAKE_SLURM_CLUSTER": "stresscluster",
            "PATH": str(bin_dir) + os.pathsep + env.get("PATH", ""),
        }
    )
    return env


class FakeSlurmScheduler:
    def __init__(self, env: dict[str, str]) -> None:
        self.env = env
        self.submit_calls = 0

    async def _run(self, argv):
        return await asyncio.to_thread(
            subprocess.run,
            argv,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=30,
        )

    async def submit(self, record):
        self.submit_calls += 1
        proc = await self._run(
            [
                "sbatch",
                "--parsable",
                f"--comment={record.correlation_token}",
                "--wrap",
                "true",
            ]
        )
        if proc.returncode != 0:
            raise jobcore.SchedulerRejected(proc.stderr.strip())
        line = (proc.stdout or "").strip().splitlines()[-1]
        return line

    async def query(self, slurm_job_id):
        proc = await self._run(
            ["squeue", "--noheader", "--format=%T", "--jobs", slurm_job_id]
        )
        text = (proc.stdout or "").strip()
        return text.splitlines()[0].strip() if text else None

    async def cancel(self, slurm_job_id):
        await self._run(["scancel", slurm_job_id])


def count_commands(log_path: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    try:
        lines = log_path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return counts
    for line in lines:
        if not line.strip():
            continue
        entry = json.loads(line)
        command = entry.get("command", "")
        counts[command] = counts.get(command, 0) + 1
    return counts


async def run(n: int, quota: int, workers: int) -> dict:
    root = Path(tempfile.mkdtemp(prefix="cmcp-stress-"))
    try:
        env = build_fake_slurm(root)
        scheduler = FakeSlurmScheduler(env)
        store = jobcore.JobStore(root / "gateway-state")
        quota_manager = jobcore.QuotaManager(
            {"agent-a": {"frontier": quota}},
            default_limit=jobcore.DEFAULT_JOB_LIMIT,
            snapshot=store.scan,
            guard=store.quota_lock,
        )
        manager = jobcore.JobManager(store, scheduler, quota_manager)

        semaphore = asyncio.Semaphore(workers)

        async def one(index: int):
            async with semaphore:
                request = jobcore.JobRequest(
                    client_id="agent-a",
                    target="frontier",
                    request_id=f"stress-{index}",
                    kind="private",
                    parameters={},
                )
                return await manager.submit(request)

        started = time.perf_counter()
        results = await asyncio.wait_for(
            asyncio.gather(*(one(i) for i in range(n)), return_exceptions=True),
            timeout=300,
        )
        wall = time.perf_counter() - started

        records = [item for item in results if isinstance(item, jobcore.JobRecord)]
        quota_errors = [
            item for item in results if isinstance(item, jobcore.QuotaExceeded)
        ]
        other_errors = [
            item
            for item in results
            if isinstance(item, BaseException)
            and not isinstance(item, jobcore.QuotaExceeded)
        ]
        counts = count_commands(root / "commands.jsonl")
        slurm_ids = [record.slurm_job_id for record in records]
        report = {
            "n": n,
            "quota": quota,
            "workers": workers,
            "cpu_count": os.cpu_count(),
            "wall_seconds": round(wall, 3),
            "accepted": len(records),
            "quota_errors": len(quota_errors),
            "other_errors": [repr(item) for item in other_errors],
            "sbatch_count": counts.get("sbatch", 0),
            "scancel_count": counts.get("scancel", 0),
            "durable_records": len(store.scan()),
            "unique_job_ids": len({record.job_id for record in records}),
            "unique_slurm_ids": len(set(slurm_ids)),
            "ok": (
                len(records) == quota
                and len(quota_errors) == n - quota
                and not other_errors
                and counts.get("sbatch", 0) == quota
                and len(store.scan()) == quota
                and len({record.job_id for record in records}) == quota
                and len(set(slurm_ids)) == quota
            ),
        }
        return report
    finally:
        shutil.rmtree(root, ignore_errors=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=200)
    parser.add_argument("--quota", type=int, default=5)
    parser.add_argument("--workers", type=int, default=200)
    parser.add_argument("--json", type=str, default=None)
    args = parser.parse_args(argv)
    report = asyncio.run(run(args.n, args.quota, args.workers))
    print(json.dumps(report, indent=2, sort_keys=True))
    if args.json:
        Path(args.json).write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n"
        )
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""A tiny, stateful fake Slurm cluster for multi-job tests.

The module is deliberately stdlib-only (Python 3.8+) and dispatches on the
basename of ``argv[0]``.  Place symlinks named ``sbatch``, ``squeue``,
``scancel``, ``srun``, ``sinfo`` and ``slurmctl`` next to it (or call
``python fake_slurm.py <command> ...``); every invocation is one short-lived
process, so the queue lives in a JSON file guarded by a ``flock`` lock.

Environment
-----------
``FAKE_SLURM_STATE``
    Directory holding ``queue.json`` and the default log (required).
``FAKE_SLURM_LOG``
    JSON-lines command log path (default: ``$FAKE_SLURM_STATE/log.jsonl``).
``FAKE_SLURM_CLUSTER``
    Cluster name reported by ``sbatch``/``squeue`` (default ``testcluster``).
``FAKE_SLURM_USER``
    Job owner used when ``sbatch`` does not get ``--user`` (default: the
    current login name).
``FAKE_SLURM_PARTITION``
    Partition name used by ``sinfo``/default jobs (default ``normal``).
``FAKE_SLURM_MAX_ACTIVE`` (alias ``FAKE_SLURM_MAX_JOBS``)
    Maximum number of PENDING/CONFIGURING/RUNNING/COMPLETING jobs.  ``0`` or
    unset means unlimited.  A submit over the cap fails with exit 1.
``FAKE_SLURM_BARRIER_DIR`` / ``FAKE_SLURM_BARRIER_N``
    Optional submit barrier: every ``sbatch`` drops an arrival marker and
    waits until ``FAKE_SLURM_BARRIER_N`` markers exist before entering the
    slot-check critical section.  This makes a last-slot race deterministic.
``FAKE_SLURM_BARRIER_TIMEOUT``
    Barrier release timeout in seconds (default ``10``); on expiry the
    submit proceeds instead of hanging the test.
``FAKE_SLURM_SUBMIT_DELAY``
    Optional float seconds slept before parsing, to widen race windows.
``FAKE_SLURM_AUTO_LAUNCH``
    ``1`` makes ``sbatch`` record the job as RUNNING instead of PENDING.
``FAKE_SLURM_SRUN_EXEC``
    ``1`` makes ``srun ... -- <cmd>`` execute the command; otherwise srun is
    a logging no-op and exits 0.

All queue mutations happen under an exclusive ``flock`` on
``$FAKE_SLURM_STATE/queue.lock`` (an O_EXCL lockfile fallback is used on
platforms without ``fcntl``), and ``queue.json`` is replaced atomically.
Each invocation appends exactly one JSON object to the command log, so tests
can assert e.g. that a shutdown path emitted zero ``scancel`` records.
"""

from __future__ import annotations

import contextlib
import errno
import getpass
import json
import os
import sys
import time
import uuid

try:  # POSIX
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback
    fcntl = None


COMMANDS = ("sbatch", "squeue", "scancel", "srun", "sinfo", "slurmctl")

DEFAULT_CLUSTER = "testcluster"
DEFAULT_PARTITION = "normal"

TERMINAL_STATES = frozenset(
    {"COMPLETED", "CANCELLED", "FAILED", "TIMEOUT", "NODE_FAIL"}
)
ACTIVE_STATES = frozenset({"PENDING", "CONFIGURING", "RUNNING", "COMPLETING"})
KNOWN_STATES = frozenset(
    {
        "PENDING",
        "CONFIGURING",
        "RUNNING",
        "COMPLETING",
        "COMPLETED",
        "CANCELLED",
        "FAILED",
        "TIMEOUT",
        "NODE_FAIL",
    }
)

SHORT_STATES = {
    "PENDING": "PD",
    "CONFIGURING": "CF",
    "RUNNING": "R",
    "COMPLETING": "CG",
    "COMPLETED": "CD",
    "CANCELLED": "CA",
    "FAILED": "F",
    "TIMEOUT": "TO",
    "NODE_FAIL": "NF",
}

SQUEUE_LABELS = {
    "%i": "JOBID",
    "%A": "JOBID",
    "%T": "STATE",
    "%t": "ST",
    "%j": "NAME",
    "%u": "USER",
    "%P": "PARTITION",
    "%C": "CPUS",
    "%D": "NODES",
    "%M": "TIME",
    "%R": "NODELIST(REASON)",
    "%N": "NODELIST",
}

SINFO_LABELS = {
    "%P": "PARTITION",
    "%a": "AVAIL",
    "%l": "TIMELIMIT",
    "%D": "NODES",
    "%t": "STATE",
    "%T": "STATE",
    "%N": "NODELIST",
    "%c": "CPUS",
    "%m": "MEMORY",
}

# Long options of ``sbatch`` that consume the following argv element when the
# ``--key=value`` spelling is not used.  Unknown ``--key`` is treated as a flag.
_SBATCH_VALUE_OPTS = frozenset(
    {
        "comment",
        "job-name",
        "partition",
        "user",
        "nodes",
        "ntasks",
        "cpus-per-task",
        "gpus",
        "gres",
        "mem",
        "memory",
        "mem-per-cpu",
        "mem-per-gpu",
        "time",
        "output",
        "error",
        "chdir",
        "wrap",
        "export",
        "cluster",
        "clusters",
        "account",
        "qos",
        "nodelist",
        "exclude",
        "array",
        "dependency",
        "mail-type",
        "mail-user",
        "signal",
        "constraint",
        "distribution",
        "threads-per-core",
        "ntasks-per-node",
        "gpus-per-node",
        "gpus-per-task",
        "cpus-per-gpu",
        "tmp",
        "input",
        "open-mode",
        "propagate",
        "uenv",
        "reservation",
        "begin",
        "deadline",
        "wckey",
        "tres-per-node",
        "tres-per-task",
        "switches",
        "core-spec",
    }
)

_SBATCH_SHORT_VALUE = {
    "J": "job-name",
    "p": "partition",
    "N": "nodes",
    "n": "ntasks",
    "c": "cpus-per-task",
    "t": "time",
    "o": "output",
    "e": "error",
    "D": "chdir",
    "a": "array",
    "w": "nodelist",
    "x": "exclude",
    "C": "constraint",
    "A": "account",
    "q": "qos",
    "M": "clusters",
    "G": "gpus",
    "d": "dependency",
}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _env(name, default=None):
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def _float_env(name, default):
    try:
        return float(_env(name, str(default)))
    except (TypeError, ValueError):
        return default


def _int_env(name, default=0):
    try:
        return int(_env(name, str(default)))
    except (TypeError, ValueError):
        return default


def _truthy(name):
    return _env(name, "").strip().lower() in ("1", "true", "yes", "on")


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _state_dir():
    path = _env("FAKE_SLURM_STATE")
    if not path:
        sys.stderr.write("fake_slurm: FAKE_SLURM_STATE is not set\n")
        raise SystemExit(2)
    return path


def _log_path(state_dir):
    return _env("FAKE_SLURM_LOG", os.path.join(state_dir, "log.jsonl"))


def _cluster():
    return _env("FAKE_SLURM_CLUSTER", DEFAULT_CLUSTER)


def _partition():
    return _env("FAKE_SLURM_PARTITION", DEFAULT_PARTITION)


def _user():
    return _env("FAKE_SLURM_USER", getpass.getuser())


def _append_log(state_dir, command, argv, **fields):
    """Append exactly one JSON line for this invocation (atomic O_APPEND)."""
    path = _log_path(state_dir)
    record = {
        "ts": time.time(),
        "time": _now(),
        "pid": os.getpid(),
        "command": command,
        "argv": list(argv),
    }
    record.update(fields)
    parent = os.path.dirname(os.path.abspath(path))
    with contextlib.suppress(OSError):
        os.makedirs(parent, exist_ok=True)
    line = (json.dumps(record, sort_keys=True) + "\n").encode("utf-8")
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o644)
    try:
        os.write(fd, line)
    finally:
        os.close(fd)


def _truncate_log(state_dir):
    path = _log_path(state_dir)
    parent = os.path.dirname(os.path.abspath(path))
    with contextlib.suppress(OSError):
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8"):
        pass


# ---------------------------------------------------------------------------
# Locking and state
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _state_lock(state_dir, timeout=15.0):
    """Exclusive cross-process lock for one state directory."""
    os.makedirs(state_dir, exist_ok=True)
    lock_path = os.path.join(state_dir, "queue.lock")
    deadline = time.monotonic() + timeout
    if fcntl is not None:
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError as exc:
                    if exc.errno not in (errno.EACCES, errno.EAGAIN):
                        raise
                    if time.monotonic() > deadline:
                        raise TimeoutError(
                            "timed out waiting for fake Slurm state lock"
                        )
                    time.sleep(0.002)
            yield
        finally:
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        return

    # Portable fallback: O_EXCL lockfile with stale-owner recovery.
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            os.write(fd, str(os.getpid()).encode("ascii"))
            os.close(fd)
            break
        except FileExistsError:
            if time.monotonic() > deadline:
                raise TimeoutError("timed out waiting for fake Slurm state lock")
            stale = False
            try:
                with open(lock_path, "r", encoding="ascii") as fh:
                    owner = int(fh.read().strip() or "0")
                if owner <= 0:
                    stale = True
                else:
                    try:
                        os.kill(owner, 0)
                    except ProcessLookupError:
                        stale = True
            except (OSError, ValueError):
                stale = True
            if stale:
                with contextlib.suppress(OSError):
                    os.unlink(lock_path)
            time.sleep(0.002)
    try:
        yield
    finally:
        with contextlib.suppress(OSError):
            os.unlink(lock_path)


def _state_file(state_dir):
    return os.path.join(state_dir, "queue.json")


def _default_state(cluster):
    return {"version": 1, "next_id": 1, "cluster": cluster, "jobs": {}}


def _load_state(state_dir):
    path = _state_file(state_dir)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return _default_state(_cluster())
    if not isinstance(data, dict):
        raise ValueError("fake Slurm state is not a JSON object")
    if not isinstance(data.get("jobs"), dict):
        data["jobs"] = {}
    if not isinstance(data.get("next_id"), int) or data["next_id"] < 1:
        data["next_id"] = 1
    data.setdefault("cluster", _cluster())
    return data


def _save_state(state_dir, state):
    path = _state_file(state_dir)
    tmp = "%s.tmp.%d.%s" % (path, os.getpid(), uuid.uuid4().hex[:8])
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, sort_keys=True, indent=2)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


@contextlib.contextmanager
def _transaction(state_dir):
    """Load, mutate and atomically persist the queue under one lock."""
    with _state_lock(state_dir):
        state = _load_state(state_dir)
        yield state
        _save_state(state_dir, state)


@contextlib.contextmanager
def _snapshot(state_dir):
    """Read the queue under the lock without rewriting it."""
    with _state_lock(state_dir):
        yield _load_state(state_dir)


# ---------------------------------------------------------------------------
# argv parsing
# ---------------------------------------------------------------------------


def _parse_options(args, value_options=frozenset(), short_value=None):
    """Split argv into ``{name: [values...]}`` plus positional arguments.

    Supports ``--key=value``, ``--key value`` (only for known value options),
    ``--flag``, ``-k value`` and ``-kvalue``.  Everything after ``--`` is
    positional.
    """
    short_value = short_value or {}
    options = {}
    positional = []
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--":
            positional.extend(args[i + 1 :])
            break
        if arg.startswith("--") and len(arg) > 2:
            body = arg[2:]
            if "=" in body:
                key, value = body.split("=", 1)
            else:
                key = body
                if key in value_options and i + 1 < len(args):
                    value = args[i + 1]
                    i += 1
                else:
                    value = True
            options.setdefault(key, []).append(value)
        elif arg.startswith("-") and arg not in ("-", "--"):
            body = arg[1:]
            if body in short_value:
                if i + 1 < len(args):
                    value = args[i + 1]
                    i += 1
                else:
                    value = True
                options.setdefault(short_value[body], []).append(value)
            elif len(body) > 1 and body[0] in short_value:
                options.setdefault(short_value[body[0]], []).append(body[1:])
            else:
                options.setdefault(body, []).append(True)
        else:
            positional.append(arg)
        i += 1
    return options, positional


def _last(options, key, default=None):
    values = options.get(key)
    if not values:
        return default
    return values[-1]


def _as_bool(value):
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _render_format(template, mapping):
    out = []
    i = 0
    while i < len(template):
        char = template[i]
        if char == "%" and i + 1 < len(template):
            token = "%" + template[i + 1]
            if token == "%%":
                out.append("%")
            else:
                out.append(str(mapping.get(token, token)))
            i += 2
            continue
        out.append(char)
        i += 1
    return "".join(out)


# ---------------------------------------------------------------------------
# sbatch
# ---------------------------------------------------------------------------


def _job_name(options, script_path, wrap):
    name = _last(options, "job-name")
    if name:
        return str(name)
    if script_path:
        return os.path.basename(script_path)
    if wrap:
        return "wrap"
    return "sbatch"


def _read_script(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return fh.read()
    except OSError:
        return ""


def _barrier_wait():
    barrier_dir = _env("FAKE_SLURM_BARRIER_DIR")
    count = _int_env("FAKE_SLURM_BARRIER_N", 0)
    if not barrier_dir or count <= 1:
        return
    os.makedirs(barrier_dir, exist_ok=True)
    marker = os.path.join(
        barrier_dir, "arrive-%d-%s" % (os.getpid(), uuid.uuid4().hex[:8])
    )
    with open(marker, "w", encoding="utf-8"):
        pass
    release = os.path.join(barrier_dir, "release")
    deadline = time.monotonic() + _float_env("FAKE_SLURM_BARRIER_TIMEOUT", 10.0)
    while time.monotonic() < deadline:
        if os.path.exists(release):
            return
        arrivals = [
            name for name in os.listdir(barrier_dir) if name.startswith("arrive-")
        ]
        if len(arrivals) >= count:
            try:
                fd = os.open(release, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                os.close(fd)
            except FileExistsError:
                pass
            return
        time.sleep(0.002)
    # Timeout: proceed rather than wedge the caller; the log still records it.


def _cmd_sbatch(state_dir, argv):
    args = argv[1:]
    delay = _float_env("FAKE_SLURM_SUBMIT_DELAY", 0.0)
    if delay > 0:
        time.sleep(delay)
    _barrier_wait()

    options, positional = _parse_options(
        args, _SBATCH_VALUE_OPTS, _SBATCH_SHORT_VALUE
    )
    parsable = _as_bool(_last(options, "parsable", False))
    wrap = _last(options, "wrap")
    script_path = positional[-1] if positional else None
    if wrap:
        script_body = str(wrap)
        script_path = None
    elif script_path is not None:
        script_body = _read_script(script_path)
    else:
        _append_log(state_dir, "sbatch", argv, result="error", exit_code=1,
                    error="no batch script or --wrap")
        sys.stderr.write(
            "sbatch: error: No job script or --wrap was specified\n"
        )
        return 1

    comment = _last(options, "comment")
    params = {}
    for key, values in options.items():
        params[key] = values[-1] if len(values) == 1 else list(values)
    cluster = (
        _last(options, "clusters")
        or _last(options, "cluster")
        or _cluster()
    )
    cluster = str(cluster)
    partition = str(_last(options, "partition", _partition()))
    user = str(_last(options, "user", _user()))
    name = _job_name(options, script_path, wrap)
    max_active = _int_env(
        "FAKE_SLURM_MAX_ACTIVE", _int_env("FAKE_SLURM_MAX_JOBS", 0)
    )

    rejected = False
    job_id = None
    with _transaction(state_dir) as state:
        active = sum(
            1
            for job in state["jobs"].values()
            if job.get("state") in ACTIVE_STATES
        )
        if max_active > 0 and active >= max_active:
            rejected = True
        else:
            job_id = int(state["next_id"])
            state["next_id"] = job_id + 1
            stamp = _now()
            state_value = "RUNNING" if _truthy("FAKE_SLURM_AUTO_LAUNCH") else "PENDING"
            state["jobs"][str(job_id)] = {
                "id": job_id,
                "state": state_value,
                "name": name,
                "user": user,
                "cluster": cluster,
                "partition": partition,
                "script_path": script_path,
                "script_body": script_body,
                "comment": None if comment is None else str(comment),
                "params": params,
                "argv": list(argv),
                "submitted_at": stamp,
                "updated_at": stamp,
                "started_at": stamp if state_value == "RUNNING" else None,
                "cancelled_at": None,
                "completed_at": None,
                "late_ready_at": None,
                "late_ready_after_cancel": False,
            }

    if rejected:
        _append_log(
            state_dir,
            "sbatch",
            argv,
            result="rejected",
            exit_code=1,
            reason="max-active",
            max_active=max_active,
        )
        sys.stderr.write(
            "sbatch: error: Batch job submission failed: "
            "queue is full (max active jobs %d)\n" % max_active
        )
        return 1

    _append_log(
        state_dir,
        "sbatch",
        argv,
        result="ok",
        exit_code=0,
        job_id=job_id,
        cluster=cluster,
        comment=None if comment is None else str(comment),
    )
    if parsable:
        sys.stdout.write("%d;%s\n" % (job_id, cluster))
    else:
        sys.stdout.write("Submitted batch job %d\n" % job_id)
    return 0


# ---------------------------------------------------------------------------
# squeue
# ---------------------------------------------------------------------------


_SQUEUE_VALUE_OPTS = frozenset(
    {
        "user",
        "format",
        "clusters",
        "cluster",
        "jobs",
        "job",
        "states",
        "state",
        "name",
        "partition",
        "sort",
        "nodelist",
        "account",
        "qos",
    }
)

_SQUEUE_SHORT_VALUE = {
    "u": "user",
    "o": "format",
    "M": "clusters",
    "j": "jobs",
    "t": "states",
    "p": "partition",
    "n": "name",
    "A": "account",
    "q": "qos",
}


def _job_row(job):
    state = job.get("state", "PENDING")
    params = job.get("params") or {}
    return {
        "%i": job.get("id", ""),
        "%A": job.get("id", ""),
        "%T": state,
        "%t": SHORT_STATES.get(state, state[:2]),
        "%j": job.get("name", ""),
        "%u": job.get("user", ""),
        "%P": job.get("partition", ""),
        "%C": params.get("cpus-per-task", "1"),
        "%D": params.get("nodes", "1"),
        "%M": "0:00",
        "%R": "(Resources)" if state not in ("RUNNING", "COMPLETING") else "fake-node",
        "%N": "fake-node" if state in ("RUNNING", "COMPLETING") else "",
    }


def _cmd_squeue(state_dir, argv):
    args = argv[1:]
    options, _positional = _parse_options(
        args, _SQUEUE_VALUE_OPTS, _SQUEUE_SHORT_VALUE
    )
    noheader = _as_bool(_last(options, "noheader", False)) or _as_bool(
        _last(options, "h", False)
    )
    show_all = _as_bool(_last(options, "all", False)) or _as_bool(
        _last(options, "a", False)
    )
    fmt = _last(options, "format")
    user = _last(options, "user")
    clusters = _last(options, "clusters") or _last(options, "cluster")
    jobs_arg = _last(options, "jobs") or _last(options, "job")
    states_arg = _last(options, "states") or _last(options, "state")
    name = _last(options, "name")
    partition = _last(options, "partition")

    selected_states = None
    if states_arg:
        selected_states = {
            part.strip().upper() for part in str(states_arg).split(",") if part.strip()
        }
    selected_clusters = None
    if clusters:
        selected_clusters = {
            part.strip() for part in str(clusters).split(",") if part.strip()
        }
    selected_jobs = None
    if jobs_arg:
        selected_jobs = {
            part.strip() for part in str(jobs_arg).split(",") if part.strip()
        }

    with _snapshot(state_dir) as state:
        jobs = sorted(state["jobs"].values(), key=lambda job: job.get("id", 0))

    rows = []
    for job in jobs:
        job_state = job.get("state", "PENDING")
        if selected_states is not None:
            if job_state not in selected_states:
                continue
        elif not show_all and job_state in TERMINAL_STATES:
            continue
        if user is not None and job.get("user") != str(user):
            continue
        if selected_clusters is not None and job.get("cluster") not in selected_clusters:
            continue
        if selected_jobs is not None and str(job.get("id")) not in selected_jobs:
            continue
        if name is not None and job.get("name") != str(name):
            continue
        if partition is not None and job.get("partition") != str(partition):
            continue
        rows.append(job)

    lines = []
    if fmt:
        if not noheader:
            lines.append(_render_format(str(fmt), SQUEUE_LABELS))
        for job in rows:
            lines.append(_render_format(str(fmt), _job_row(job)))
    else:
        if not noheader:
            lines.append("JOBID PARTITION NAME USER ST TIME NODES NODELIST(REASON)")
        for job in rows:
            row = _job_row(job)
            lines.append(
                "{id} {partition} {name} {user} {short} {time} {nodes} {nodelist}".format(
                    id=row["%i"],
                    partition=row["%P"],
                    name=row["%j"],
                    user=row["%u"],
                    short=row["%t"],
                    time=row["%M"],
                    nodes=row["%D"],
                    nodelist=row["%R"],
                )
            )

    if lines:
        sys.stdout.write("\n".join(lines) + "\n")
    _append_log(
        state_dir,
        "squeue",
        argv,
        result="ok",
        exit_code=0,
        jobs=[job.get("id") for job in rows],
    )
    return 0


# ---------------------------------------------------------------------------
# scancel
# ---------------------------------------------------------------------------


def _cmd_scancel(state_dir, argv):
    args = argv[1:]
    options, positional = _parse_options(
        args,
        frozenset(
            {"user", "name", "state", "signal", "account", "qos", "partition", "nodelist"}
        ),
        {"u": "user", "n": "name", "t": "state", "s": "signal"},
    )
    ids = set()
    for item in positional:
        for part in str(item).split(","):
            part = part.strip()
            if part:
                ids.add(part)
    user = _last(options, "user")
    name = _last(options, "name")
    state_filter = _last(options, "state")
    selected_states = None
    if state_filter:
        selected_states = {
            part.strip().upper()
            for part in str(state_filter).split(",")
            if part.strip()
        }

    cancelled = []
    with _transaction(state_dir) as state:
        for key, job in state["jobs"].items():
            if ids and key not in ids:
                continue
            if user is not None and job.get("user") != str(user):
                continue
            if name is not None and job.get("name") != str(name):
                continue
            if selected_states is not None and job.get("state") not in selected_states:
                continue
            if job.get("state") == "CANCELLED":
                continue
            stamp = _now()
            job["state"] = "CANCELLED"
            job["cancelled_at"] = stamp
            job["updated_at"] = stamp
            cancelled.append(job.get("id"))

    if ids and not cancelled and user is None and name is None and state_filter is None:
        _append_log(
            state_dir,
            "scancel",
            argv,
            result="error",
            exit_code=1,
            error="no matching job",
            requested=sorted(ids),
        )
        sys.stderr.write(
            "scancel: error: Invalid job id specification %s\n"
            % ",".join(sorted(ids))
        )
        return 1

    _append_log(
        state_dir,
        "scancel",
        argv,
        result="ok",
        exit_code=0,
        cancelled=cancelled,
    )
    return 0


# ---------------------------------------------------------------------------
# srun
# ---------------------------------------------------------------------------


def _cmd_srun(state_dir, argv):
    args = argv[1:]
    options, positional = _parse_options(
        args,
        frozenset(
            {
                "jobid",
                "job-name",
                "ntasks",
                "nodes",
                "cpus-per-task",
                "gpus",
                "partition",
                "clusters",
                "chdir",
                "export",
                "input",
                "output",
                "error",
                "time",
                "mem",
                "nodelist",
                "exclude",
            }
        ),
        {
            "J": "job-name",
            "n": "ntasks",
            "N": "nodes",
            "c": "cpus-per-task",
            "p": "partition",
            "w": "nodelist",
            "x": "exclude",
        },
    )
    job_id = _last(options, "jobid")
    _append_log(
        state_dir,
        "srun",
        argv,
        result="ok",
        exit_code=0,
        job_id=None if job_id is None else str(job_id),
        args=positional,
    )
    if _truthy("FAKE_SLURM_SRUN_EXEC") and positional:
        os.execvp(positional[0], positional)
    return 0


# ---------------------------------------------------------------------------
# sinfo
# ---------------------------------------------------------------------------


_SINFO_VALUE_OPTS = frozenset(
    {"format", "partition", "clusters", "cluster", "nodes", "nodelist", "sort"}
)
_SINFO_SHORT_VALUE = {"o": "format", "p": "partition", "M": "clusters", "n": "nodes"}


def _cmd_sinfo(state_dir, argv):
    args = argv[1:]
    options, _positional = _parse_options(
        args, _SINFO_VALUE_OPTS, _SINFO_SHORT_VALUE
    )
    noheader = _as_bool(_last(options, "noheader", False)) or _as_bool(
        _last(options, "h", False)
    )
    partition = _last(options, "partition")
    if partition:
        partition_name = str(partition)
    else:
        try:
            with _snapshot(state_dir) as state:
                partition_name = str(state.get("cluster", _cluster()))
        except (OSError, ValueError):
            partition_name = _cluster()
    fmt = _last(options, "format")
    values = {
        "%P": partition_name,
        "%a": "up",
        "%l": "infinite",
        "%D": "1",
        "%t": "idle",
        "%T": "idle",
        "%N": "fake-node",
        "%c": "64",
        "%m": "128000",
    }
    lines = []
    if fmt:
        if not noheader:
            lines.append(_render_format(str(fmt), SINFO_LABELS))
        lines.append(_render_format(str(fmt), values))
    else:
        if not noheader:
            lines.append("PARTITION AVAIL TIMELIMIT NODES STATE NODELIST")
        lines.append(
            "{partition} up infinite 1 idle fake-node".format(
                partition=partition_name
            )
        )
    sys.stdout.write("\n".join(lines) + "\n")
    _append_log(
        state_dir,
        "sinfo",
        argv,
        result="ok",
        exit_code=0,
        partition=partition_name,
    )
    return 0


# ---------------------------------------------------------------------------
# slurmctl
# ---------------------------------------------------------------------------


def _find_job(state, job_id):
    return state["jobs"].get(str(job_id))


def _set_state(state_dir, job_id, new_state):
    new_state = str(new_state).strip().upper()
    if new_state not in KNOWN_STATES:
        sys.stderr.write(
            "slurmctl: unknown state %r (known: %s)\n"
            % (new_state, ", ".join(sorted(KNOWN_STATES)))
        )
        return None, 2
    with _transaction(state_dir) as state:
        job = _find_job(state, job_id)
        if job is None:
            sys.stderr.write("slurmctl: no such job %s\n" % job_id)
            return None, 1
        stamp = _now()
        job["state"] = new_state
        job["updated_at"] = stamp
        if new_state == "RUNNING" and not job.get("started_at"):
            job["started_at"] = stamp
        if new_state in TERMINAL_STATES:
            job["completed_at"] = stamp
        return job.get("id"), 0


def _cmd_slurmctl(state_dir, argv):
    args = argv[1:]
    if not args:
        sys.stderr.write(
            "usage: slurmctl {set-state <id> <STATE>|start-job <id>|"
            "late-ready <id>|list|log|reset|get <id>}\n"
        )
        _append_log(state_dir, "slurmctl", argv, result="error", exit_code=2)
        return 2

    sub = args[0]
    rest = args[1:]

    if sub == "set-state":
        if len(rest) < 2:
            sys.stderr.write("usage: slurmctl set-state <id> <STATE>\n")
            _append_log(state_dir, "slurmctl", argv, result="error", exit_code=2)
            return 2
        job_id, code = _set_state(state_dir, rest[0], rest[1])
        _append_log(
            state_dir,
            "slurmctl",
            argv,
            result="ok" if code == 0 else "error",
            exit_code=code,
            subcommand=sub,
            job_id=job_id,
            state=str(rest[1]).upper() if code == 0 else None,
        )
        return code

    if sub == "start-job":
        if len(rest) < 1:
            sys.stderr.write("usage: slurmctl start-job <id>\n")
            _append_log(state_dir, "slurmctl", argv, result="error", exit_code=2)
            return 2
        job_id, code = _set_state(state_dir, rest[0], "RUNNING")
        _append_log(
            state_dir,
            "slurmctl",
            argv,
            result="ok" if code == 0 else "error",
            exit_code=code,
            subcommand=sub,
            job_id=job_id,
        )
        return code

    if sub == "late-ready":
        if len(rest) < 1:
            sys.stderr.write("usage: slurmctl late-ready <id>\n")
            _append_log(state_dir, "slurmctl", argv, result="error", exit_code=2)
            return 2
        with _transaction(state_dir) as state:
            job = _find_job(state, rest[0])
            if job is None:
                _append_log(
                    state_dir,
                    "slurmctl",
                    argv,
                    result="error",
                    exit_code=1,
                    subcommand=sub,
                )
                sys.stderr.write("slurmctl: no such job %s\n" % rest[0])
                return 1
            stamp = _now()
            job["late_ready_at"] = stamp
            job["updated_at"] = stamp
            # A readiness event after a terminal transition must not revive
            # the job; only record that it arrived late.
            if job.get("state") in TERMINAL_STATES:
                job["late_ready_after_cancel"] = True
            else:
                job["state"] = "RUNNING"
                if not job.get("started_at"):
                    job["started_at"] = stamp
            job_id = job.get("id")
        _append_log(
            state_dir,
            "slurmctl",
            argv,
            result="ok",
            exit_code=0,
            subcommand=sub,
            job_id=job_id,
        )
        return 0

    if sub == "get":
        if len(rest) < 1:
            sys.stderr.write("usage: slurmctl get <id>\n")
            _append_log(state_dir, "slurmctl", argv, result="error", exit_code=2)
            return 2
        with _snapshot(state_dir) as state:
            job = _find_job(state, rest[0])
        if job is None:
            _append_log(
                state_dir,
                "slurmctl",
                argv,
                result="error",
                exit_code=1,
                subcommand=sub,
            )
            sys.stderr.write("slurmctl: no such job %s\n" % rest[0])
            return 1
        sys.stdout.write(json.dumps(job, sort_keys=True, indent=2) + "\n")
        _append_log(
            state_dir,
            "slurmctl",
            argv,
            result="ok",
            exit_code=0,
            subcommand=sub,
            job_id=job.get("id"),
        )
        return 0

    if sub == "list":
        with _snapshot(state_dir) as state:
            payload = json.dumps(state, sort_keys=True, indent=2)
        sys.stdout.write(payload + "\n")
        _append_log(
            state_dir,
            "slurmctl",
            argv,
            result="ok",
            exit_code=0,
            subcommand=sub,
        )
        return 0

    if sub == "log":
        path = _log_path(state_dir)
        try:
            with open(path, "r", encoding="utf-8") as fh:
                payload = fh.read()
        except FileNotFoundError:
            payload = ""
        sys.stdout.write(payload)
        _append_log(
            state_dir,
            "slurmctl",
            argv,
            result="ok",
            exit_code=0,
            subcommand=sub,
        )
        return 0

    if sub == "reset":
        with _state_lock(state_dir):
            _save_state(state_dir, _default_state(_cluster()))
            _truncate_log(state_dir)
        _append_log(
            state_dir,
            "slurmctl",
            argv,
            result="ok",
            exit_code=0,
            subcommand=sub,
        )
        return 0

    sys.stderr.write("slurmctl: unknown subcommand %r\n" % sub)
    _append_log(
        state_dir,
        "slurmctl",
        argv,
        result="error",
        exit_code=2,
        subcommand=sub,
    )
    return 2


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


_HANDLERS = {
    "sbatch": _cmd_sbatch,
    "squeue": _cmd_squeue,
    "scancel": _cmd_scancel,
    "srun": _cmd_srun,
    "sinfo": _cmd_sinfo,
    "slurmctl": _cmd_slurmctl,
}


def main(argv=None):
    argv = list(sys.argv if argv is None else argv)
    if not argv:
        sys.stderr.write("fake_slurm: missing argv\n")
        return 2
    program = os.path.basename(argv[0])
    rest = argv[1:]
    if program in COMMANDS:
        command = program
    elif rest and rest[0] in COMMANDS:
        command = rest[0]
        rest = rest[1:]
    else:
        sys.stderr.write(
            "fake_slurm: unknown command %r (expected one of %s)\n"
            % (program, ", ".join(COMMANDS))
        )
        return 2
    state_dir = _state_dir()
    return _HANDLERS[command](state_dir, [command] + rest)


if __name__ == "__main__":
    sys.exit(main())

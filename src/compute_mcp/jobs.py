# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Durable job registry, quota reservation and crash-consistent recovery.

This module is the gateway-independent core for computeMCP multi-job Slurm
support.  It knows nothing about HTTP, SSH or the control stream: callers hand
it a :class:`Scheduler` implementation and a state root, and it persists every
job decision *before* the action it describes can be observed by a peer.

Design invariants
-----------------
* Each job owns ``<state_root>/jobs/<job_id>/`` and only ever writes inside it,
  using temp-file + ``fsync`` + ``os.replace`` + directory ``fsync``.  A crash
  leaves either the previous or the next complete file, never a torn one.
* A private Slurm job consumes one quota slot from the moment it is reserved
  (before ``sbatch``) until a *definitive* failure or a scheduler-confirmed
  terminal state.  An ambiguous submit, a disconnected client or a lost route
  never frees the slot.
* The correlation token is persisted before ``sbatch``.  If submission
  acknowledgement is lost, the record is marked ``submission_unknown`` and is
  never resubmitted automatically; reconciliation looks the token up in the
  scheduler before a new request is accepted.
* Teardown detaches.  Only an explicit stop calls ``scancel``.
* Shared-job creation takes :meth:`JobStore.shared_lock`, an ``O_EXCL``
  lockfile keyed by target, so two gateway processes cannot both pass the
  get-or-create check and submit a second shared job.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import hashlib
import inspect
import json
import os
import re
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, ContextManager, Iterable, Mapping, Protocol, Sequence

# ---------------------------------------------------------------------------
# Job states and transitions
# ---------------------------------------------------------------------------

SUBMITTING = "submitting"
PENDING = "pending"
STARTING = "starting"
READY = "ready"
STOPPING = "stopping"
TERMINAL = "terminal"
SUBMISSION_UNKNOWN = "submission_unknown"

VALID_STATES = frozenset(
    {
        SUBMITTING,
        PENDING,
        STARTING,
        READY,
        STOPPING,
        TERMINAL,
        SUBMISSION_UNKNOWN,
    }
)

#: States during which a job still owns its durable record / quota slot.
ACTIVE_STATES = frozenset(
    {SUBMITTING, PENDING, STARTING, READY, STOPPING, SUBMISSION_UNKNOWN}
)

#: Explicit legal state graph.  A terminal record is absorbing; repeating the
#: current state is an idempotent no-op rather than a transition.
ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    SUBMITTING: frozenset({PENDING, SUBMISSION_UNKNOWN, TERMINAL}),
    SUBMISSION_UNKNOWN: frozenset({PENDING, TERMINAL}),
    PENDING: frozenset({STARTING, STOPPING, TERMINAL}),
    STARTING: frozenset({READY, STOPPING, TERMINAL}),
    READY: frozenset({STOPPING, TERMINAL}),
    STOPPING: frozenset({TERMINAL}),
    TERMINAL: frozenset(),
}

#: Quota buckets.  ``None`` means the state does not consume a private slot.
QUOTA_CATEGORY_BY_STATE: dict[str, str | None] = {
    SUBMITTING: "reserved",
    SUBMISSION_UNKNOWN: "uncertain",
    PENDING: "pending",
    STARTING: "starting",
    READY: "ready",
    STOPPING: "cancelling",
    TERMINAL: None,
}

COUNTED_CATEGORIES = frozenset(
    {"reserved", "uncertain", "pending", "starting", "ready", "cancelling"}
)

KINDS = frozenset({"shared", "private"})

#: Native Slurm states that prove a job can no longer be running.
SCHEDULER_TERMINAL_STATES = frozenset(
    {"COMPLETED", "CANCELLED", "FAILED", "TIMEOUT", "NODE_FAIL", "PREEMPTED", "OUT_OF_MEMORY"}
)
SCHEDULER_PENDING_STATES = frozenset({"PENDING", "CONFIGURING"})
SCHEDULER_RUNNING_STATES = frozenset({"RUNNING", "COMPLETING"})

_JOB_ID_RE = re.compile(r"^job-[0-9a-f]{32}$")

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class JobError(Exception):
    """Base class for job registry errors."""


class IllegalTransition(JobError):
    """A state transition that the lifecycle graph does not permit."""

    def __init__(self, current: str, requested: str) -> None:
        super().__init__(f"illegal job transition {current!r} -> {requested!r}")
        self.current = current
        self.requested = requested


class InvalidJobId(JobError):
    """A job id is malformed or would escape the state root."""

    def __init__(self, job_id: object) -> None:
        super().__init__(f"invalid job id: {job_id!r}")
        self.job_id = job_id


class JobNotFound(JobError):
    """No persisted record exists for the requested job id."""

    def __init__(self, job_id: str) -> None:
        super().__init__(f"job not found: {job_id}")
        self.job_id = job_id


class LockTimeout(JobError):
    """A per-job or per-quota lock could not be acquired in time."""

    def __init__(self, path: object, timeout: float) -> None:
        super().__init__(f"timed out after {timeout}s waiting for lock {path}")
        self.path = str(path)
        self.timeout = timeout


class QuotaExceeded(JobError):
    """A private job would exceed the client's per-target limit."""

    def __init__(self, client_id: str, target: str, limit: int) -> None:
        super().__init__(
            f"private job quota exhausted for client {client_id!r} on target "
            f"{target!r} (limit {limit})"
        )
        self.client_id = client_id
        self.target = target
        self.limit = limit


class RequestIdConflict(JobError):
    """A request id was reused with a different effective request."""

    def __init__(self, request_id: str) -> None:
        super().__init__(f"request id {request_id!r} reused with different parameters")
        self.request_id = request_id


class SchedulerRejected(Exception):
    """Raised by a :class:`Scheduler` when submission was *definitely* refused.

    The manager treats this as a definitive failure: the record becomes
    terminal and the quota slot is released.  Any other exception from
    ``submit`` is ambiguous and keeps both the record and the slot.
    """


class SubmissionRejected(JobError):
    """The scheduler definitively refused the submission."""

    def __init__(self, record: "JobRecord", cause: BaseException) -> None:
        super().__init__(f"submission rejected for {record.job_id}: {cause}")
        self.record = record
        self.cause = cause


class SubmissionUncertain(JobError):
    """Submission acknowledgement was lost; the record must be reconciled.

    The job is persisted as ``submission_unknown`` and is never resubmitted
    automatically.
    """

    def __init__(self, record: "JobRecord", cause: BaseException) -> None:
        super().__init__(f"submission outcome unknown for {record.job_id}: {cause}")
        self.record = record
        self.cause = cause


# ---------------------------------------------------------------------------
# Identifiers and helpers
# ---------------------------------------------------------------------------


def new_job_id() -> str:
    """Return a new opaque, path-safe job id."""
    return "job-" + uuid.uuid4().hex


def new_correlation_token() -> str:
    """Return a scheduler-visible correlation token (e.g. a job comment)."""
    return "cmcp-" + uuid.uuid4().hex


def _now() -> float:
    return time.time()


def _iso(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp))


def normalize_scheduler_state(state: object) -> str | None:
    """Normalise a scheduler-reported state to an upper-case token."""
    if state is None:
        return None
    text = str(state).strip().upper()
    return text or None


def validate_job_id(job_id: object) -> str:
    """Validate a job id and reject anything that could traverse paths.

    The canonical form is ``job-<32 lowercase hex>``; dot segments, separators
    and NUL bytes can therefore never reach the filesystem.
    """
    if not isinstance(job_id, str) or not _JOB_ID_RE.match(job_id):
        raise InvalidJobId(job_id)
    if job_id in (".", "..") or "/" in job_id or "\\" in job_id or "\x00" in job_id:
        raise InvalidJobId(job_id)
    return job_id


def transition(
    record: "JobRecord",
    new_state: str,
    *,
    reason: str | None = None,
    scheduler_state: str | None = None,
    now: float | None = None,
) -> "JobRecord":
    """Apply a state transition to ``record`` after checking the legal graph."""
    if new_state not in VALID_STATES:
        raise IllegalTransition(record.state, new_state)
    whenever = _now() if now is None else now

    if record.state == new_state:
        # Idempotent repeat; refresh only the observation fields.
        if scheduler_state is not None:
            record.scheduler_state = scheduler_state
        if new_state == TERMINAL and reason is not None:
            record.terminal_reason = reason
        record.updated_at = whenever
        return record

    allowed = ALLOWED_TRANSITIONS.get(record.state, frozenset())
    if new_state not in allowed:
        raise IllegalTransition(record.state, new_state)

    record.state = new_state
    record.updated_at = whenever
    if scheduler_state is not None:
        record.scheduler_state = scheduler_state
    if new_state == PENDING and record.submitted_at is None:
        record.submitted_at = whenever
    if new_state == READY:
        record.ready_at = whenever
    if new_state == TERMINAL:
        record.terminal_at = whenever
        if reason is not None:
            record.terminal_reason = reason
    return record


# ---------------------------------------------------------------------------
# Job record
# ---------------------------------------------------------------------------


@dataclass
class JobRecord:
    """Durable description of one computeMCP job.

    The dataclass is deliberately plain: :meth:`to_dict` / :meth:`from_dict`
    are the on-disk contract, so adding fields is a versioned, additive change.
    """

    job_id: str
    request_id: str
    owner_client_id: str
    target: str
    kind: str
    parameters: dict[str, Any] = field(default_factory=dict)
    state: str = SUBMITTING
    slurm_job_id: str | None = None
    cluster: str | None = None
    scheduler_state: str | None = None
    root_mode: str | None = None
    base_digest: str | None = None
    home: str | None = None
    node: str | None = None
    container_endpoint: str | None = None
    relay_endpoint: str | None = None
    relay_host: str | None = None
    relay_port: int | None = None
    relay_pid: int | None = None
    generation: int = 0
    terminal_reason: str | None = None
    correlation_token: str = field(default_factory=new_correlation_token)
    created_at: float = field(default_factory=_now)
    updated_at: float = field(default_factory=_now)
    submitted_at: float | None = None
    ready_at: float | None = None
    terminal_at: float | None = None

    def __post_init__(self) -> None:
        validate_job_id(self.job_id)
        if self.kind not in KINDS:
            raise JobError(f"invalid job kind: {self.kind!r}")
        if self.state not in VALID_STATES:
            raise JobError(f"invalid job state: {self.state!r}")
        if not isinstance(self.request_id, str) or not self.request_id:
            raise JobError("request_id must be a non-empty string")
        if not isinstance(self.owner_client_id, str) or not self.owner_client_id:
            raise JobError("owner_client_id must be a non-empty string")
        if not isinstance(self.target, str) or not self.target:
            raise JobError("target must be a non-empty string")
        if not isinstance(self.parameters, dict):
            raise JobError("parameters must be a dict")
        if not isinstance(self.correlation_token, str) or not self.correlation_token:
            raise JobError("correlation_token must be a non-empty string")

    # -- serialisation ------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": 1,
            "job_id": self.job_id,
            "request_id": self.request_id,
            "owner_client_id": self.owner_client_id,
            "target": self.target,
            "kind": self.kind,
            "parameters": dict(self.parameters),
            "state": self.state,
            "slurm_job_id": self.slurm_job_id,
            "cluster": self.cluster,
            "scheduler_state": self.scheduler_state,
            "root_mode": self.root_mode,
            "base_digest": self.base_digest,
            "home": self.home,
            "node": self.node,
            "container_endpoint": self.container_endpoint,
            "relay_endpoint": self.relay_endpoint,
            "relay_host": self.relay_host,
            "relay_port": self.relay_port,
            "relay_pid": self.relay_pid,
            "generation": self.generation,
            "terminal_reason": self.terminal_reason,
            "correlation_token": self.correlation_token,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "submitted_at": self.submitted_at,
            "ready_at": self.ready_at,
            "terminal_at": self.terminal_at,
            "created_at_iso": _iso(self.created_at),
            "updated_at_iso": _iso(self.updated_at),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "JobRecord":
        if not isinstance(payload, Mapping):
            raise JobError("job record is not a mapping")
        required = ("job_id", "request_id", "owner_client_id", "target", "kind")
        missing = [name for name in required if name not in payload]
        if missing:
            raise JobError(f"job record is missing fields: {', '.join(missing)}")
        return cls(
            job_id=str(payload["job_id"]),
            request_id=str(payload["request_id"]),
            owner_client_id=str(payload["owner_client_id"]),
            target=str(payload["target"]),
            kind=str(payload["kind"]),
            parameters=dict(payload.get("parameters") or {}),
            state=str(payload.get("state", SUBMITTING)),
            slurm_job_id=payload.get("slurm_job_id"),
            cluster=payload.get("cluster"),
            scheduler_state=payload.get("scheduler_state"),
            root_mode=payload.get("root_mode"),
            base_digest=payload.get("base_digest"),
            home=payload.get("home"),
            node=payload.get("node"),
            container_endpoint=payload.get("container_endpoint"),
            relay_endpoint=payload.get("relay_endpoint"),
            relay_host=payload.get("relay_host"),
            relay_port=payload.get("relay_port"),
            relay_pid=payload.get("relay_pid"),
            generation=int(payload.get("generation") or 0),
            terminal_reason=payload.get("terminal_reason"),
            correlation_token=payload.get("correlation_token") or new_correlation_token(),
            created_at=float(payload.get("created_at") or _now()),
            updated_at=float(payload.get("updated_at") or _now()),
            submitted_at=payload.get("submitted_at"),
            ready_at=payload.get("ready_at"),
            terminal_at=payload.get("terminal_at"),
        )


# ---------------------------------------------------------------------------
# Durable store
# ---------------------------------------------------------------------------

_JOB_FILES = frozenset(
    {
        "job.json",
        "submission.json",
        "scheduler.json",
        "settings.env",
        "ready.json",
        "relay.json",
        "terminal.json",
    }
)

LOCK_FILE = "job.lock"


def _boot_id() -> str:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    except OSError:
        return ""


def _proc_start_ticks(pid: int) -> int | None:
    """Return field 22 (starttime) from ``/proc/<pid>/stat`` when available."""
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except OSError:
        return None
    try:
        tail = text.rsplit(")", 1)[1].split()
        # tail[0] is field 3 (state); starttime is field 22.
        return int(tail[19])
    except (IndexError, ValueError):
        return None


def _pid_alive(pid: int) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _owner_payload(
    *, lease_seconds: float, identity: str | None = None
) -> dict[str, Any]:
    pid = os.getpid()
    now = time.time()
    payload = {
        "host": socket.gethostname(),
        "boot_id": _boot_id(),
        "pid": pid,
        "start_ticks": _proc_start_ticks(pid),
        "lease_until": now + float(lease_seconds),
        "created_at": now,
        "token": uuid.uuid4().hex,
    }
    if identity is not None:
        payload["identity"] = identity
    return payload


class _FileLock:
    """An acquired O_EXCL lock; release is token-checked."""

    def __init__(self, path: Path, owner: dict[str, Any]) -> None:
        self.path = path
        self.owner = owner
        self._released = False

    def renew(self, lease_seconds: float = 60.0) -> None:
        if self._released:
            raise JobError("cannot renew a released lock")
        owner = dict(self.owner)
        owner["lease_until"] = time.time() + float(lease_seconds)
        owner["token"] = uuid.uuid4().hex
        _atomic_write_bytes(self.path, json.dumps(owner).encode("utf-8"))
        self.owner = owner

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        current = _read_json(self.path)
        if current is not None and current.get("token") == self.owner.get("token"):
            with contextlib.suppress(OSError):
                os.unlink(self.path)
                _fsync_dir(self.path.parent)

    def __enter__(self) -> "_FileLock":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()


class JobStore:
    """Crash-consistent, filesystem-backed job records under a state root.

    ``state_root`` is the provisioner's ``$COMPUTEMCP_STATE_DIR`` (normally
    ``$STORAGE_ROOT/$SYSTEM/state``); jobs live under ``<state_root>/jobs``.
    """

    def __init__(self, state_root: str | os.PathLike[str]) -> None:
        self.state_root = Path(state_root)
        self.jobs_dir = self.state_root / "jobs"
        self.locks_dir = self.jobs_dir / ".locks"
        self.last_scan_errors: list[tuple[str, str]] = []
        self._ensure_dir(self.state_root)
        self._ensure_dir(self.jobs_dir)

    # -- paths --------------------------------------------------------------

    @staticmethod
    def _ensure_dir(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        with contextlib.suppress(OSError):
            os.chmod(path, 0o700)

    def job_dir(self, job_id: str) -> Path:
        validate_job_id(job_id)
        path = self.jobs_dir / job_id
        # Defence in depth: a validated id cannot escape, but prove it.
        try:
            resolved = path.resolve()
            resolved.relative_to(self.jobs_dir.resolve())
        except (ValueError, OSError) as exc:  # pragma: no cover - defensive
            raise InvalidJobId(job_id) from exc
        return path

    def _file(self, job_id: str, name: str) -> Path:
        if name not in _JOB_FILES:
            raise JobError(f"unknown job file: {name!r}")
        return self.job_dir(job_id) / name

    def ensure_job_dir(self, job_id: str) -> Path:
        path = self.job_dir(job_id)
        self._ensure_dir(path)
        return path

    # -- records ------------------------------------------------------------

    def save_record(self, record: JobRecord, *, now: float | None = None) -> JobRecord:
        if now is not None:
            record.updated_at = now
        self.ensure_job_dir(record.job_id)
        _atomic_write_json(self._file(record.job_id, "job.json"), record.to_dict())
        return record

    def load_record(self, job_id: str) -> JobRecord:
        path = self._file(job_id, "job.json")
        payload = _read_json(path)
        if payload is None:
            raise JobNotFound(job_id)
        return JobRecord.from_dict(payload)

    def try_load_record(self, job_id: str) -> JobRecord | None:
        try:
            return self.load_record(job_id)
        except (JobNotFound, JobError, OSError):
            return None

    def scan(self) -> list[JobRecord]:
        """Load every valid job record; corrupted entries are reported and skipped."""
        self.last_scan_errors = []
        records: list[JobRecord] = []
        if not self.jobs_dir.is_dir():
            return records
        for entry in os.scandir(self.jobs_dir):
            if not entry.is_dir(follow_symlinks=False):
                continue
            if not _JOB_ID_RE.match(entry.name):
                continue
            try:
                records.append(self.load_record(entry.name))
            except Exception as exc:  # noqa: BLE001 - reconciliation must continue
                self.last_scan_errors.append((entry.name, str(exc)))
        records.sort(key=lambda item: item.created_at)
        return records

    def list_jobs(self) -> list[JobRecord]:
        return self.scan()

    def iter_job_ids(self) -> Iterable[str]:
        for record in self.scan():
            yield record.job_id

    def find_by_request_id(
        self, request_id: str, owner_client_id: str | None = None
    ) -> JobRecord | None:
        """Return a record carrying ``request_id``, optionally owner-scoped.

        Submission paths must pass ``owner_client_id``: two clients may use
        the same idempotency key, and a caller must never observe another
        client's job through a request-id replay.  ``None`` is an
        administrative scan and returns the first match.
        """
        for record in self.scan():
            if record.request_id != request_id:
                continue
            if (
                owner_client_id is not None
                and record.owner_client_id != owner_client_id
            ):
                continue
            return record
        return None

    # -- per-job auxiliary files -------------------------------------------

    def _write_json(self, job_id: str, name: str, payload: Mapping[str, Any]) -> None:
        self.ensure_job_dir(job_id)
        _atomic_write_json(self._file(job_id, name), dict(payload))

    def _read_json(self, job_id: str, name: str) -> dict[str, Any] | None:
        return _read_json(self._file(job_id, name))

    def save_submission(self, job_id: str, payload: Mapping[str, Any]) -> None:
        self._write_json(job_id, "submission.json", payload)

    def load_submission(self, job_id: str) -> dict[str, Any] | None:
        return self._read_json(job_id, "submission.json")

    def save_scheduler(self, job_id: str, payload: Mapping[str, Any]) -> None:
        self._write_json(job_id, "scheduler.json", payload)

    def load_scheduler(self, job_id: str) -> dict[str, Any] | None:
        return self._read_json(job_id, "scheduler.json")

    def write_settings(self, job_id: str, settings: Mapping[str, Any]) -> None:
        """Write ``settings.env`` in ``KEY=VALUE`` form, atomically."""
        lines = []
        for key in sorted(settings):
            value = settings[key]
            text = str(value)
            if "\n" in text or "\r" in text:
                raise JobError(f"setting {key!r} contains a newline")
            lines.append(f"{key}={text}\n")
        self.ensure_job_dir(job_id)
        _atomic_write_bytes(
            self._file(job_id, "settings.env"), "".join(lines).encode("utf-8")
        )

    def read_settings(self, job_id: str) -> str | None:
        path = self._file(job_id, "settings.env")
        try:
            return path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None

    def save_ready(self, job_id: str, payload: Mapping[str, Any]) -> None:
        self._write_json(job_id, "ready.json", payload)

    def load_ready(self, job_id: str) -> dict[str, Any] | None:
        return self._read_json(job_id, "ready.json")

    def save_relay(self, job_id: str, payload: Mapping[str, Any]) -> None:
        self._write_json(job_id, "relay.json", payload)

    def load_relay(self, job_id: str) -> dict[str, Any] | None:
        return self._read_json(job_id, "relay.json")

    def save_terminal(self, job_id: str, payload: Mapping[str, Any]) -> None:
        self._write_json(job_id, "terminal.json", payload)

    def load_terminal(self, job_id: str) -> dict[str, Any] | None:
        return self._read_json(job_id, "terminal.json")

    # -- locking ------------------------------------------------------------

    def lock_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / LOCK_FILE

    def read_lock(self, job_id: str) -> dict[str, Any] | None:
        return _read_json(self.lock_path(job_id))

    def lock_is_stale(self, job_id: str, *, now: float | None = None) -> bool:
        return self._lock_file_is_stale(self.lock_path(job_id), now=now)

    def acquire_lock(
        self,
        job_id: str,
        *,
        timeout: float = 5.0,
        lease_seconds: float = 60.0,
        poll_interval: float = 0.01,
    ) -> _FileLock:
        self.ensure_job_dir(job_id)
        return self._acquire_file_lock(
            self.lock_path(job_id),
            timeout=timeout,
            lease_seconds=lease_seconds,
            poll_interval=poll_interval,
        )

    def lock(self, job_id: str, **kwargs: Any) -> _FileLock:
        return self.acquire_lock(job_id, **kwargs)

    def quota_lock(
        self,
        client_id: str,
        target: str,
        *,
        timeout: float = 10.0,
        lease_seconds: float = 60.0,
        poll_interval: float = 0.01,
    ) -> _FileLock:
        digest = hashlib.sha256(
            f"{client_id}\x00{target}".encode("utf-8", "surrogatepass")
        ).hexdigest()[:32]
        self._ensure_dir(self.locks_dir)
        return self._acquire_file_lock(
            self.locks_dir / f"quota-{digest}.lock",
            timeout=timeout,
            lease_seconds=lease_seconds,
            poll_interval=poll_interval,
        )

    def shared_lock_path(self, target: str) -> Path:
        """Lockfile path for the per-target shared-job creation lock."""
        if not isinstance(target, str) or not target:
            raise JobError("shared lock target must be a non-empty string")
        digest = hashlib.sha256(
            target.encode("utf-8", "surrogatepass")
        ).hexdigest()[:32]
        return self.locks_dir / f"shared-{digest}.lock"

    def shared_lock(
        self,
        target: str,
        owner: str,
        *,
        timeout: float = 10.0,
        lease_seconds: float = 60.0,
        poll_interval: float = 0.01,
    ) -> _FileLock:
        """Cross-process, per-target lock around shared-job creation.

        The lock is keyed by ``target`` only, so two gateway processes using
        different owner labels still contend for the same target: at most one
        of them can pass the get-or-create check and persist a shared record
        at a time.  ``owner`` is validated and recorded in the lock payload
        for diagnostics.  It reuses the same ``O_EXCL`` + lease/staleness
        machinery as :meth:`acquire_lock` and :meth:`quota_lock`, so a dead
        holder is recovered after its lease expires or its pid disappears.
        """
        if not isinstance(owner, str) or not owner:
            raise JobError("shared lock owner must be a non-empty string")
        self._ensure_dir(self.locks_dir)
        return self._acquire_file_lock(
            self.shared_lock_path(target),
            timeout=timeout,
            lease_seconds=lease_seconds,
            poll_interval=poll_interval,
            identity=f"shared:{target}:{owner}",
        )

    @staticmethod
    def _read_lock_file(path: Path) -> dict[str, Any] | None:
        return _read_json(path)

    @classmethod
    def _lock_file_is_stale(cls, path: Path, *, now: float | None = None) -> bool:
        payload = cls._read_lock_file(path)
        if payload is None:
            return False
        if not isinstance(payload, dict):
            return True
        current = time.time() if now is None else now
        lease = payload.get("lease_until")
        if not isinstance(lease, (int, float)) or isinstance(lease, bool):
            return True
        if float(lease) <= current:
            return True
        host = payload.get("host")
        if host == socket.gethostname():
            boot = payload.get("boot_id")
            if boot and boot != _boot_id():
                return True
            pid = payload.get("pid")
            if not isinstance(pid, int) or not _pid_alive(pid):
                return True
            recorded_ticks = payload.get("start_ticks")
            live_ticks = _proc_start_ticks(pid)
            if (
                isinstance(recorded_ticks, int)
                and live_ticks is not None
                and recorded_ticks != live_ticks
            ):
                return True
        return False

    def _acquire_file_lock(
        self,
        path: Path,
        *,
        timeout: float,
        lease_seconds: float,
        poll_interval: float,
        identity: str | None = None,
    ) -> _FileLock:
        self._ensure_dir(path.parent)
        deadline = time.monotonic() + float(timeout)
        while True:
            owner = _owner_payload(lease_seconds=lease_seconds, identity=identity)
            try:
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                payload = self._read_lock_file(path)
                token = payload.get("token") if isinstance(payload, dict) else None
                if self._lock_file_is_stale(path):
                    # Only remove the exact lock instance we observed as stale.
                    self._unlink_if_token(path, token)
                    continue
                if time.monotonic() >= deadline:
                    raise LockTimeout(path, timeout)
                time.sleep(poll_interval)
                continue
            try:
                data = json.dumps(owner, sort_keys=True).encode("utf-8")
                _write_all(fd, data)
                os.fsync(fd)
            finally:
                os.close(fd)
            _fsync_dir(path.parent)
            return _FileLock(path, owner)

    @staticmethod
    def _unlink_if_token(path: Path, token: str | None) -> None:
        """Remove a stale lock only if its token still matches the snapshot."""
        payload = _read_json(path)
        if payload is None:
            return
        if token is not None and payload.get("token") != token:
            return
        with contextlib.suppress(OSError):
            os.unlink(path)
            _fsync_dir(path.parent)


# ---------------------------------------------------------------------------
# Atomic file helpers
# ---------------------------------------------------------------------------


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:  # pragma: no cover - defensive
            raise OSError("short write")
        view = view[written:]


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError as exc:  # some filesystems refuse directory fsync
        if exc.errno not in (errno.EINVAL, errno.ENOTSUP, errno.EBADF):
            raise
    finally:
        os.close(fd)


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        _write_all(fd, data)
        os.fsync(fd)
    except BaseException:
        os.close(fd)
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise
    else:
        os.close(fd)
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    text = json.dumps(payload, sort_keys=True, indent=2, default=str) + "\n"
    _atomic_write_bytes(path, text.encode("utf-8"))


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (FileNotFoundError, NotADirectoryError):
        return None
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


# ---------------------------------------------------------------------------
# Quota
# ---------------------------------------------------------------------------

DEFAULT_JOB_LIMIT = 1

#: A guard factory: ``guard(client_id, target)`` returns a context manager that
#: serialises a quota transaction with other processes.
GuardFactory = Callable[[str, str], ContextManager[Any]]


@dataclass(frozen=True)
class QuotaUsage:
    client_id: str
    target: str
    limit: int
    counted: int
    jobs: Mapping[str, str]

    @property
    def available(self) -> int:
        return max(0, self.limit - self.counted)


class QuotaManager:
    """Per-client, per-target private-job quota with durable-aware snapshots.

    The manager is safe for concurrent asyncio tasks: every mutation is taken
    under one asyncio lock (and a thread lock, so synchronous inspection from a
    worker thread cannot observe a half-updated map).  When ``snapshot`` and
    ``guard`` are supplied, a reservation first re-reads the durable records
    under a cross-process lock, so two gateway processes cannot both consume
    the last slot.
    """

    def __init__(
        self,
        job_limits: Mapping[str, Mapping[str, int]] | None = None,
        *,
        default_limit: int = DEFAULT_JOB_LIMIT,
        snapshot: Callable[[], Sequence[JobRecord]] | None = None,
        guard: GuardFactory | None = None,
    ) -> None:
        if default_limit < 0:
            raise ValueError("default_limit must be non-negative")
        self._limits: dict[str, dict[str, int]] = {}
        for client_id, limits in (job_limits or {}).items():
            self._limits[str(client_id)] = {
                str(target): int(limit) for target, limit in dict(limits).items()
            }
        self._default_limit = int(default_limit)
        self._snapshot = snapshot
        self._guard = guard
        self._entries: dict[str, dict[str, str]] = {}
        self._kinds: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._thread_lock = threading.RLock()

    # -- configuration ------------------------------------------------------

    @staticmethod
    def _coerce_limit(limit: int) -> int:
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
            raise ValueError("job limit must be a non-negative integer")
        return int(limit)

    def limit_for(self, client_id: str, target: str) -> int:
        return self._limits.get(client_id, {}).get(target, self._default_limit)

    def set_limit(self, client_id: str, target: str, limit: int) -> None:
        value = self._coerce_limit(limit)
        with self._thread_lock:
            self._limits.setdefault(str(client_id), {})[str(target)] = value

    def set_limits(self, limits: Mapping[str, Mapping[str, int]]) -> None:
        """Replace the override map *exactly* with ``limits``.

        An absent client/target falls back to ``default_limit`` and an
        explicit ``0`` disables private jobs.  Incremental merging is wrong
        here: removing a ``job_limits`` entry from the config must not leave
        the stale override (and its old value) in force after a reload.
        Validation is done before the swap so a malformed map leaves the
        current overrides untouched.
        """
        replacement: dict[str, dict[str, int]] = {}
        for client_id, per_target in limits.items():
            if not isinstance(per_target, Mapping):
                raise ValueError("job limits must map targets to integers")
            replacement[str(client_id)] = {
                str(target): self._coerce_limit(limit)
                for target, limit in per_target.items()
            }
        with self._thread_lock:
            self._limits = replacement

    # -- inspection ---------------------------------------------------------

    def usage(self, client_id: str, target: str) -> QuotaUsage:
        with self._thread_lock:
            jobs = dict(self._entries.get(self._bucket(client_id, target), {}))
            counted = sum(1 for cat in jobs.values() if cat in COUNTED_CATEGORIES)
            return QuotaUsage(
                client_id=client_id,
                target=target,
                limit=self.limit_for(client_id, target),
                counted=counted,
                jobs=jobs,
            )

    def counted(self, client_id: str, target: str) -> int:
        return self.usage(client_id, target).counted

    def available(self, client_id: str, target: str) -> int:
        return self.usage(client_id, target).available

    def category_of(self, job_id: str) -> str | None:
        with self._thread_lock:
            for jobs in self._entries.values():
                if job_id in jobs:
                    return jobs[job_id]
            return None

    # -- reservation --------------------------------------------------------

    async def reserve(
        self,
        client_id: str,
        target: str,
        kind: str,
        job_id: str,
        *,
        persist: Callable[[], None] | None = None,
    ) -> str | None:
        """Reserve a slot before submission; returns the quota category.

        ``persist`` runs *inside* the cross-process guard, so the durable
        submission intent becomes visible before another process can re-read
        the records and claim the same last slot.  A failing ``persist`` rolls
        the reservation back and re-raises.
        """
        if kind not in KINDS:
            raise ValueError(f"invalid job kind: {kind!r}")
        async with self._lock:
            if self._snapshot is not None and self._guard is not None:
                with self._guard(client_id, target):
                    self._restore_locked(self._snapshot())
                    return self._reserve_and_persist(
                        client_id, target, kind, job_id, persist
                    )
            return self._reserve_and_persist(client_id, target, kind, job_id, persist)

    def _reserve_and_persist(
        self,
        client_id: str,
        target: str,
        kind: str,
        job_id: str,
        persist: Callable[[], None] | None,
    ) -> str | None:
        category = self._reserve_locked(client_id, target, kind, job_id)
        if persist is not None:
            try:
                persist()
            except BaseException:
                self._release_locked(job_id)
                raise
        return category

    def _reserve_locked(
        self, client_id: str, target: str, kind: str, job_id: str
    ) -> str | None:
        with self._thread_lock:
            existing = self.category_of(job_id)
            if existing is not None:
                if self._kinds.get(job_id) != kind:
                    raise JobError(f"job {job_id} already reserved with another kind")
                return existing
            if kind == "shared":
                # Shared jobs are tracked for observability but never counted.
                self._entries.setdefault(self._bucket(client_id, target), {})[
                    job_id
                ] = "shared"
                self._kinds[job_id] = kind
                return "shared"

            limit = self.limit_for(client_id, target)
            if limit == 0:
                raise QuotaExceeded(client_id, target, limit)
            bucket = self._entries.setdefault(self._bucket(client_id, target), {})
            used = sum(1 for cat in bucket.values() if cat in COUNTED_CATEGORIES)
            if used >= limit:
                raise QuotaExceeded(client_id, target, limit)
            bucket[job_id] = "reserved"
            self._kinds[job_id] = kind
            return "reserved"

    async def mark(
        self,
        job_id: str,
        state: str,
        *,
        definitive: bool = False,
    ) -> str | None:
        """Update the bucket for a job after a persisted state change.

        A ``terminal`` state only frees the slot when ``definitive`` is true
        (a scheduler-confirmed terminal state or a definitive submission
        failure).  Otherwise the previous category is retained.
        """
        async with self._lock:
            with self._thread_lock:
                return self._mark_locked(job_id, state, definitive=definitive)

    def _mark_locked(
        self, job_id: str, state: str, *, definitive: bool
    ) -> str | None:
        category = QUOTA_CATEGORY_BY_STATE.get(state)
        for bucket in self._entries.values():
            if job_id not in bucket:
                continue
            if category is None:
                if definitive:
                    bucket.pop(job_id, None)
                    self._kinds.pop(job_id, None)
                    return None
                return bucket[job_id]
            if bucket[job_id] == "shared":
                return "shared"
            bucket[job_id] = category
            return category
        return None

    async def release(self, job_id: str, *, definitive: bool) -> bool:
        """Release a slot only for a definitive outcome; otherwise keep it."""
        if not definitive:
            return False
        async with self._lock:
            return self._release_locked(job_id)

    def _release_locked(self, job_id: str) -> bool:
        with self._thread_lock:
            removed = False
            for bucket in self._entries.values():
                if bucket.pop(job_id, None) is not None:
                    removed = True
            self._kinds.pop(job_id, None)
            return removed

    # -- reconciliation -----------------------------------------------------

    async def restore(self, records: Iterable[JobRecord]) -> None:
        """Rebuild quota usage from durable records (terminal jobs excluded)."""
        async with self._lock:
            self._restore_locked(list(records))

    def _restore_locked(self, records: Iterable[JobRecord]) -> None:
        with self._thread_lock:
            self._entries = {}
            self._kinds = {}
            for record in records:
                if record.kind != "private":
                    continue
                category = QUOTA_CATEGORY_BY_STATE.get(record.state)
                if category is None:
                    continue
                self._entries.setdefault(self._bucket(record.owner_client_id, record.target), {})[
                    record.job_id
                ] = category
                self._kinds[record.job_id] = record.kind

    @staticmethod
    def _bucket(client_id: str, target: str) -> str:
        return f"{client_id}\x00{target}"


# ---------------------------------------------------------------------------
# Scheduler protocol and requests
# ---------------------------------------------------------------------------


class Scheduler(Protocol):
    """Injectable scheduler boundary used by :class:`JobManager`.

    ``submit`` returns the numeric scheduler job id (a ``"<id>;<cluster>"``
    tuple is also accepted).  Implementations should raise
    :class:`SchedulerRejected` only when the scheduler *definitely* refused the
    job; any other error is treated as an ambiguous outcome.
    """

    def submit(self, record: JobRecord) -> str: ...

    def query(self, slurm_job_id: str) -> str | None: ...

    def cancel(self, slurm_job_id: str) -> None: ...


def _split_submit_result(result: Any) -> tuple[str, str | None]:
    if isinstance(result, (tuple, list)) and len(result) == 2:
        return str(result[0]), (str(result[1]) if result[1] else None)
    text = str(result).strip()
    if ";" in text:
        job_id, cluster = text.split(";", 1)
        return job_id.strip(), cluster.strip() or None
    return text, None


def _valid_slurm_id(value: str | None) -> bool:
    return bool(value) and bool(re.match(r"^[0-9]+$", str(value)))


@dataclass
class JobRequest:
    """One caller submission; ``request_id`` makes it idempotent."""

    client_id: str
    target: str
    request_id: str
    kind: str = "private"
    parameters: dict[str, Any] = field(default_factory=dict)
    root_mode: str | None = None
    base_digest: str | None = None
    home: str | None = None
    settings: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        for name in ("client_id", "target", "request_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise JobError(f"{name} must be a non-empty string")
        if self.kind not in KINDS:
            raise JobError(f"invalid job kind: {self.kind!r}")
        if not isinstance(self.parameters, dict):
            raise JobError("parameters must be a dict")
        try:
            json.dumps(self.parameters)
        except (TypeError, ValueError) as exc:
            raise JobError(f"parameters are not JSON serialisable: {exc}") from exc


def _request_signature(
    *,
    kind: str,
    target: str,
    owner_client_id: str,
    parameters: Mapping[str, Any],
    root_mode: str | None,
    base_digest: str | None,
    home: str | None,
) -> str:
    # The owner is part of the signature even though lookups are scoped by
    # owner: it keeps a same-named request from another client from ever
    # comparing equal, so a lookup bug degrades to a conflict, not a leak.
    return json.dumps(
        {
            "kind": kind,
            "target": target,
            "owner_client_id": owner_client_id,
            "parameters": dict(parameters),
            "root_mode": root_mode,
            "base_digest": base_digest,
            "home": home,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _record_signature(record: JobRecord) -> str:
    return _request_signature(
        kind=record.kind,
        target=record.target,
        owner_client_id=record.owner_client_id,
        parameters=record.parameters,
        root_mode=record.root_mode,
        base_digest=record.base_digest,
        home=record.home,
    )


def _request_signature_of(request: JobRequest) -> str:
    return _request_signature(
        kind=request.kind,
        target=request.target,
        owner_client_id=request.client_id,
        parameters=request.parameters,
        root_mode=request.root_mode,
        base_digest=request.base_digest,
        home=request.home,
    )


@dataclass
class ReconciliationReport:
    total: int = 0
    recovered: int = 0
    active: int = 0
    terminal: int = 0
    uncertain: int = 0
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Job manager
# ---------------------------------------------------------------------------

PhaseHook = Callable[[str, JobRecord], None]


class JobManager:
    """Coordinates durable submission, readiness, cancellation and recovery.

    The manager is gateway-independent.  A caller (gateway, CLI, tests) injects
    a :class:`Scheduler`, a :class:`JobStore` and optionally a
    :class:`QuotaManager`; the manager never talks to SSH or HTTP itself.
    """

    def __init__(
        self,
        store: JobStore,
        scheduler: Scheduler,
        quota: QuotaManager | None = None,
        *,
        default_limit: int = DEFAULT_JOB_LIMIT,
        job_limits: Mapping[str, Mapping[str, int]] | None = None,
        phase_hook: PhaseHook | None = None,
        run_scheduler_in_thread: bool = True,
        clock: Callable[[], float] = _now,
    ) -> None:
        self.store = store
        self.scheduler = scheduler
        self.quota = quota or QuotaManager(
            job_limits,
            default_limit=default_limit,
            snapshot=store.scan,
            guard=store.quota_lock,
        )
        self._phase_hook = phase_hook
        self._run_scheduler_in_thread = run_scheduler_in_thread
        self._clock = clock
        self._submit_lock = asyncio.Lock()
        self._reconcile_lock = asyncio.Lock()
        self._reconciled = False

    # -- internals ----------------------------------------------------------

    def _now(self) -> float:
        return float(self._clock())

    def _phase(self, name: str, record: JobRecord) -> None:
        if self._phase_hook is not None:
            self._phase_hook(name, record)

    async def _call(self, func: Callable[..., Any], *args: Any) -> Any:
        if inspect.iscoroutinefunction(func):
            return await func(*args)
        if self._run_scheduler_in_thread:
            result = await asyncio.to_thread(func, *args)
        else:
            result = func(*args)
        if inspect.isawaitable(result):
            result = await result
        return result

    def _find_by_token(self, token: str) -> str | None:
        finder = getattr(self.scheduler, "find_by_token", None)
        if finder is None:
            return None
        try:
            found = finder(token)
        except Exception:  # noqa: BLE001 - recovery must not crash on this
            return None
        if found is None:
            return None
        value = str(found).strip()
        return value or None

    async def _find_by_token_async(self, token: str) -> str | None:
        finder = getattr(self.scheduler, "find_by_token", None)
        if finder is None:
            return None
        try:
            found = await self._call(finder, token)
        except Exception:  # noqa: BLE001
            return None
        if found is None:
            return None
        value = str(found).strip()
        return value or None

    def _persist_intent(self, record: JobRecord, request: JobRequest) -> None:
        """Write the pre-submit intent; called inside the quota guard."""
        if request.settings:
            self.store.write_settings(record.job_id, request.settings)
        self.store.save_submission(
            record.job_id, self._submission_payload(record, "intent")
        )
        self.store.save_record(record)

    def _submission_payload(
        self, record: JobRecord, status: str
    ) -> dict[str, Any]:
        return {
            "request_id": record.request_id,
            "correlation_token": record.correlation_token,
            "kind": record.kind,
            "target": record.target,
            "parameters": dict(record.parameters),
            "root_mode": record.root_mode,
            "base_digest": record.base_digest,
            "home": record.home,
            "status": status,
            "slurm_job_id": record.slurm_job_id,
            "updated_at": self._now(),
        }

    def _scheduler_payload(self, record: JobRecord, state: str | None) -> dict[str, Any]:
        return {
            "slurm_job_id": record.slurm_job_id,
            "cluster": record.cluster,
            "scheduler_state": state,
            "updated_at": self._now(),
        }

    def _persist_terminal(self, record: JobRecord, reason: str) -> None:
        transition(record, TERMINAL, reason=reason, now=self._now())
        self.store.save_record(record)
        self.store.save_terminal(
            record.job_id,
            {
                "state": record.state,
                "reason": record.terminal_reason,
                "scheduler_state": record.scheduler_state,
                "slurm_job_id": record.slurm_job_id,
                "at": record.terminal_at,
            },
        )

    # -- submission ---------------------------------------------------------

    async def submit(self, request: JobRequest) -> JobRecord:
        """Reserve, persist intent and submit once (idempotent by request id)."""
        if not isinstance(request, JobRequest):
            raise TypeError("submit() expects a JobRequest")
        await self.ensure_reconciled()
        signature = _request_signature_of(request)

        async with self._submit_lock:
            existing = self.store.find_by_request_id(
                request.request_id, request.client_id
            )
            if existing is not None:
                if _record_signature(existing) != signature:
                    raise RequestIdConflict(request.request_id)
                return existing

            job_id = new_job_id()
            record = JobRecord(
                job_id=job_id,
                request_id=request.request_id,
                owner_client_id=request.client_id,
                target=request.target,
                kind=request.kind,
                parameters=dict(request.parameters),
                root_mode=request.root_mode,
                base_digest=request.base_digest,
                home=request.home,
                state=SUBMITTING,
                created_at=self._now(),
                updated_at=self._now(),
            )
            await self.quota.reserve(
                record.owner_client_id,
                record.target,
                record.kind,
                job_id,
                persist=lambda: self._persist_intent(record, request),
            )

        # The reservation and intent are durable; submission happens outside
        # the idempotency lock so distinct requests can overlap.
        self._phase("before_submit", record)
        try:
            result = await self._call(self.scheduler.submit, record)
        except SchedulerRejected as exc:
            self._persist_terminal(record, f"scheduler_rejected: {exc}")
            self.store.save_submission(
                record.job_id, self._submission_payload(record, "rejected")
            )
            await self.quota.release(record.job_id, definitive=True)
            raise SubmissionRejected(record, exc) from exc
        except Exception as exc:  # noqa: BLE001 - outcome is ambiguous
            transition(
                record,
                SUBMISSION_UNKNOWN,
                reason=f"submit_error: {exc!r}",
                now=self._now(),
            )
            self.store.save_record(record)
            self.store.save_submission(
                record.job_id, self._submission_payload(record, "unknown")
            )
            await self.quota.mark(record.job_id, record.state)
            raise SubmissionUncertain(record, exc) from exc

        slurm_job_id, cluster = _split_submit_result(result)
        if not _valid_slurm_id(slurm_job_id):
            transition(
                record,
                SUBMISSION_UNKNOWN,
                reason=f"invalid_scheduler_id: {result!r}",
                now=self._now(),
            )
            self.store.save_record(record)
            self.store.save_submission(
                record.job_id, self._submission_payload(record, "unknown")
            )
            await self.quota.mark(record.job_id, record.state)
            raise SubmissionUncertain(record, ValueError("invalid scheduler job id"))

        # ---- crash window: scheduler accepted, numeric id not yet persisted.
        self._phase("after_submit_accept", record)

        record.slurm_job_id = slurm_job_id
        record.cluster = cluster
        transition(record, PENDING, now=self._now())
        self.store.save_scheduler(
            record.job_id, self._scheduler_payload(record, None)
        )
        self.store.save_submission(
            record.job_id, self._submission_payload(record, "accepted")
        )
        self.store.save_record(record)
        await self.quota.mark(record.job_id, record.state)
        self._phase("after_submit_persist", record)
        self._phase("before_response", record)
        return record

    # -- readiness ----------------------------------------------------------

    async def mark_ready(
        self,
        job_id: str,
        *,
        node: str,
        container_endpoint: str | None = None,
        relay_endpoint: str | None = None,
        relay_pid: int | None = None,
        generation: int | None = None,
    ) -> JobRecord:
        """Publish readiness atomically after the container confirms SSH."""
        record = self.store.load_record(job_id)
        if record.state in (SUBMITTING, SUBMISSION_UNKNOWN):
            transition(record, PENDING, now=self._now())
        if record.state == PENDING:
            transition(record, STARTING, now=self._now())
        if record.state != STARTING:
            raise IllegalTransition(record.state, READY)

        self._phase("before_ready_publish", record)
        host, port = _split_endpoint(relay_endpoint)
        payload = {
            "node": node,
            "container_endpoint": container_endpoint,
            "relay_endpoint": relay_endpoint,
            "generation": generation if generation is not None else record.generation,
            "published_at": self._now(),
        }
        self.store.save_ready(job_id, payload)
        if relay_endpoint:
            self.store.save_relay(
                job_id,
                {
                    "endpoint": relay_endpoint,
                    "host": host,
                    "port": port,
                    "pid": relay_pid,
                    "generation": payload["generation"],
                    "updated_at": self._now(),
                },
            )

        record.node = node
        record.container_endpoint = container_endpoint
        record.relay_endpoint = relay_endpoint
        record.relay_host = host
        record.relay_port = port
        if relay_pid is not None:
            record.relay_pid = relay_pid
        if generation is not None:
            record.generation = generation
        transition(record, READY, now=self._now())
        self.store.save_record(record)
        await self.quota.mark(job_id, record.state)
        # ---- crash window: readiness persisted, caller has not been answered.
        self._phase("after_ready_publish", record)
        return record

    # -- observation --------------------------------------------------------

    async def poll(self, job_id: str) -> JobRecord:
        """Query the scheduler once and fold the result into the record."""
        record = self.store.load_record(job_id)
        if record.state == TERMINAL:
            return record
        if not record.slurm_job_id:
            return record
        scheduler_state = await self._call(self.scheduler.query, record.slurm_job_id)
        return await self._apply_scheduler_state(record, scheduler_state)

    async def _apply_scheduler_state(
        self, record: JobRecord, scheduler_state: Any
    ) -> JobRecord:
        state = normalize_scheduler_state(scheduler_state)
        if state is None:
            return record
        record.scheduler_state = state

        if state in SCHEDULER_TERMINAL_STATES:
            self.store.save_scheduler(record.job_id, self._scheduler_payload(record, state))
            self._persist_terminal(record, f"scheduler_{state.lower()}")
            await self.quota.release(record.job_id, definitive=True)
            return record

        if state in SCHEDULER_PENDING_STATES:
            if record.state in (SUBMITTING, SUBMISSION_UNKNOWN):
                transition(record, PENDING, scheduler_state=state, now=self._now())
            # A job already starting/ready does not go backwards on a stale
            # PENDING snapshot.
            self.store.save_scheduler(record.job_id, self._scheduler_payload(record, state))
            self.store.save_record(record)
            await self.quota.mark(record.job_id, record.state)
            return record

        if state in SCHEDULER_RUNNING_STATES:
            if record.state in (SUBMITTING, SUBMISSION_UNKNOWN):
                transition(record, PENDING, scheduler_state=state, now=self._now())
            if record.state == PENDING:
                transition(record, STARTING, scheduler_state=state, now=self._now())
            ready = self.store.load_ready(record.job_id)
            if record.state == STARTING and ready is not None:
                self._apply_ready_payload(record, ready)
                transition(record, READY, scheduler_state=state, now=self._now())
            self.store.save_scheduler(record.job_id, self._scheduler_payload(record, state))
            self.store.save_record(record)
            await self.quota.mark(record.job_id, record.state)
            return record

        # Unknown / suspended / other states: record the observation only.
        self.store.save_scheduler(record.job_id, self._scheduler_payload(record, state))
        self.store.save_record(record)
        return record

    @staticmethod
    def _apply_ready_payload(record: JobRecord, payload: Mapping[str, Any]) -> None:
        record.node = payload.get("node", record.node)
        record.container_endpoint = payload.get(
            "container_endpoint", record.container_endpoint
        )
        record.relay_endpoint = payload.get("relay_endpoint", record.relay_endpoint)
        record.relay_host, record.relay_port = _split_endpoint(record.relay_endpoint)
        generation = payload.get("generation")
        if isinstance(generation, int):
            record.generation = generation

    # -- cancellation and detach -------------------------------------------

    async def stop(self, job_id: str, *, reason: str = "cancelled") -> JobRecord:
        """Explicitly cancel a job; this is the only path that calls scancel."""
        record = self.store.load_record(job_id)
        if record.state == TERMINAL:
            return record
        transition(
            record, STOPPING, reason=reason, scheduler_state="CANCELLING", now=self._now()
        )
        self.store.save_record(record)
        await self.quota.mark(job_id, record.state)
        if record.slurm_job_id:
            await self._call(self.scheduler.cancel, record.slurm_job_id)
        return record

    async def shutdown(self) -> list[JobRecord]:
        """Detach from every job without cancelling anything.

        A gateway shutdown, route loss or reconnect must not signal Slurm.  The
        durable records remain the source of truth for the next reconciliation.
        """
        return self.store.scan()

    async def detach_all(self) -> list[JobRecord]:
        return await self.shutdown()

    # -- reconciliation -----------------------------------------------------

    async def ensure_reconciled(self) -> ReconciliationReport:
        """Run reconciliation once before accepting submissions."""
        if self._reconciled:
            return ReconciliationReport()
        async with self._reconcile_lock:
            if self._reconciled:
                return ReconciliationReport()
            report = await self.reconcile_all()
            self._reconciled = True
            return report

    async def reconcile_all(self) -> ReconciliationReport:
        """Reconcile every durable record with the scheduler; never submits."""
        records = self.store.scan()
        await self.quota.restore(records)
        report = ReconciliationReport(total=len(records))
        for record in records:
            try:
                await self._reconcile_one(record, report)
            except Exception as exc:  # noqa: BLE001 - keep reconciling others
                report.errors.append(f"{record.job_id}: {exc}")
        # Transitions above persisted first; rebuild once more so the in-memory
        # quota exactly matches disk even if a step partially failed.
        await self.quota.restore(self.store.scan())
        return report

    async def _reconcile_one(
        self, record: JobRecord, report: ReconciliationReport
    ) -> None:
        if record.state == TERMINAL:
            report.terminal += 1
            return

        if not record.slurm_job_id:
            # Close the window between persisting scheduler.json and job.json.
            persisted = self.store.load_scheduler(record.job_id)
            if persisted is not None and _valid_slurm_id(persisted.get("slurm_job_id")):
                record.slurm_job_id = str(persisted["slurm_job_id"])
                record.cluster = persisted.get("cluster") or record.cluster
                if record.state in (SUBMITTING, SUBMISSION_UNKNOWN):
                    transition(record, PENDING, now=self._now())
                self.store.save_record(record)
                self.store.save_submission(
                    record.job_id, self._submission_payload(record, "accepted")
                )
                report.recovered += 1

        if not record.slurm_job_id:
            if record.state not in (SUBMITTING, SUBMISSION_UNKNOWN):
                # A non-submitting record without a scheduler id is corrupt but
                # still occupies its slot; leave it for an operator.
                report.uncertain += 1
                return
            found = await self._find_by_token_async(record.correlation_token)
            if not found:
                if record.state == SUBMITTING:
                    transition(
                        record,
                        SUBMISSION_UNKNOWN,
                        reason="reconcile_no_scheduler_record",
                        now=self._now(),
                    )
                    self.store.save_record(record)
                    self.store.save_submission(
                        record.job_id, self._submission_payload(record, "unknown")
                    )
                await self.quota.mark(record.job_id, record.state)
                report.uncertain += 1
                return
            record.slurm_job_id = found
            if record.state == SUBMITTING:
                transition(record, PENDING, now=self._now())
            elif record.state == SUBMISSION_UNKNOWN:
                transition(record, PENDING, now=self._now())
            self.store.save_scheduler(
                record.job_id, self._scheduler_payload(record, None)
            )
            self.store.save_submission(
                record.job_id, self._submission_payload(record, "accepted")
            )
            self.store.save_record(record)
            report.recovered += 1

        scheduler_state = await self._call(self.scheduler.query, record.slurm_job_id)
        state = normalize_scheduler_state(scheduler_state)
        if state is None:
            await self.quota.mark(record.job_id, record.state)
            report.uncertain += 1
            return

        await self._apply_scheduler_state(record, state)
        if record.state == TERMINAL:
            report.terminal += 1
        else:
            report.active += 1


def _split_endpoint(endpoint: str | None) -> tuple[str | None, int | None]:
    if not endpoint:
        return None, None
    text = str(endpoint).strip()
    if not text:
        return None, None
    if text.startswith("["):
        # [ipv6]:port
        end = text.find("]")
        if end != -1:
            host = text[1:end]
            rest = text[end + 1 :]
            port = None
            if rest.startswith(":") and rest[1:].isdigit():
                port = int(rest[1:])
            return host, port
    if ":" in text:
        host, port_text = text.rsplit(":", 1)
        if port_text.isdigit():
            return host, int(port_text)
    return text, None

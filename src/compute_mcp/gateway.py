# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Gateway daemon: target state machine, HTTP API and interactive console."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import contextvars
import dataclasses
import getpass
import inspect
import json
import logging
import os
import posixpath
import signal
import sys
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable

import asyncssh
from aiohttp import web

from . import __version__
from . import jobs as jobcore
from .auth import AuthError, Authenticator, Client, ForbiddenTarget
from .allocation import (
    compute_plan,
    plan_summary,
    render_args,
    validate_conflicts,
)
from .bundle import BundleError, public_key_for, resolve_deploy_dir
from .config import (
    ConfigError,
    GatewayConfig,
    TargetConfig,
    append_client,
    append_token_hash,
    container_login_user,
    default_config_path,
    default_token_path,
    load_config,
    validate_target_name,
)
from .enrollment import (
    APPROVED,
    PENDING,
    EnrollmentError,
    EnrollmentManager,
    EnrollmentQueueFull,
    PendingEnrollment,
)
from .factor import FactorPromptError, prompt_factor
from .files import JOB_ACCESS_DENIED_MESSAGE, FileService, JobAccessError
from .files import chmod as files_chmod
from .files import list_dir as files_list
from .files import mkdir as files_mkdir
from .files import parse_mode as files_parse_mode
from .files import read as files_read
from .files import remove as files_remove
from .files import rename as files_rename
from .files import stat as files_stat
from .files import write as files_write
from .sessions import (
    SessionError,
    SessionForbidden,
    SessionLimitError,
    SessionManager,
    SessionNotFound,
)
from .ssh_backend import HostKeyError, SSHError, SSHBackend
from .tunnel import (
    ForwardError,
    RouteTunnel,
    Tunnel,
    TunnelError,
    TunnelManager,
    mux_helper_argv,
)

log = logging.getLogger("compute_mcp.gateway")

RECOVERY_POLL_SECONDS = 5.0

# A close command may need a short-lived route connection when the live tunnel
# is already gone (gateway shutdown after a lost route).  Bound the dial so
# shutdown cannot hang on an unreachable login node.
_CLOSE_ROUTE_TIMEOUT = 15.0


def _container_env_fields(target: TargetConfig) -> dict[str, str]:
    """The container-description fields of the gateway->provisioner contract.

    Shared by :func:`build_provision_env` (plan/argv path) and
    ``Gateway._target_provision_env`` (best-effort path) so the two cannot
    drift.  ``COMPUTEMCP_SSH_USER`` is emitted only when a container or bundle
    block is present, matching the dialable-account rule.
    """
    container = target.container
    fields = {
        "COMPUTEMCP_CONTAINER_RUNTIME": container.runtime if container else "",
        "COMPUTEMCP_STORAGE_ROOT": (container.storage_root or "") if container else "",
        "COMPUTEMCP_IMAGE": (container.image or "") if container else "",
        "COMPUTEMCP_GPU_VENDORS": ",".join(container.gpus) if container else "",
        "COMPUTEMCP_HOST_HOME": (container.host_home or "") if container else "",
        "COMPUTEMCP_SANDBOX": (
            "true" if container is not None and container.sandbox else "false"
        ),
        # Where the provisioning helper builds the sandbox: "login" (default)
        # builds on the login/head node; "compute" lets the batch job build it
        # on the compute node (required on an architecture-mismatched partition).
        # Kept in this shared helper so the dial and provision paths cannot drift.
        "COMPUTEMCP_BUILD_LOCATION": (
            container.build_location if container is not None else "login"
        ),
    }
    if target.container is not None or target.bundle is not None:
        # The container sshd allows only its own login account (default
        # ``ubuntu``); the helper creates it and AllowUsers it.  The gateway
        # dials the container as the same account, resolved through the single
        # ``container_login_user`` source of truth so the two cannot drift.
        fields["COMPUTEMCP_SSH_USER"] = container_login_user(target)
    return fields


def build_provision_env(target: TargetConfig, plan, sbatch_args, srun_args) -> dict[str, str]:
    """Build the gateway -> provisioner environment contract.

    The gateway converts the resolved plan, the rendered per-stage arguments
    and the container description into ``COMPUTEMCP_*`` variables that the
    trusted provisioning command consumes (see the design doc,
    "Gateway-to-provisioner interface").  Only the two ARGS variables and the
    plan/container variables are emitted: nothing here can collide with a user
    environment variable.

    ``COMPUTEMCP_SBATCH_ARGS`` / ``COMPUTEMCP_SRUN_ARGS`` carry one complete
    argument per newline with no trailing newline; an empty string means an
    empty argument list.  The two stages are rendered independently and never
    copied between each other.  Numeric per-node fields become an empty string
    when the plan has no value; ``COMPUTEMCP_NODES``, ``COMPUTEMCP_EXCLUSIVE``
    and ``COMPUTEMCP_MODE`` are always concrete.
    """
    def _num(value) -> str:
        return "" if value is None else str(value)

    env = {
        "COMPUTEMCP_SBATCH_ARGS": "\n".join(sbatch_args),
        "COMPUTEMCP_SRUN_ARGS": "\n".join(srun_args),
        "COMPUTEMCP_NODES": str(plan.nodes),
        "COMPUTEMCP_CPUS_PER_NODE": _num(plan.cpus_per_node),
        "COMPUTEMCP_GPUS_PER_NODE": _num(plan.gpus_per_node),
        "COMPUTEMCP_MEMORY_PER_NODE_MIB": _num(plan.memory_per_node_mib),
        "COMPUTEMCP_EXCLUSIVE": "true" if plan.exclusive else "false",
        "COMPUTEMCP_MODE": plan.mode,
        "COMPUTEMCP_SYSTEM": target.name,
    }
    env.update(_container_env_fields(target))
    # A deployed bundle target gets the container's authorized public key derived
    # from the target's SSH client key, so no per-site key placement is needed.
    # An explicit provision_command target keeps the previous environment.
    if target.bundle is not None:
        try:
            public_key = public_key_for(target)
        except BundleError as exc:
            raise ConfigError(str(exc)) from exc
        if public_key:
            env["COMPUTEMCP_SSH_PUBLIC_KEY"] = public_key
        # Pre-provision hooks run on the remote before the container runtime is
        # used.  The joined lines carry no trailing newline; an empty tuple
        # yields an empty string, which the helper treats as a no-op.
        env["COMPUTEMCP_PROVISION_ENV"] = "\n".join(target.bundle.provision_env)
    # A shell string cannot carry NUL or carriage return; refuse both here
    # (HTTP 400 at the edge) rather than let them reach the trusted provision
    # command (a CR in a value could smuggle an extra shell line).  LF stays
    # allowed: it is the intentional delimiter of the two ARGS variables, whose
    # individual arguments are already validated (allocation.py).
    for name, value in env.items():
        if "\x00" in value or "\r" in value:
            raise ConfigError(
                f"provisioning environment {name!r} contains a NUL byte or "
                f"carriage return"
            )
        if "\x00" in name or "\r" in name:
            raise ConfigError(
                f"provisioning environment name {name!r} contains a NUL byte "
                f"or carriage return"
            )
    return env


def plan_signature(summary: dict | None) -> tuple | None:
    """Effective per-node request of a stored plan summary, for mismatch checks.

    Two override sets that resolve to the same concrete request are a no-op;
    only a value difference requires a refresh.
    """
    if not summary:
        return None
    plan = summary.get("plan") or {}
    return (
        plan.get("nodes"),
        plan.get("cpus_per_node"),
        plan.get("gpus_per_node"),
        plan.get("memory_per_node_mib"),
        plan.get("exclusive"),
        plan.get("mode"),
    )


class InteractiveAuthRequired(RuntimeError):
    """A target needs a second factor and no factor was supplied for the request."""

    def __init__(self, target: str, detail: str | None = None) -> None:
        message = (
            f"target {target!r} requires interactive authentication (second "
            f"factor); run target-connect --2fa {target}"
        )
        # The actionable sentence stays the prefix so existing substring
        # assertions keep matching; the dial detail follows when known so the
        # operator can see why the container hop failed in the awaiting case.
        if detail:
            message = f"{message} (container hop failed: {detail})"
        super().__init__(message)
        self.target = target
        self.detail = detail


class _RouteChangedDuringDial(SSHError):
    """A container dial raced a refresh and could not be re-based on the route."""


@dataclass
class TargetRuntime:
    name: str
    state: str = "disconnected"
    active_route: str | None = None
    local_port: int | None = None
    tunnel: Tunnel | None = None
    last_error: str | None = None
    connected_since: datetime | None = None
    needs_refresh: bool = False
    backoff: float = 0.0
    provisioned_endpoint: str | None = None
    # True while an interactive (2FA) target still needs a factor before it can
    # be used.  Never carries a secret; only tells operators why a connect was
    # refused/failed so a real tunnel error is not masked as "needs 2FA".
    awaiting_factor: bool = False
    # Resolved allocation retained across a connect/refresh so recovery and a
    # later refresh reuse it instead of silently reverting to defaults.  Empty
    # for a target with no allocation block.
    resolved_plan: dict | None = None
    resolved_overrides: dict[str, Any] = field(default_factory=dict)
    recovery_task: asyncio.Task | None = field(default=None, repr=False)
    # Monotonic guard for concurrent container dials.  A dial that passed
    # ``ensure_connected`` remembers this value; a refresh advances it before
    # disturbing the route and again after adopting the new one, so the dial
    # cannot hand out a connection on the old forward or stale endpoint.
    route_epoch: int = 0

    def public(self, clients: int = 0) -> dict:
        return {
            "name": self.name,
            "state": self.state,
            "active_route": self.active_route,
            "local_port": self.local_port,
            "last_error": self.last_error,
            "connected_since": self.connected_since.isoformat()
            if self.connected_since
            else None,
            "clients": clients,
            "needs_refresh": self.needs_refresh,
            "provisioned_endpoint": self.provisioned_endpoint,
            "awaiting_factor": self.awaiting_factor,
            # Additive: a target without a Slurm allocation reports None so the
            # existing keys/format stay unchanged for existing callers.
            "resolved_plan": self.resolved_plan,
            "resolved_overrides": dict(self.resolved_overrides),
        }


# ---------------------------------------------------------------------------
# Job provisioning boundary (route state stays in TargetRuntime)
# ---------------------------------------------------------------------------
#
# One public error for "unknown job" and "someone else's job": a caller must
# not be able to probe another client's job ids.
JOB_NOT_FOUND_TEXT = "job not found"

#: Owner stored on a shared job record.  Shared jobs belong to the target, not
#: to the client that happened to start it, so visibility never depends on the
#: creator.
SHARED_OWNER = "@shared"


class JobNotReady(RuntimeError):
    """A job has no usable endpoint yet (queued, starting or terminal).

    Kept distinct from :class:`JobAccessError` so an authorized caller learns
    that their own job is not ready instead of the indistinguishable
    not-found/forbidden answer reserved for an unknown or foreign id.
    """

    def __init__(self, job_id: str, state: str | None = None) -> None:
        detail = f" (state {state})" if state else ""
        super().__init__(f"job {job_id!r} is not ready{detail}")
        self.job_id = job_id
        self.state = state


async def _maybe_await(value: Any) -> Any:
    """Await ``value`` when it is awaitable, otherwise return it unchanged."""
    if inspect.isawaitable(value):
        return await value
    return value


@runtime_checkable
class JobProvisioner(Protocol):
    """Boundary between the gateway scheduler code and the bundle stages.

    T8 supplies the concrete implementation: ``submit`` renders and runs the
    batch stage, ``observe`` maps a native scheduler id to a Slurm state,
    ``attach`` publishes the compute-node/relay endpoint once the container is
    reachable, ``cancel`` is the *only* path that scancels, and ``detach``
    closes local resources without signalling the scheduler.
    """

    async def submit(self, record: "jobcore.JobRecord") -> Any: ...

    async def observe(self, job: "jobcore.JobRecord | str") -> str | None: ...

    async def attach(
        self, record: "jobcore.JobRecord"
    ) -> Mapping[str, Any] | None: ...

    async def cancel(self, job: "jobcore.JobRecord | str") -> None: ...

    async def detach(self, record: "jobcore.JobRecord") -> None: ...


def _slurm_id_of(job: "jobcore.JobRecord | str") -> str | None:
    if isinstance(job, jobcore.JobRecord):
        return job.slurm_job_id
    value = str(job).strip()
    return value or None


class SlurmProvisioner:
    """Thin :class:`JobProvisioner` adapter over the T8 bundle stages.

    ``stages`` is the T8 injection point: an object exposing
    ``submit``/``observe``/``attach``/``cancel``/``detach``.  Until those are
    wired, the same adapter renders the exact scheduler argv and executes it
    through ``run`` (an async command runner on the route; tests pass a
    subprocess runner).  This keeps the scheduler calls in one place:
    ``sbatch --parsable --comment=<token>``, a bounded ``squeue`` query, and
    ``scancel`` only for an explicit cancel.
    """

    def __init__(
        self,
        *,
        stages: Any = None,
        run: Callable[[Sequence[str]], Any] | None = None,
        sbatch_args: Sequence[str] = (),
        script: str | None = None,
    ) -> None:
        self._stages = stages
        self._run = run
        self._sbatch_args = tuple(str(arg) for arg in sbatch_args)
        self._script = script

    async def _execute(self, argv: Sequence[str]) -> tuple[int, str, str]:
        if self._run is None:
            raise jobcore.JobError(
                "no scheduler command runner or bundle stages are configured"
            )
        result = await _maybe_await(self._run(list(argv)))
        if hasattr(result, "returncode"):
            return (
                int(result.returncode),
                result.stdout or "",
                result.stderr or "",
            )
        if isinstance(result, tuple) and len(result) >= 3:
            return int(result[0]), str(result[1] or ""), str(result[2] or "")
        return 0, str(result or ""), ""

    async def _delegate(self, name: str, *args: Any) -> Any:
        if self._stages is not None:
            return await _maybe_await(getattr(self._stages, name)(*args))
        return await getattr(self, f"_default_{name}")(*args)

    async def _default_submit(self, record: "jobcore.JobRecord") -> str:
        argv = [
            "sbatch",
            "--parsable",
            f"--comment={record.correlation_token}",
        ]
        argv.extend(self._sbatch_args)
        script = record.parameters.get("sbatch_script") or self._script
        if script:
            argv.append(str(script))
        else:
            argv.extend(["--wrap", str(record.parameters.get("wrap", "true"))])
        code, out, err = await self._execute(argv)
        if code != 0:
            raise jobcore.SchedulerRejected(err.strip() or f"sbatch exited {code}")
        line = out.strip().splitlines()[-1] if out.strip() else ""
        if not line:
            raise RuntimeError("sbatch produced no job id")
        return line

    async def _default_observe(
        self, job: "jobcore.JobRecord | str"
    ) -> str | None:
        sid = _slurm_id_of(job)
        if not sid:
            return None
        code, out, err = await self._execute(
            ["squeue", "--noheader", "--format=%T", "--jobs", sid]
        )
        if code != 0:
            raise RuntimeError(err.strip() or f"squeue exited {code}")
        text = out.strip()
        return text.splitlines()[0].strip() if text else None

    async def _default_attach(
        self, record: "jobcore.JobRecord"
    ) -> Mapping[str, Any] | None:
        # The T8 bundle stage publishes the ready endpoint; the default adapter
        # has nothing to attach until it is wired.
        return None

    async def _default_cancel(self, job: "jobcore.JobRecord | str") -> None:
        sid = _slurm_id_of(job)
        if not sid:
            return
        code, _out, err = await self._execute(["scancel", sid])
        if code != 0:
            raise RuntimeError(err.strip() or f"scancel exited {code}")

    async def _default_detach(self, record: "jobcore.JobRecord") -> None:
        # Detach never signals the scheduler; nothing to do for the adapter.
        return None

    async def find_by_token(self, token: str) -> str | None:
        """Reconcile an accepted-but-unrecorded submission by job comment."""
        code, out, _err = await self._execute(
            ["squeue", "--noheader", "--format=%i %k"]
        )
        if code != 0:
            return None
        for line in out.splitlines():
            parts = line.split(None, 1)
            if len(parts) == 2 and parts[1].strip() == token:
                return parts[0].strip()
        return None

    async def submit(self, record: "jobcore.JobRecord") -> Any:
        return await self._delegate("submit", record)

    async def observe(self, job: "jobcore.JobRecord | str") -> str | None:
        return await self._delegate("observe", job)

    async def attach(
        self, record: "jobcore.JobRecord"
    ) -> Mapping[str, Any] | None:
        return await self._delegate("attach", record)

    async def cancel(self, job: "jobcore.JobRecord | str") -> None:
        await self._delegate("cancel", job)

    async def detach(self, record: "jobcore.JobRecord") -> None:
        await self._delegate("detach", record)


class _RouteOnlyTunnel:
    """A dialed login route with no provisioning and no local forwarding.

    The durable multi-job path opens the route (and, once T8 lands, the mux
    helper) separately from any job.  This wrapper gives the runtime the small
    surface it needs (``is_alive``/``stop``/``connection``) without starting a
    container.
    """

    def __init__(self, target: TargetConfig, route: str | None, connection: Any):
        self.target = target
        self.route = route
        self.connection = connection
        self.local_port: int | None = None
        self.provisioned_endpoint: tuple[str, int] | None = None

    def is_alive(self) -> bool:
        conn = self.connection
        if conn is None:
            return False
        checker = getattr(conn, "is_closed", None)
        return not (callable(checker) and checker())

    async def stop(self, *, close_connection: bool = True) -> None:
        conn = self.connection
        self.connection = None
        if conn is not None and close_connection:
            with contextlib.suppress(Exception):
                conn.close()
            with contextlib.suppress(Exception):
                await conn.wait_closed()


class _GatewayScheduler:
    """Adapt the dynamic :class:`JobProvisioner` to ``jobs.Scheduler``.

    ``jobs.JobManager`` takes one scheduler object; the gateway may resolve a
    different provisioner per target (or have one injected after construction
    by a test).  This binding looks the provisioner up lazily, so injection
    order never matters.
    """

    def __init__(self, gateway: "Gateway") -> None:
        self._gateway = gateway

    async def submit(self, record: "jobcore.JobRecord") -> Any:
        provisioner = self._gateway._provisioner_for_target(record.target)
        return await _maybe_await(provisioner.submit(record))

    async def query(self, slurm_job_id: str) -> str | None:
        record = self._gateway._find_record_by_slurm_id(slurm_job_id)
        if record is None:
            return None
        provisioner = self._gateway._provisioner_for_target(record.target)
        return await _maybe_await(provisioner.observe(slurm_job_id))

    async def cancel(self, slurm_job_id: str) -> None:
        record = self._gateway._find_record_by_slurm_id(slurm_job_id)
        if record is None:
            return
        provisioner = self._gateway._provisioner_for_target(record.target)
        await _maybe_await(provisioner.cancel(slurm_job_id))

    async def find_by_token(self, token: str) -> str | None:
        for name, target in self._gateway.config.targets.items():
            if not self._gateway._is_multijob(target):
                continue
            try:
                provisioner = self._gateway._provisioner_for_target(name)
            except jobcore.JobError:
                continue
            finder = getattr(provisioner, "find_by_token", None)
            if finder is None:
                continue
            try:
                found = await _maybe_await(finder(token))
            except Exception:  # noqa: BLE001 - recovery must continue
                continue
            if found:
                return str(found)
        return None


def _settings_signature(
    parameters: Mapping[str, Any] | None,
    root_mode: str | None,
    home: str | None,
) -> str:
    return json.dumps(
        {
            "parameters": dict(parameters or {}),
            "root_mode": root_mode,
            "home": home,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _public_job(
    record: "jobcore.JobRecord", *, include_owner: bool = False
) -> dict[str, Any]:
    """The caller-facing view of a job; relay ports stay gateway-internal."""
    data = {
        "job_id": record.job_id,
        "target": record.target,
        "kind": "shared" if record.kind == "shared" else "own",
        "state": record.state,
        "slurm_job_id": record.slurm_job_id,
        "cluster": record.cluster,
        "scheduler_state": record.scheduler_state,
        "root_mode": record.root_mode,
        "base_digest": record.base_digest,
        "home": record.home,
        "node": record.node,
        "container_endpoint": record.container_endpoint,
        "generation": record.generation,
        "terminal_reason": record.terminal_reason,
        "parameters": dict(record.parameters),
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "ready_at": record.ready_at,
        "terminal_at": record.terminal_at,
    }
    if include_owner:
        data["owner"] = record.owner_client_id
    return data


def validate_config_allocations(config: GatewayConfig) -> None:
    """Cross-check every target's enabled mappings against its manual options.

    This runs at gateway startup and before a reload is applied, so
    ``computeMCP-gatewayctl reload`` refuses a bad configuration (and keeps the
    previous one) instead of discovering the conflict at connect time.
    ``validate_conflicts`` is imported lazily to avoid a circular import
    (allocation imports config); it is pure and side-effect free.
    """
    for target in config.targets.values():
        validate_conflicts(target)


class Gateway:
    def __init__(
        self,
        config: GatewayConfig,
        transport_override: str | None = None,
        *,
        state_root: str | os.PathLike[str] | None = None,
        provisioner: Any = None,
        provisioner_factory: Callable[[TargetConfig], Any] | None = None,
        job_poll_interval: float = 2.0,
    ):
        validate_config_allocations(config)
        self.config = config
        self.transport_override = transport_override
        self._apply_transport_override()
        self.auth = Authenticator(config.clients)
        self.backend = SSHBackend()
        self.tunnels = TunnelManager(config.ssh)
        # Request-scoped caller identity for the job-scoped services.  The
        # FileService/SessionManager callbacks only receive a job id, so the
        # authenticated client is bound per request and consulted before any
        # connection or network work.  A context variable is correct under
        # concurrency: two clients operating on different jobs never share it.
        self._file_client: contextvars.ContextVar[Client | None] = (
            contextvars.ContextVar("compute_mcp_file_client", default=None)
        )
        self._session_client: contextvars.ContextVar[Client | None] = (
            contextvars.ContextVar("compute_mcp_session_client", default=None)
        )
        self.sessions = SessionManager(
            self.backend,
            self._connection_provider,
            config.sessions,
            dedicated_provider=self._dedicated_connection_provider,
            job_provider=self._job_connection_provider,
            job_authorizer=self._session_job_authorizer,
        )
        # Job-scoped SFTP: every operation carries an explicit job id, resolves
        # through the job connection cache (never the target-keyed legacy cache)
        # and shares the backend's channel budgeting/retry behaviour.
        self.files = FileService(
            self._file_resolve_job,
            self._file_authorize_job,
            sftp_session=self.backend.sftp_client,
        )
        # Per-route job_id -> relay endpoint registry handed to the mux helper.
        # The helper reloads it on every OPEN, so a job that becomes ready after
        # the helper started is still routable.
        self._mux_registries: dict[str, dict[str, str]] = {}
        # Test/integration hook: receives (target, mapping) after every registry
        # write so an in-process helper can observe it without a real file.
        self.route_registry_sink: Callable[[str, dict[str, str]], None] | None = None
        self.runtimes: dict[str, TargetRuntime] = {
            name: TargetRuntime(name=name) for name in config.targets
        }
        self._locks: dict[str, asyncio.Lock] = {}
        self._watch_task: asyncio.Task | None = None
        self._stopping = False
        # Interactive-authentication plumbing for the console.
        self._interactive = False
        self._prompt_lock = asyncio.Lock()
        # Out-of-band enrollment queue (bounded + expiring).
        self.enrollments = EnrollmentManager(
            ttl=config.server.enroll_ttl,
            max_pending=config.server.enroll_max_pending,
        )
        # Per-job state lives outside TargetRuntime (which stays route state).
        # The store/quota/manager are created lazily so a deployment that never
        # uses jobs, and the existing single-container tests, touch no disk.
        self.state_root = Path(state_root) if state_root is not None else None
        self.job_store: jobcore.JobStore | None = None
        self.quota: jobcore.QuotaManager | None = None
        self.jobs: jobcore.JobManager | None = None
        # Provisioner injection: tests set ``gw.provisioner``; T8 supplies a
        # factory that builds the per-target bundle-stage adapter.
        self.provisioner = provisioner
        self.provisioner_factory = provisioner_factory
        self.route_opener: Callable[..., Any] | None = None
        self.job_poll_interval = float(job_poll_interval)
        self._job_monitor_task: asyncio.Task | None = None
        self._shared_locks: dict[str, asyncio.Lock] = {}

    # -- helpers -----------------------------------------------------------
    def _apply_transport_override(self) -> None:
        if not self.transport_override:
            return
        kind = self.transport_override
        new_targets: dict[str, TargetConfig] = {}
        for name, target in self.config.targets.items():
            transport = dataclasses.replace(target.transport, kind=kind)
            if kind == "tunnel" and not transport.ssh_targets:
                transport = dataclasses.replace(transport, ssh_targets=(name,))
            new_targets[name] = dataclasses.replace(target, transport=transport)
        self.config = dataclasses.replace(self.config, targets=new_targets)

    def _target(self, name: str) -> TargetConfig:
        target = self.config.targets.get(name)
        if target is None:
            raise ForbiddenTarget(name)
        return target

    def _lock(self, name: str) -> asyncio.Lock:
        return self._locks.setdefault(name, asyncio.Lock())

    def _shared_lock(self, name: str) -> asyncio.Lock:
        return self._shared_locks.setdefault(name, asyncio.Lock())

    # -- job state helpers -------------------------------------------------
    @staticmethod
    def _is_multijob(target: TargetConfig) -> bool:
        """Whether the durable multi-job Slurm path owns this target.

        The split route/job path needs the deployable bundle that carries the
        T8 stage functions; a target with only a bare ``slurm`` block keeps the
        legacy single-allocation connect path (and its tests).
        """
        return target.slurm is not None and target.bundle is not None

    def _default_state_root(self) -> Path:
        env = os.environ.get("COMPUTEMCP_STATE_DIR")
        if env:
            return Path(env)
        if self.config.config_path:
            return Path(self.config.config_path).resolve().parent / "state"
        return Path(tempfile.mkdtemp(prefix="computemcp-gateway-jobs-"))

    def _ensure_jobs(self) -> jobcore.JobManager:
        """Create the durable job registry on first use (idempotent)."""
        if self.jobs is not None:
            return self.jobs
        root = self.state_root or self._default_state_root()
        self.state_root = Path(root)
        store = jobcore.JobStore(self.state_root)
        quota = jobcore.QuotaManager(
            {
                client_id: dict(cfg.job_limits)
                for client_id, cfg in self.config.clients.items()
            },
            default_limit=jobcore.DEFAULT_JOB_LIMIT,
            snapshot=store.scan,
            guard=store.quota_lock,
        )
        self.job_store = store
        self.quota = quota
        self.jobs = jobcore.JobManager(store, _GatewayScheduler(self), quota)
        return self.jobs

    def _provisioner_for_target(self, target: str) -> Any:
        if self.provisioner_factory is not None:
            return self.provisioner_factory(self._target(target))
        if self.provisioner is not None:
            return self.provisioner
        raise jobcore.JobError("no job provisioner is configured for this gateway")

    def _provisioner_available(self) -> bool:
        return self.provisioner is not None or self.provisioner_factory is not None

    def _find_record_by_slurm_id(
        self, slurm_job_id: str
    ) -> "jobcore.JobRecord | None":
        if self.job_store is None:
            return None
        wanted = str(slurm_job_id)
        for record in self.job_store.scan():
            if record.slurm_job_id == wanted:
                return record
        return None

    def _active_shared_record(self, name: str) -> "jobcore.JobRecord | None":
        if self.job_store is None:
            return None
        active = [
            record
            for record in self.job_store.scan()
            if record.target == name
            and record.kind == "shared"
            and record.state != jobcore.TERMINAL
        ]
        if not active:
            return None
        return max(active, key=lambda record: record.created_at)

    def _operator_client(self) -> Client:
        return Client(
            client_id=SHARED_OWNER,
            allow_all=True,
            targets=frozenset(self.config.targets),
        )

    def _authorized_job(
        self, client: Client, job_id: str
    ) -> "jobcore.JobRecord":
        """Resolve a job for ``client``; foreign and unknown ids look identical."""
        self._ensure_jobs()
        record = self.job_store.try_load_record(str(job_id))  # type: ignore[union-attr]
        if record is None or not client.may_access(record.target):
            raise web.HTTPNotFound(text=JOB_NOT_FOUND_TEXT)
        if record.kind == "shared":
            return record
        if not client.is_admin and record.owner_client_id != client.client_id:
            raise web.HTTPNotFound(text=JOB_NOT_FOUND_TEXT)
        return record

    def resolve_job(
        self, client: Client, target: str, job_id: str | None = None
    ) -> "jobcore.JobRecord":
        """Select the caller's job for a target.

        An omitted ``job_id`` selects the target's shared job only; it never
        falls back to a caller's private job.  An explicit stale shared id
        resolves to that (terminal) record, never to its replacement.
        """
        client.require_target(target)
        if job_id:
            record = self._authorized_job(client, job_id)
            if record.target != target:
                raise web.HTTPNotFound(text=JOB_NOT_FOUND_TEXT)
            return record
        self._ensure_jobs()
        shared = self._active_shared_record(target)
        if shared is None:
            raise web.HTTPNotFound(text=JOB_NOT_FOUND_TEXT)
        return shared

    # -- job-scoped transport wiring --------------------------------------
    @staticmethod
    def _split_endpoint(endpoint: str) -> tuple[str, int]:
        host, sep, port = endpoint.rpartition(":")
        if not sep or not host:
            raise SSHError(f"malformed job endpoint: {endpoint!r}")
        try:
            return host, int(port)
        except ValueError as exc:
            raise SSHError(f"malformed job endpoint: {endpoint!r}") from exc

    def _job_endpoint(
        self, record: "jobcore.JobRecord", *, over_mux: bool
    ) -> tuple[str, int]:
        """The address a job connection is keyed/dialed on.

        Over the mux the helper already resolves the loopback relay, so the
        relay endpoint is authoritative; on a direct per-job endpoint the
        container endpoint is.  Either way the value is stable per generation,
        which is what the backend cache uses to fence a stale endpoint.
        """
        if over_mux:
            endpoint = record.relay_endpoint or record.container_endpoint
        else:
            endpoint = record.container_endpoint or record.relay_endpoint
        if not endpoint:
            raise JobNotReady(record.job_id, record.state)
        return self._split_endpoint(endpoint)

    async def _job_mux(self, target_name: str):
        """Ensure the route is open and return its mux transport.

        ``None`` means the target has no outer route to multiplex over (a direct
        transport or a test-injected route opener); the caller then dials the
        job endpoint directly.
        """
        target = self._target(target_name)
        runtime = self.runtimes.get(target_name)
        if runtime is None or not self._target_connected_live(target, runtime):
            await self.open_target(target_name)
            runtime = self.runtimes.get(target_name)
        if runtime is None:
            raise JobNotReady(target_name)
        tunnel = runtime.tunnel
        if tunnel is None:
            return None
        mux = getattr(tunnel, "mux", None)
        if mux is None:
            # A test-injected route opener (or a direct transport) has no mux.
            return None
        if not tunnel.is_alive():
            raise TunnelError(f"target {target_name!r} route is not alive")
        return mux

    async def _job_connection(self, record: "jobcore.JobRecord"):
        """Return the connection for exactly this job/generation.

        Slurm jobs are never served from the target-keyed legacy cache: the
        backend cache key is ``(job_id, generation)`` so a redial or stop of one
        job cannot invalidate another.  With a mux route the container SSH bytes
        travel over the job's stream on the one outer channel.
        """
        target = self._target(record.target)
        if record.state != jobcore.READY:
            # Only a job with a published, verified endpoint has a container to
            # dial; a queued or stopping job must not fall through to a stale
            # endpoint.
            raise JobNotReady(record.job_id, record.state)
        mux = await self._job_mux(record.target)
        host, port = self._job_endpoint(record, over_mux=mux is not None)
        if mux is not None:
            # Publish the endpoint before the helper is asked to OPEN it.
            self._write_route_registry(record.target)
        return await self.backend.job_connection(
            target,
            record.job_id,
            host,
            port,
            generation=record.generation,
            transport=mux,
        )

    def _job_record(self, target_name: str, job_id: str) -> "jobcore.JobRecord":
        self._ensure_jobs()
        record = self.job_store.try_load_record(str(job_id))  # type: ignore[union-attr]
        if record is None or record.target != target_name:
            raise JobAccessError()
        return record

    async def _job_connection_provider(self, name: str, job_id: str):
        """SessionManager job provider: (target, job-scoped connection)."""
        record = self._job_record(name, job_id)
        return self._target(name), await self._job_connection(record)

    async def _session_job_authorizer(self, target: str, job_id: str) -> bool:
        client = self._session_client.get()
        if client is None:
            return False
        try:
            record = self._authorized_job(client, job_id)
        except web.HTTPNotFound:
            return False
        return record.target == target

    async def _file_authorize_job(self, job_id: str) -> bool:
        client = self._file_client.get()
        if client is None:
            return False
        try:
            self._authorized_job(client, job_id)
        except web.HTTPNotFound:
            return False
        return True

    async def _file_resolve_job(self, job_id: str):
        client = self._file_client.get()
        if client is None:
            raise JobAccessError()
        record = self._authorized_job(client, job_id)
        return await self._job_connection(record)

    @staticmethod
    def _job_id_from_body(body: Mapping[str, Any] | None) -> str | None:
        if not body:
            return None
        value = body.get("job_id")
        if value is None:
            return None
        if not isinstance(value, str) or not value:
            raise web.HTTPBadRequest(text="'job_id' must be a non-empty string")
        return value

    def _file_is_job_scoped(self, target_name: str | None) -> bool:
        target = self.config.targets.get(target_name) if target_name else None
        return target is not None and self._is_multijob(target)

    def _resolve_file_job_id(
        self, client: Client, target_name: str, raw_job_id: str | None
    ) -> str:
        """Resolve the id before the FileService sees it.

        An omitted id selects the target's shared job only.  Unknown and foreign
        ids collapse to one public answer; the FileService repeats the same
        check and this message, so no handler path can leak a difference.
        """
        try:
            record = self.resolve_job(client, target_name, raw_job_id or None)
        except web.HTTPNotFound:
            raise web.HTTPNotFound(text=JOB_ACCESS_DENIED_MESSAGE) from None
        return record.job_id

    async def _run_file_op(self, client: Client, job_id: str, op):
        token = self._file_client.set(client)
        try:
            return await op(job_id)
        except JobAccessError:
            raise web.HTTPNotFound(text=JOB_ACCESS_DENIED_MESSAGE) from None
        finally:
            self._file_client.reset(token)

    @contextlib.asynccontextmanager
    async def _file_client_context(self, client: Client):
        token = self._file_client.set(client)
        try:
            yield
        finally:
            self._file_client.reset(token)

    def _file_job_id(self, client: Client, request: web.Request, body=None) -> str | None:
        """Resolve the per-request job id, or ``None`` for a legacy target."""
        target_name = request.query.get("target")
        if not target_name:
            raise web.HTTPBadRequest(text="'target' query parameter is required")
        client.require_target(target_name)
        if not self._file_is_job_scoped(target_name):
            return None
        raw_job_id = request.query.get("job_id") or self._job_id_from_body(body)
        return self._resolve_file_job_id(client, target_name, raw_job_id)

    # -- mux relay registry -----------------------------------------------
    def _registry_path(self, target_name: str) -> Path:
        self._ensure_jobs()
        directory = self.state_root / "mux"  # type: ignore[operator]
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"{target_name}.json"

    def _route_registry(self, target_name: str) -> dict[str, str]:
        """Every non-terminal endpoint currently usable for this target."""
        mapping: dict[str, str] = {}
        if self.job_store is None:
            return mapping
        for record in self.job_store.scan():
            if record.target != target_name:
                continue
            if record.state == jobcore.TERMINAL:
                continue
            endpoint = record.relay_endpoint or record.container_endpoint
            if endpoint:
                mapping[record.job_id] = endpoint
        return mapping

    def _write_route_registry(self, target_name: str) -> Path:
        path = self._registry_path(target_name)
        mapping = self._route_registry(target_name)
        # A unique temp name lets concurrent readiness/exec updates publish
        # atomically without one replace invalidating another's temp file.
        tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
        tmp.write_text(json.dumps(mapping, sort_keys=True), encoding="utf-8")
        os.replace(tmp, path)
        self._mux_registries[target_name] = mapping
        if self.route_registry_sink is not None:
            with contextlib.suppress(Exception):
                self.route_registry_sink(target_name, dict(mapping))
        return path

    def _mux_helper_argv(self, target: TargetConfig, route: str) -> tuple[str, ...]:
        path = self._write_route_registry(target.name)
        deploy_dir = resolve_deploy_dir(target)
        return mux_helper_argv(target, deploy_dir, str(path))

    def _resolve_clean_home(self, clean_home: str | None) -> str | None:
        if clean_home is None:
            return None
        if not isinstance(clean_home, str) or not clean_home.strip():
            raise ConfigError("clean_home must be a non-empty path")
        relative = clean_home.strip().lstrip("/")
        parts = Path(relative).parts
        if not relative or relative in (".", "..") or ".." in parts:
            raise ConfigError("clean_home must be a relative path inside the home")
        return relative

    def _endpoint(self, runtime: TargetRuntime) -> tuple[str, int]:
        target = self.config.targets[runtime.name]
        if target.transport.kind == "direct":
            # The gateway dials ``remote_host:remote_port`` itself.  A loopback
            # remote_host here therefore points at the gateway's OWN loopback,
            # which can only work when the container really runs next to the
            # gateway.  For a tunnel target the same loopback value is valid and
            # is intentionally NOT rejected: the forward target host:port is
            # resolved REMOTELY at the route host, so ``127.0.0.1`` means the
            # container on that remote host.  Runtime behavior is unchanged; the
            # distinction is documented here so the two cases are not confused.
            return target.transport.remote_host, target.transport.remote_port
        if runtime.local_port is None:
            raise TunnelError(f"target {runtime.name!r} has no active tunnel")
        return "127.0.0.1", runtime.local_port

    # -- lifecycle ---------------------------------------------------------
    async def start(self) -> None:
        self.sessions.start()
        self._watch_task = asyncio.create_task(self._watch_tunnels())
        if any(self._is_multijob(target) for target in self.config.targets.values()):
            self._ensure_jobs()
            self._start_job_monitor()
        for name, target in self.config.targets.items():
            if target.auto_connect and not target.interactive_auth:
                asyncio.create_task(self._safe_connect(name))
            elif target.auto_connect:
                log.info(
                    "target %s: skipping auto-connect (interactive_auth requires "
                    "a second factor)",
                    name,
                )

    async def stop(self) -> None:
        self._stopping = True
        if self._watch_task is not None:
            self._watch_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._watch_task
            self._watch_task = None
        if self._job_monitor_task is not None:
            self._job_monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._job_monitor_task
            self._job_monitor_task = None
        for name in list(self.runtimes):
            target = self.config.targets.get(name)
            if target is not None and self._is_multijob(target):
                # Slurm teardown detaches: no close_command and no scancel.
                with contextlib.suppress(Exception):
                    await self._detach_slurm_target(name)
            else:
                with contextlib.suppress(Exception):
                    await self.stop_target(name)
        if self.jobs is not None:
            with contextlib.suppress(Exception):
                await self.jobs.detach_all()
        with contextlib.suppress(Exception):
            await self.files.close_all()
        await self.sessions.stop()
        await self.backend.close_all()

    async def _safe_connect(self, name: str) -> None:
        target = self.config.targets.get(name)
        with contextlib.suppress(TunnelError, SSHError, HostKeyError, jobcore.JobError):
            if target is not None and self._is_multijob(target):
                # auto_connect opens the route only: no submission, no container.
                await self.open_target(name)
            else:
                await self.ensure_connected(name)

    async def _detach_slurm_target(self, name: str) -> None:
        """Close the route for a Slurm target without signalling the scheduler."""
        async with self._lock(name):
            await self._stop_locked(name)
        await self.backend.disconnect(name)
        await self._disconnect_jobs_for_target(name)
        await self.sessions.close_for_target(name, reason="detached")

    async def _disconnect_jobs_for_target(self, name: str) -> None:
        """Drop job-keyed connections for one target; never touches another."""
        if self.job_store is None:
            return
        try:
            records = list(self.job_store.scan())
        except Exception:  # noqa: BLE001 - teardown must not be blocked by disk
            return
        for record in records:
            if record.target == name:
                with contextlib.suppress(Exception):
                    await self.backend.disconnect_job(record.job_id)

    # -- background job monitor -------------------------------------------
    def _start_job_monitor(self) -> None:
        if self._job_monitor_task is not None and not self._job_monitor_task.done():
            return
        self._job_monitor_task = asyncio.create_task(self._job_monitor_loop())

    async def _job_monitor_loop(self) -> None:
        try:
            while not self._stopping:
                await asyncio.sleep(self.job_poll_interval)
                with contextlib.suppress(Exception):
                    await self.poll_jobs_once()
        except asyncio.CancelledError:
            raise

    async def poll_jobs_once(self) -> None:
        """One non-blocking scheduler pass over every non-terminal job.

        There is no computeMCP queue deadline: a job may sit in ``pending`` as
        long as Slurm keeps it.  A pending -> starting -> ready transition is
        surfaced here, never by blocking a request.
        """
        manager = self._ensure_jobs()
        for record in manager.store.scan():
            if record.state == jobcore.TERMINAL or not record.slurm_job_id:
                continue
            await self._poll_one(record.job_id)

    async def _poll_one(self, job_id: str) -> "jobcore.JobRecord":
        manager = self._ensure_jobs()
        updated = await manager.poll(job_id)
        if updated.state == jobcore.STARTING and self._provisioner_available():
            try:
                payload = await _maybe_await(
                    self._provisioner_for_target(updated.target).attach(updated)
                )
            except Exception as exc:  # noqa: BLE001 - keep polling others
                log.debug("job %s: attach failed: %s", job_id, exc)
                return updated
            if payload:
                try:
                    updated = await manager.mark_ready(
                        job_id,
                        node=str(payload.get("node") or "unknown"),
                        container_endpoint=payload.get("container_endpoint"),
                        relay_endpoint=payload.get("relay_endpoint"),
                        relay_pid=payload.get("relay_pid"),
                        generation=payload.get("generation"),
                    )
                    # Publish the freshly usable endpoint to the mux registry
                    # so the helper can OPEN this job without waiting for the
                    # next request.
                    self._write_route_registry(updated.target)
                except jobcore.JobError as exc:
                    log.debug("job %s: readiness was not applied: %s", job_id, exc)
        if updated.state == jobcore.TERMINAL:
            await self._cleanup_job_resources(job_id, reason="job terminal")
        return updated

    # -- allocation resolution --------------------------------------------
    @staticmethod
    def _allocation_configured(target: TargetConfig) -> bool:
        return (
            target.node is not None
            or target.allocation is not None
            or target.slurm is not None
        )

    @staticmethod
    def _env_configured(target: TargetConfig) -> bool:
        return (
            target.node is not None
            or target.allocation is not None
            or target.slurm is not None
            or target.container is not None
            or target.bundle is not None
        )

    def _resolve_allocation(
        self, target: TargetConfig, overrides: dict[str, Any] | None
    ) -> tuple[Any, dict[str, str]]:
        """Resolve plan, per-stage args and the provision environment.

        Overrides resolve before mapping: they are validated by
        :func:`compute_plan`, which also re-checks mapping/manual conflicts.
        An override on a target with no ``node``/``allocation``/``slurm`` block
        is refused, so a missing mapping can never be mistaken for successful
        enforcement.  Raises :class:`ConfigError` (HTTP 400 at the edge).
        """
        if overrides and not self._allocation_configured(target):
            raise ConfigError(
                f"target {target.name!r} has no Slurm allocation configuration; "
                "--set overrides cannot be applied (configure [node], "
                "[allocation] or [slurm] first)"
            )
        validate_conflicts(target)
        plan = compute_plan(target, overrides)
        sbatch, srun = render_args(target, plan)
        provision_env = build_provision_env(target, plan, sbatch, srun)
        return plan, provision_env

    def _target_provision_env(self, target: TargetConfig) -> dict[str, str]:
        """The ``COMPUTEMCP_*`` contract for a target, best-effort.

        Shared by the connect and close paths so a bundle target gets the same
        environment in both and cannot drift.  For a target whose allocation
        cannot be resolved (for example a bundle with no node/allocation block,
        or a plan error) a minimal dict with the system and container fields is
        returned rather than raising: close/connect are advisory and must not
        crash teardown.
        """
        try:
            _, provision_env = self._resolve_allocation(target, None)
            return provision_env
        except (ConfigError, ValueError):
            pass
        env: dict[str, str] = {"COMPUTEMCP_SYSTEM": target.name}
        env.update(_container_env_fields(target))
        return env

    # -- state machine -----------------------------------------------------
    async def ensure_connected(self, name: str) -> TargetRuntime:
        target = self._target(name)
        async with self._lock(name):
            runtime = self.runtimes[name]
            if runtime.state == "connected":
                # A live target is by definition no longer awaiting a factor, so
                # clear a flag left over from a factor-less refresh or a previous
                # loss.  Otherwise the stale flag could mask a real dial failure.
                if target.transport.kind == "direct":
                    runtime.awaiting_factor = False
                    return runtime
                if runtime.tunnel and runtime.tunnel.is_alive():
                    runtime.awaiting_factor = False
                    return runtime
                self._mark_lost(runtime, "tunnel connection closed")
            # Recovery/automatic reconnect must retain the resolved settings
            # instead of silently reverting to the configured defaults.
            return await self._connect_locked(
                target, runtime, overrides=(runtime.resolved_overrides or None)
            )

    async def _connect_locked(
        self,
        target: TargetConfig,
        runtime: TargetRuntime,
        factor: str | None = None,
        *,
        overrides: dict[str, Any] | None = None,
    ) -> TargetRuntime:
        if target.interactive_auth and factor is None:
            # Fail closed before dialing.  Without a factor the route login is
            # guaranteed to fail, and every such attempt counts as a failed
            # authentication at the site.  Only connect_target/refresh_target
            # may supply the per-request factor.
            runtime.awaiting_factor = True
            runtime.last_error = (
                "interactive authentication required; run "
                f"'target-connect --2fa {target.name}'"
            )
            raise InteractiveAuthRequired(target.name)
        # Invalidate any container dial that already passed ensure_connected.
        runtime.route_epoch += 1
        runtime.state = "connecting"
        runtime.last_error = None
        provision_env: dict[str, str] | None = None
        plan = None
        if self._env_configured(target) or overrides:
            try:
                plan, provision_env = self._resolve_allocation(target, overrides)
            except (ConfigError, ValueError) as exc:
                # A bad override must not wedge the runtime in "connecting":
                # record the failure and re-raise (the HTTP layer maps it to 400).
                runtime.state = "failed"
                runtime.last_error = str(exc)
                raise
        connect_kwargs: dict[str, Any] = {"factor": factor}
        if provision_env is not None:
            # Only allocation/container targets carry the environment contract;
            # every other target keeps the exact previous call shape.
            connect_kwargs["provision_env"] = provision_env
        try:
            tunnel = await self.tunnels.connect(
                target,
                on_route=lambda route, exc: log.debug("route %s failed: %s", route, exc),
                **connect_kwargs,
            )
        except TunnelError as exc:
            runtime.state = "failed"
            runtime.last_error = str(exc)
            runtime.active_route = None
            runtime.local_port = None
            raise
        runtime.tunnel = tunnel
        runtime.active_route = tunnel.route
        runtime.local_port = tunnel.local_port
        # Endpoint lifecycle: after a successful provision (or a static direct
        # transport) the freshly resolved endpoint is authoritative for this
        # connect.  A re-provision on refresh/recovery re-resolves it here, so
        # ``runtime.provisioned_endpoint`` always mirrors the latest successful
        # provision rather than a stale port from a previous allocation.  The
        # cached container connection is keyed to the endpoint and dropped by
        # the backend when the port changes (see SSHBackend.connection).
        if tunnel.provisioned_endpoint is not None:
            host, port = tunnel.provisioned_endpoint
            runtime.provisioned_endpoint = f"{host}:{port}"
        else:
            runtime.provisioned_endpoint = None
        if plan is not None:
            runtime.resolved_plan = plan_summary(target, plan)
            runtime.resolved_overrides = dict(plan.overrides)
        else:
            runtime.resolved_plan = None
            runtime.resolved_overrides = {}
        runtime.connected_since = datetime.now(timezone.utc)
        runtime.needs_refresh = False
        runtime.backoff = 0.0
        runtime.state = "connected"
        # A successful connect resolves any outstanding factor: the invariant
        # after this point is ``state == "connected"`` and
        # ``awaiting_factor is False`` together.
        runtime.awaiting_factor = False
        return runtime

    def _release_tunnel(self, tunnel: Any) -> None:
        """Release a reserved loopback port for a legacy forwarding tunnel.

        A :class:`~compute_mcp.tunnel.JobRouteTunnel` owns no local forward, so
        there is no port to release; only a ``RouteTunnel`` does.
        """
        if hasattr(tunnel, "local_port"):
            self.tunnels.release(tunnel)

    def _mark_lost(self, runtime: TargetRuntime, reason: str) -> None:
        # Invalidate any container dial that captured the lost route.
        runtime.route_epoch += 1
        if runtime.tunnel is not None:
            self._release_tunnel(runtime.tunnel)
            runtime.tunnel = None
        runtime.state = "disconnected"
        runtime.active_route = None
        runtime.local_port = None
        # Drop the endpoint so a recovery that skips re-provision (static
        # transport) does not dial a no-longer-published port; a re-provision
        # re-resolves it in ``_connect_locked``.
        runtime.provisioned_endpoint = None
        runtime.last_error = reason
        runtime.connected_since = None

    async def connect_target(
        self,
        name: str,
        factor: str | None = None,
        *,
        overrides: dict[str, Any] | None = None,
        client: Client | None = None,
    ) -> dict:
        """Connect a target, preserving the legacy non-Slurm behaviour.

        Compatibility change for durable Slurm targets: ``target-connect`` now
        means "open the route + get-or-create the shared job".  It can return
        ``pending`` and makes no promise that container SSH is immediately
        available.  Automatic route opening likewise no longer provisions a
        shared container; that is what ``target-shared``/``target-connect`` do.
        """
        target = self._target(name)
        if self._is_multijob(target):
            # Slurm: connect == open the route + get-or-create the shared job.
            # The response may be ``pending``; it does not promise container SSH.
            result = await self.open_target(name, factor=factor)
            shared = await self.target_shared(
                name,
                client or self._operator_client(),
                factor=factor,
                overrides=overrides,
            )
            result.update(shared)
            result["state"] = self.runtimes[name].state
            result["route_state"] = result["state"]
            result["job_state"] = result["job"]["state"]
            return result
        async with self._lock(name):
            runtime = self.runtimes[name]
            if runtime.state == "connected":
                if overrides:
                    # A connect with overrides on a live target must not
                    # silently tear down or silently accept a different
                    # allocation: compare the effective settings and require an
                    # explicit refresh when they differ.
                    plan, _ = self._resolve_allocation(target, overrides)
                    new_sig = plan_signature(plan_summary(target, plan))
                    if new_sig != plan_signature(runtime.resolved_plan):
                        result = runtime.public(
                            self.sessions.count_for_target(name)
                        )
                        result["needs_refresh"] = True
                        result["warning"] = (
                            "requested allocation settings differ from the "
                            f"active allocation for target {name!r}; run "
                            f"'target-refresh {name} --set ...' to apply them"
                        )
                        return result
                    # Effective settings are equal: no-op, allocation preserved.
                return runtime.public(self.sessions.count_for_target(name))
            warning = self._factor_warning(target, factor)
            if target.interactive_auth and factor is None:
                return self._interactive_skip(runtime)
            if warning is not None:
                factor = None  # key-only downstream
            await self._connect_locked(
                target, runtime, factor=factor, overrides=overrides
            )
            result = runtime.public(self.sessions.count_for_target(name))
        if warning:
            result["warning"] = warning
        return result

    async def refresh_target(
        self,
        name: str,
        factor: str | None = None,
        *,
        overrides: dict[str, Any] | None = None,
    ) -> dict:
        target = self._target(name)
        if self._is_multijob(target):
            # Route-only refresh: close the login transport and reopen it.  It
            # never scancels, so private and shared jobs survive.  Replacing a
            # shared job is a separate explicit operator action.
            async with self._lock(name):
                runtime = self.runtimes[name]
                runtime.route_epoch += 1
                if runtime.tunnel is not None:
                    with contextlib.suppress(Exception):
                        await runtime.tunnel.stop()
                    runtime.tunnel = None
                runtime.state = "disconnected"
                await self._open_slurm_route_locked(target, runtime, factor)
            result = self.public_status(name)
            shared = self._active_shared_record(name)
            result["shared_job_id"] = shared.job_id if shared else None
            return result
        async with self._lock(name):
            runtime = self.runtimes[name]
            # Validate/resolve first.  A bad override must raise for a healthy
            # target without touching the tunnel, state or allocation.
            effective = (
                overrides
                if overrides is not None
                else (runtime.resolved_overrides or None)
            )
            plan = None
            provision_env: dict[str, str] | None = None
            if effective or self._env_configured(target):
                plan, provision_env = self._resolve_allocation(target, effective)
            warning = self._factor_warning(target, factor)
            if warning is not None:
                factor = None  # key-only downstream

            direct = target.transport.kind == "direct"
            live = self._target_connected_live(target, runtime)
            retained = (
                live
                and not direct
                and runtime.tunnel is not None
                and runtime.tunnel.connection is not None
            )

            if retained:
                # A usable authenticated login connection is reused: no dial
                # and no second factor.  A supplied factor stays unused.
                await self._refresh_live_tunnel(
                    name, target, runtime, plan, effective, provision_env
                )
            else:
                # Missing/closed route (or direct transport, which has no
                # retained login connection): preserve the old behaviour.  An
                # interactive target without a factor is refused before any
                # teardown or close command.
                if target.interactive_auth and factor is None:
                    return self._interactive_skip(runtime)
                # Release the old allocation before provisioning a new one.
                # The route connection (if any) is still alive here.
                runtime.route_epoch += 1
                await self._run_close_command(name)
                await self._stop_locked(name)
                # Drop the cached container connection so the next exec/session
                # dials the freshly provisioned endpoint.
                await self.backend.disconnect(name)
                await self.sessions.close_for_target(name, reason="refresh")
                await self._connect_locked(
                    target, runtime, factor=factor, overrides=effective
                )
            result = runtime.public(self.sessions.count_for_target(name))
        if warning:
            result["warning"] = warning
        return result

    async def _refresh_live_tunnel(
        self,
        name: str,
        target: TargetConfig,
        runtime: TargetRuntime,
        plan,
        effective: dict[str, Any] | None,
        provision_env: dict[str, str] | None,
    ) -> None:
        """Re-provision over the retained login connection and adopt it.

        The old forward is fenced and every accepted direct-tcpip channel is
        closed and awaited before the close/provision session is opened, so a
        login node with ``MaxSessions=1`` cannot reject the refresh.  No factor
        is consumed and no second allocation is started.
        """
        old_tunnel = runtime.tunnel
        assert old_tunnel is not None
        route = runtime.active_route or old_tunnel.route
        if route is None:
            raise TunnelError(
                f"target {name!r}: no active route to refresh"
            )
        # The retained listener is bound to the old tunnel's port.  Fence it
        # before rebinding; reusing that exact port keeps clients and the
        # reservation consistent.
        local_port = self.tunnels.reserve_local_port(old_tunnel.local_port)
        runtime.state = "refreshing"
        runtime.route_epoch += 1
        stage = "fence"
        new_tunnel: RouteTunnel | None = None
        try:
            await old_tunnel.fence_forward()
            stage = "sessions"
            await self.sessions.close_for_target(name, reason="refresh")
            await self.backend.disconnect(name)
            stage = "drain"
            await old_tunnel.drain_forwarded_channels()
            stage = "close"
            await self._run_close_command(name)
            stage = "provision"
            try:
                new_tunnel = await self.tunnels.reprovision_on_route(
                    target,
                    route,
                    local_port,
                    target.transport,
                    old_tunnel.connection,
                    provision_env,
                )
            except ForwardError:
                stage = "forward"
                raise
            stage = "adopt"
            runtime.tunnel = new_tunnel
            runtime.active_route = new_tunnel.route
            runtime.local_port = new_tunnel.local_port
            if new_tunnel.provisioned_endpoint is not None:
                host, port = new_tunnel.provisioned_endpoint
                runtime.provisioned_endpoint = f"{host}:{port}"
            else:
                runtime.provisioned_endpoint = None
            if plan is not None:
                runtime.resolved_plan = plan_summary(target, plan)
                runtime.resolved_overrides = dict(plan.overrides)
            else:
                runtime.resolved_plan = None
                runtime.resolved_overrides = (
                    dict(effective) if effective is not None else {}
                )
            runtime.connected_since = datetime.now(timezone.utc)
            runtime.needs_refresh = False
            runtime.backoff = 0.0
            runtime.last_error = None
            runtime.state = "connected"
            runtime.awaiting_factor = False
            runtime.route_epoch += 1
        except Exception as exc:  # noqa: BLE001 - classified and re-raised below
            message = f"refresh {stage} stage failed: {exc}"
            await self._fail_live_refresh(
                name, target, runtime, old_tunnel, new_tunnel, local_port, message
            )
            raise TunnelError(message) from exc

    async def _fail_live_refresh(
        self,
        name: str,
        target: TargetConfig,
        runtime: TargetRuntime,
        old_tunnel: RouteTunnel,
        new_tunnel: RouteTunnel | None,
        local_port: int,
        message: str,
    ) -> None:
        """Leave an accurate, recoverable failed state after a live refresh."""
        log.warning("target %s: %s", name, message)
        runtime.state = "failed"
        runtime.last_error = message
        runtime.active_route = None
        runtime.local_port = None
        runtime.provisioned_endpoint = None
        runtime.connected_since = None
        runtime.awaiting_factor = target.interactive_auth
        # The retained connection may already be dead.  Close only what this
        # refresh still owns, then release the port.  A later explicit
        # target-refresh (with a factor for an interactive target) may retry.
        if new_tunnel is not None and new_tunnel is not old_tunnel:
            with contextlib.suppress(Exception):
                await new_tunnel.stop()
        with contextlib.suppress(Exception):
            await old_tunnel.stop()
        runtime.tunnel = None
        self.tunnels.release_local_port(local_port)

    def preview_target(
        self, name: str, overrides: dict[str, Any] | None = None
    ) -> dict:
        """Pure allocation preview: no connection, no allocation, no state change.

        Returns the resolved plan summary, the per-stage rendered args and the
        provision environment contract.  For a connected target, a preview whose
        effective settings differ from the retained ones reports
        ``needs_refresh``; a plain connect and a preview never disturb the live
        allocation.
        """
        target = self._target(name)
        runtime = self.runtimes[name]
        plan, provision_env = self._resolve_allocation(target, overrides)
        sbatch, srun = render_args(target, plan)
        summary = plan_summary(target, plan)
        result = {
            "target": name,
            "connected": runtime.state == "connected",
            "planned": summary,
            "sbatch_args": list(sbatch),
            "srun_args": list(srun),
            "provision_env": provision_env,
            "would_emit": {
                "sbatch": list(sbatch),
                "srun": list(srun),
                "not_emitted": list(summary.get("not_emitted", [])),
            },
        }
        if runtime.state == "connected":
            new_sig = plan_signature(summary)
            if new_sig != plan_signature(runtime.resolved_plan):
                result["needs_refresh"] = True
                result["warning"] = (
                    "previewed allocation settings differ from the active "
                    f"allocation for target {name!r}; run 'target-refresh "
                    f"{name} --set ...' to apply them"
                )
        return result

    def _factor_warning(self, target: TargetConfig, factor: str | None) -> str | None:
        """Return a user-facing warning for a mismatched factor, never the secret."""
        if not target.interactive_auth and factor is not None:
            message = (
                "second factor provided but target does not use "
                "interactive_auth; ignoring"
            )
            log.warning("target %s: %s", target.name, message)
            return message
        return None

    def _interactive_skip(self, runtime: TargetRuntime) -> dict:
        """Refuse a connect/refresh needing a factor without tearing anything down."""
        message = (
            "target requires interactive authentication; run "
            f"target-connect --2fa {runtime.name}"
        )
        log.warning("target %s: %s", runtime.name, message)
        runtime.awaiting_factor = True
        result = runtime.public(self.sessions.count_for_target(runtime.name))
        result["warning"] = message
        return result

    async def stop_target(self, name: str) -> dict:
        target = self._target(name)
        if self._is_multijob(target):
            # Slurm teardown detaches the route; cancellation is a job operation.
            async with self._lock(name):
                await self._stop_locked(name)
            await self.backend.disconnect(name)
            await self._disconnect_jobs_for_target(name)
            await self.sessions.close_for_target(name, reason="target stopped")
            return self.public_status(name)
        async with self._lock(name):
            # Explicit stop and gateway shutdown both go through here; release
            # the remote allocation while the route connection is still alive.
            await self._run_close_command(name)
            await self._stop_locked(name)
        await self.backend.disconnect(name)
        await self.sessions.close_for_target(name, reason="target stopped")
        return self.runtimes[name].public(self.sessions.count_for_target(name))

    async def _run_close_command(self, name: str) -> None:
        """Run the target's trusted ``close_command`` on the live route.

        Advisory: a missing connection, a non-zero exit or a timeout is logged
        and swallowed so teardown still proceeds.  Deliberately NOT part of
        ``_stop_locked``: config-reload removal uses ``_stop_locked`` directly
        and must not release the allocation.
        """
        target = self.config.targets.get(name)
        if target is None:
            return
        # A bundle target without an explicit close_command releases the
        # allocation through the deployed helper's `stop` action, so do not
        # skip it merely because close_command is empty.
        if not target.close_command and target.bundle is None:
            return
        runtime = self.runtimes[name]
        connection = runtime.tunnel.connection if runtime.tunnel else None
        route = runtime.active_route or (
            target.transport.ssh_targets[0] if target.transport.ssh_targets else "direct"
        )
        # The derived bundle `stop` needs the same COMPUTEMCP_* contract as the
        # provision path; build it from the target (best-effort) so teardown
        # never crashes on a missing allocation.
        provision_env = (
            self._target_provision_env(target) if target.bundle is not None else None
        )
        close_kwargs: dict[str, Any] = {}
        if provision_env is not None:
            close_kwargs["provision_env"] = provision_env

        if connection is not None:
            await self._run_close_on_connection(
                name, target, route, connection, close_kwargs
            )
            return

        # No live tunnel: at shutdown (or after a lost route) the route
        # connection is gone, but the login node is the only place that can
        # scancel.  Open a short-lived route connection and run the close over
        # it, bounded so a dead login node cannot stall shutdown.  Direct
        # transport has no separate login node to dial, so keep the old no-op.
        # An interactive_auth target is deliberately NOT dialed here: the key-only
        # route connection cannot supply the second factor, so the dial would only
        # produce a guaranteed login failure that can trip fail2ban / account
        # lockout policies.  The config already tells us a factor is required, so
        # we skip the factor-less reconnect and log the manual scancel.
        if target.transport.kind == "direct":
            log.warning(
                "target %s: close_command skipped, no live route connection",
                name,
            )
            return
        if target.interactive_auth:
            log.warning(
                "target %s: close_command skipped, interactive_auth requires a "
                "second factor and the route connection is gone; the allocation "
                "may need a manual scancel",
                name,
            )
            return
        short_route = None
        try:
            short_route = await self.tunnels.open_route_connection(
                target, route, connection_timeout=_CLOSE_ROUTE_TIMEOUT
            )
        except Exception as exc:  # noqa: BLE001 - advisory, never fatal
            log.warning(
                "target %s: close_command skipped, could not open a route "
                "connection: %s",
                name,
                exc,
            )
            return
        try:
            await self._run_close_on_connection(
                name, target, route, short_route, close_kwargs
            )
        finally:
            await self._close_route_connection(short_route)

    async def _run_close_on_connection(
        self,
        name: str,
        target: TargetConfig,
        route: str,
        connection: Any,
        close_kwargs: dict[str, Any],
    ) -> None:
        """Run the close command over ``connection``; advisory, never raises."""
        try:
            await self.tunnels.run_close_command(
                target, route, connection=connection, **close_kwargs
            )
        except Exception as exc:  # noqa: BLE001 - advisory, never fatal
            log.warning("target %s: close_command failed: %s", name, exc)

    async def _close_route_connection(self, connection: Any) -> None:
        """Close a short-lived route connection; failures are swallowed."""
        with contextlib.suppress(Exception):
            connection.close()
        with contextlib.suppress(Exception):
            await connection.wait_closed()

    async def _stop_locked(self, name: str) -> None:
        runtime = self.runtimes[name]
        runtime.route_epoch += 1
        if runtime.recovery_task is not None:
            runtime.recovery_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runtime.recovery_task
            runtime.recovery_task = None
        if runtime.tunnel is not None:
            await runtime.tunnel.stop()
            self._release_tunnel(runtime.tunnel)
        runtime.tunnel = None
        runtime.state = "disconnected"
        runtime.active_route = None
        runtime.local_port = None
        # The allocation is gone, so the endpoint provisioned for it is stale.
        # Clear it here (the one place every teardown/refresh/removal passes
        # through); a reconnect re-resolves it from a fresh provision in
        # ``_connect_locked``.  This is what keeps a refresh from dialing the
        # previous container port.
        runtime.provisioned_endpoint = None
        runtime.connected_since = None
        runtime.awaiting_factor = False

    # -- recovery ----------------------------------------------------------
    def _schedule_recovery(self, name: str, runtime: TargetRuntime) -> None:
        """Start the reconnect loop once, storing the handle for cancellation."""
        existing = runtime.recovery_task
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(self._reconnect_with_backoff(name))
        runtime.recovery_task = task

        def _clear(_task: asyncio.Task, rt: TargetRuntime = runtime) -> None:
            # Only clear our own handle; a newer loop may have replaced it.
            if rt.recovery_task is _task:
                rt.recovery_task = None

        task.add_done_callback(_clear)

    async def _watch_tunnels(self) -> None:
        try:
            while not self._stopping:
                await asyncio.sleep(RECOVERY_POLL_SECONDS)
                for name, runtime in list(self.runtimes.items()):
                    target = self.config.targets.get(name)
                    if target is None or target.transport.kind == "direct":
                        continue
                    if runtime.state == "connected" and runtime.tunnel is not None:
                        if not runtime.tunnel.is_alive():
                            await self._handle_loss(name, "tunnel connection closed")
                    elif (
                        runtime.state in ("disconnected", "failed")
                        and runtime.needs_refresh
                        and not target.interactive_auth
                    ):
                        self._schedule_recovery(name, runtime)
        except asyncio.CancelledError:
            raise

    async def _handle_loss(self, name: str, reason: str) -> None:
        async with self._lock(name):
            runtime = self.runtimes[name]
            if runtime.state != "connected":
                return
            target = self.config.targets.get(name)
            log.warning("target %s lost: %s", name, reason)
            self._mark_lost(runtime, reason)
            if target is not None and target.interactive_auth:
                # Fail closed: reconnecting needs a second factor that only a
                # new target-connect request can supply.  Do not auto-reconnect.
                runtime.awaiting_factor = True
                runtime.last_error = (
                    "connection lost; reconnect requires a second factor, run "
                    "target-connect --2fa"
                )
                runtime.needs_refresh = False
                reconnect = False
            else:
                runtime.needs_refresh = True
                reconnect = True
        await self.backend.disconnect(name)
        target = self.config.targets.get(name)
        if target is not None and self._is_multijob(target):
            await self._disconnect_jobs_for_target(name)
        await self.sessions.close_for_target(name, reason="tunnel lost")
        if reconnect:
            self._schedule_recovery(name, runtime)

    async def _reconnect_with_backoff(self, name: str) -> None:
        target = self.config.targets.get(name)
        runtime = self.runtimes.get(name)
        if target is None or runtime is None:
            return
        runtime.backoff = max(runtime.backoff, target.connect_backoff_initial)
        try:
            while not self._stopping:
                await asyncio.sleep(runtime.backoff)
                # The target may have been stopped/removed or replaced while we
                # slept; never dial for an obsolete runtime or a stopping gateway.
                if self._stopping or self.runtimes.get(name) is not runtime:
                    return
                target = self.config.targets.get(name)
                if target is None:
                    return
                if runtime.state == "connected":
                    return
                try:
                    await self.ensure_connected(name)
                    return
                except (TunnelError, SSHError, HostKeyError) as exc:
                    runtime.last_error = str(exc)
                    runtime.backoff = min(runtime.backoff * 2, target.connect_backoff_max)
        except asyncio.CancelledError:
            raise

    # -- dynamic provisioning moved into tunnel.connect -------------------
    # -- interactive authentication --------------------------------------
    async def _prompt_line(self, prompt: str, echo: bool) -> str | None:
        """Read a line from the console, off the event loop.

        A hidden prompt (``echo=False``) is used for passwords/OTP so they are
        not shown on screen.  An empty answer cancels the attempt.
        """
        loop = asyncio.get_running_loop()
        try:
            if echo:
                answer = await loop.run_in_executor(None, input, prompt)
            else:
                answer = await loop.run_in_executor(None, getpass.getpass, prompt)
        except (EOFError, KeyboardInterrupt):
            return None
        answer = (answer or "").strip()
        return answer or None

    async def _prompter(self, target_name: str, prompt: str, echo: bool):
        """Collect an authentication secret from the operator console.

        Returns ``None`` (so AsyncSSH tries another method) when the gateway is
        not running an interactive console.  Serialized so two targets never
        interleave prompts.
        """
        if not self._interactive:
            log.warning(
                "target %s requested interactive authentication while the "
                "gateway is not interactive; authentication cannot proceed",
                target_name,
            )
            return None
        label = prompt if prompt.endswith(" ") else prompt + " "
        async with self._prompt_lock:
            return await self._prompt_line(f"[{target_name}] {label}", echo)

    MAX_AUTH_PROMPTS = 3

    def _make_prompter(self, target_name: str):
        attempts = 0

        async def prompter(prompt: str, echo: bool):
            nonlocal attempts
            attempts += 1
            if attempts > self.MAX_AUTH_PROMPTS:
                log.warning(
                    "target %s: giving up after %d interactive auth attempt(s)",
                    target_name,
                    attempts - 1,
                )
                return None
            return await self._prompter(target_name, prompt, echo)

        return prompter

    # -- connection providers for exec/sessions --------------------------
    def _target_connected_live(
        self, target: TargetConfig, runtime: TargetRuntime
    ) -> bool:
        """Whether the target is successfully connected with a usable route.

        Mirrors the early-return liveness test in ``ensure_connected``: a direct
        transport is live as soon as it is connected; a tunnel target also needs
        an alive tunnel.
        """
        if runtime.state != "connected":
            return False
        if target.transport.kind == "direct":
            return True
        return runtime.tunnel is not None and runtime.tunnel.is_alive()

    def _factor_outstanding(
        self, target: TargetConfig, runtime: TargetRuntime
    ) -> bool:
        """Whether a second factor is genuinely still outstanding.

        ``awaiting_factor`` is authoritative only while the target is not
        successfully connected with a usable route.  A live target is treated as
        resolved even if a stale flag remains, so a real container-dial failure
        surfaces as-is instead of being masked as a 2FA-required 503.
        """
        return (
            target.interactive_auth
            and runtime.awaiting_factor
            and not self._target_connected_live(target, runtime)
        )

    async def _open_container_conn(self, name: str, force_dedicated: bool = False):
        """Open the container SSH connection for a target.

        The container hop is key-only: an interactive (2FA) target must already
        have been connected by an explicit ``--2fa`` request.  Otherwise this
        raises :class:`InteractiveAuthRequired` with the actionable command.
        """
        target = self._target(name)
        try:
            await self.ensure_connected(name)
        except (TunnelError, SSHError, HostKeyError):
            # A real tunnel/host-key/dial failure on an interactive target must
            # surface as-is (502).  Map to 503 only when this target is known to
            # still be awaiting a factor.
            if self._factor_outstanding(target, self.runtimes[name]):
                raise InteractiveAuthRequired(name) from None
            raise
        runtime = self.runtimes[name]
        # Pre-dial gate: while the target is not connected at all a factor is
        # genuinely outstanding, so refuse before dialing.  A target that claims
        # to be connected is dialed, and any failure is classified by the final
        # gate below (which also carries the dial detail).
        if (
            target.interactive_auth
            and runtime.awaiting_factor
            and runtime.state != "connected"
        ):
            raise InteractiveAuthRequired(name)
        dedicated = force_dedicated or target.connect_mode == "dedicated"

        # A bundle-backed recovery command needs the COMPUTEMCP_* contract too,
        # so a stopped container can be brought back up.  Best-effort: never
        # fail the connect path building the advisory environment.
        connect_kwargs: dict[str, Any] = {}
        if target.bundle is not None:
            connect_kwargs["provision_env"] = self._target_provision_env(target)

        async def attempt(host: str, port: int):
            if dedicated:
                return await self.backend.open_connection(target, host, port)
            return await self.backend.connection(target, host, port)

        # "always": run the trusted recovery command before every open.  It must
        # be a no-op when the container already runs.
        if target.connect_command and target.connect_command_mode == "always":
            await self._run_connect_command_locked(
                name, target, runtime, connect_kwargs
            )

        try:
            conn = await self._dial_container_guarded(
                name, target, dedicated, attempt
            )
            return target, conn
        except _RouteChangedDuringDial:
            raise
        except SSHError as first:
            # The tunnel is up but the container is unreachable (e.g. it is
            # stopped).  In "on_failure" mode run the recovery command on the
            # remote host and retry once.
            if target.connect_command and target.connect_command_mode == "on_failure":
                log.info(
                    "target %s unreachable (%s); running connect_command",
                    name, first,
                )
                await self._run_connect_command_locked(
                    name, target, runtime, connect_kwargs
                )
                # Drop any cached (dead) connection before retrying.
                await self.backend.disconnect(name)
                try:
                    conn = await self._dial_container_guarded(
                        name, target, dedicated, attempt
                    )
                    return target, conn
                except _RouteChangedDuringDial:
                    raise
                except SSHError:
                    pass
            # Only map to 503 while a factor is genuinely outstanding.  A real
            # container-dial failure (stopped container, refused port, ...) on
            # an interactive target must surface as-is (502) with its message.
            if self._factor_outstanding(target, runtime):
                raise InteractiveAuthRequired(name, detail=str(first)) from None
            raise first

    def _route_current(
        self,
        runtime: TargetRuntime,
        epoch: int,
        host: str,
        port: int,
    ) -> bool:
        """Whether a dial's captured route is still the live one."""
        if runtime.route_epoch != epoch or runtime.state == "refreshing":
            return False
        try:
            return self._endpoint(runtime) == (host, port)
        except TunnelError:
            return False

    async def _discard_dialed_conn(
        self, name: str, conn, dedicated: bool
    ) -> None:
        """Drop/close a connection dialed against a route a refresh replaced."""
        if not dedicated:
            # Only removes it from the cache when this exact connection is
            # cached; a dedicated connection is not tracked there.
            await self.backend.discard_connection(conn)
        await self.backend.close_connection(conn)

    async def _dial_container_guarded(
        self,
        name: str,
        target: TargetConfig,
        dedicated: bool,
        attempt,
    ):
        """Dial once, then re-check the route epoch/state.

        A request can pass ``ensure_connected`` while a refresh is about to
        fence the old forward.  The epoch advances before disruption and again
        after adoption, so a connection opened on the old forward/stale
        endpoint is discarded.  One retry waits for the refresh lock, then
        dials the new route; if the route is still moving (or the refresh
        failed) the request fails rather than reopening a second allocation.
        """
        runtime = self.runtimes[name]
        epoch = runtime.route_epoch
        host, port = self._endpoint(runtime)
        conn = await attempt(host, port)
        if self._route_current(runtime, epoch, host, port):
            return conn
        await self._discard_dialed_conn(name, conn, dedicated)

        async with self._lock(name):
            current = self.runtimes[name]
            if not self._target_connected_live(target, current):
                raise _RouteChangedDuringDial(
                    f"target {name!r} route changed during container "
                    "connection attempt"
                )
            epoch = current.route_epoch
            host, port = self._endpoint(current)
        conn = await attempt(host, port)
        if self._route_current(self.runtimes[name], epoch, host, port):
            return conn
        await self._discard_dialed_conn(name, conn, dedicated)
        raise _RouteChangedDuringDial(
            f"target {name!r} route changed during container connection attempt"
        )

    async def _run_connect_command_locked(
        self,
        name: str,
        target: TargetConfig,
        runtime: TargetRuntime,
        connect_kwargs: dict[str, Any],
    ) -> None:
        """Run ``connect_command`` on the route with the target lock held.

        The command uses a session channel on the route connection, and a login
        node can allow only one session channel per connection.  Serializing on
        the target prevents two concurrent container dials from opening two
        route sessions at once.
        """
        route = runtime.active_route or (
            target.transport.ssh_targets[0]
            if target.transport.ssh_targets
            else "direct"
        )
        async with self._lock(name):
            connection = runtime.tunnel.connection if runtime.tunnel else None
            await self.tunnels.run_connect_command(
                target, route, connection=connection, **connect_kwargs
            )

    async def _connection_provider(self, name: str):
        return await self._open_container_conn(name)

    async def _dedicated_connection_provider(self, name: str):
        return await self._open_container_conn(name, force_dedicated=True)

    # -- status ------------------------------------------------------------
    def public_status(self, name: str) -> dict:
        status = self.runtimes[name].public(self.sessions.count_for_target(name))
        target = self.config.targets.get(name)
        if target is not None:
            status["sharing"] = target.sharing
            status["node_info"] = list(target.node_info)
            status["agent"] = [
                {"agent": agent, "model": model} for agent, model in target.agent
            ]
            if self._is_multijob(target):
                status["job_transport"] = "mux"
                shared = self._active_shared_record(name)
                status["shared_job_id"] = shared.job_id if shared else None
        else:
            status["node_info"] = []
            status["agent"] = []
        return status

    def list_targets(self, client: Client) -> list[dict]:
        return [
            self.public_status(name)
            for name in self.config.targets
            if client.may_access(name)
        ]

    # -- reload ------------------------------------------------------------
    async def reload(self) -> dict:
        path = self.config.config_path
        if not path:
            raise ConfigError("no config path recorded; cannot reload")
        new_config = load_config(path, token_file=self.config.token_file)
        return await self.apply_config(new_config)

    async def apply_config(self, new_config: GatewayConfig) -> dict:
        """Atomically swap in a validated configuration.

        The new configuration is fully parsed and validated by the caller
        before this runs, so a malformed file never reaches here.  Unchanged
        connected targets keep their tunnels; removed targets are stopped;
        changed targets are marked for refresh (and refreshed if connected).

        Allocation conflicts are re-checked here, before any state changes, so
        a reload that would introduce an inconsistent mapping is rejected and
        the active configuration is retained.
        """
        validate_config_allocations(new_config)
        if self.transport_override:
            # Re-apply the test/debug transport override to the new config.
            saved = self.transport_override
            self.transport_override = None
            self.config = new_config
            self.transport_override = saved
            self._apply_transport_override()
        else:
            self.config = new_config
        self.auth = Authenticator(self.config.clients)
        if self.quota is not None:
            # Lowering a limit preserves existing jobs (the counts come from
            # durable records) and blocks only new submissions above the limit.
            self.quota.set_limits(
                {
                    client_id: dict(cfg.job_limits)
                    for client_id, cfg in self.config.clients.items()
                }
            )
        if any(
            self._is_multijob(target) for target in self.config.targets.values()
        ):
            self._ensure_jobs()
            self._start_job_monitor()

        report = {"added": [], "removed": [], "changed": [], "unchanged": []}

        for name in list(self.runtimes):
            if name not in self.config.targets:
                async with self._lock(name):
                    await self._stop_locked(name)
                await self.backend.disconnect(name)
                await self.sessions.close_for_target(name, reason="target removed")
                del self.runtimes[name]
                self._locks.pop(name, None)
                report["removed"].append(name)

        for name, target in self.config.targets.items():
            if name not in self.runtimes:
                self.runtimes[name] = TargetRuntime(name=name)
                report["added"].append(name)
                if target.auto_connect and not target.interactive_auth:
                    asyncio.create_task(self._safe_connect(name))
                elif target.auto_connect:
                    log.info(
                        "target %s: skipping auto-connect (interactive_auth "
                        "requires a second factor)",
                        name,
                    )
                continue
            runtime = self.runtimes[name]
            if self._target_changed(runtime, target):
                runtime.needs_refresh = True
                report["changed"].append(name)
                if runtime.state == "connected":
                    asyncio.create_task(self._safe_refresh(name))
            else:
                report["unchanged"].append(name)
        return report

    def _target_changed(self, runtime: TargetRuntime, target: TargetConfig) -> bool:
        # A connected target whose route is still valid is not changed unless
        # the configured ssh_targets no longer include the active route.
        if runtime.state == "connected" and target.transport.kind == "tunnel":
            return runtime.active_route not in target.transport.ssh_targets
        return False

    async def _safe_refresh(self, name: str) -> None:
        target = self.config.targets.get(name)
        if target is not None and target.interactive_auth:
            log.info(
                "target %s: skipping automatic refresh (interactive_auth requires "
                "a second factor)",
                name,
            )
            return
        with contextlib.suppress(TunnelError, SSHError, HostKeyError):
            await self.refresh_target(name)

    # -- route/job service (shared by CLI, HTTP and MCP) -------------------
    async def _open_route_transport(
        self,
        target: TargetConfig,
        route: str,
        factor: str | None,
        generation: int = 0,
    ) -> Any:
        if self.route_opener is not None:
            return await _maybe_await(self.route_opener(target, route, factor))
        # The durable multi-job path is the single-channel mux transport: one
        # authenticated route connection and exactly one persistent helper
        # channel.  Provisioning stays off the route here; per-job relays are
        # opened as mux streams addressed by job id, so route opening never
        # submits a job or starts a container.
        helper_argv = self._mux_helper_argv(target, route)
        return await self.tunnels.open_job_route(
            target,
            route,
            helper_argv=helper_argv,
            generation=generation,
            provision=False,
            factor=factor,
        )

    async def _open_slurm_route_locked(
        self,
        target: TargetConfig,
        runtime: TargetRuntime,
        factor: str | None,
    ) -> None:
        if runtime.state == "connected" and runtime.tunnel is not None:
            if runtime.tunnel.is_alive():
                return
            self._mark_lost(runtime, "route connection closed")
        if target.interactive_auth and factor is None:
            # Fail closed before dialing: a factorless login is a guaranteed
            # site-side authentication failure.
            runtime.awaiting_factor = True
            runtime.last_error = (
                "interactive authentication required; run "
                f"'target-connect --2fa {target.name}'"
            )
            raise InteractiveAuthRequired(target.name)
        if target.transport.kind == "direct":
            # A direct target has no separate login hop; the route is available
            # before any container exists.
            runtime.state = "connected"
            runtime.last_error = None
            runtime.awaiting_factor = False
            runtime.connected_since = datetime.now(timezone.utc)
            return
        route = runtime.active_route or (
            target.transport.ssh_targets[0]
            if target.transport.ssh_targets
            else target.name
        )
        runtime.state = "connecting"
        runtime.last_error = None
        runtime.route_epoch += 1
        try:
            tunnel = await self._open_route_transport(
                target, route, factor, generation=runtime.route_epoch
            )
        except Exception as exc:
            runtime.state = "failed"
            runtime.last_error = str(exc)
            raise
        runtime.tunnel = tunnel
        runtime.active_route = getattr(tunnel, "route", route)
        runtime.local_port = getattr(tunnel, "local_port", None)
        runtime.provisioned_endpoint = getattr(tunnel, "provisioned_endpoint", None)
        runtime.connected_since = datetime.now(timezone.utc)
        runtime.needs_refresh = False
        runtime.backoff = 0.0
        runtime.state = "connected"
        runtime.last_error = None
        runtime.awaiting_factor = False

    async def open_target(
        self,
        name: str,
        factor: str | None = None,
        *,
        overrides: dict[str, Any] | None = None,
    ) -> dict:
        """Open or reuse the login route and reconcile jobs; never submits.

        For a durable multi-job Slurm target this dials the route only (no
        ``sbatch``, no container start, no job record).  Re-running it is
        idempotent and also refreshes the shared-job view from disk.
        """
        target = self._target(name)
        if self._is_multijob(target):
            async with self._lock(name):
                await self._open_slurm_route_locked(
                    target, self.runtimes[name], factor
                )
            self._ensure_jobs()
            with contextlib.suppress(Exception):
                await self.jobs.ensure_reconciled()  # type: ignore[union-attr]
        return self.public_status(name)

    async def target_shared(
        self,
        name: str,
        client: Client,
        factor: str | None = None,
        *,
        overrides: dict[str, Any] | None = None,
        parameters: Mapping[str, Any] | None = None,
        root_mode: str | None = None,
        clean_home: str | None = None,
    ) -> dict:
        """Get-or-create the target's single shared job (no quota cost)."""
        target = self._target(name)
        if not self._is_multijob(target):
            raise web.HTTPBadRequest(text="target does not use Slurm jobs")
        await self.open_target(name, factor=factor)
        manager = self._ensure_jobs()
        params = dict(parameters or {})
        if overrides:
            params["set"] = dict(overrides)
        requested = _settings_signature(params, root_mode, None)

        async with self._shared_lock(name):
            existing = self._active_shared_record(name)
            if existing is not None:
                result = self.public_status(name)
                result["job"] = _public_job(existing)
                result["job_id"] = existing.job_id
                if _settings_signature(
                    existing.parameters, existing.root_mode, existing.home
                ) != requested:
                    result["settings_conflict"] = True
                    result["error"] = (
                        "shared job settings differ from the active shared job; "
                        "it is not replaced automatically"
                    )
                return result
            request = jobcore.JobRequest(
                client_id=client.client_id,
                target=name,
                request_id=f"shared:{name}:{uuid.uuid4().hex}",
                kind="shared",
                parameters=params,
                root_mode=root_mode,
                home=None,
            )
            record = await manager.submit(request)
            result = self.public_status(name)
            result["job"] = _public_job(record)
            result["job_id"] = record.job_id
            return result

    async def target_sbatch(
        self,
        name: str,
        client: Client,
        *,
        request_id: str | None = None,
        parameters: Mapping[str, Any] | None = None,
        overrides: dict[str, Any] | None = None,
        root_mode: str | None = None,
        clean_home: str | None = None,
    ) -> dict:
        """Reserve a quota slot, submit one private job, return it as pending."""
        target = self._target(name)
        if not self._is_multijob(target):
            raise web.HTTPBadRequest(
                text="private Slurm jobs are not available on this target"
            )
        await self.open_target(name)
        manager = self._ensure_jobs()
        params = dict(parameters or {})
        if overrides:
            params["set"] = dict(overrides)
        request = jobcore.JobRequest(
            client_id=client.client_id,
            target=name,
            request_id=request_id or uuid.uuid4().hex,
            kind="private",
            parameters=params,
            root_mode=root_mode or target.root_mode,
            home=self._resolve_clean_home(clean_home),
        )
        record = await manager.submit(request)
        return _public_job(record)

    def list_jobs(
        self, client: Client, target: str | None = None
    ) -> list[dict[str, Any]]:
        self._ensure_jobs()
        if target is not None:
            client.require_target(target)
            if target not in self.config.targets:
                raise web.HTTPNotFound(text="unknown target")
        visible: list[dict[str, Any]] = []
        for record in self.job_store.scan():  # type: ignore[union-attr]
            if target is not None and record.target != target:
                continue
            if not client.may_access(record.target):
                continue
            if record.kind == "shared":
                visible.append(_public_job(record))
            elif client.is_admin or record.owner_client_id == client.client_id:
                visible.append(_public_job(record, include_owner=client.is_admin))
        visible.sort(key=lambda item: item["created_at"])
        return visible

    async def wait_job(
        self, job_id: str, *, timeout: float = 30.0
    ) -> "jobcore.JobRecord":
        manager = self._ensure_jobs()
        record = manager.store.load_record(job_id)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, float(timeout))
        while record.state not in (jobcore.READY, jobcore.TERMINAL):
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            record = await self._poll_one(job_id)
            if record.state in (jobcore.READY, jobcore.TERMINAL):
                break
            await asyncio.sleep(min(0.05, max(0.0, remaining)))
        return record

    async def _cleanup_job_resources(self, job_id: str, reason: str) -> None:
        """Close only one job's PTY/SFTP/SSH resources.

        A stop or terminal transition of job B must never invalidate job A:
        sessions are filtered by ``job_id``, the file service retires only that
        job's state and the backend drops only that job's cached generation.
        """
        with contextlib.suppress(Exception):
            await self.sessions.close_for_job(job_id, reason=reason)
        with contextlib.suppress(Exception):
            await self.files.close_job(job_id)
        with contextlib.suppress(Exception):
            await self.backend.disconnect_job(job_id)

    async def cancel_job(self, client: Client, job_id: str) -> dict[str, Any]:
        record = self._authorized_job(client, job_id)
        if record.kind == "shared" and not client.is_admin:
            raise web.HTTPForbidden(text="forbidden")
        manager = self._ensure_jobs()
        try:
            updated = await manager.stop(record.job_id)
        except jobcore.IllegalTransition as exc:
            raise web.HTTPConflict(text=str(exc)) from exc
        # The cancellation is authoritative: this job's sessions, SFTP state
        # and cached connection go away; other jobs are untouched.
        await self._cleanup_job_resources(updated.job_id, reason="job stopped")
        with contextlib.suppress(Exception):
            self._write_route_registry(updated.target)
        return _public_job(updated)

    # -- HTTP handlers -----------------------------------------------------
    def _auth(self, request: web.Request) -> Client:
        try:
            return self.auth.authenticate_bearer(request.headers.get("Authorization"))
        except AuthError as exc:
            raise web.HTTPUnauthorized(
                text=str(exc),
                headers={"WWW-Authenticate": "Bearer"},
            ) from exc

    async def h_list_targets(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        return web.json_response({"targets": self.list_targets(client)})

    async def h_get_target(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        name = request.match_info["target"]
        client.require_target(name)
        if name not in self.config.targets:
            raise web.HTTPNotFound(text="unknown target")
        return web.json_response(self.public_status(name))

    async def _request_json(self, request: web.Request) -> dict:
        """Read and validate the optional JSON object body once.

        The request body can only be read once; callers must derive both the
        2FA factor and the allocation overrides from this single result.  An
        absent or empty body is an empty object; malformed JSON or a non-object
        body is a 400.
        """
        if not request.can_read_body:
            return {}
        try:
            body = await request.json()
        except Exception as exc:  # noqa: BLE001 - bad client body
            raise web.HTTPBadRequest(text="body must be JSON") from exc
        if body in (None, {}):
            return {}
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="body must be a JSON object")
        return body

    @staticmethod
    def _body_factor(body: dict) -> str | None:
        factor = body.get("factor")
        if factor is None:
            return None
        if not isinstance(factor, str):
            raise web.HTTPBadRequest(text="'factor' must be a string")
        return factor or None

    @staticmethod
    def _body_overrides(body: dict) -> dict[str, Any] | None:
        overrides = body.get("set")
        if overrides is None:
            return None
        if not isinstance(overrides, dict):
            raise web.HTTPBadRequest(text="'set' must be an object of override values")
        return {str(k): v for k, v in overrides.items()}

    @staticmethod
    def _body_parameters(body: dict) -> dict[str, Any] | None:
        parameters = body.get("parameters")
        if parameters is None:
            return None
        if not isinstance(parameters, dict):
            raise web.HTTPBadRequest(text="'parameters' must be an object")
        return {str(k): v for k, v in parameters.items()}

    @staticmethod
    def _body_request_id(body: dict) -> str | None:
        value = body.get("request_id")
        if value is None:
            return None
        if not isinstance(value, str) or not value:
            raise web.HTTPBadRequest(text="'request_id' must be a non-empty string")
        return value

    @staticmethod
    def _body_root_mode(body: dict) -> str | None:
        value = body.get("root_mode")
        if value is None:
            return None
        if not isinstance(value, str) or value not in ("fakeroot", "clean"):
            raise web.HTTPBadRequest(
                text="'root_mode' must be 'fakeroot' or 'clean'"
            )
        return value

    @staticmethod
    def _body_clean_home(body: dict) -> str | None:
        value = body.get("clean_home")
        if value is None:
            return None
        if not isinstance(value, str):
            raise web.HTTPBadRequest(text="'clean_home' must be a string")
        return value

    async def h_connect(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        name = request.match_info["target"]
        client.require_target(name)
        body = await self._request_json(request)
        factor = self._body_factor(body)
        overrides = self._body_overrides(body)
        return web.json_response(
            await self.connect_target(
                name, factor=factor, overrides=overrides, client=client
            )
        )

    async def h_refresh(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        name = request.match_info["target"]
        client.require_target(name)
        if (
            name in self.config.targets
            and self._is_multijob(self.config.targets[name])
            and not client.is_admin
        ):
            # A destructive route refresh (and any future shared-job replace)
            # is operator-only.
            raise web.HTTPForbidden(text="forbidden")
        body = await self._request_json(request)
        factor = self._body_factor(body)
        overrides = self._body_overrides(body)
        return web.json_response(
            await self.refresh_target(name, factor=factor, overrides=overrides)
        )

    async def h_preview(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        name = request.match_info["target"]
        client.require_target(name)
        body = await self._request_json(request)
        overrides = self._body_overrides(body)
        return web.json_response(self.preview_target(name, overrides=overrides))

    async def h_stop(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        name = request.match_info["target"]
        client.require_target(name)
        if (
            name in self.config.targets
            and self._is_multijob(self.config.targets[name])
            and not client.is_admin
        ):
            raise web.HTTPForbidden(text="forbidden")
        return web.json_response(await self.stop_target(name))

    # -- route/job HTTP endpoints -----------------------------------------
    async def h_target_open(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        name = request.match_info["target"]
        client.require_target(name)
        if name not in self.config.targets:
            raise web.HTTPNotFound(text="unknown target")
        body = await self._request_json(request)
        factor = self._body_factor(body)
        overrides = self._body_overrides(body)
        return web.json_response(
            await self.open_target(name, factor=factor, overrides=overrides)
        )

    async def h_target_shared(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        name = request.match_info["target"]
        client.require_target(name)
        if name not in self.config.targets:
            raise web.HTTPNotFound(text="unknown target")
        body = await self._request_json(request)
        factor = self._body_factor(body)
        overrides = self._body_overrides(body)
        return web.json_response(
            await self.target_shared(
                name,
                client,
                factor=factor,
                overrides=overrides,
                parameters=self._body_parameters(body),
                root_mode=self._body_root_mode(body),
            )
        )

    async def h_target_sbatch(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        name = request.match_info["target"]
        client.require_target(name)
        if name not in self.config.targets:
            raise web.HTTPNotFound(text="unknown target")
        body = await self._request_json(request)
        return web.json_response(
            await self.target_sbatch(
                name,
                client,
                request_id=self._body_request_id(body),
                parameters=self._body_parameters(body),
                overrides=self._body_overrides(body),
                root_mode=self._body_root_mode(body),
                clean_home=self._body_clean_home(body),
            )
        )

    async def h_list_jobs(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        target = request.query.get("target")
        return web.json_response({"jobs": self.list_jobs(client, target)})

    async def h_get_job(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        job_id = request.match_info["job_id"]
        record = self._authorized_job(client, job_id)
        if self._query_bool(request, "wait"):
            try:
                timeout = float(request.query.get("timeout", "30"))
            except (TypeError, ValueError):
                raise web.HTTPBadRequest(text="'timeout' must be a number")
            record = await self.wait_job(record.job_id, timeout=timeout)
        return web.json_response(_public_job(record))

    async def h_cancel_job(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        job_id = request.match_info["job_id"]
        return web.json_response(await self.cancel_job(client, job_id))

    async def h_exec(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        body = await request.json()
        name = body.get("target")
        command = body.get("command")
        if not name or not isinstance(command, str):
            raise web.HTTPBadRequest(text="'target' and 'command' are required")
        client.require_target(name)
        target = self.config.targets.get(name)
        if target is not None and self._is_multijob(target):
            # Slurm jobs are addressed explicitly (or the target's shared job
            # when omitted); exec never falls back to the target-keyed legacy
            # container cache.
            raw_job_id = self._job_id_from_body(body) or request.query.get("job_id")
            try:
                record = self.resolve_job(client, name, raw_job_id or None)
            except web.HTTPNotFound:
                raise web.HTTPNotFound(text=JOB_ACCESS_DENIED_MESSAGE) from None
            try:
                target, conn = await self._job_connection_provider(
                    name, record.job_id
                )
            except JobAccessError:
                raise web.HTTPNotFound(text=JOB_ACCESS_DENIED_MESSAGE) from None
            except JobNotReady as exc:
                raise web.HTTPConflict(text=str(exc)) from exc
            job_scoped = True
        else:
            target, conn = await self._connection_provider(name)
            job_scoped = False
        timeout = body.get("timeout")
        if timeout is None:
            timeout = self.config.server.exec_timeout
        env = body.get("env")
        if env is not None and not isinstance(env, dict):
            raise web.HTTPBadRequest(text="'env' must be an object of strings")
        env = {str(k): str(v) for k, v in env.items()} if env else None
        try:
            result = await self.backend.run(
                conn,
                command,
                cwd=body.get("cwd"),
                timeout=float(timeout),
                env=env,
                stdin=body.get("stdin"),
            )
        finally:
            # A job connection is cached per (job, generation) and shared by
            # that job's exec/SFTP/PTY; only the legacy dedicated mode is
            # one-shot.
            if not job_scoped and target.connect_mode == "dedicated":
                await self.backend.close_connection(conn)
        return web.json_response(
            {
                "target": name,
                "exit_status": result.exit_status,
                "stdout": _decode(result.stdout),
                "stderr": _decode(result.stderr),
            }
        )

    # sessions
    async def h_session_create(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        body = await request.json()
        name = body.get("target")
        if not name:
            raise web.HTTPBadRequest(text="'target' is required")
        client.require_target(name)
        target = self._target(name)
        raw_job_id = self._job_id_from_body(body) or request.query.get("job_id")
        if self._is_multijob(target):
            # An omitted id selects the shared job only; an explicit id is
            # resolved (and authorized) before any PTY work starts.
            try:
                record = self.resolve_job(client, name, raw_job_id or None)
            except web.HTTPNotFound:
                raise web.HTTPNotFound(text=JOB_ACCESS_DENIED_MESSAGE) from None
            job_id: str | None = record.job_id
            token = self._session_client.set(client)
        else:
            if raw_job_id:
                raise web.HTTPBadRequest(
                    text="'job_id' is only valid on a Slurm job target"
                )
            job_id = None
            token = None
        try:
            session = await self.sessions.create(
                client.client_id,
                target,
                cwd=body.get("cwd"),
                columns=int(body.get("columns", 160)),
                rows=int(body.get("rows", 50)),
                job_id=job_id,
            )
        except JobAccessError:
            raise web.HTTPNotFound(text=JOB_ACCESS_DENIED_MESSAGE) from None
        except JobNotReady as exc:
            raise web.HTTPConflict(text=str(exc)) from exc
        finally:
            if token is not None:
                self._session_client.reset(token)
        return web.json_response(
            {
                "session_id": session.id,
                "target": session.target,
                "job_id": session.job_id,
                "created": True,
            }
        )

    async def h_session_list(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        target = request.query.get("target")
        if target is not None:
            client.require_target(target)
        all_clients = self._query_bool(request, "all")
        client_filter = request.query.get("client")
        if all_clients or client_filter is not None:
            # Cross-client view is admin-only.
            client.require_admin()
            return web.json_response(
                {"sessions": self.sessions.list_all(target, client_filter)}
            )
        return web.json_response({"sessions": self.sessions.list(client.client_id, target)})

    async def h_session_get(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        session = self.sessions.get_public(
            request.match_info["session"], client.client_id, admin=client.is_admin
        )
        return web.json_response(
            {
                "session_id": session.id,
                "client": session.client_id,
                "target": session.target,
                "job_id": session.job_id,
                "closed": session.closed,
                "exit_status": session.exit_status,
                "buffered_bytes": len(session.output_buffer),
            }
        )

    async def h_session_write(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        body = await request.json()
        data = body.get("data", "")
        written = await self.sessions.write(
            request.match_info["session"], client.client_id, data
        )
        return web.json_response({"written": written})

    async def h_session_read(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        body = {}
        if request.can_read_body:
            with contextlib.suppress(Exception):
                body = await request.json()
        max_bytes = int(body.get("max_bytes", 0))
        wait = float(body.get("wait", 0) or 0)
        result = await self.sessions.read(
            request.match_info["session"], client.client_id, max_bytes, wait=wait
        )
        return web.json_response(result)

    async def h_session_resize(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        body = await request.json()
        await self.sessions.resize(
            request.match_info["session"],
            client.client_id,
            int(body["columns"]),
            int(body["rows"]),
        )
        return web.json_response({"resized": True})

    async def h_session_delete(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        owner = None if client.is_admin else client.client_id
        await self.sessions.close(request.match_info["session"], owner=owner)
        return web.json_response({"closed": True})

    # -- operator / admin endpoints ---------------------------------------
    def _query_bool(self, request: web.Request, name: str) -> bool:
        return request.query.get(name, "").lower() in ("1", "true", "yes")

    def _client_public(self, cfg) -> dict:
        return {
            "name": cfg.client_id,
            "label": cfg.label,
            "allow_all": cfg.allow_all,
            "targets": sorted(self.config.targets) if cfg.allow_all else list(cfg.targets),
            "session_count": self.sessions.count_for_client(cfg.client_id),
        }

    async def h_list_clients(self, request: web.Request) -> web.Response:
        """List every configured client/token with its ACL and live sessions.

        Admin-only.  Never returns token values, only a short hash prefix used
        as a stable fingerprint.
        """
        client = self._auth(request)
        client.require_admin()
        clients = []
        for cfg in self.config.clients.values():
            item = self._client_public(cfg)
            digest = cfg.token_sha256.split(":", 1)[-1]
            item["token_fingerprint"] = "sha256:" + digest[:12]
            item["sessions"] = self.sessions.list(client_id=cfg.client_id)
            clients.append(item)
        return web.json_response({"clients": clients})

    async def h_get_client(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        client.require_admin()
        name = request.match_info["client"]
        cfg = self.config.clients.get(name)
        if cfg is None:
            raise web.HTTPNotFound(text="unknown client")
        item = self._client_public(cfg)
        digest = cfg.token_sha256.split(":", 1)[-1]
        item["token_fingerprint"] = "sha256:" + digest[:12]
        item["sessions"] = self.sessions.list(client_id=cfg.client_id)
        return web.json_response(item)

    async def h_client_sessions(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        client.require_admin()
        name = request.match_info["client"]
        cfg = self.config.clients.get(name)
        if cfg is None:
            raise web.HTTPNotFound(text="unknown client")
        return web.json_response({"sessions": self.sessions.list(client_id=name)})

    async def h_client_sessions_close(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        client.require_admin()
        name = request.match_info["client"]
        if name not in self.config.clients:
            raise web.HTTPNotFound(text="unknown client")
        closed = await self.sessions.close_all_for_client(name, reason="admin closed")
        return web.json_response({"client": name, "closed": closed})

    async def h_reload(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        client.require_admin()
        report = await self.reload()
        return web.json_response({"reloaded": True, "report": report})

    # -- enrollment --------------------------------------------------------
    def _enrollment_enabled(self) -> None:
        if not self.config.server.allow_enrollment:
            raise web.HTTPForbidden(text="enrollment is disabled")

    def _check_enroll_targets(self, targets: tuple[str, ...]) -> None:
        known = set(self.config.targets)
        for target in targets:
            if target != "*" and target not in known:
                raise web.HTTPBadRequest(
                    text=f"unknown target {target!r}; known: "
                    f"{', '.join(sorted(known)) or '(none)'}"
                )

    async def h_enroll_create(self, request: web.Request) -> web.Response:
        """Unauthenticated: queue an enrollment request for operator approval."""
        self._enrollment_enabled()
        try:
            body = await request.json()
        except Exception as exc:  # noqa: BLE001
            raise web.HTTPBadRequest(text="body must be JSON") from exc
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="body must be a JSON object")
        client_id = body.get("client_id")
        if not isinstance(client_id, str) or not client_id:
            raise web.HTTPBadRequest(text="'client_id' is required")
        try:
            validate_target_name(client_id)
        except ConfigError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
        if client_id in self.config.clients:
            raise web.HTTPConflict(text=f"client {client_id!r} already exists")
        raw_targets = body.get("targets", [])
        if not isinstance(raw_targets, list) or not all(
            isinstance(t, str) for t in raw_targets
        ):
            raise web.HTTPBadRequest(text="'targets' must be a list of strings")
        targets = tuple(dict.fromkeys(raw_targets))  # de-dup, keep order
        self._check_enroll_targets(targets)
        label = body.get("label")
        if label is not None and not isinstance(label, str):
            raise web.HTTPBadRequest(text="'label' must be a string")

        source = request.remote
        try:
            pending, secret = self.enrollments.create(client_id, targets, label, source)
        except EnrollmentQueueFull as exc:
            raise web.HTTPTooManyRequests(text=str(exc)) from exc
        except EnrollmentError as exc:
            raise web.HTTPConflict(text=str(exc)) from exc

        log.info(
            "enrollment request %s from %s for client %r (targets: %s) - "
            "awaiting operator approval",
            pending.request_id,
            source,
            client_id,
            ", ".join(targets) or "(none)",
        )
        return web.json_response(
            {
                "request_id": pending.request_id,
                "poll_secret": secret,
                "expires_at": pending.expires_at,
                "status": pending.status,
            }
        )

    async def h_enroll_poll(self, request: web.Request) -> web.Response:
        """Poll a request with its poll secret; deliver the token once approved."""
        self._enrollment_enabled()
        request_id = request.match_info["request"]
        secret = request.headers.get("X-Enroll-Secret", "")
        try:
            pending = self.enrollments.get(request_id, secret)
        except EnrollmentError as exc:
            raise web.HTTPNotFound(text=str(exc)) from exc
        if pending.status == PENDING:
            return web.json_response({"status": PENDING})
        if pending.status == APPROVED:
            token = self.enrollments.consume(pending)
            return web.json_response(
                {"status": APPROVED, "client_id": pending.client_id, "token": token}
            )
        # DENIED / CONSUMED / EXPIRED are all terminal for the requester.
        return web.json_response({"status": pending.status})

    async def approve_enrollment(self, request_id: str) -> PendingEnrollment:
        """Operator action: mint a token, append the client, reload, approve."""
        path = self.config.config_path
        if not path:
            raise ConfigError("no config path recorded; cannot enroll")
        pending = self.enrollments.get_by_id(request_id)
        if pending.status != PENDING:
            raise EnrollmentError(f"request is {pending.status}, not pending")
        if pending.client_id in self.config.clients:
            self.enrollments.deny(request_id)
            raise ConfigError(f"client {pending.client_id!r} already exists")

        # Mint the token with the same generator the CLI uses, so the hash is
        # consistent; the plaintext is written nowhere but returned later.
        from .auth import new_token

        token = new_token()
        token_path = self.config.token_file or str(Path(path).parent / "tokens.toml")
        # Write the hash first: append_client re-validates the whole config, and
        # a client with neither an inline token nor a token-file entry would be
        # rejected.  If the config append then fails, drop the orphan hash.
        append_token_hash(token_path, pending.client_id, token)
        try:
            append_client(path, pending.client_id, pending.targets, pending.label)
        except Exception:
            self._rollback_token(token_path, pending.client_id)
            raise

        await self.reload()
        self.enrollments.approve(request_id)
        pending.token = token
        log.info(
            "approved enrollment %s: client %r -> targets %s",
            request_id,
            pending.client_id,
            ", ".join(pending.targets) or "(none)",
        )
        return pending

    @staticmethod
    def _rollback_token(token_path: str, client_id: str) -> None:
        """Best-effort removal of an orphan token entry after a failed append."""
        try:
            from .config import _atomic_write, _toml_quote

            path = Path(token_path)
            lines = path.read_text().splitlines()
            out = [
                line
                for line in lines
                if not line.strip().startswith(f"{_toml_quote(client_id)} =")
                and not line.strip().startswith(f"{client_id} =")
            ]
            _atomic_write(path, "\n".join(out).rstrip("\n") + "\n", mode=0o600)
        except Exception:  # noqa: BLE001
            log.exception("rollback of token for %r failed", client_id)

    async def h_enroll_list(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        client.require_admin()
        pending = [
            {
                "request_id": r.request_id,
                "client_id": r.client_id,
                "targets": list(r.targets),
                "label": r.label,
                "source": r.source,
                "status": r.status,
                "created_at": r.created_at,
                "expires_at": r.expires_at,
            }
            for r in self.enrollments.list_pending()
        ]
        return web.json_response({"pending": pending})

    async def h_enroll_approve(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        client.require_admin()
        try:
            pending = await self.approve_enrollment(request.match_info["request"])
        except EnrollmentError as exc:
            # An unknown or expired id, and a request already consumed, all
            # share one message after _expire(), so 404 is the honest status
            # here, mirroring h_enroll_deny.
            raise web.HTTPNotFound(text=str(exc)) from exc
        return web.json_response(
            {
                "request_id": pending.request_id,
                "client_id": pending.client_id,
                "targets": list(pending.targets),
                "status": pending.status,
            }
        )

    async def h_enroll_deny(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        client.require_admin()
        try:
            pending = self.enrollments.deny(request.match_info["request"])
        except EnrollmentError as exc:
            raise web.HTTPNotFound(text=str(exc)) from exc
        return web.json_response(
            {"request_id": pending.request_id, "status": pending.status}
        )

    # files
    @contextlib.asynccontextmanager
    async def _sftp_session(self, client: Client, request: web.Request):
        """Yield an SFTP client, closing per-operation connections afterward.

        For ``connect_mode = "dedicated"`` targets each file operation gets its
        own transport, which is closed on exit.  Shared targets reuse the
        cached connection.
        """
        target_name = request.query.get("target")
        if not target_name:
            raise web.HTTPBadRequest(text="'target' query parameter is required")
        client.require_target(target_name)
        target, conn = await self._connection_provider(target_name)
        owned = target.connect_mode == "dedicated"
        try:
            async with self.backend.sftp_client(conn) as sftp:
                yield sftp
        finally:
            if owned:
                await self.backend.close_connection(conn)

    async def h_file_stat(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        path = request.query["path"]
        target_name = request.query.get("target")
        if self._file_is_job_scoped(target_name):
            job_id = self._file_job_id(client, request)
            return web.json_response(
                await self._run_file_op(
                    client, job_id, lambda j: self.files.stat(j, path)
                )
            )
        async with self._sftp_session(client, request) as sftp:
            return web.json_response(await files_stat(sftp, path))

    async def h_file_list(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        path = request.query["path"]
        target_name = request.query.get("target")
        if self._file_is_job_scoped(target_name):
            job_id = self._file_job_id(client, request)
            entries = await self._run_file_op(
                client, job_id, lambda j: self.files.list_dir(j, path)
            )
            return web.json_response({"path": path, "entries": entries})
        async with self._sftp_session(client, request) as sftp:
            return web.json_response({"path": path, "entries": await files_list(sftp, path)})

    async def h_file_read(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        path = request.query["path"]
        encoding = request.query.get("encoding")
        target_name = request.query.get("target")
        job_id = (
            self._file_job_id(client, request)
            if self._file_is_job_scoped(target_name)
            else None
        )
        if encoding == "stream":
            # Stream raw bytes without buffering or round-tripping through JSON.
            if job_id is not None:
                return await self._stream_job_file(client, request, job_id, path)
            client.require_target(target_name)
            target, conn = await self._connection_provider(target_name)
            owned = target.connect_mode == "dedicated"
            try:
                async with self.backend.sftp_client(conn) as sftp:
                    info = await files_stat(sftp, path)
                    if info["type"] != "file":
                        raise web.HTTPBadRequest(text="not a regular file")
                    resp = web.StreamResponse(
                        headers={
                            "Content-Type": "application/octet-stream",
                            "Content-Disposition": f'attachment; filename="{posixpath.basename(path)}"',
                        }
                    )
                    if info.get("size") is not None:
                        resp.content_length = info["size"]
                    try:
                        await resp.prepare(request)
                        async with sftp.open(path, "rb") as handle:
                            while True:
                                chunk = await handle.read(262144)
                                if not chunk:
                                    break
                                await resp.write(chunk)
                        await resp.write_eof()
                    except (ConnectionResetError, BrokenPipeError) as exc:
                        # A client that aborts a download closes the transport
                        # under us (TCP RST or EPIPE). aiohttp's
                        # ClientConnectionResetError subclasses the stdlib
                        # ConnectionResetError, so this covers both; the bare
                        # stdlib errors cover older aiohttp versions. This is
                        # ordinary client behaviour, not a server fault.
                        log.debug(
                            "file read aborted: client disconnected mid-stream "
                            "target=%s path=%s (%s)",
                            target_name,
                            path,
                            type(exc).__name__,
                        )
                        return resp
                    return resp
            finally:
                if owned:
                    await self.backend.close_connection(conn)
        if job_id is not None:
            data, info = await self._run_file_op(
                client, job_id, lambda j: self.files.read(j, path)
            )
        else:
            async with self._sftp_session(client, request) as sftp:
                data, info = await files_read(sftp, path)
        if encoding == "base64":
            import base64

            return web.json_response(
                {"path": path, "encoding": "base64",
                 "content": base64.b64encode(data).decode("ascii"), "stat": info}
            )
        return web.json_response(
            {"path": path, "encoding": "utf-8-replace",
             "content": data.decode("utf-8", errors="replace"), "stat": info}
        )

    async def _stream_job_file(
        self, client: Client, request: web.Request, job_id: str, path: str
    ) -> web.Response:
        async with self._file_client_context(client):
            try:
                info = await self.files.stat(job_id, path)
                if info["type"] != "file":
                    raise web.HTTPBadRequest(text="not a regular file")
                resp = web.StreamResponse(
                    headers={
                        "Content-Type": "application/octet-stream",
                        "Content-Disposition": f'attachment; filename="{posixpath.basename(path)}"',
                    }
                )
                if info.get("size") is not None:
                    resp.content_length = info["size"]
                try:
                    await resp.prepare(request)
                    async for chunk in self.files.download(job_id, path):
                        await resp.write(chunk)
                    await resp.write_eof()
                except (ConnectionResetError, BrokenPipeError) as exc:
                    log.debug(
                        "file read aborted: client disconnected mid-stream "
                        "target=%s job=%s path=%s (%s)",
                        request.query.get("target"),
                        job_id,
                        path,
                        type(exc).__name__,
                    )
                return resp
            except JobAccessError:
                raise web.HTTPNotFound(text=JOB_ACCESS_DENIED_MESSAGE) from None

    async def h_file_write(self, request: web.Request) -> web.Response:
        import base64

        client = self._auth(request)
        body = await request.json()
        path = body["path"]
        if body.get("encoding") == "base64":
            content = base64.b64decode(body.get("content", ""))
        else:
            content = body.get("content", "").encode("utf-8")
        target_name = request.query.get("target")
        if self._file_is_job_scoped(target_name):
            job_id = self._file_job_id(client, request, body)
            return web.json_response(
                await self._run_file_op(
                    client, job_id, lambda j: self.files.write(j, path, content)
                )
            )
        async with self._sftp_session(client, request) as sftp:
            return web.json_response(await files_write(sftp, path, content))

    async def h_file_upload(self, request: web.Request) -> web.Response:
        """Stream a request body into a remote file without buffering it all.

        The body is written chunk-by-chunk to SFTP; nothing is held in memory
        beyond one chunk and no bytes are returned to the caller.
        """
        client = self._auth(request)
        path = request.query["path"]
        append = request.query.get("append", "false").lower() in ("1", "true", "yes")
        parents = request.query.get("parents", "false").lower() in ("1", "true", "yes")
        content_length = request.content_length
        target_name = request.query.get("target")
        if self._file_is_job_scoped(target_name):
            job_id = self._file_job_id(client, request)
            result = await self._run_file_op(
                client,
                job_id,
                lambda j: self.files.upload(
                    j, path, request.content, append=append, parents=parents
                ),
            )
            written = result["written"]
            return web.json_response(
                {
                    "path": path,
                    "written": written,
                    "content_length": content_length,
                    "complete": content_length is None or written == content_length,
                    "stat": result["stat"],
                }
            )
        async with self._sftp_session(client, request) as sftp:
            if parents:
                parent = posixpath.dirname(path)
                if parent:
                    with contextlib.suppress(Exception):
                        await sftp.makedirs(parent, exist_ok=True)
            written = 0
            async with sftp.open(path, "ab" if append else "wb") as handle:
                while True:
                    chunk = await request.content.readany()
                    if not chunk:
                        break
                    await handle.write(chunk)
                    written += len(chunk)
            info = await files_stat(sftp, path)
        return web.json_response(
            {
                "path": path,
                "written": written,
                "content_length": content_length,
                "complete": content_length is None or written == content_length,
                "stat": info,
            }
        )

    async def h_file_mkdir(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        body = await request.json()
        path = body["path"]
        target_name = request.query.get("target")
        if self._file_is_job_scoped(target_name):
            job_id = self._file_job_id(client, request, body)
            return web.json_response(
                await self._run_file_op(
                    client, job_id, lambda j: self.files.mkdir(j, path)
                )
            )
        async with self._sftp_session(client, request) as sftp:
            return web.json_response(await files_mkdir(sftp, path))

    async def h_file_remove(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        body = await request.json()
        path = body["path"]
        target_name = request.query.get("target")
        if self._file_is_job_scoped(target_name):
            job_id = self._file_job_id(client, request, body)
            return web.json_response(
                await self._run_file_op(
                    client, job_id, lambda j: self.files.remove(j, path)
                )
            )
        async with self._sftp_session(client, request) as sftp:
            return web.json_response(await files_remove(sftp, path))

    async def h_file_rename(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        body = await request.json()
        target_name = request.query.get("target")
        if self._file_is_job_scoped(target_name):
            job_id = self._file_job_id(client, request, body)
            return web.json_response(
                await self._run_file_op(
                    client,
                    job_id,
                    lambda j: self.files.rename(
                        j, body["source"], body["destination"]
                    ),
                )
            )
        async with self._sftp_session(client, request) as sftp:
            return web.json_response(
                await files_rename(sftp, body["source"], body["destination"])
            )

    async def h_file_chmod(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        body = await request.json()
        mode = files_parse_mode(body["mode"])
        target_name = request.query.get("target")
        if self._file_is_job_scoped(target_name):
            job_id = self._file_job_id(client, request, body)
            return web.json_response(
                await self._run_file_op(
                    client, job_id, lambda j: self.files.chmod(j, body["path"], mode)
                )
            )
        async with self._sftp_session(client, request) as sftp:
            return web.json_response(await files_chmod(sftp, body["path"], mode))

    async def h_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "version": __version__})

    # -- aiohttp app -------------------------------------------------------
    def create_app(self) -> web.Application:
        app = web.Application(client_max_size=self.config.server.max_body_bytes)
        app.middlewares.append(_error_middleware)
        router = app.router
        router.add_get("/v1/health", self.h_health)
        router.add_get("/v1/targets", self.h_list_targets)
        router.add_get("/v1/targets/{target}", self.h_get_target)
        router.add_post("/v1/targets/{target}/open", self.h_target_open)
        router.add_post("/v1/targets/{target}/shared", self.h_target_shared)
        router.add_post("/v1/targets/{target}/sbatch", self.h_target_sbatch)
        router.add_post("/v1/targets/{target}/connect", self.h_connect)
        router.add_post("/v1/targets/{target}/refresh", self.h_refresh)
        router.add_post("/v1/targets/{target}/preview", self.h_preview)
        router.add_post("/v1/targets/{target}/stop", self.h_stop)
        router.add_get("/v1/jobs", self.h_list_jobs)
        router.add_get("/v1/jobs/{job_id}", self.h_get_job)
        router.add_post("/v1/jobs/{job_id}/cancel", self.h_cancel_job)
        router.add_post("/v1/exec", self.h_exec)
        router.add_post("/v1/sessions", self.h_session_create)
        router.add_get("/v1/sessions", self.h_session_list)
        router.add_get("/v1/sessions/{session}", self.h_session_get)
        router.add_delete("/v1/sessions/{session}", self.h_session_delete)
        router.add_post("/v1/sessions/{session}/write", self.h_session_write)
        router.add_post("/v1/sessions/{session}/read", self.h_session_read)
        router.add_post("/v1/sessions/{session}/resize", self.h_session_resize)
        router.add_post("/v1/reload", self.h_reload)
        router.add_get("/v1/clients", self.h_list_clients)
        router.add_get("/v1/clients/{client}", self.h_get_client)
        router.add_get("/v1/clients/{client}/sessions", self.h_client_sessions)
        router.add_delete("/v1/clients/{client}/sessions", self.h_client_sessions_close)
        router.add_post("/v1/enroll", self.h_enroll_create)
        router.add_get("/v1/enroll/{request}", self.h_enroll_poll)
        router.add_get("/v1/enroll-requests", self.h_enroll_list)
        router.add_post("/v1/enroll-requests/{request}/approve", self.h_enroll_approve)
        router.add_post("/v1/enroll-requests/{request}/deny", self.h_enroll_deny)
        router.add_get("/v1/files/stat", self.h_file_stat)
        router.add_get("/v1/files/list", self.h_file_list)
        router.add_get("/v1/files/read", self.h_file_read)
        router.add_put("/v1/files/write", self.h_file_write)
        router.add_put("/v1/files/upload", self.h_file_upload)
        router.add_post("/v1/files/mkdir", self.h_file_mkdir)
        router.add_post("/v1/files/remove", self.h_file_remove)
        router.add_post("/v1/files/rename", self.h_file_rename)
        router.add_post("/v1/files/chmod", self.h_file_chmod)
        app.on_startup.append(self._on_startup)
        app.on_cleanup.append(self._on_cleanup)
        return app

    async def _on_startup(self, app: web.Application) -> None:
        await self.start()

    async def _on_cleanup(self, app: web.Application) -> None:
        await self.stop()

    # -- interactive console ----------------------------------------------
    async def run_console(self) -> None:
        self._interactive = True
        banner = (
            "computeMCP-gateway console. Type 'help' for commands.\n"
            "Targets with interactive_auth will prompt for password/OTP here."
        )
        if self.config.server.allow_enrollment:
            banner += (
                "\nEnrollment is enabled: approve a request with 'approve <id>' "
                "(list with 'enrollments')."
            )
        print(banner)
        try:
            while True:
                try:
                    line = await asyncio.get_running_loop().run_in_executor(
                        None, input, "gateway> "
                    )
                except (EOFError, KeyboardInterrupt):
                    print()
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    if not await self._console_command(line):
                        break
                except Exception as exc:  # noqa: BLE001
                    print(f"error: {exc}")
        finally:
            self._interactive = False

    @staticmethod
    def _parse_2fa_flag(args: list[str]) -> tuple[bool, list[str]]:
        """Split a value-less ``--2fa`` flag out of console args."""
        use_2fa = False
        rest: list[str] = []
        for token in args:
            if token == "--2fa":
                use_2fa = True
            else:
                rest.append(token)
        return use_2fa, rest

    async def _console_command(self, line: str) -> bool:
        parts = line.split()
        cmd, args = parts[0], parts[1:]
        if cmd in ("quit", "exit"):
            return False
        if cmd == "help":
            print(
                "targets | status [target] | connect [--2fa] <t> | "
                "refresh [--2fa] <t> | reconnect <t> | stop <t> | "
                "connect-all | stop-all | reload |\n"
                "clients | client <name> | client-refresh <name> | "
                "client-connect <name> | client-stop <name> | client-kill <name> |\n"
                "sessions [target] | close-session <id> |\n"
                "enrollments | approve <id> | deny <id> | quit"
            )
        elif cmd == "targets":
            for name in self.config.targets:
                print(name)
        elif cmd == "status":
            if args:
                print(self.public_status(args[0]))
            else:
                self._print_status_table()
        elif cmd in ("connect", "refresh"):
            use_2fa, rest = self._parse_2fa_flag(list(args))
            if len(rest) != 1:
                print(f"usage: {cmd} <target> [--2fa]")
                return True
            factor = None
            if use_2fa:
                try:
                    factor = prompt_factor()
                except FactorPromptError as exc:
                    print(f"error: {exc}")
                    return True
            if cmd == "connect":
                print(await self.connect_target(rest[0], factor=factor))
            else:
                print(await self.refresh_target(rest[0], factor=factor))
        elif cmd == "open":
            if len(args) != 1:
                print("usage: open <target>")
                return True
            print(await self.open_target(args[0]))
        elif cmd == "shared":
            if len(args) != 1:
                print("usage: shared <target>")
                return True
            print(await self.target_shared(args[0], self._operator_client()))
        elif cmd == "sbatch":
            if len(args) != 1:
                print("usage: sbatch <target>")
                return True
            print(await self.target_sbatch(args[0], self._operator_client()))
        elif cmd == "jobs":
            target = args[0] if args else None
            for job in self.list_jobs(self._operator_client(), target):
                print(
                    f"{job['job_id']}  {job['kind']}  {job['target']}  "
                    f"{job['state']}  slurm={job['slurm_job_id'] or '-'}"
                )
        elif cmd in ("job", "job-cancel"):
            if len(args) != 1:
                print(f"usage: {cmd} <job-id>")
                return True
            if cmd == "job":
                print(_public_job(self._authorized_job(self._operator_client(), args[0])))
            else:
                print(await self.cancel_job(self._operator_client(), args[0]))
        elif cmd in ("reconnect", "stop"):
            if len(args) != 1:
                print(f"usage: {cmd} <target>")
                return True
            if cmd == "reconnect":
                # refresh without a factor: interactive targets warn and skip.
                print(await self.refresh_target(args[0]))
            else:
                print(await self.stop_target(args[0]))
        elif cmd == "connect-all":
            for name in self.config.targets:
                with contextlib.suppress(Exception):
                    print(name, (await self.connect_target(name))["state"])
        elif cmd == "stop-all":
            for name in list(self.runtimes):
                with contextlib.suppress(Exception):
                    await self.stop_target(name)
            print("all stopped")
        elif cmd == "reload":
            print(await self.reload())
        elif cmd == "clients":
            self._print_clients_table()
        elif cmd == "client":
            if not args:
                print("usage: client <name>")
                return True
            self._print_client(args[0])
        elif cmd in ("client-refresh", "client-connect", "client-stop"):
            await self._client_target_action(cmd, args)
        elif cmd == "enrollments":
            self._print_enrollments()
        elif cmd in ("approve", "deny"):
            if not args:
                print(f"usage: {cmd} <request-id>")
                return True
            await self._console_enrollment(cmd, args[0])
        elif cmd == "client-kill":
            if not args:
                print("usage: client-kill <client>")
                return True
            closed = await self.sessions.close_all_for_client(args[0], reason="console")
            print(f"closed {closed} session(s) for client {args[0]}")
        elif cmd == "sessions":
            target = args[0] if args else None
            for s in self.sessions.list_all(target):
                print(
                    f"{s['session_id']}  client={s['client']}  target={s['target']}  "
                    f"age={s['age']}  idle={s['idle']}  {s['connection']}"
                )
        elif cmd == "close-session":
            if not args:
                print("usage: close-session <session-id>")
                return True
            await self.sessions.close(args[0], owner=None, reason="console")
            print(f"closed {args[0]}")
        else:
            print(f"unknown command: {cmd}")
        return True

    def _client_targets(self, cfg) -> list[str]:
        if cfg.allow_all:
            return list(self.config.targets)
        return list(cfg.targets)

    def _print_clients_table(self) -> None:
        print(f"{'CLIENT':<18}{'LABEL':<22}{'TARGETS':<40}{'SESSIONS'}")
        for cfg in self.config.clients.values():
            targets = "*" if cfg.allow_all else ",".join(self._client_targets(cfg))
            print(
                f"{cfg.client_id:<18}{(cfg.label or '-'):<22}{targets:<40}"
                f"{self.sessions.count_for_client(cfg.client_id)}"
            )

    def _print_client(self, name: str) -> None:
        cfg = self.config.clients.get(name)
        if cfg is None:
            print(f"unknown client: {name}")
            return
        digest = cfg.token_sha256.split(":", 1)[-1]
        print(f"client:      {cfg.client_id}")
        print(f"label:       {cfg.label or '-'}")
        print(f"token fp:    sha256:{digest[:12]}")
        print(f"allow_all:   {cfg.allow_all}")
        print(f"targets:     {', '.join(self._client_targets(cfg)) or '-'}")
        print(f"sessions:    {self.sessions.count_for_client(name)}")
        for s in self.sessions.list(client_id=name):
            print(
                f"  {s['session_id']}  target={s['target']}  idle={s['idle']}  "
                f"{s['connection']}"
            )

    def _print_enrollments(self) -> None:
        pending = self.enrollments.list_pending()
        if not pending:
            print("no pending enrollment requests")
            return
        print(f"{'REQUEST':<14}{'STATUS':<10}{'CLIENT':<22}{'TARGETS':<28}SOURCE")
        for r in pending:
            print(
                f"{r.request_id:<14}{r.status:<10}{r.client_id:<22}"
                f"{','.join(r.targets) or '-':<28}{r.source or '-'}"
            )

    async def _console_enrollment(self, action: str, request_id: str) -> None:
        try:
            if action == "approve":
                pending = await self.approve_enrollment(request_id)
                print(
                    f"approved {pending.request_id}: client {pending.client_id!r} "
                    f"targets {', '.join(pending.targets) or '(none)'}; the "
                    "requester will receive its token on the next poll"
                )
            else:
                pending = self.enrollments.deny(request_id)
                print(f"denied {pending.request_id}")
        except (ConfigError, EnrollmentError) as exc:
            print(f"error: {exc}")

    async def _client_target_action(self, cmd: str, args: list[str]) -> None:
        if not args:
            print(f"usage: {cmd} <client> [target]")
            return
        action, name = cmd[len("client-"):], args[0]
        cfg = self.config.clients.get(name)
        if cfg is None:
            print(f"unknown client: {name}")
            return
        allowed = set(self._client_targets(cfg))
        targets = [args[1]] if len(args) > 1 else sorted(allowed)
        for target in targets:
            if target not in allowed:
                print(f"client {name} has no access to {target}")
                continue
            if target not in self.config.targets:
                print(f"unknown target: {target}")
                continue
            try:
                if action == "refresh":
                    result = await self.refresh_target(target)
                elif action == "connect":
                    result = await self.connect_target(target)
                else:
                    result = await self.stop_target(target)
                print(f"{name}: {target} -> {result['state']}")
            except Exception as exc:  # noqa: BLE001
                print(f"{name}: {target} -> error: {exc}")

    def _print_status_table(self) -> None:
        print(
            f"{'TARGET':<16}{'STATE':<14}{'ROUTE':<14}{'LOCAL':<8}"
            f"{'SHARING':<11}{'NODE_INFO':<42}{'AGENT':<30}{'CLIENTS':<8}UPTIME"
        )
        now = datetime.now(timezone.utc)
        for name in self.config.targets:
            runtime = self.runtimes[name]
            uptime = "-"
            if runtime.connected_since:
                delta = now - runtime.connected_since
                uptime = str(delta).split(".")[0]
            node_info = "; ".join(self.config.targets[name].node_info or [])
            if len(node_info) > 40:
                node_info = node_info[:39] + "\u2026"
            agent = ", ".join(
                f"{a}@{m}" for a, m in self.config.targets[name].agent
            )
            if len(agent) > 28:
                agent = agent[:27] + "\u2026"
            print(
                f"{name:<16}{runtime.state:<14}"
                f"{runtime.active_route or '-':<14}"
                f"{runtime.local_port if runtime.local_port else '-':<8}"
                f"{self.config.targets[name].sharing:<11}"
                f"{node_info or '-':<42}"
                f"{agent or '-':<30}"
                f"{self.sessions.count_for_target(name):<8}{uptime}"
            )


def _decode(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


@web.middleware
async def _error_middleware(request: web.Request, handler):
    try:
        return await handler(request)
    except web.HTTPException:
        raise
    except AuthError as exc:
        raise web.HTTPUnauthorized(text=str(exc)) from exc
    except ForbiddenTarget:
        # Do not leak whether an unauthorized target exists.
        raise web.HTTPForbidden(text="forbidden") from None
    except InteractiveAuthRequired as exc:
        raise web.HTTPServiceUnavailable(text=str(exc)) from exc
    except (HostKeyError, SSHError, TunnelError) as exc:
        raise web.HTTPBadGateway(text=str(exc)) from exc
    except asyncssh.Error as exc:
        # Defense in depth: an SSH failure that escapes a handler-specific
        # mapping must be an upstream failure (502), never a raw 500.
        raise web.HTTPBadGateway(text=str(exc)) from exc
    except SessionNotFound:
        raise web.HTTPNotFound(text="unknown session") from None
    except SessionLimitError as exc:
        raise web.HTTPTooManyRequests(text=str(exc)) from exc
    except SessionForbidden:
        raise web.HTTPForbidden(text="forbidden") from None
    except SessionError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    except JobAccessError:
        # One public answer for an unknown or foreign job id on any job-scoped
        # file/exec path; never distinguish the two.
        raise web.HTTPNotFound(text=JOB_ACCESS_DENIED_MESSAGE) from None
    except JobNotReady as exc:
        raise web.HTTPConflict(text=str(exc)) from exc
    except jobcore.QuotaExceeded as exc:
        raise web.HTTPTooManyRequests(text=str(exc)) from exc
    except (jobcore.JobNotFound, jobcore.InvalidJobId):
        # Foreign and unknown ids already collapse to the same 404 in the
        # handler; this is the defence-in-depth mapping.
        raise web.HTTPNotFound(text=JOB_NOT_FOUND_TEXT) from None
    except jobcore.RequestIdConflict as exc:
        raise web.HTTPConflict(text=str(exc)) from exc
    except jobcore.SubmissionRejected as exc:
        raise web.HTTPBadGateway(text=str(exc)) from exc
    except jobcore.SubmissionUncertain as exc:
        raise web.HTTPServiceUnavailable(text=str(exc)) from exc
    except jobcore.JobError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    except ConfigError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc


# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="computeMCP-gateway")
    parser.add_argument(
        "--config",
        type=Path,
        default=default_config_path(),
        help="gateway config.toml (default: "
        "~/.config/computeMCP-gateway/config.toml)",
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--listen", help="override server.listen")
    parser.add_argument("--port", type=int, help="override server.port")
    parser.add_argument(
        "--transport",
        choices=["tunnel", "direct"],
        help="force a transport for all targets (development/testing only)",
    )
    parser.add_argument(
        "--no-console",
        action="store_true",
        help="run headless (required under systemd)",
    )
    parser.add_argument(
        "--token-file",
        help="path to a tokens.toml with per-client token hashes "
        "(overrides [auth] token_file)",
    )
    parser.add_argument(
        "--generate-tokens",
        nargs="?",
        const=str(default_token_path()),
        metavar="OUT",
        help="generate a fresh high-entropy token for every configured client, "
        "write token hashes to OUT (default: "
        "~/.config/computeMCP-gateway/tokens.toml), print the plaintext "
        "tokens once, and exit",
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="interactively create the initial gateway configuration (server, "
        "auth, a hashed client token and optionally a target) and exit",
    )
    parser.add_argument(
        "--config-dir",
        metavar="DIR",
        help="directory for --bootstrap (default: the directory of --config)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="with --bootstrap, overwrite an existing configuration file",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="refuse interactive prompts (for scripts); --bootstrap then fails "
        "instead of waiting for input",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def generate_tokens(config_path: str, out_path: str, token_file: str | None) -> int:
    """Mint one token per configured client; store only hashes on the gateway.

    The plaintext is printed once for the operator to inject into the matching
    Terok project's credential store.  The gateway TOML keeps only hashes, so a
    leaked config file does not reveal usable tokens.
    """
    import tomllib

    from .auth import hash_token, new_token
    from .config import ConfigError

    try:
        with Path(config_path).open("rb") as handle:
            raw = tomllib.load(handle)
    except FileNotFoundError as exc:
        raise ConfigError(f"configuration file not found: {config_path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"malformed TOML in {config_path}: {exc}") from exc

    client_ids = list(raw.get("clients", {}))
    if not client_ids:
        raise ConfigError("no [clients.*] configured; nothing to generate")

    token_path = Path(out_path)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Generated by computeMCP-gateway --generate-tokens.",
        "# Contains sha256 hashes only; keep plaintext tokens out of this file.",
        "",
        "[tokens]",
    ]
    printed = []
    for client_id in sorted(client_ids):
        token = new_token()
        lines.append(f'"{client_id}" = "{hash_token(token)}"')
        printed.append((client_id, token))
    token_path.write_text("\n".join(lines) + "\n")
    try:
        token_path.chmod(0o600)
    except OSError:
        pass
    print(f"Wrote hashed tokens for {len(printed)} client(s) to {token_path}")
    print("Inject these plaintext tokens into the matching Terok projects:")
    for client_id, token in printed:
        print(f"  {client_id}: {token}")
    print(
        "\nSet [auth] token_file (or pass --token-file), then set in each Terok "
        "container:\n  COMPUTEMCP_GATEWAY=<gateway url>\n  COMPUTEMCP_TOKEN=<its token>"
    )
    print(
        "\nAlternatively, a container can request its own token interactively with\n"
        "  computeMCP-handshake <client-id> --port <gateway port> [--system hal,fwk394]\n"
        "and you approve it on the gateway console ('approve <id>')."
    )
    return 0


def _print_route_summary(runtime: TargetRuntime) -> str:
    if runtime.state == "connected":
        return f"connected via {runtime.active_route or 'direct'}"
    return f"{runtime.state}" + (f" ({runtime.last_error})" if runtime.last_error else "")


async def _amain(args: argparse.Namespace) -> int:
    config = load_config(args.config, token_file=args.token_file)
    if args.listen:
        config = dataclasses.replace(
            config, server=dataclasses.replace(config.server, listen=args.listen)
        )
    if args.port:
        config = dataclasses.replace(
            config, server=dataclasses.replace(config.server, port=args.port)
        )
    gateway = Gateway(config, transport_override=args.transport)
    app = gateway.create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.server.listen, config.server.port)
    await site.start()
    log.info(
        "gateway listening on http://%s:%d (%d targets, %d clients)",
        config.server.listen,
        config.server.port,
        len(config.targets),
        len(config.clients),
    )

    stop_event = asyncio.Event()

    def _signal(*_):
        stop_event.set()

    async def _reload_signal():
        try:
            report = await gateway.reload()
            log.info("reloaded via SIGHUP: %s", report)
        except Exception:  # noqa: BLE001
            log.exception("reload via SIGHUP failed; keeping previous configuration")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _signal)
    if hasattr(signal, "SIGHUP"):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(
                signal.SIGHUP, lambda: asyncio.create_task(_reload_signal())
            )

    if args.no_console or not sys.stdin.isatty():
        await stop_event.wait()
    else:
        console = asyncio.create_task(gateway.run_console())
        done, _pending = await asyncio.wait(
            {console, asyncio.create_task(stop_event.wait())},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if console in done:
            stop_event.set()

    await runner.cleanup()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        if args.generate_tokens:
            return generate_tokens(args.config, args.generate_tokens, args.token_file)
        if args.bootstrap:
            from .setup import Wizard, WizardAbort, run_bootstrap

            config_arg = args.config or default_config_path()
            if args.config_dir:
                target_path = Path(args.config_dir) / Path(default_config_path()).name
            else:
                target_path = Path(config_arg)
            wizard = Wizard(terminal=False if args.non_interactive else None)
            try:
                return run_bootstrap(target_path, force=args.force, wizard=wizard)
            except WizardAbort as exc:
                print(f"bootstrap: {exc}", file=sys.stderr)
                return 2
        return asyncio.run(_amain(args))
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

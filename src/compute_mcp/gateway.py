# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Gateway daemon: target state machine, HTTP API and interactive console."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import getpass
import logging
import posixpath
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from aiohttp import web

from . import __version__
from .auth import AuthError, Authenticator, Client, ForbiddenTarget
from .allocation import (
    compute_plan,
    plan_summary,
    render_args,
    validate_conflicts,
)
from .bundle import BundleError, public_key_for
from .config import (
    ConfigError,
    GatewayConfig,
    TargetConfig,
    append_client,
    append_token_hash,
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
from .files import sftp_client
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
from .tunnel import Tunnel, TunnelError, TunnelManager

log = logging.getLogger("compute_mcp.gateway")

RECOVERY_POLL_SECONDS = 5.0


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
    container = target.container
    env["COMPUTEMCP_CONTAINER_RUNTIME"] = container.runtime if container else ""
    env["COMPUTEMCP_STORAGE_ROOT"] = (container.storage_root or "") if container else ""
    env["COMPUTEMCP_IMAGE"] = (container.image or "") if container else ""
    env["COMPUTEMCP_GPU_VENDORS"] = ",".join(container.gpus) if container else ""
    env["COMPUTEMCP_HOST_HOME"] = (container.host_home or "") if container else ""
    env["COMPUTEMCP_SANDBOX"] = (
        "true" if container is not None and container.sandbox else "false"
    )
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

    def __init__(self, target: str) -> None:
        super().__init__(
            f"target {target!r} requires interactive authentication (second "
            f"factor); run target-connect --2fa <secret> {target}"
        )
        self.target = target


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
    def __init__(self, config: GatewayConfig, transport_override: str | None = None):
        validate_config_allocations(config)
        self.config = config
        self.transport_override = transport_override
        self._apply_transport_override()
        self.auth = Authenticator(config.clients)
        self.backend = SSHBackend()
        self.tunnels = TunnelManager(config.ssh)
        self.sessions = SessionManager(
            self.backend,
            self._connection_provider,
            config.sessions,
            dedicated_provider=self._dedicated_connection_provider,
        )
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

    def _endpoint(self, runtime: TargetRuntime) -> tuple[str, int]:
        target = self.config.targets[runtime.name]
        if target.transport.kind == "direct":
            return target.transport.remote_host, target.transport.remote_port
        if runtime.local_port is None:
            raise TunnelError(f"target {runtime.name!r} has no active tunnel")
        return "127.0.0.1", runtime.local_port

    # -- lifecycle ---------------------------------------------------------
    async def start(self) -> None:
        self.sessions.start()
        self._watch_task = asyncio.create_task(self._watch_tunnels())
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
        for name in list(self.runtimes):
            with contextlib.suppress(Exception):
                await self.stop_target(name)
        await self.sessions.stop()
        await self.backend.close_all()

    async def _safe_connect(self, name: str) -> None:
        with contextlib.suppress(TunnelError, SSHError, HostKeyError):
            await self.ensure_connected(name)

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

    # -- state machine -----------------------------------------------------
    async def ensure_connected(self, name: str) -> TargetRuntime:
        target = self._target(name)
        async with self._lock(name):
            runtime = self.runtimes[name]
            if runtime.state == "connected":
                if target.transport.kind == "direct":
                    return runtime
                if runtime.tunnel and runtime.tunnel.is_alive():
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
        runtime.awaiting_factor = False
        return runtime

    def _mark_lost(self, runtime: TargetRuntime, reason: str) -> None:
        if runtime.tunnel is not None:
            self.tunnels.release(runtime.tunnel)
            runtime.tunnel = None
        runtime.state = "disconnected"
        runtime.active_route = None
        runtime.local_port = None
        runtime.last_error = reason
        runtime.connected_since = None

    async def connect_target(
        self,
        name: str,
        factor: str | None = None,
        *,
        overrides: dict[str, Any] | None = None,
    ) -> dict:
        target = self._target(name)
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
        async with self._lock(name):
            runtime = self.runtimes[name]
            # Fail closed before any teardown when a second factor is needed
            # but not supplied for this request.
            if target.interactive_auth and factor is None:
                return self._interactive_skip(runtime)
            warning = self._factor_warning(target, factor)
            if warning is not None:
                factor = None  # key-only downstream
            # A refresh without explicit overrides re-applies the settings the
            # target already retained, so recovery never silently reverts to
            # defaults.  Resolve/validate before any teardown: a bad override
            # must not destroy a still-active allocation.
            effective = (
                overrides
                if overrides is not None
                else (runtime.resolved_overrides or None)
            )
            if effective or self._env_configured(target):
                self._resolve_allocation(target, effective)
            # Release the old allocation before provisioning a new one.  The
            # route connection is still alive here, so the command can run.
            await self._run_close_command(name)
            await self._stop_locked(name)
            await self.backend.disconnect(name)
            await self.sessions.close_for_target(name, reason="refresh")
            await self._connect_locked(
                target, runtime, factor=factor, overrides=effective
            )
            result = runtime.public(self.sessions.count_for_target(name))
        if warning:
            result["warning"] = warning
        return result

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
            f"target-connect --2fa <secret> {runtime.name}"
        )
        log.warning("target %s: %s", runtime.name, message)
        runtime.awaiting_factor = True
        result = runtime.public(self.sessions.count_for_target(runtime.name))
        result["warning"] = message
        return result

    async def stop_target(self, name: str) -> dict:
        self._target(name)
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
        if connection is None:
            log.warning(
                "target %s: close_command skipped, no live route connection",
                name,
            )
            return
        route = runtime.active_route or (
            target.transport.ssh_targets[0] if target.transport.ssh_targets else "direct"
        )
        try:
            await self.tunnels.run_close_command(target, route, connection=connection)
        except Exception as exc:  # noqa: BLE001 - advisory, never fatal
            log.warning("target %s: close_command failed: %s", name, exc)

    async def _stop_locked(self, name: str) -> None:
        runtime = self.runtimes[name]
        if runtime.recovery_task is not None:
            runtime.recovery_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runtime.recovery_task
            runtime.recovery_task = None
        if runtime.tunnel is not None:
            await runtime.tunnel.stop()
            self.tunnels.release(runtime.tunnel)
        runtime.tunnel = None
        runtime.state = "disconnected"
        runtime.active_route = None
        runtime.local_port = None
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
            if target.interactive_auth and self.runtimes[name].awaiting_factor:
                raise InteractiveAuthRequired(name) from None
            raise
        runtime = self.runtimes[name]
        if (
            target.interactive_auth
            and runtime.awaiting_factor
            and runtime.state != "connected"
        ):
            raise InteractiveAuthRequired(name)
        host, port = self._endpoint(runtime)
        dedicated = force_dedicated or target.connect_mode == "dedicated"

        async def attempt():
            if dedicated:
                return await self.backend.open_connection(target, host, port)
            return await self.backend.connection(target, host, port)

        # "always": run the trusted recovery command before every open.  It must
        # be a no-op when the container already runs.
        if target.connect_command and target.connect_command_mode == "always":
            route = runtime.active_route or (
                target.transport.ssh_targets[0] if target.transport.ssh_targets else "direct"
            )
            connection = runtime.tunnel.connection if runtime.tunnel else None
            await self.tunnels.run_connect_command(target, route, connection=connection)

        try:
            conn = await attempt()
            return target, conn
        except SSHError as first:
            # The tunnel is up but the container is unreachable (e.g. it is
            # stopped).  In "on_failure" mode run the recovery command on the
            # remote host and retry once.
            if target.connect_command and target.connect_command_mode == "on_failure":
                route = runtime.active_route or (
                    target.transport.ssh_targets[0] if target.transport.ssh_targets else "direct"
                )
                log.info(
                    "target %s unreachable (%s); running connect_command",
                    name, first,
                )
                connection = runtime.tunnel.connection if runtime.tunnel else None
                await self.tunnels.run_connect_command(target, route, connection=connection)
                # Drop any cached (dead) connection before retrying.
                await self.backend.disconnect(name)
                try:
                    conn = await attempt()
                    return target, conn
                except SSHError:
                    pass
            if target.interactive_auth:
                raise InteractiveAuthRequired(name) from None
            raise first

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

    async def h_connect(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        name = request.match_info["target"]
        client.require_target(name)
        body = await self._request_json(request)
        factor = self._body_factor(body)
        overrides = self._body_overrides(body)
        return web.json_response(
            await self.connect_target(name, factor=factor, overrides=overrides)
        )

    async def h_refresh(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        name = request.match_info["target"]
        client.require_target(name)
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
        return web.json_response(await self.stop_target(name))

    async def h_exec(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        body = await request.json()
        name = body.get("target")
        command = body.get("command")
        if not name or not isinstance(command, str):
            raise web.HTTPBadRequest(text="'target' and 'command' are required")
        client.require_target(name)
        target, conn = await self._connection_provider(name)
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
            if target.connect_mode == "dedicated":
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
        session = await self.sessions.create(
            client.client_id,
            target,
            cwd=body.get("cwd"),
            columns=int(body.get("columns", 160)),
            rows=int(body.get("rows", 50)),
        )
        return web.json_response(
            {"session_id": session.id, "target": session.target, "created": True}
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
            async with sftp_client(conn) as sftp:
                yield sftp
        finally:
            if owned:
                await self.backend.close_connection(conn)

    async def h_file_stat(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        path = request.query["path"]
        async with self._sftp_session(client, request) as sftp:
            return web.json_response(await files_stat(sftp, path))

    async def h_file_list(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        path = request.query["path"]
        async with self._sftp_session(client, request) as sftp:
            return web.json_response({"path": path, "entries": await files_list(sftp, path)})

    async def h_file_read(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        path = request.query["path"]
        encoding = request.query.get("encoding")
        if encoding == "stream":
            # Stream raw bytes without buffering or round-tripping through JSON.
            target_name = request.query.get("target")
            client.require_target(target_name)
            target, conn = await self._connection_provider(target_name)
            owned = target.connect_mode == "dedicated"
            try:
                async with sftp_client(conn) as sftp:
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
                    await resp.prepare(request)
                    async with sftp.open(path, "rb") as handle:
                        while True:
                            chunk = await handle.read(262144)
                            if not chunk:
                                break
                            await resp.write(chunk)
                    await resp.write_eof()
                    return resp
            finally:
                if owned:
                    await self.backend.close_connection(conn)
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

    async def h_file_write(self, request: web.Request) -> web.Response:
        import base64

        client = self._auth(request)
        body = await request.json()
        path = body["path"]
        if body.get("encoding") == "base64":
            content = base64.b64decode(body.get("content", ""))
        else:
            content = body.get("content", "").encode("utf-8")
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
        content_length = request.content_length
        async with self._sftp_session(client, request) as sftp:
            if request.query.get("parents", "false").lower() in ("1", "true", "yes"):
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
        path = (await request.json())["path"]
        async with self._sftp_session(client, request) as sftp:
            return web.json_response(await files_mkdir(sftp, path))

    async def h_file_remove(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        path = (await request.json())["path"]
        async with self._sftp_session(client, request) as sftp:
            return web.json_response(await files_remove(sftp, path))

    async def h_file_rename(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        body = await request.json()
        async with self._sftp_session(client, request) as sftp:
            return web.json_response(
                await files_rename(sftp, body["source"], body["destination"])
            )

    async def h_file_chmod(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        body = await request.json()
        mode = files_parse_mode(body["mode"])
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
        router.add_post("/v1/targets/{target}/connect", self.h_connect)
        router.add_post("/v1/targets/{target}/refresh", self.h_refresh)
        router.add_post("/v1/targets/{target}/preview", self.h_preview)
        router.add_post("/v1/targets/{target}/stop", self.h_stop)
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
    def _parse_factor_args(args: list[str]) -> tuple[str | None, list[str]]:
        """Split ``[--2fa SECRET]`` out of console args, preserving order.

        Supports both ``connect --2fa SECRET <target>`` and
        ``connect <target> --2fa SECRET``.  Raises :class:`ValueError` when
        ``--2fa`` has no value.  The secret is returned, never logged.
        """
        factor: str | None = None
        rest: list[str] = []
        index = 0
        while index < len(args):
            token = args[index]
            if token == "--2fa":
                if index + 1 >= len(args) or args[index + 1].startswith("--"):
                    raise ValueError("--2fa requires a value")
                factor = args[index + 1]
                index += 2
                continue
            rest.append(token)
            index += 1
        return factor, rest

    async def _console_command(self, line: str) -> bool:
        parts = line.split()
        cmd, args = parts[0], parts[1:]
        if cmd in ("quit", "exit"):
            return False
        if cmd == "help":
            print(
                "targets | status [target] | connect [--2fa SECRET] <t> | "
                "refresh [--2fa SECRET] <t> | reconnect <t> | stop <t> | "
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
            try:
                factor, rest = self._parse_factor_args(list(args))
            except ValueError:
                print(f"usage: {cmd} <target> [--2fa SECRET]")
                return True
            if len(rest) != 1:
                print(f"usage: {cmd} <target> [--2fa SECRET]")
                return True
            if cmd == "connect":
                print(await self.connect_target(rest[0], factor=factor))
            else:
                print(await self.refresh_target(rest[0], factor=factor))
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
    except SessionNotFound:
        raise web.HTTPNotFound(text="unknown session") from None
    except SessionLimitError as exc:
        raise web.HTTPTooManyRequests(text=str(exc)) from exc
    except SessionForbidden:
        raise web.HTTPForbidden(text="forbidden") from None
    except SessionError as exc:
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

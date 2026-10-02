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
import re
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import web

from . import __version__
from .auth import AuthError, Authenticator, Client, ForbiddenTarget
from .config import (
    ConfigError,
    GatewayConfig,
    TargetConfig,
    load_config,
)
from .files import sftp_client
from .files import list_dir as files_list
from .files import mkdir as files_mkdir
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
from .tunnel import Tunnel, TunnelError, TunnelManager, probe

log = logging.getLogger("terok_compute.gateway")

RECOVERY_POLL_SECONDS = 5.0


class ProvisionError(RuntimeError):
    """A target's provisioning command failed or printed no usable endpoint."""


class InteractiveAuthRequired(RuntimeError):
    """A target needs interactive authentication but no console is available."""

    def __init__(self, target: str) -> None:
        super().__init__(
            f"target {target!r} requires interactive authentication (e.g. 2FA), "
            "but the gateway is not running an interactive console or is "
            "answering a non-interactive request. Run the gateway in the "
            "foreground console and retry."
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
        }


class Gateway:
    def __init__(self, config: GatewayConfig, transport_override: str | None = None):
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
            if target.auto_connect:
                asyncio.create_task(self._safe_connect(name))

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

    # -- state machine -----------------------------------------------------
    async def ensure_connected(self, name: str) -> TargetRuntime:
        target = self._target(name)
        async with self._lock(name):
            runtime = self.runtimes[name]
            if runtime.state == "connected":
                if target.transport.kind == "direct":
                    return runtime
                if runtime.tunnel and runtime.tunnel.process and runtime.tunnel.process.returncode is None:
                    return runtime
                self._mark_lost(runtime, "tunnel process exited")
            return await self._connect_locked(target, runtime)

    async def _connect_locked(self, target: TargetConfig, runtime: TargetRuntime) -> TargetRuntime:
        runtime.state = "connecting"
        runtime.last_error = None
        transport = target.transport
        if target.provision_command:
            try:
                transport = await self._provision(target)
            except ProvisionError as exc:
                runtime.state = "failed"
                runtime.last_error = str(exc)
                runtime.active_route = None
                runtime.local_port = None
                raise TunnelError(str(exc)) from exc
            runtime.provisioned_endpoint = (
                f"{transport.remote_host}:{transport.remote_port}"
            )
        try:
            tunnel = await self.tunnels.connect(
                target,
                on_route=lambda route, exc: log.debug("route %s failed: %s", route, exc),
                transport=transport,
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
        runtime.connected_since = datetime.now(timezone.utc)
        runtime.needs_refresh = False
        runtime.backoff = 0.0
        runtime.state = "connected"
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

    async def connect_target(self, name: str) -> dict:
        target = self._target(name)
        async with self._lock(name):
            runtime = self.runtimes[name]
            if runtime.state == "connected":
                return runtime.public(self.sessions.count_for_target(name))
            await self._connect_locked(target, runtime)
        await self._preauth_if_interactive(name)
        return self.runtimes[name].public(self.sessions.count_for_target(name))

    async def _preauth_if_interactive(self, name: str) -> None:
        """Force the container handshake now so the console can prompt for 2FA.

        Establishing the tunnel does not authenticate to the container; doing it
        here makes ``connect``/``refresh`` an accurate "is this target usable"
        check and collects any second factor while the operator is present.
        """
        target = self.config.targets[name]
        if not target.interactive_auth:
            return
        host, port = self._endpoint(self.runtimes[name])
        prompter = self._make_prompter(name)
        try:
            if target.connect_mode == "dedicated":
                conn = await self.backend.open_connection(target, host, port, prompter)
                await self.backend.close_connection(conn)
            else:
                await self.backend.connection(target, host, port, prompter)
        except SSHError:
            if not self._interactive:
                raise InteractiveAuthRequired(name) from None
            raise

    async def refresh_target(self, name: str) -> dict:
        target = self._target(name)
        async with self._lock(name):
            runtime = self.runtimes[name]
            await self._stop_locked(name)
            await self.backend.disconnect(name)
            await self.sessions.close_for_target(name, reason="refresh")
            await self._connect_locked(target, runtime)
        await self._preauth_if_interactive(name)
        return self.runtimes[name].public(self.sessions.count_for_target(name))

    async def stop_target(self, name: str) -> dict:
        self._target(name)
        async with self._lock(name):
            await self._stop_locked(name)
        await self.backend.disconnect(name)
        await self.sessions.close_for_target(name, reason="target stopped")
        return self.runtimes[name].public(self.sessions.count_for_target(name))

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

    # -- recovery ----------------------------------------------------------
    async def _watch_tunnels(self) -> None:
        try:
            while not self._stopping:
                await asyncio.sleep(RECOVERY_POLL_SECONDS)
                for name, runtime in list(self.runtimes.items()):
                    target = self.config.targets.get(name)
                    if target is None or target.transport.kind == "direct":
                        continue
                    if runtime.state == "connected" and runtime.tunnel is not None:
                        proc = runtime.tunnel.process
                        if proc is not None and proc.returncode is not None:
                            await self._handle_loss(name, f"tunnel exited rc={proc.returncode}")
                    elif runtime.state in ("disconnected", "failed") and runtime.needs_refresh:
                        asyncio.create_task(self._reconnect_with_backoff(name))
        except asyncio.CancelledError:
            raise

    async def _handle_loss(self, name: str, reason: str) -> None:
        async with self._lock(name):
            runtime = self.runtimes[name]
            if runtime.state != "connected":
                return
            log.warning("target %s lost: %s", name, reason)
            self._mark_lost(runtime, reason)
            runtime.needs_refresh = True
        await self.backend.disconnect(name)
        await self.sessions.close_for_target(name, reason="tunnel lost")
        asyncio.create_task(self._reconnect_with_backoff(name))

    async def _reconnect_with_backoff(self, name: str) -> None:
        target = self.config.targets.get(name)
        if target is None:
            return
        runtime = self.runtimes[name]
        runtime.backoff = max(runtime.backoff, target.connect_backoff_initial)
        try:
            while not self._stopping:
                await asyncio.sleep(runtime.backoff)
                if self.runtimes[name].state == "connected":
                    return
                try:
                    await self.ensure_connected(name)
                    return
                except (TunnelError, SSHError, HostKeyError) as exc:
                    runtime.last_error = str(exc)
                    runtime.backoff = min(runtime.backoff * 2, target.connect_backoff_max)
        except asyncio.CancelledError:
            raise

    # -- dynamic provisioning (Slurm and similar) ------------------------
    async def _provision(self, target: TargetConfig) -> "TransportConfig":
        """Run the target's trusted provisioning command and parse an endpoint.

        The command must be from the trusted TOML (never client input).  It is
        executed without a shell.  It should start/attach a job (e.g. a Slurm
        allocation) and arrange a forward to the container's SSH port, then
        print ``host:port`` (or ``ENDPOINT host:port``) on stdout.  The gateway
        tunnels through an ssh alias to that discovered endpoint.
        """
        argv = list(target.provision_command)
        log.info("provisioning target %s: %s", target.name, " ".join(argv))
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except (OSError, ValueError) as exc:
            raise ProvisionError(
                f"target {target.name!r} provisioning command could not start: {exc}"
            ) from exc
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=target.provision_timeout
            )
        except asyncio.TimeoutError:
            proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()
            raise ProvisionError(
                f"target {target.name!r} provisioning timed out after "
                f"{target.provision_timeout:.0f}s"
            ) from None
        if proc.returncode != 0:
            detail = stderr.decode(errors="replace").strip() or "no stderr"
            raise ProvisionError(
                f"target {target.name!r} provisioning exited {proc.returncode}: {detail}"
            )
        endpoint = _parse_provision_endpoint(stdout.decode(errors="replace"))
        if endpoint is None:
            raise ProvisionError(
                f"target {target.name!r} provisioning printed no host:port endpoint"
            )
        host, port = endpoint
        log.info("target %s provisioned endpoint %s:%d", target.name, host, port)
        return dataclasses.replace(
            target.transport, remote_host=host, remote_port=port
        )

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

        Raises :class:`InteractiveAuthRequired` when the target needs 2FA but
        the gateway is not interactive, instead of a generic SSH error.
        """
        target = self._target(name)
        await self.ensure_connected(name)
        host, port = self._endpoint(self.runtimes[name])
        prompter = self._make_prompter(name) if target.interactive_auth else None
        dedicated = force_dedicated or target.connect_mode == "dedicated"
        try:
            if dedicated:
                conn = await self.backend.open_connection(target, host, port, prompter)
            else:
                conn = await self.backend.connection(target, host, port, prompter)
            return target, conn
        except SSHError:
            if target.interactive_auth and not self._interactive:
                raise InteractiveAuthRequired(name) from None
            raise

    async def _connection_provider(self, name: str):
        return await self._open_container_conn(name)

    async def _dedicated_connection_provider(self, name: str):
        return await self._open_container_conn(name, force_dedicated=True)

    # -- status ------------------------------------------------------------
    def public_status(self, name: str) -> dict:
        return self.runtimes[name].public(self.sessions.count_for_target(name))

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
        """
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
                if target.auto_connect:
                    asyncio.create_task(self._safe_connect(name))
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

    async def h_connect(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        name = request.match_info["target"]
        client.require_target(name)
        return web.json_response(await self.connect_target(name))

    async def h_refresh(self, request: web.Request) -> web.Response:
        client = self._auth(request)
        name = request.match_info["target"]
        client.require_target(name)
        return web.json_response(await self.refresh_target(name))

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
        try:
            result = await self.backend.run(
                conn, command, cwd=body.get("cwd"), timeout=float(timeout)
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
        return web.json_response(
            self.sessions.read(request.match_info["session"], client.client_id, max_bytes)
        )

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
        router.add_get("/v1/files/stat", self.h_file_stat)
        router.add_get("/v1/files/list", self.h_file_list)
        router.add_get("/v1/files/read", self.h_file_read)
        router.add_put("/v1/files/write", self.h_file_write)
        router.add_put("/v1/files/upload", self.h_file_upload)
        router.add_post("/v1/files/mkdir", self.h_file_mkdir)
        router.add_post("/v1/files/remove", self.h_file_remove)
        router.add_post("/v1/files/rename", self.h_file_rename)
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
        print(
            "terok-compute-gateway console. Type 'help' for commands.\n"
            "Targets with interactive_auth will prompt for password/OTP here."
        )
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

    async def _console_command(self, line: str) -> bool:
        parts = line.split()
        cmd, args = parts[0], parts[1:]
        if cmd in ("quit", "exit"):
            return False
        if cmd == "help":
            print(
                "targets | status [target] | connect <t> | refresh <t> | "
                "reconnect <t> | stop <t> | connect-all | stop-all | reload |\n"
                "clients | client <name> | client-refresh <name> | "
                "client-connect <name> | client-stop <name> | client-kill <name> |\n"
                "sessions [target] | close-session <id> | quit"
            )
        elif cmd == "targets":
            for name in self.config.targets:
                print(name)
        elif cmd == "status":
            if args:
                print(self.public_status(args[0]))
            else:
                self._print_status_table()
        elif cmd == "connect":
            print(await self.connect_target(args[0]))
        elif cmd == "refresh":
            print(await self.refresh_target(args[0]))
        elif cmd == "reconnect":
            print(await self.refresh_target(args[0]))
        elif cmd == "stop":
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
            self._print_client(args[0])
        elif cmd in ("client-refresh", "client-connect", "client-stop"):
            await self._client_target_action(cmd, args)
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
        print(f"{'TARGET':<16}{'STATE':<14}{'ROUTE':<14}{'LOCAL':<8}{'CLIENTS':<8}UPTIME")
        now = datetime.now(timezone.utc)
        for name in self.config.targets:
            runtime = self.runtimes[name]
            uptime = "-"
            if runtime.connected_since:
                delta = now - runtime.connected_since
                uptime = str(delta).split(".")[0]
            print(
                f"{name:<16}{runtime.state:<14}"
                f"{runtime.active_route or '-':<14}"
                f"{runtime.local_port if runtime.local_port else '-':<8}"
                f"{self.sessions.count_for_target(name):<8}{uptime}"
            )


_ENDPOINT_RE = re.compile(
    r"^(?:ENDPOINT\s+)?(?P<host>[A-Za-z0-9_.\-]+):(?P<port>\d{1,5})\s*$",
    re.IGNORECASE,
)


def _parse_provision_endpoint(output: str) -> tuple[str, int] | None:
    """Return the first ``host:port`` line from provisioning stdout.

    A leading ``ENDPOINT`` marker is accepted but optional, so a script can
    print other diagnostic lines and one final endpoint line.
    """
    for line in output.splitlines():
        match = _ENDPOINT_RE.match(line.strip())
        if not match:
            continue
        port = int(match.group("port"))
        if 0 < port < 65536:
            return match.group("host"), port
    return None


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
    parser = argparse.ArgumentParser(prog="terok-compute-gateway")
    parser.add_argument("--config", required=True, type=Path)
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
        metavar="OUT",
        help="generate a fresh high-entropy token for every configured client, "
        "write token hashes to OUT, print the plaintext tokens once, and exit",
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
        "# Generated by terok-compute-gateway --generate-tokens.",
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
        "container:\n  TEROK_COMPUTE_GATEWAY=<gateway url>\n  TEROK_COMPUTE_TOKEN=<its token>"
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
        return asyncio.run(_amain(args))
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

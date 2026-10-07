# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Route-first SSH tunneling for the gateway.

Instead of launching an external ``ssh -N -L ...`` process, the gateway now
opens the gateway -> login/route host connection itself with asyncssh (see
``ssh_backend.dial_route``) and asks that live connection to forward a local
loopback port to the container endpoint.  The same connection can run trusted
remote commands (dynamic provisioning and container recovery).

OpenSSH aliases from ``ssh_targets`` are still resolved by the local ``ssh``
client via ``ssh -G`` (no shell), so per-alias ``HostName``/``User``/``Port``/
``IdentityFile``/``ProxyJump`` settings are honored.  ``ProxyCommand`` is
rejected because it has no asyncssh equivalent.

In ``direct`` transport no connection is opened; the target is reached directly
at ``remote_host:remote_port``.  This is useful when the gateway already runs
next to the container or in tests, and keeps the same state machine for both
modes.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import shlex
import socket
from dataclasses import dataclass
from datetime import datetime, timezone

import asyncssh

from .config import SSHConfig, TargetConfig, TransportConfig
from .ssh_backend import (
    SSHError,
    InteractiveSSHClient,
    _empty_known_hosts,
    dial_route,
    make_factor_prompter,
)

log = logging.getLogger("compute_mcp.tunnel")


class TunnelError(RuntimeError):
    pass


# Mirrors ``gateway._parse_provision_endpoint``.  Duplicated here (rather than
# imported) to avoid a circular import: gateway imports tunnel, so tunnel must
# not import gateway at module load time.
_ENDPOINT_RE = re.compile(
    r"^(?:ENDPOINT\s+)?(?P<host>[A-Za-z0-9_.\-]+):(?P<port>\d{1,5})\s*$",
    re.IGNORECASE,
)


def format_provision_env(provision_env: dict[str, str] | None) -> str:
    """Render ``provision_env`` as quoted ``export`` statements for one command.

    The gateway cannot rely on SSH ``AcceptEnv`` (a site may not forward an
    arbitrary variable), so the resolved values are shipped as shell-quoted
    ``export NAME='value';`` statements prepended to the trusted provision
    command.  ``shlex.quote`` protects embedded whitespace and newlines; the
    command itself is still executed as a shell command string over the route
    connection, exactly as before.  Every value is validated to contain no NUL,
    which a shell string cannot carry safely.
    """
    if not provision_env:
        return ""
    statements: list[str] = []
    for name, value in provision_env.items():
        text = "" if value is None else str(value)
        if "\x00" in name or "\x00" in text:
            raise TunnelError(
                f"provisioning environment {name!r} contains a NUL byte"
            )
        statements.append(f"export {name}={shlex.quote(text)};")
    return " ".join(statements)


def parse_provision_endpoint(output: str) -> tuple[str, int] | None:
    """Return the first ``host:port`` line from provisioning stdout.

    A leading ``ENDPOINT`` marker is accepted but optional, so a script can
    print other diagnostic lines and one final endpoint line.  Kept identical
    to ``gateway._parse_provision_endpoint``.
    """
    for line in output.splitlines():
        match = _ENDPOINT_RE.match(line.strip())
        if not match:
            continue
        port = int(match.group("port"))
        if 0 < port < 65536:
            return match.group("host"), port
    return None


def _unquote(value: str) -> str:
    """Strip a single layer of surrounding quotes from an ``ssh -G`` value."""
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def _split_jump(token: str) -> tuple[str | None, str, int | None]:
    """Split a ``[user@]host[:port]`` ProxyJump element into its parts."""
    user: str | None = None
    host = token
    port: int | None = None
    if "@" in host:
        user, host = host.rsplit("@", 1)
    if host.startswith("["):
        end = host.find("]")
        if end != -1:
            rest = host[end + 1:]
            host = host[1:end]
            if rest.startswith(":") and rest[1:].isdigit():
                port = int(rest[1:])
    elif ":" in host:
        candidate_host, _, candidate_port = host.rpartition(":")
        if candidate_port.isdigit():
            host = candidate_host
            port = int(candidate_port)
    return (user or None), host, port


async def _ssh_g(alias: str, ssh: SSHConfig) -> str:
    """Run the local ``ssh`` client in config-query mode for ``alias``."""
    argv = ["ssh"]
    if ssh.config:
        argv += ["-F", ssh.config]
    argv += ["-G", alias]
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except (OSError, ValueError) as exc:
        raise TunnelError(
            f"could not run local ssh to resolve alias {alias!r}: {exc}"
        ) from exc
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=ssh.connect_timeout
        )
    except asyncio.TimeoutError:
        with contextlib.suppress(Exception):
            proc.kill()
            await proc.wait()
        raise TunnelError(
            f"ssh -G {alias!r} timed out after {ssh.connect_timeout:g}s"
        ) from None
    if proc.returncode != 0:
        detail = stderr.decode(errors="replace").strip() or "no stderr"
        raise TunnelError(
            f"ssh -G {alias!r} failed (rc={proc.returncode}): {detail}"
        )
    return stdout.decode(errors="replace")


async def _resolve_route(
    alias: str, ssh: SSHConfig, _seen: set[str] | None = None
) -> dict:
    """Resolve an OpenSSH alias into connection parameters via ``ssh -G``.

    Returns a dict with ``hostname``, ``user`` (or ``None``), ``port``,
    ``identityfiles`` (expanded, all repetitions) and ``jumps``: an ordered
    tuple of recursively resolved :func:`_resolve_route` results for the
    ``ProxyJump`` chain, client-nearest first.  Nothing is cached, so a config
    reload takes effect on the next call.
    """
    seen = set(_seen or ())
    if alias in seen:
        raise TunnelError(f"ProxyJump cycle detected at alias {alias!r}")
    seen.add(alias)

    output = await _ssh_g(alias, ssh)

    hostname: str | None = None
    user: str | None = None
    port: int | None = None
    identityfiles: list[str] = []
    proxyjump = ""
    proxycommand = ""

    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        key, _, value = line.partition(" ")
        key = key.strip().lower()
        value = value.strip()
        if key == "hostname" and value:
            hostname = _unquote(value)
        elif key == "user" and value:
            user = _unquote(value)
        elif key == "port" and value:
            with contextlib.suppress(ValueError):
                port = int(value)
        elif key == "identityfile" and value:
            identityfiles.append(os.path.expanduser(_unquote(value)))
        elif key == "proxyjump":
            proxyjump = value
        elif key == "proxycommand":
            proxycommand = value

    if proxycommand:
        raise TunnelError(
            f"alias {alias!r} uses ProxyCommand, which is not supported; "
            "use ProxyJump instead"
        )

    jumps: list[dict] = []
    for token in proxyjump.split(","):
        token = token.strip()
        if not token:
            continue
        jump_user, jump_host, jump_port = _split_jump(token)
        jump_info = await _resolve_route(jump_host, ssh, seen)
        if jump_user or jump_port is not None:
            jump_info = dict(jump_info)
            if jump_user:
                jump_info["user"] = jump_user
            if jump_port is not None:
                jump_info["port"] = jump_port
        jumps.append(jump_info)

    return {
        "alias": alias,
        "hostname": hostname or alias,
        "user": user,
        "port": port if port is not None else 22,
        "identityfiles": tuple(identityfiles),
        "jumps": tuple(jumps),
    }


def _iter_hops(info: dict):
    """Yield resolved hops in dial order (jump hosts first, route last)."""
    for jump in info.get("jumps") or ():
        yield from _iter_hops(jump)
    yield info


async def _apply_configured_proxy_jump(
    route: str, info: dict, proxy_jump: str, ssh: SSHConfig
) -> dict:
    """Merge a target-level ``proxy_jump`` into the resolved route chain.

    The target's explicit ``proxy_jump`` used to be applied as
    ``ssh -J <proxy_jump> <route>`` on every attempt.  Reproduce that by
    resolving the configured alias and prepending it to the route's own
    ``ProxyJump`` chain, so ``_iter_hops`` dials the configured jump first and
    the route alias last through the same single connection chain.

    The alias's own config already honoring the same jump is left untouched, and
    an alias that would close a cycle is rejected instead of looping.
    """
    if proxy_jump == route:
        raise TunnelError(
            f"configured proxy_jump {proxy_jump!r} is the route alias itself"
        )
    if proxy_jump in {hop.get("alias") for hop in _iter_hops(info)}:
        # The route's own ProxyJump chain already goes through this alias.
        return info
    jump_info = await _resolve_route(proxy_jump, ssh)
    if route in {hop.get("alias") for hop in _iter_hops(jump_info)}:
        raise TunnelError(
            f"configured proxy_jump {proxy_jump!r} forms a cycle with "
            f"route {route!r}"
        )
    merged = dict(info)
    merged["jumps"] = (jump_info,) + tuple(info.get("jumps") or ())
    return merged


def _route_client_keys(target: TargetConfig, info: dict) -> list[str]:
    """Build the route key list from resolved identities plus the config key.

    Every hop's ``IdentityFile`` entries are collected (jump hosts first).  The
    local ``ssh -G`` reports the OpenSSH default identity filenames whether or
    not they exist; OpenSSH ignores missing ones, asyncssh raises on them.  Skip
    non-existent inherited identities, but always keep an explicitly configured
    ``client_key`` so a bad path fails loudly.
    """
    keys: list[str] = []
    for hop in _iter_hops(info):
        for path in hop.get("identityfiles", ()):
            if os.path.isfile(path) and path not in keys:
                keys.append(path)
    if target.client_key and target.client_key not in keys:
        keys.append(target.client_key)
    return keys


async def _close_connection(conn: asyncssh.SSHClientConnection | None) -> None:
    if conn is None:
        return
    with contextlib.suppress(Exception):
        conn.close()
    with contextlib.suppress(Exception):
        await conn.wait_closed()


async def _dial_hop(
    info: dict,
    *,
    tunnel: asyncssh.SSHClientConnection | None,
    client_keys: list[str],
    prompter,
    pin: str | None,
    ssh: SSHConfig,
    name: str,
    fallback_user: str,
    passphrase: str | None = None,
) -> asyncssh.SSHClientConnection:
    """Dial one route hop, optionally through an already-open jump connection."""
    host = info["hostname"]
    port = info["port"]
    username = info["user"] or fallback_user
    keys = list(client_keys) if client_keys else None
    if tunnel is None:
        # First hop: use the dedicated route primitive (pin + prompter aware).
        return await dial_route(
            name=name,
            host=host,
            port=port,
            username=username,
            client_keys=keys,
            passphrase=passphrase,
            prompter=prompter,
            host_key_sha256=pin,
            known_hosts=None,
            host_key_algorithms=(),
            host_key_check="on",
            connect_timeout=ssh.connect_timeout,
            keepalive_interval=ssh.server_alive_interval,
            keepalive_count_max=ssh.server_alive_count_max,
        )

    # Subsequent hop: asyncssh forwards the TCP connection over ``tunnel``.
    # ``dial_route`` does not expose asyncssh's ``tunnel=`` keyword, so mirror
    # its client factory / host-key contract here (same pin, prompter, keys).
    # When a pin is set, pass an empty trusted-key set so asyncssh still drives
    # the client hook (which validates the pin) instead of silently disabling
    # validation.  Without a pin, defer to asyncssh's ~/.ssh/known_hosts.
    known_hosts = _empty_known_hosts if pin else None
    client_factory = lambda: InteractiveSSHClient(  # noqa: E731
        pin, prompter, accept_any=False
    )
    try:
        return await asyncssh.connect(
            host,
            port=port,
            username=username,
            client_keys=keys,
            passphrase=passphrase,
            known_hosts=known_hosts,
            client_factory=client_factory,
            server_host_key_algs=(),
            tunnel=tunnel,
            connect_timeout=ssh.connect_timeout,
            keepalive_interval=ssh.server_alive_interval,
            keepalive_count_max=ssh.server_alive_count_max,
        )
    except (asyncssh.Error, OSError) as exc:
        raise SSHError(
            f"SSH route connection to {name!r} failed: {exc}"
        ) from exc


def _probe_once(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.75):
            return True
    except OSError:
        return False


async def probe(host: str, port: int, timeout: float = 8.0) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        if await loop.run_in_executor(None, _probe_once, host, port):
            return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(0.2)


def allocate_loopback_port(ssh: SSHConfig, reserved: set[int]) -> int:
    """Allocate a loopback port from the configured range, avoiding ``reserved``."""

    for port in range(ssh.internal_port_min, ssh.internal_port_max + 1):
        if port in reserved:
            continue
        sock = socket.socket()
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("127.0.0.1", port))
        except OSError:
            continue
        finally:
            sock.close()
        reserved.add(port)
        return port
    raise TunnelError("no free internal port in configured range")


@dataclass
class RouteTunnel:
    """A managed local port forwarding backed by a live asyncssh connection.

    ``connection`` is ``None`` for ``direct`` transport, where the endpoint is
    reached without any forwarding.  ``route`` is the alias name (or ``None``
    for direct) and ``local_port`` the loopback port the gateway hands to the
    container backend.  ``provisioned_endpoint`` carries the ``(host, port)``
    discovered by an on-route provisioning command, if any.
    """

    target: TargetConfig
    route: str | None
    local_port: int
    connection: asyncssh.SSHClientConnection | None = None
    provisioned_endpoint: tuple[str, int] | None = None

    @property
    def is_direct(self) -> bool:
        return self.target.transport.kind == "direct"

    def is_alive(self) -> bool:
        """True while the backing connection exists and has not closed."""
        conn = self.connection
        return conn is not None and not conn.is_closed()

    async def stop(self) -> None:
        await _close_connection(self.connection)
        self.connection = None


# Backwards-compatible alias for callers (e.g. the gateway) that still import
# the old name.  The process-based implementation is gone.
Tunnel = RouteTunnel


class TunnelManager:
    """Owns one :class:`RouteTunnel` per target and handles failover."""

    def __init__(self, ssh: SSHConfig) -> None:
        self.ssh = ssh
        self._reserved: set[int] = set()

    async def open_for_route(
        self,
        target: TargetConfig,
        route: str,
        local_port: int,
        transport: TransportConfig | None = None,
        *,
        factor: str | None = None,
        provision: bool = True,
        provision_env: dict[str, str] | None = None,
    ) -> RouteTunnel:
        """Open a route connection and forward ``local_port`` to the endpoint.

        ``transport`` defaults to the target's static transport; the gateway
        passes an override carrying an endpoint discovered by provisioning.
        When ``provision`` is true and the target has a ``provision_command``,
        that command runs on the freshly opened route connection and its
        ``host:port`` output replaces the endpoint; the parsed value is exposed
        as :attr:`RouteTunnel.provisioned_endpoint`.

        ``provision_env`` is the resolved allocation/container environment
        contract (see gateway).  When set, shell-quoted ``export NAME='value';``
        statements are prepended to the provision command string, before the
        trusted argv.  The argv itself is unchanged; SSH ``AcceptEnv`` is never
        relied on.
        """
        transport = transport or target.transport

        if transport.kind == "direct":
            if not await probe(transport.remote_host, transport.remote_port):
                raise TunnelError(
                    f"direct endpoint {transport.remote_host}:{transport.remote_port} unreachable"
                )
            return RouteTunnel(
                target=target, route=None, local_port=transport.remote_port
            )

        route_info = await _resolve_route(route, self.ssh)
        proxy_jump = transport.proxy_jump or target.transport.proxy_jump
        if proxy_jump:
            route_info = await _apply_configured_proxy_jump(
                route, route_info, proxy_jump, self.ssh
            )

        client_keys = _route_client_keys(target, route_info)
        prompter = make_factor_prompter(factor) if factor is not None else None
        pin = target.route_host_key_sha256 or None

        opened: list[asyncssh.SSHClientConnection] = []
        conn: asyncssh.SSHClientConnection | None = None
        try:
            for hop in _iter_hops(route_info):
                is_final = hop is route_info
                tunnel_conn = await _dial_hop(
                    hop,
                    tunnel=conn,
                    client_keys=client_keys,
                    prompter=prompter,
                    pin=pin if is_final else None,
                    ssh=self.ssh,
                    name=route,
                    fallback_user=target.user,
                    passphrase=factor,
                )
                opened.append(tunnel_conn)
                conn = tunnel_conn
        except (SSHError, TunnelError, asyncssh.KeyImportError) as exc:
            for opened_conn in reversed(opened):
                await _close_connection(opened_conn)
            raise TunnelError(f"route {route!r} connection failed: {exc}") from exc

        assert conn is not None  # _iter_hops always yields at least the route

        provisioned: tuple[str, int] | None = None
        dest_host = transport.remote_host
        dest_port = transport.remote_port
        if provision and target.provision_command:
            argv = " ".join(shlex.quote(p) for p in target.provision_command)
            prefix = format_provision_env(provision_env)
            command = f"{prefix} {argv}" if prefix else argv
            log.info(
                "target %s: provisioning on route %s: %s",
                target.name, route, " ".join(target.provision_command),
            )
            try:
                result = await conn.run(
                    command,
                    check=False,
                    timeout=target.provision_timeout,
                    encoding=None,
                )
            except (asyncssh.Error, OSError, asyncio.TimeoutError) as exc:
                for opened_conn in reversed(opened):
                    await _close_connection(opened_conn)
                raise TunnelError(
                    f"target {target.name!r} provisioning on route {route!r} "
                    f"failed: {exc}"
                ) from exc
            stdout = result.stdout or b""
            stderr = result.stderr or b""
            if result.exit_status != 0:
                detail = stderr.decode(errors="replace").strip() or "no stderr"
                for opened_conn in reversed(opened):
                    await _close_connection(opened_conn)
                raise TunnelError(
                    f"target {target.name!r} provisioning exited "
                    f"{result.exit_status}: {detail}"
                )
            endpoint = parse_provision_endpoint(stdout.decode(errors="replace"))
            if endpoint is None:
                for opened_conn in reversed(opened):
                    await _close_connection(opened_conn)
                raise TunnelError(
                    f"target {target.name!r} provisioning printed no "
                    "host:port endpoint"
                )
            dest_host, dest_port = endpoint
            provisioned = endpoint
            log.info(
                "target %s provisioned endpoint %s:%d", target.name, dest_host, dest_port
            )

        try:
            await conn.forward_local_port(
                "127.0.0.1", local_port, dest_host, dest_port
            )
        except (OSError, asyncssh.Error) as exc:
            for opened_conn in reversed(opened):
                await _close_connection(opened_conn)
            raise TunnelError(
                f"route {route!r} could not establish forwarding to "
                f"{dest_host}:{dest_port}: {exc}"
            ) from exc

        if not await probe("127.0.0.1", local_port):
            for opened_conn in reversed(opened):
                await _close_connection(opened_conn)
            raise TunnelError(
                f"route {route!r} did not establish forwarding to "
                f"{dest_host}:{dest_port}"
            )

        return RouteTunnel(
            target=target,
            route=route,
            local_port=local_port,
            connection=conn,
            provisioned_endpoint=provisioned,
        )

    async def _run_advisory_command(
        self,
        label: str,
        argv: tuple[str, ...],
        timeout: float,
        target: TargetConfig,
        route: str,
        connection: asyncssh.SSHClientConnection | None,
    ) -> None:
        """Run a trusted advisory command on the remote route host.

        The command runs on the REMOTE side of the live route connection (the
        machine that hosts the development container) so the gateway can
        forward stdout/stderr.  Exit status and output are logged but never
        fatal: the caller decides what to do next.

        Callers must pass the live ``connection`` (the route connection stored
        on the tunnel).  When it is missing the call is a logged no-op.
        """
        if connection is None:
            log.warning(
                "target %s: %s requested without a live route connection",
                target.name, label,
            )
            return
        command = " ".join(shlex.quote(p) for p in argv)
        log.info(
            "target %s: running %s via %s: %s",
            target.name, label, route, " ".join(argv),
        )
        try:
            result = await connection.run(
                command,
                check=False,
                timeout=timeout,
                encoding=None,
            )
        except Exception as exc:  # noqa: BLE001 - advisory, never fatal
            log.warning(
                "target %s: %s failed: %s", target.name, label, exc
            )
            return
        stdout = result.stdout or b""
        stderr = result.stderr or b""
        if stdout:
            log.debug(
                "target %s: %s stdout: %s",
                target.name, label, stdout.decode(errors="replace").strip(),
            )
        if result.exit_status != 0:
            detail = stderr.decode(errors="replace").strip() or "no stderr"
            log.warning(
                "target %s: %s exited %s: %s",
                target.name, label, result.exit_status, detail,
            )
        else:
            log.info("target %s: %s completed", target.name, label)

    async def run_connect_command(
        self,
        target: TargetConfig,
        route: str,
        connection: asyncssh.SSHClientConnection | None = None,
    ) -> None:
        """Run the target's trusted ``connect_command`` on the route host.

        The command runs on the REMOTE side of the live route connection (the
        machine that hosts the development container) so the gateway can
        forward stdout/stderr.  It is used to bring a stopped container back up.
        Exit status and output are logged but never fatal: the caller re-tries
        the connection to decide whether recovery worked.

        Signature changed for the route-first model: callers must pass the live
        ``connection`` (the route connection stored on the tunnel).  When it is
        missing the call is a logged no-op.
        """
        await self._run_advisory_command(
            "connect_command",
            target.connect_command,
            target.connect_command_timeout,
            target,
            route,
            connection,
        )

    async def run_close_command(
        self,
        target: TargetConfig,
        route: str,
        connection: asyncssh.SSHClientConnection | None = None,
    ) -> None:
        """Run the target's trusted ``close_command`` on the route host.

        The command runs on the REMOTE side of the live route connection to
        release the target (e.g. ``scancel`` the Slurm job) before the tunnel is
        torn down.  Exit status and output are advisory: a non-zero exit or
        timeout is logged and teardown still proceeds.  When there is no live
        ``connection`` the call is a logged no-op.
        """
        await self._run_advisory_command(
            "close_command",
            target.close_command,
            target.close_command_timeout,
            target,
            route,
            connection,
        )

    async def connect(
        self,
        target: TargetConfig,
        on_route: callable | None = None,
        transport: TransportConfig | None = None,
        *,
        factor: str | None = None,
        provision_env: dict[str, str] | None = None,
    ) -> RouteTunnel:
        """Try each configured route in order; return the first working tunnel.

        ``transport`` overrides the target's static transport, which the gateway
        uses to inject an endpoint discovered by a provisioning command.  When
        an override is supplied the endpoint is already known, so provisioning
        is not run again; otherwise a target ``provision_command`` runs on the
        route connection.  Failover always follows the target's own
        ``transport.ssh_targets``.

        ``provision_env`` is forwarded verbatim to :meth:`open_for_route` so the
        resolved allocation/container export statements reach the provision
        command.
        """

        override = transport is not None
        transport = transport or target.transport
        # Only forward ``provision_env`` when set, so the existing call shape
        # for a target without an allocation/container description is unchanged.
        env_kwargs = {"provision_env": provision_env} if provision_env is not None else {}
        if transport.kind == "direct":
            return await self.open_for_route(
                target, "direct", transport.remote_port, transport,
                factor=factor, provision=False, **env_kwargs,
            )

        last_error: Exception | None = None
        for route in target.transport.ssh_targets:
            local_port = allocate_loopback_port(self.ssh, self._reserved)
            try:
                tunnel = await self.open_for_route(
                    target, route, local_port, transport,
                    factor=factor, provision=not override,
                    **env_kwargs,
                )
                log.info("target %s connected via route %s on 127.0.0.1:%d",
                         target.name, route, local_port)
                return tunnel
            except TunnelError as exc:
                last_error = exc
                self._reserved.discard(local_port)
                log.warning("target %s route %s failed: %s", target.name, route, exc)
                if on_route is not None:
                    on_route(route, exc)
        raise TunnelError(
            f"no working route for target {target.name!r}: {last_error}"
        )

    def release(self, tunnel: RouteTunnel) -> None:
        if tunnel.route is not None:
            self._reserved.discard(tunnel.local_port)

    @property
    def reserved(self) -> frozenset[int]:
        return frozenset(self._reserved)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)

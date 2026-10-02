# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Trusted gateway configuration loading and validation.

The gateway TOML is the single source of truth for which targets exist and how
they are reached.  Nothing that arrives over the gateway protocol may ever be
used as an SSH destination; only values loaded here are legal.
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

TARGET_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")

VALID_STATES = ("disconnected", "connecting", "connected", "failed")


class ConfigError(ValueError):
    """Raised when a configuration file is missing, malformed or unsafe."""


def validate_target_name(name: str) -> str:
    if not isinstance(name, str) or not TARGET_NAME_RE.match(name):
        raise ConfigError(f"invalid target name {name!r}")
    return name


@dataclass(frozen=True)
class TransportConfig:
    """How the gateway reaches the development-container SSH server.

    ``transport = "direct"`` connects straight to ``remote_host:remote_port``
    (used when the gateway already runs somewhere that can reach the container,
    or in tests).  ``transport = "tunnel"`` (default) starts a local SSH tunnel
    through the first working ``ssh_targets`` alias.
    """

    kind: str = "tunnel"
    remote_host: str = "127.0.0.1"
    remote_port: int = 2222
    ssh_targets: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.kind not in ("tunnel", "direct"):
            raise ConfigError(f"invalid transport kind {self.kind!r}")
        if not (0 < self.remote_port < 65536):
            raise ConfigError(f"invalid remote_port {self.remote_port!r}")
        if self.kind == "tunnel" and not self.ssh_targets:
            raise ConfigError("tunnel transport requires at least one ssh_target")


@dataclass(frozen=True)
class TargetConfig:
    name: str
    user: str
    transport: TransportConfig
    client_key: str | None = None
    known_hosts: str | None = None
    host_key_sha256: str | None = None
    host_key_algorithms: tuple[str, ...] = ()
    connect_mode: str = "shared"
    auto_connect: bool = False
    connect_backoff_initial: float = 1.0
    connect_backoff_max: float = 60.0

    def __post_init__(self) -> None:
        validate_target_name(self.name)
        if not self.user:
            raise ConfigError(f"target {self.name!r} has no user")
        if self.connect_mode not in ("shared", "dedicated"):
            raise ConfigError(
                f"target {self.name!r} connect_mode must be 'shared' or 'dedicated'"
            )
        if self.host_key_sha256 is not None:
            fp = self.host_key_sha256.strip()
            if not fp.startswith("SHA256:") or len(fp) < 12:
                raise ConfigError(
                    f"target {self.name!r} host_key_sha256 must look like 'SHA256:...'"
                )
            object.__setattr__(self, "host_key_sha256", fp)


@dataclass(frozen=True)
class ServerConfig:
    listen: str = "127.0.0.1"
    port: int = 2222
    request_timeout: float = 30.0
    exec_timeout: float = 900.0
    max_body_bytes: int = 256 * 1024 * 1024


@dataclass(frozen=True)
class SSHConfig:
    connect_timeout: float = 10.0
    server_alive_interval: int = 30
    server_alive_count_max: int = 3
    internal_port_min: int = 31000
    internal_port_max: int = 31999
    config: str | None = None


@dataclass(frozen=True)
class SessionConfig:
    idle_timeout: float = 3600.0
    max_per_client: int = 16
    output_buffer_bytes: int = 4 * 1024 * 1024


@dataclass(frozen=True)
class ClientConfig:
    client_id: str
    token_sha256: str
    targets: tuple[str, ...] = ()
    allow_all: bool = False

    def may_access(self, target: str) -> bool:
        return self.allow_all or target in self.targets


@dataclass(frozen=True)
class GatewayConfig:
    server: ServerConfig
    ssh: SSHConfig
    sessions: SessionConfig
    targets: dict[str, TargetConfig]
    clients: dict[str, ClientConfig]
    token_file: str | None = None
    config_path: str | None = None
    raw: dict = field(default_factory=dict, repr=False)


def _require_table(raw: dict, name: str) -> dict:
    value = raw.get(name, {})
    if not isinstance(value, dict):
        raise ConfigError(f"[{name}] must be a table")
    return value


def _int(value, key, default):
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{key} must be an integer") from exc


def _float(value, key, default):
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{key} must be a number") from exc


def _load_target(name: str, value: dict, ssh: SSHConfig) -> TargetConfig:
    validate_target_name(name)
    if not isinstance(value, dict):
        raise ConfigError(f"[targets.{name}] must be a table")

    user = value.get("user", "agent")
    has_ssh_targets = "ssh_targets" in value
    ssh_targets = tuple(value.get("ssh_targets", ()))
    kind = value.get("transport")
    if kind is None:
        # An explicit ssh_targets key (even empty) selects tunnel transport and
        # is then validated to be non-empty; absence selects direct transport.
        kind = "tunnel" if has_ssh_targets else "direct"
    remote_host = value.get("remote_host", "127.0.0.1")
    remote_port = _int(value.get("remote_port"), f"targets.{name}.remote_port", 2222)

    transport = TransportConfig(
        kind=kind,
        remote_host=remote_host,
        remote_port=remote_port,
        ssh_targets=ssh_targets,
    )

    return TargetConfig(
        name=name,
        user=user,
        transport=transport,
        client_key=value.get("client_key"),
        known_hosts=value.get("known_hosts"),
        host_key_sha256=value.get("host_key_sha256"),
        host_key_algorithms=tuple(value.get("host_key_algorithms", ())),
        connect_mode=value.get("connect_mode", "shared"),
        auto_connect=bool(value.get("auto_connect", False)),
        connect_backoff_initial=_float(
            value.get("connect_backoff_initial"), "connect_backoff_initial", 1.0
        ),
        connect_backoff_max=_float(
            value.get("connect_backoff_max"), "connect_backoff_max", 60.0
        ),
    )


def _load_clients(
    raw: dict,
    targets: dict[str, TargetConfig],
    external_hashes: dict[str, str] | None = None,
) -> dict[str, ClientConfig]:
    external_hashes = external_hashes or {}
    clients: dict[str, ClientConfig] = {}
    table = _require_table(raw, "clients")
    for client_id, value in table.items():
        validate_target_name(client_id)
        if not isinstance(value, dict):
            raise ConfigError(f"[clients.{client_id}] must be a table")
        token_hash = value.get("token_hash")
        token = value.get("token")
        if token_hash is None and token is None:
            # Fall back to a token supplied by the external tokens file.
            token_hash = external_hashes.get(client_id)
        if token_hash is None and token is not None:
            from .auth import hash_token

            token_hash = hash_token(token)
        if token_hash is None:
            raise ConfigError(
                f"[clients.{client_id}] needs token_hash, token, or an entry in the tokens file"
            )
        if not str(token_hash).startswith("sha256:"):
            raise ConfigError(
                f"[clients.{client_id}] token_hash must look like 'sha256:...'"
            )
        allowed = tuple(value.get("targets", ()))
        for target in allowed:
            if target != "*" and target not in targets:
                raise ConfigError(
                    f"[clients.{client_id}] references unknown target {target!r}"
                )
        clients[client_id] = ClientConfig(
            client_id=client_id,
            token_sha256=token_hash,
            targets=tuple(t for t in allowed if t != "*"),
            allow_all="*" in allowed,
        )
    return clients


def _token_hashes_from_file_table(table: dict) -> dict[str, str]:
    """Normalize a tokens table into ``client_id -> sha256:hash``.

    Accepted shapes::

        [tokens]
        "alpaka" = "plaintext-token"          # key is the client id

        [tokens]
        "alpaka" = "sha256:<hex>"             # value already hashed

        [tokens]
        "sha256:<hex>" = "alpaka"             # legacy reverse form
    """
    from .auth import hash_token

    result: dict[str, str] = {}
    for key, value in table.items():
        key, value = str(key), str(value)
        if key.startswith("sha256:"):
            result[value] = key
        elif value.startswith("sha256:"):
            result[key] = value
        else:
            result[key] = hash_token(value)
    return result


def parse_config(
    raw: dict,
    config_path: str | None = None,
    token_file: str | None = None,
) -> GatewayConfig:
    if not isinstance(raw, dict):
        raise ConfigError("configuration root must be a table")

    server_raw = _require_table(raw, "server")
    server = ServerConfig(
        listen=server_raw.get("listen", "127.0.0.1"),
        port=_int(server_raw.get("port"), "server.port", 2222),
        request_timeout=_float(server_raw.get("request_timeout"), "request_timeout", 30.0),
        exec_timeout=_float(server_raw.get("exec_timeout"), "exec_timeout", 900.0),
        max_body_bytes=_int(server_raw.get("max_body_bytes"), "max_body_bytes", 256 * 1024 * 1024),
    )

    ssh_raw = _require_table(raw, "ssh")
    ssh = SSHConfig(
        connect_timeout=_float(ssh_raw.get("connect_timeout"), "connect_timeout", 10.0),
        server_alive_interval=_int(ssh_raw.get("server_alive_interval"), "server_alive_interval", 30),
        server_alive_count_max=_int(ssh_raw.get("server_alive_count_max"), "server_alive_count_max", 3),
        internal_port_min=_int(ssh_raw.get("internal_port_min"), "internal_port_min", 31000),
        internal_port_max=_int(ssh_raw.get("internal_port_max"), "internal_port_max", 31999),
        config=ssh_raw.get("config"),
    )
    if ssh.internal_port_min > ssh.internal_port_max:
        raise ConfigError("ssh.internal_port_min cannot exceed internal_port_max")

    session_raw = _require_table(raw, "sessions")
    sessions = SessionConfig(
        idle_timeout=_float(session_raw.get("idle_timeout"), "sessions.idle_timeout", 3600.0),
        max_per_client=_int(session_raw.get("max_per_client"), "sessions.max_per_client", 16),
        output_buffer_bytes=_int(
            session_raw.get("output_buffer_bytes"), "sessions.output_buffer_bytes", 4 * 1024 * 1024
        ),
    )

    targets: dict[str, TargetConfig] = {}
    for name, value in _require_table(raw, "targets").items():
        targets[name] = _load_target(name, value, ssh)

    auth_raw = _require_table(raw, "auth")
    token_path = token_file or auth_raw.get("token_file")
    external_hashes: dict[str, str] = {}
    if token_path:
        external_hashes = _token_hashes_from_file_table(load_tokens(token_path))
    clients = _load_clients(raw, targets, external_hashes)

    if not clients:
        raise ConfigError("no clients configured; at least one is required")
    if not targets and not any(c.allow_all for c in clients.values()):
        pass  # an empty target set is allowed (nothing to serve yet)

    return GatewayConfig(
        server=server,
        ssh=ssh,
        sessions=sessions,
        targets=targets,
        clients=clients,
        token_file=token_path,
        config_path=config_path,
        raw=raw,
    )


def load_config(
    path: str | Path, token_file: str | Path | None = None
) -> GatewayConfig:
    path = Path(path)
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except FileNotFoundError as exc:
        raise ConfigError(f"configuration file not found: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"malformed TOML in {path}: {exc}") from exc
    return parse_config(
        raw,
        config_path=str(path),
        token_file=str(token_file) if token_file else None,
    )


def load_tokens(path: str | Path) -> dict[str, str]:
    """Load project tokens -> project id.

    Two accepted shapes::

        [tokens]
        "alpaka" = "plaintext-token"

        [tokens]
        "sha256:<hex>" = "alpaka"
    """

    path = Path(path)
    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    table = raw.get("tokens", raw)
    if not isinstance(table, dict):
        raise ConfigError("token file must contain a [tokens] table")
    return {str(k): str(v) for k, v in table.items()}

# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Trusted gateway configuration loading and validation.

The gateway TOML is the single source of truth for which targets exist and how
they are reached.  Nothing that arrives over the gateway protocol may ever be
used as an SSH destination; only values loaded here are legal.
"""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

TARGET_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")

VALID_STATES = ("disconnected", "connecting", "connected", "failed")

# Default location used when the operator does not point at a file explicitly.
# ``$XDG_CONFIG_HOME`` is honored, falling back to ``~/.config``.
DEFAULT_CONFIG_DIR = "computeMCP-gateway"
DEFAULT_CONFIG_NAME = "config.toml"
DEFAULT_TOKEN_NAME = "tokens.toml"


class ConfigError(ValueError):
    """Raised when a configuration file is missing, malformed or unsafe."""


def _xdg_config_home() -> Path:
    return Path(os.environ.get("XDG_CONFIG_HOME") or (Path.home() / ".config"))


def default_config_path() -> Path:
    """Return the default ``config.toml`` path (used when --config is omitted)."""
    return _xdg_config_home() / DEFAULT_CONFIG_DIR / DEFAULT_CONFIG_NAME


def default_token_path() -> Path:
    """Return the default ``tokens.toml`` path (used when --token-file is omitted)."""
    return _xdg_config_home() / DEFAULT_CONFIG_DIR / DEFAULT_TOKEN_NAME


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
    # Optional ProxyJump alias for the gateway -> login-node hop, e.g.
    # ``proxy_jump = "rosi5"`` dials ``rosi5`` first, then the route alias.
    proxy_jump: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in ("tunnel", "direct"):
            raise ConfigError(f"invalid transport kind {self.kind!r}")
        if not (0 < self.remote_port < 65536):
            raise ConfigError(f"invalid remote_port {self.remote_port!r}")
        if self.kind == "tunnel" and not self.ssh_targets:
            raise ConfigError("tunnel transport requires at least one ssh_target")
        if self.proxy_jump is not None and self.kind != "tunnel":
            raise ConfigError("proxy_jump is only valid with tunnel transport")


@dataclass(frozen=True)
class TargetConfig:
    name: str
    user: str
    transport: TransportConfig
    client_key: str | None = None
    known_hosts: str | None = None
    host_key_sha256: str | None = None
    # Optional SHA256 pin for the LOGIN/ROUTE host key, distinct from
    # ``host_key_sha256`` which pins the CONTAINER host.  Used by the route-first
    # connection (gateway -> login node) before any container exists.
    route_host_key_sha256: str | None = None
    host_key_algorithms: tuple[str, ...] = ()
    # Host-key verification policy.  "on" (default) requires host_key_sha256 or
    # known_hosts and refuses to connect otherwise.  "off" explicitly disables
    # verification for this target and is only safe when the forwarded endpoint
    # itself is trusted (never on a host shared with others).
    host_key_check: str = "on"
    connect_mode: str = "shared"
    # Whether the underlying system is dedicated to this job or shared with
    # other users/jobs.  Relevant for benchmark trust; "unknown" when the
    # operator does not know.  Free-form but one of the three constants.
    sharing: str = "unknown"
    # Free-form, operator-authored hints about the underlying system (e.g.
    # ["GPU nvidia", "x86 CPU"]).  Unstructured: no fixed schema or meaning.
    # Optional; an unset value is treated as an empty list.  Exposed to agents
    # via computeMCP_targets()/computeMCP_status().
    node_info: tuple[str, ...] = ()
    # Optional ordered list of remote AI agents this target can delegate to,
    # stored as ``(agent, model)`` pairs.  The list order is the priority
    # order: the caller should try the entries in order and fall back to the
    # first working one.  Empty means no remote agent is configured.  Unrelated
    # to the SSH ``user = "agent"`` account name.  Exposed to agents via
    # computeMCP_targets()/computeMCP_status().
    agent: tuple[tuple[str, str], ...] = ()
    interactive_auth: bool = False
    # Optional trusted provisioning command (argv, no shell).  Run before the
    # tunnel is opened to discover a dynamic endpoint (e.g. a Slurm job's
    # compute node + port).  The first ``host:port`` token on stdout wins.
    provision_command: tuple[str, ...] = ()
    provision_timeout: float = 900.0
    # Optional trusted release command (argv, no shell) that runs ON THE REMOTE
    # machine over the live route connection to release the target (e.g.
    # ``scancel`` the Slurm job).  It runs on an explicit stop, gateway shutdown
    # and before a refresh, but NOT when a config reload removes a target.
    # Advisory: a non-zero exit or timeout is logged and teardown proceeds.
    close_command: tuple[str, ...] = ()
    close_command_timeout: float = 120.0
    # Optional trusted recovery command (argv, no shell) that runs ON THE REMOTE
    # HOST (through the try-route ssh alias, plus proxy_jump) when the container
    # cannot be reached over the tunnel -- i.e. when ``probe`` of the forwarded
    # port fails.  Unlike provision_command it does not move the endpoint; it is
    # meant to bring the container back up (e.g. start a stopped Docker
    # container).  The gateway re-probes the tunnel after it exits.
    connect_command: tuple[str, ...] = ()
    connect_command_timeout: float = 120.0
    # "on_failure" (default) run connect_command only when the initial probe
    # fails (fast path: a healthy container never pays for it).  "always" run it
    # before every connect attempt, which requires the script to be a no-op when
    # the container already runs.
    connect_command_mode: str = "on_failure"
    auto_connect: bool = False
    connect_backoff_initial: float = 1.0
    connect_backoff_max: float = 60.0

    def __post_init__(self) -> None:
        if self.provision_command and self.transport.kind != "tunnel":
            raise ConfigError(
                f"target {self.name!r} provision_command requires tunnel transport"
            )
        if self.close_command and self.transport.kind != "tunnel":
            raise ConfigError(
                f"target {self.name!r} close_command requires tunnel transport"
            )
        if self.connect_command and self.transport.kind != "tunnel":
            raise ConfigError(
                f"target {self.name!r} connect_command requires tunnel transport"
            )
        if self.connect_command_mode not in ("on_failure", "always"):
            raise ConfigError(
                f"target {self.name!r} connect_command_mode must be "
                "'on_failure' or 'always'"
            )
        validate_target_name(self.name)
        if not self.user:
            raise ConfigError(f"target {self.name!r} has no user")
        if self.connect_mode not in ("shared", "dedicated"):
            raise ConfigError(
                f"target {self.name!r} connect_mode must be 'shared' or 'dedicated'"
            )
        if self.sharing not in ("exclusive", "shared", "unknown"):
            raise ConfigError(
                f"target {self.name!r} sharing must be 'exclusive', 'shared' or 'unknown'"
            )
        if not isinstance(self.node_info, (tuple, list)):
            raise ConfigError(
                f"target {self.name!r} node_info must be a list of strings"
            )
        if not all(isinstance(entry, str) for entry in self.node_info):
            raise ConfigError(
                f"target {self.name!r} node_info must be a list of strings"
            )
        object.__setattr__(self, "node_info", tuple(self.node_info))
        if not isinstance(self.agent, (tuple, list)):
            raise ConfigError(
                f"target {self.name!r} agent must be a list of tables"
            )
        normalized_agent: list[tuple[str, str]] = []
        for entry in self.agent:
            if isinstance(entry, dict):
                if set(entry) != {"agent", "model"}:
                    raise ConfigError(
                        f"target {self.name!r} agent entries must have exactly "
                        "the keys 'agent' and 'model'"
                    )
                agent_name = entry["agent"]
                model = entry["model"]
            elif isinstance(entry, tuple) and len(entry) == 2:
                # An already-normalized ``(agent, model)`` pair, e.g. after a
                # ``dataclasses.replace`` on a loaded target.  TOML arrays are
                # lists, so a 2-element list is not a valid entry here.
                agent_name, model = entry
            else:
                raise ConfigError(
                    f"target {self.name!r} agent entries must be tables with "
                    "'agent' and 'model'"
                )
            if not isinstance(agent_name, str) or not agent_name:
                raise ConfigError(
                    f"target {self.name!r} agent entry 'agent' must be a "
                    "non-empty string"
                )
            if not isinstance(model, str) or not model:
                raise ConfigError(
                    f"target {self.name!r} agent entry 'model' must be a "
                    "non-empty string"
                )
            normalized_agent.append((agent_name, model))
        object.__setattr__(self, "agent", tuple(normalized_agent))
        if self.host_key_sha256 is not None:
            fp = self.host_key_sha256.strip()
            if not fp.startswith("SHA256:") or len(fp) < 12:
                raise ConfigError(
                    f"target {self.name!r} host_key_sha256 must look like 'SHA256:...'"
                )
            object.__setattr__(self, "host_key_sha256", fp)
        if self.route_host_key_sha256 is not None:
            fp = self.route_host_key_sha256.strip()
            if not fp.startswith("SHA256:") or len(fp) < 12:
                raise ConfigError(
                    f"target {self.name!r} route_host_key_sha256 must look like "
                    "'SHA256:...'"
                )
            object.__setattr__(self, "route_host_key_sha256", fp)
        if self.host_key_check not in ("on", "off"):
            raise ConfigError(
                f"target {self.name!r} host_key_check must be 'on' or 'off'"
            )


@dataclass(frozen=True)
class ServerConfig:
    listen: str = "127.0.0.1"
    port: int = 2222
    request_timeout: float = 30.0
    exec_timeout: float = 900.0
    max_body_bytes: int = 256 * 1024 * 1024
    # Out-of-band client enrollment (see enrollment.py).  Approval is always an
    # explicit operator action; these only bound the unauthenticated surface.
    allow_enrollment: bool = True
    enroll_ttl: float = 600.0
    enroll_max_pending: int = 32


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
    label: str | None = None

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

    if "node_info" in value and not isinstance(value.get("node_info"), list):
        raise ConfigError(f"target {name!r} node_info must be a list of strings")

    if "agent" in value and not isinstance(value.get("agent"), (list, tuple)):
        raise ConfigError(f"target {name!r} agent must be a list of tables")

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
        proxy_jump=value.get("proxy_jump"),
    )

    return TargetConfig(
        name=name,
        user=user,
        transport=transport,
        client_key=value.get("client_key"),
        known_hosts=value.get("known_hosts"),
        host_key_sha256=value.get("host_key_sha256"),
        route_host_key_sha256=value.get("route_host_key_sha256"),
        host_key_algorithms=tuple(value.get("host_key_algorithms", ())),
        host_key_check=value.get("host_key_check", "on"),
        connect_mode=value.get("connect_mode", "shared"),
        sharing=value.get("sharing", "unknown"),
        node_info=tuple(value.get("node_info", ())),
        agent=value.get("agent", ()),
        interactive_auth=bool(value.get("interactive_auth", False)),
        provision_command=tuple(value.get("provision_command", ())),
        provision_timeout=_float(
            value.get("provision_timeout"), "provision_timeout", 900.0
        ),
        close_command=tuple(value.get("close_command", ())),
        close_command_timeout=_float(
            value.get("close_command_timeout"), "close_command_timeout", 120.0
        ),
        connect_command=tuple(value.get("connect_command", ())),
        connect_command_timeout=_float(
            value.get("connect_command_timeout"), "connect_command_timeout", 120.0
        ),
        connect_command_mode=value.get("connect_command_mode", "on_failure"),
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
        label = value.get("label")
        clients[client_id] = ClientConfig(
            client_id=client_id,
            token_sha256=token_hash,
            targets=tuple(t for t in allowed if t != "*"),
            allow_all="*" in allowed,
            label=str(label) if label is not None else None,
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
        allow_enrollment=bool(server_raw.get("allow_enrollment", True)),
        enroll_ttl=_float(server_raw.get("enroll_ttl"), "server.enroll_ttl", 600.0),
        enroll_max_pending=_int(
            server_raw.get("enroll_max_pending"), "server.enroll_max_pending", 32
        ),
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
    # Resolution order: explicit --token-file, then [auth] token_file, then the
    # conventional tokens.toml next to the config when it exists.  A missing
    # default is not an error -- inline client tokens may still be in use.
    token_path = token_file or auth_raw.get("token_file")
    explicit_token = token_path is not None
    if token_path is None:
        candidate = (
            Path(config_path).parent / DEFAULT_TOKEN_NAME
            if config_path
            else default_token_path()
        )
        if candidate.exists():
            token_path = str(candidate)
    external_hashes: dict[str, str] = {}
    if token_path:
        try:
            external_hashes = _token_hashes_from_file_table(load_tokens(token_path))
        except FileNotFoundError:
            # Only an explicitly named token file must exist.
            if explicit_token:
                raise
            token_path = None
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
    path: str | Path | None = None, token_file: str | Path | None = None
) -> GatewayConfig:
    # An omitted --config falls back to the conventional location.
    path = Path(path) if path is not None else default_config_path()
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


def _toml_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _atomic_write(path: Path, text: str, mode: int | None = None) -> None:
    """Write ``text`` to ``path`` via a temp file + rename (atomic on POSIX)."""
    path = Path(path)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    tmp.write_text(text)
    if mode is not None:
        try:
            tmp.chmod(mode)
        except OSError:
            pass
    os.replace(tmp, path)


def append_client(
    config_path: str | Path,
    client_id: str,
    targets: tuple[str, ...],
    label: str | None = None,
) -> None:
    """Append a ``[clients.<id>]`` block to the config, then re-validate it.

    The block is only ever *appended*; existing content (including operator
    comments) is preserved.  The whole file is re-parsed afterwards, and the
    caller should only rely on it once that validation passes.  A duplicate
    client id is refused.
    """
    validate_target_name(client_id)
    path = Path(config_path)
    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    if client_id in raw.get("clients", {}):
        raise ConfigError(f"client {client_id!r} already exists in {path}")

    known = set(raw.get("targets", {}))
    for target in targets:
        if target != "*" and target not in known:
            raise ConfigError(
                f"unknown target {target!r} for client {client_id!r}; "
                f"known targets: {', '.join(sorted(known)) or '(none)'}"
            )

    lines = [
        "",
        f"[clients.{client_id}]",
        f"targets = [{', '.join(_toml_quote(t) for t in targets)}]",
    ]
    if label:
        lines.append(f"label = {_toml_quote(label)}")
    block = "\n".join(lines) + "\n"

    existing = path.read_text()
    if existing and not existing.endswith("\n"):
        existing += "\n"
    _atomic_write(path, existing + block)

    # Validate what we just wrote; leave the file in place only if it is safe.
    try:
        load_config(path, token_file=None)
    except ConfigError:
        # Restore the previous content so a bad append cannot brick reloads.
        _atomic_write(path, existing)
        raise


def append_token_hash(
    token_path: str | Path, client_id: str, token: str, *, create: bool = True
) -> None:
    """Append ``client_id = sha256:<hash>`` under ``[tokens]`` atomically.

    If the file does not exist it is created (chmod 600).  An existing entry for
    the same client id is replaced in place so rotation is idempotent.
    """
    from .auth import hash_token

    validate_target_name(client_id)
    path = Path(token_path)
    entry = f"{_toml_quote(client_id)} = {_toml_quote(hash_token(token))}"

    if not path.exists():
        if not create:
            raise ConfigError(f"token file not found: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(path, f"[tokens]\n{entry}\n", mode=0o600)
        return

    lines = path.read_text().splitlines()
    out: list[str] = []
    replaced = False
    header_seen = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("[tokens]"):
            header_seen = True
            out.append(line)
            continue
        if stripped.startswith(f"{_toml_quote(client_id)} =") or stripped.startswith(
            f"{client_id} ="
        ):
            out.append(entry)
            replaced = True
            continue
        out.append(line)
    if not header_seen:
        out.append("")
        out.append("[tokens]")
    if not replaced:
        out.append(entry)
    _atomic_write(path, "\n".join(out).rstrip("\n") + "\n", mode=0o600)

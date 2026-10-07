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
# Host-local plaintext operator token written by --bootstrap (mode 0600).  It is
# separate from tokens.toml, which holds hashes only, so the operator CLI can
# authenticate without the operator exporting anything.
OPERATOR_TOKEN_NAME = "operator.token"


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


def default_operator_token_path() -> Path:
    """Return the default operator token path next to the default config."""
    return _xdg_config_home() / DEFAULT_CONFIG_DIR / OPERATOR_TOKEN_NAME


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


# Allocation policy modes.  The single-node policy accepts any of them; the
# multi-node policy is intentionally restricted initially (see the design doc).
ALLOCATION_MODES = ("gpu-proportional", "cpu-proportional", "full", "exclusive")
MULTI_NODE_MODES = ("full", "exclusive")

# Supported mapping vocabulary: calculated value -> output representations.
# A missing mapping emits nothing; a present entry is validated against this
# bounded table before the plan/emit stages (T2/T3) are ever reached.
MAPPING_VOCABULARY: dict[str, frozenset[str]] = {
    "nodes": frozenset({"nodes"}),
    "gpus-per-node": frozenset({"gres", "gpus-per-node"}),
    "cpus-per-node": frozenset({"cpus-per-task"}),
    "memory-per-node": frozenset({"mem"}),
    "exclusive": frozenset({"exclusive"}),
}

# Bundle identifiers shipped inside the ``compute_mcp.bundles`` package.  The
# gateway deploys the exact revision it was built from; a target names one of
# these instead of pointing provision_command at a hand-placed copy.
KNOWN_BUNDLES = ("computemcp-slurm",)


@dataclass(frozen=True)
class NodeConfig:
    """Allocatable resources of a single compute node.

    ``cpus`` are Slurm CPUs under the site/SMT policy (not necessarily physical
    cores); ``memory`` is allocatable host memory in any Slurm memory unit
    (e.g. "378000M"); ``gpus`` are the scheduler-visible GPU units.  All fields
    are optional descriptions: an unset field simply carries no capacity
    information and is not an error.
    """

    cpus: int | None = None
    gpus: int | None = None
    memory: str | None = None

    def __post_init__(self) -> None:
        for label, value in (("cpus", self.cpus), ("gpus", self.gpus)):
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value <= 0
            ):
                raise ConfigError(f"node.{label} must be a positive integer")
        if self.memory is not None and (
            not isinstance(self.memory, str) or not self.memory.strip()
        ):
            raise ConfigError("node.memory must be a non-empty string (e.g. '378000M')")


@dataclass(frozen=True)
class AllocationConfig:
    """Defaults and allocation policies for Slurm allocations.

    ``single_node`` selects the mode used when one node is allocated (one node
    is the default).  ``multi_node`` is the mode used when several nodes are
    allocated; initially only ``full`` and ``exclusive`` are admitted there.
    ``max_nodes`` bounds the allocation and defaults to one node.
    """

    default_gpus: int | None = None
    default_cpus: int | None = None
    single_node: str | None = None
    multi_node: str | None = None
    max_nodes: int | None = None

    def __post_init__(self) -> None:
        prefix = "allocation."
        for label, value in (("default_gpus", self.default_gpus), ("default_cpus", self.default_cpus)):
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value <= 0
            ):
                raise ConfigError(f"{prefix}{label} must be a positive integer")
        if self.single_node is not None and self.single_node not in ALLOCATION_MODES:
            raise ConfigError(
                f"{prefix}single_node must be one of {', '.join(ALLOCATION_MODES)}"
            )
        if self.multi_node is not None and self.multi_node not in MULTI_NODE_MODES:
            raise ConfigError(
                f"{prefix}multi_node must be one of {', '.join(MULTI_NODE_MODES)}"
            )
        if self.max_nodes is not None and (
            not isinstance(self.max_nodes, int)
            or isinstance(self.max_nodes, bool)
            or self.max_nodes <= 0
        ):
            raise ConfigError(f"{prefix}max_nodes must be a positive integer")


@dataclass(frozen=True)
class SlurmStageConfig:
    """One Slurm stage: manual options plus an optional value mapping.

    ``options`` are the free-form manual options, e.g.
    ``ntasks-per-node = 1`` -- key spelling is preserved exactly and each value
    is a scalar, bool, or array (arrays repeat the option).  ``mapping`` maps
    calculated values to output representations, e.g.
    ``gpus-per-node = "gres"``; only the ``MAPPING_VOCABULARY`` is accepted.
    """

    options: dict[str, object] = field(default_factory=dict)
    mapping: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SlurmConfig:
    """Slurm options for the two stages: ``sbatch`` (submission) and ``srun``
    (job step).  The stages are independent: nothing is copied between them.
    Each stage independently carries its manual options and its mapping."""

    sbatch: SlurmStageConfig
    srun: SlurmStageConfig


@dataclass(frozen=True)
class ContainerConfig:
    """Container runtime description for a target.

    ``runtime`` is required whenever the block is present.  ``gpus`` names the
    GPU vendors the container can expose and must be a subset of
    ``nvidia``/``amd``/``intel`` (empty means none).  ``storage_root`` and
    ``image`` are optional.  ``host_home`` and ``sandbox`` are optional
    overrides for values the gateway would otherwise derive; they stay optional
    and minimal on purpose.
    """

    runtime: str
    storage_root: str | None = None
    image: str | None = None
    gpus: tuple[str, ...] = ()
    host_home: str | None = None
    sandbox: bool = False

    def __post_init__(self) -> None:
        if self.runtime not in ("apptainer", "docker"):
            raise ConfigError(
                f"container.runtime must be 'apptainer' or 'docker', "
                f"got {self.runtime!r}"
            )
        for label, value in (
            ("storage_root", self.storage_root),
            ("image", self.image),
            ("host_home", self.host_home),
        ):
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ConfigError(f"container.{label} must be a non-empty string")
        if self.gpus:
            vendors = tuple(dict.fromkeys(self.gpus))
            if not all(isinstance(v, str) for v in vendors):
                raise ConfigError("container.gpus must be a list of vendor names")
            unknown = sorted(set(vendors) - {"nvidia", "amd", "intel"})
            if unknown:
                raise ConfigError(
                    f"container.gpus contains unknown vendor(s): {', '.join(unknown)}"
                )
            invalid = sorted({v for v in set(vendors) if not v or not v.strip()})
            if invalid:
                raise ConfigError("container.gpus entries must be non-empty strings")
            object.__setattr__(self, "gpus", vendors)


@dataclass(frozen=True)
class BundleConfig:
    """Deployable helper bundle for a target.

    ``source`` names a bundle shipped in the ``compute_mcp.bundles`` package.
    ``deploy_dir`` is the remote directory on storage visible to login and
    compute nodes; the gateway derives a default from the container
    ``storage_root`` when it is unset.  ``auto_deploy`` controls whether the
    gateway uploads the bundle when the remote hash marker differs; setting it
    to false pins whatever copy is already deployed.  ``provision_env`` holds
    shell lines that run on the remote before the container runtime is used;
    an empty tuple is a no-op.
    """

    source: str
    deploy_dir: str | None = None
    auto_deploy: bool = True
    provision_env: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.source not in KNOWN_BUNDLES:
            raise ConfigError(
                f"bundle.source must be one of {', '.join(KNOWN_BUNDLES)}, "
                f"got {self.source!r}"
            )
        for line in self.provision_env:
            if not isinstance(line, str) or not line.strip():
                raise ConfigError(
                    "bundle.provision-env entries must be non-empty strings"
                )
            if "\x00" in line or "\r" in line:
                raise ConfigError(
                    "bundle.provision-env entries must not contain a NUL byte "
                    "or carriage return"
                )
        if self.deploy_dir is not None:
            if not isinstance(self.deploy_dir, str) or not self.deploy_dir.strip():
                raise ConfigError("bundle.deploy_dir must be a non-empty string")
            # ``$HOME``/``~`` are expanded by the remote helper; anything else
            # must be absolute so a typo cannot resolve relative to the CWD.
            if not self.deploy_dir.startswith(("/", "$HOME", "~")):
                raise ConfigError(
                    "bundle.deploy_dir must be absolute or start with $HOME/~"
                )
        if not isinstance(self.auto_deploy, bool):
            raise ConfigError("bundle.auto_deploy must be true or false")


def _validate_slurm_mapping(mapping: dict, stage: str, options: dict, target: str) -> None:
    """Validate a stage mapping against the bounded vocabulary.

    ``cpus-per-node -> cpus-per-task`` additionally requires the stage's manual
    options to declare exactly one task per node (``ntasks-per-node = 1`` or
    ``ntasks = 1``); a mismatched task layout is an error, not a silent
    translation.
    """
    where = f"targets.{target}.slurm.{stage}-map"
    if not isinstance(mapping, dict):
        raise ConfigError(f"[{where}] must be a table")
    for key, value in mapping.items():
        allowed = MAPPING_VOCABULARY.get(key)
        if allowed is None:
            raise ConfigError(
                f"{where}: unknown mapping key {key!r}; valid keys: "
                f"{', '.join(sorted(MAPPING_VOCABULARY))}"
            )
        if not isinstance(value, str) or value not in allowed:
            raise ConfigError(
                f"{where}: mapping for {key!r} must be one of "
                f"{', '.join(sorted(allowed))}"
            )
        if key == "cpus-per-node":
            effective = _effective_tasks_per_node(options)
            if effective == 0 or effective > 1:
                raise ConfigError(
                    f"{where}: mapping cpus-per-node -> cpus-per-task is only "
                    "valid with exactly one task per node; set ntasks-per-node = 1 "
                    f"(or ntasks = 1) in [targets.{target}.slurm.{stage}] "
                    f"(found an effective task count of {effective})"
                )


def _effective_tasks_per_node(options: dict) -> int:
    """Effective Slurm task count of a stage from its manual options.

    ``ntasks-per-node`` (or ``ntasks``), when present, wins; otherwise the
    options do not constrain the layout and zero is returned (the mapping
    precondition then fails with an actionable hint to set the option).
    """
    for key in ("ntasks-per-node", "ntasks"):
        if key in options:
            raw = options[key]
            if isinstance(raw, (list, tuple)):
                raw = raw[0] if raw else None
            if not isinstance(raw, int) or isinstance(raw, bool) or raw <= 0:
                raise ConfigError(
                    f"slurm {key} must be a positive integer when set"
                )
            return raw
    return 0


def _load_node_config(name: str, value: dict | None) -> NodeConfig | None:
    if value is None:
        return None
    key = f"[targets.{name}.node]"
    if not isinstance(value, dict):
        raise ConfigError(f"{key} must be a table")
    unknown = sorted(set(value) - {"cpus", "gpus", "memory"})
    if unknown:
        raise ConfigError(f"{key} has unknown key(s): {', '.join(unknown)}")
    return NodeConfig(
        cpus=value.get("cpus"),
        gpus=value.get("gpus"),
        memory=value.get("memory"),
    )


def _load_allocation_config(name: str, value: dict | None) -> AllocationConfig | None:
    if value is None:
        return None
    key = f"[targets.{name}.allocation]"
    if not isinstance(value, dict):
        raise ConfigError(f"{key} must be a table")
    unknown = sorted(
        set(value) - {"default-gpus", "default-cpus", "single-node", "multi-node", "max-nodes"}
    )
    if unknown:
        raise ConfigError(f"{key} has unknown key(s): {', '.join(unknown)}")
    return AllocationConfig(
        default_gpus=value.get("default-gpus"),
        default_cpus=value.get("default-cpus"),
        single_node=value.get("single-node"),
        multi_node=value.get("multi-node"),
        max_nodes=value.get("max-nodes"),
    )


def _load_slurm_stage(
    name: str,
    stage: str,
    options: dict | None,
    mapping_raw: dict | None,
) -> SlurmStageConfig | None:
    """Load and validate one stage's manual options plus its mapping.

    ``options``/``mapping_raw`` are the parsed ``slurm.<stage>`` dict and the
    ``slurm.<stage>-map`` dict respectively; either may be ``None``.  When both
    are ``None`` the stage is absent and ``None`` is returned so T2/T3 emit
    nothing for it.  ``options`` keys are preserved verbatim (e.g.
    ``ntasks-per-node``); each value is a scalar, bool, or array of them.
    ``mapping_raw`` is validated against ``MAPPING_VOCABULARY``.
    """
    if options is None and mapping_raw is None:
        return None
    if options is not None and not isinstance(options, dict):
        raise ConfigError(f"[targets.{name}.slurm.{stage}] must be a table")
    if mapping_raw is not None:
        if not isinstance(mapping_raw, dict):
            raise ConfigError(f"[targets.{name}.slurm.{stage}-map] must be a table")
        mapping = {str(k): v for k, v in mapping_raw.items()}
    else:
        mapping = {}
    _validate_slurm_mapping(mapping, stage, options or {}, name)
    return SlurmStageConfig(
        options=dict(options or {}),
        mapping=mapping,
    )


def _load_slurm_config(name: str, value: dict | None) -> SlurmConfig | None:
    """Split the per-target ``[slurm]`` table into sbatch/srun stages.

    The accepted TOML uses nested tables::

        [targets.X.slurm.sbatch]   # manual submission options
        [targets.X.slurm.sbatch-map]  # calculated value -> output representation
        [targets.X.slurm.srun]     # manual job-step options
        [targets.X.slurm.srun-map] # calculated value -> output representation

    After ``tomllib`` the parsed dict has exactly the keys ``sbatch``,
    ``sbatch-map``, ``srun``, ``srun-map`` (each a dict).  Each stage is
    independent: nothing is copied between them, so T2/T3 can decide
    per-stage whether to emit any resource arguments.
    """
    if value is None:
        return None
    key = f"[targets.{name}.slurm]"
    if not isinstance(value, dict):
        raise ConfigError(f"{key} must be a table")
    known = {"sbatch", "sbatch-map", "srun", "srun-map"}
    unknown = sorted(set(value) - known)
    if unknown:
        raise ConfigError(f"{key} has unknown key(s): {', '.join(unknown)}")
    for sub_name, sub in value.items():
        if not isinstance(sub, dict):
            raise ConfigError(f"[{key}.{sub_name}] must be a table")
    sbatch_stage = _load_slurm_stage(
        name, "sbatch", value.get("sbatch"), value.get("sbatch-map")
    )
    srun_stage = _load_slurm_stage(
        name, "srun", value.get("srun"), value.get("srun-map")
    )
    if sbatch_stage is None and srun_stage is None:
        # An empty `[slurm]` table is equivalent to no slurm block at all.
        return None
    return SlurmConfig(
        sbatch=sbatch_stage or SlurmStageConfig(),
        srun=srun_stage or SlurmStageConfig(),
    )


def _load_container_config(name: str, value: dict | None) -> ContainerConfig | None:
    if value is None:
        return None
    key = f"[targets.{name}.container]"
    if not isinstance(value, dict):
        raise ConfigError(f"{key} must be a table")
    unknown = sorted(
        set(value)
        - {
            "runtime",
            "storage-root",
            "image",
            "gpus",
            "host-home",
            "sandbox",
        }
    )
    if unknown:
        raise ConfigError(f"{key} has unknown key(s): {', '.join(unknown)}")
    runtime = value.get("runtime")
    if runtime is None:
        raise ConfigError(f"{key} requires 'runtime'")
    return ContainerConfig(
        runtime=runtime,
        storage_root=value.get("storage-root"),
        image=value.get("image"),
        gpus=tuple(value.get("gpus", ())),
        host_home=value.get("host-home"),
        sandbox=bool(value.get("sandbox", False)),
    )


def _load_bundle_config(name: str, value: dict | None) -> BundleConfig | None:
    if value is None:
        return None
    key = f"[targets.{name}.bundle]"
    if not isinstance(value, dict):
        raise ConfigError(f"{key} must be a table")
    unknown = sorted(
        set(value) - {"source", "deploy-dir", "auto-deploy", "provision-env"}
    )
    if unknown:
        raise ConfigError(f"{key} has unknown key(s): {', '.join(unknown)}")
    source = value.get("source")
    if source is None:
        raise ConfigError(f"{key} requires 'source'")
    raw_env = value.get("provision-env", ())
    if not isinstance(raw_env, (list, tuple)):
        raise ConfigError(f"{key}.provision-env must be an array of strings")
    return BundleConfig(
        source=source,
        deploy_dir=value.get("deploy-dir"),
        auto_deploy=value.get("auto-deploy", True),
        provision_env=tuple(raw_env),
    )


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
    # Optional Slurm resource description and allocation policy (see the
    # design doc).  ``node`` is the allocatable capacity per node;
    # ``allocation`` are the defaults and the single/multi-node policies;
    # ``slurm`` holds the manual sbatch/srun options and their optional
    # calculated-value mappings; ``container`` describes the development
    # container runtime (apptainer/docker).  Each is optional: a target without
    # a Slurm allocation simply has no such block, as before.
    node: NodeConfig | None = None
    allocation: AllocationConfig | None = None
    slurm: SlurmConfig | None = None
    container: ContainerConfig | None = None
    bundle: BundleConfig | None = None

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
        if self.bundle is not None and self.bundle.provision_env and (
            self.transport.kind != "tunnel"
        ):
            raise ConfigError(
                f"target {self.name!r} bundle.provision-env requires tunnel "
                "transport"
            )
        if self.bundle is not None and self.transport.kind != "tunnel":
            raise ConfigError(
                f"target {self.name!r} bundle requires tunnel transport"
            )
        if self.bundle is not None:
            if not self.bundle.deploy_dir:
                if self.container is None or not self.container.storage_root:
                    raise ConfigError(
                        f"target {self.name!r} bundle needs 'deploy-dir' or a "
                        "container 'storage-root' to derive it from"
                    )
            if not self.client_key:
                # The gateway derives the container's authorized key from
                # client_key; without it the container would accept no key and
                # the failure would only surface after provisioning starts.
                raise ConfigError(
                    f"target {self.name!r} bundle requires 'client_key'"
                )
        if self.connect_command_mode not in ("on_failure", "always"):
            raise ConfigError(
                f"target {self.name!r} connect_command_mode must be "
                "'on_failure' or 'always'"
            )
        validate_target_name(self.name)
        # ``user`` may be empty: the SSH config alias or the local account then
        # supplies the login user.  Only a non-string is rejected.
        if not isinstance(self.user, str):
            raise ConfigError(f"target {self.name!r} user must be a string")
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
    # Absolute paths of the TOML files merged to produce this configuration,
    # in first-seen preorder (includes first, entry last).  Empty when the
    # entry file has no ``include`` key.  Useful for diagnostics.
    include_paths: tuple[str, ...] = ()
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

    # Absent means "let the SSH config alias or local account decide"; an
    # explicit empty string is treated the same as absent.
    user = value.get("user", "")
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
        node=_load_node_config(name, value.get("node")),
        allocation=_load_allocation_config(name, value.get("allocation")),
        slurm=_load_slurm_config(name, value.get("slurm")),
        container=_load_container_config(name, value.get("container")),
        bundle=_load_bundle_config(name, value.get("bundle")),
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


def _flatten_table(body: dict, prefix: tuple[str, ...] = ()) -> dict[str, object]:
    """Flatten a nested table into ``'a.b.c' -> scalar`` leaf paths.

    Empty inner tables are omitted so a file that contributes nothing
    (e.g. only comments) never collides with another.
    """
    out: dict[str, object] = {}
    for key, value in body.items():
        path = prefix + (str(key),)
        if isinstance(value, dict):
            out.update(_flatten_table(value, path))
        else:
            out[".".join(path)] = value
    return out


def _set_leaf(root: dict, dotted: str, value: object) -> None:
    """Rebuild ``root[dotted.split('.')] = value`` in place, creating tables."""
    parts = dotted.split(".")
    cur = root
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    leaf = parts[-1]
    if leaf in cur and cur[leaf] != value:
        raise ConfigError(
            f"duplicate configuration value {dotted!r} (internal merge conflict)"
        )
    cur[leaf] = value


def _load_toml_files(root_path: Path) -> tuple[dict, list[Path]]:
    """Parse the TOML at ``root_path`` and every file its ``include`` chain
    references, and merge them into one raw table.

    Relative include paths resolve against the declaring file's directory
    (never the process CWD); absolute paths are used as-is.  Each file is
    visited exactly once; a cycle and a missing file are ``ConfigError``s.
    Two files that set the same leaf path are rejected with both file names
    in the message.  ``include`` keys themselves are stripped from the
    merged result, so the rest of ``parse_config`` sees a single ordinary
    gateway table.  The returned ordered list contains only the *included*
    files, in first-seen preorder (the entry file is excluded because it is
    reported separately via ``GatewayConfig.config_path``); for an
    include-free configuration it is empty.
    """
    merged: dict = {}
    sources: dict[str, str] = {}
    order: list[Path] = []
    visiting: list[Path] = []
    seen: set[Path] = set()

    def rec(p: Path) -> None:
        if p in seen:
            return
        if p in visiting:
            chain = " -> ".join([str(x) for x in visiting] + [str(p)])
            raise ConfigError(f"include cycle detected: {chain}")
        visiting.append(p)
        try:
            with p.open("rb") as handle:
                raw = tomllib.load(handle)
        except FileNotFoundError as exc:
            raise ConfigError(f"configuration file not found: {p}") from exc
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(f"malformed TOML in {p}: {exc}") from exc
        includes = raw.get("include", [])
        if not (
            isinstance(includes, list) and all(isinstance(e, str) for e in includes)
        ):
            raise ConfigError(
                f"in {p}: 'include' must be a list of file path strings"
            )
        for entry in includes:
            child = Path(entry)
            if not child.is_absolute():
                child = p.parent / entry
            child = child.resolve()
            rec(child)
        body = {k: v for k, v in raw.items() if k != "include"}
        for leaf, val in _flatten_table(body).items():
            if leaf in sources:
                prev = sources[leaf]
                raise ConfigError(
                    f"duplicate configuration value {leaf!r}: "
                    f"defined in both {prev} and {p}"
                )
            sources[leaf] = str(p)
            _set_leaf(merged, leaf, val)
        visiting.pop()
        seen.add(p)
        order.append(p)

    rec(root_path)
    # ``include_paths`` records only *included* files, not the entry (the
    # entry is already reported via ``GatewayConfig.config_path``).  An
    # include-free configuration yields an empty tuple, which also hands out
    # the back-compat guarantee that "nothing was included".
    if order and order[-1] == root_path:
        order.pop()
    return merged, order


def load_config(
    path: str | Path | None = None, token_file: str | Path | None = None
) -> GatewayConfig:
    """Read a TOML gateway config (with optional ``include``) and validate it.

    When the entry file carries a top-level ``include = [...]`` list, each
    list entry is parsed independently (relative paths resolve against the
    declaring file's directory; absolute paths are used as-is), then the
    parsed tables are merged.  A naive ``dict.update`` would silently override,
    so duplicates are rejected instead: two files assigning the same leaf
    ``dotted.key`` raise a ``ConfigError`` that names both source files.
    A missing include file or a cycle in the include graph is also an error.
    Files are visited depth-first; an include listed more than once applies
    only once (first-seen wins), so a diamond include with no cycle is OK.

    ``GatewayConfig.include_paths`` records every *included* file in
    first-seen preorder; it is empty when the entry file has no ``include``
    key (back-compat with single-file configs).  ``GatewayConfig.config_path``
    is always the entry the operator named, not a base path for the graph.
    """
    path = Path(path) if path is not None else default_config_path()
    merged, ordered = _load_toml_files(path)
    return parse_config(
        merged,
        config_path=str(path),
        token_file=str(token_file) if token_file else None,
        include_paths=tuple(str(p) for p in ordered),
    )


def parse_config(
    raw: dict,
    config_path: str | None = None,
    token_file: str | None = None,
    include_paths: tuple[str, ...] = (),
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
        include_paths=include_paths,
        raw=raw,
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


def append_include(config_path: str | Path, include_entry: str) -> None:
    """Add one path to the top-level ``include`` list, preserving the file.

    TOML has no incremental array append, and this must not disturb the
    operator's comments or formatting.  The function operates line-wise:

    - an ``include`` array spanning several lines gets the entry inserted on its
      own line before the closing ``]``;
    - a single-line ``include = [...]`` is rewritten into the multi-line form;
    - a file without ``include`` gets one inserted after the leading comment
      block and before the first table (where TOML allows a root key).

    An entry already present is a no-op, so callers can retry safely.  The file
    is re-validated after the edit and reverted on failure.
    """
    path = Path(config_path)
    original = path.read_text()
    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    current = raw.get("include", [])
    if not isinstance(current, list) or not all(isinstance(e, str) for e in current):
        raise ConfigError(f"{path}: 'include' must be a list of file path strings")
    if include_entry in current:
        return

    lines = original.splitlines()
    quote = _toml_quote(include_entry)
    # Match only a root-level key (column 0), never a nested table's key that
    # happens to start with "include".
    start = next(
        (
            i
            for i, line in enumerate(lines)
            if line.startswith("include") and line[len("include") :].lstrip().startswith("=")
        ),
        None,
    )
    if start is None:
        insert_at = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith("#") or not stripped:
                insert_at = i + 1
                continue
            insert_at = i
            break
        lines[insert_at:insert_at] = ["", f"include = [{quote}]"]
    else:
        # Find the closing bracket, ignoring comment-only lines that contain ']'.
        end = start
        while True:
            stripped = lines[end].strip()
            if "]" in lines[end] and not stripped.startswith("#"):
                break
            end += 1
            if end >= len(lines):
                raise ConfigError(f"{path}: unterminated include array")
        indent = "    "
        if start == end:
            # Single-line form: rebuild from the parsed values, so a bracket in
            # a trailing comment cannot confuse the rewrite.
            rebuilt = ["include = ["]
            rebuilt += [indent + _toml_quote(entry) + "," for entry in current]
            rebuilt.append(indent + quote + ",")
            rebuilt.append("]")
            lines[start : start + 1] = rebuilt
        else:
            closing = lines[end]
            indent = closing[: len(closing) - len(closing.lstrip())] or indent
            lines.insert(end, indent + quote + ",")

    updated = "\n".join(lines).rstrip("\n") + "\n"
    _atomic_write(path, updated)
    try:
        load_config(path, token_file=None)
    except (ConfigError, tomllib.TOMLDecodeError):
        _atomic_write(path, original)
        raise


def append_client(
    config_path: str | Path,
    client_id: str,
    targets: tuple[str, ...],
    label: str | None = None,
) -> None:
    """Append a ``[clients.<id>]`` block to the config, then re-validate it.

    The block is only ever *appended*; existing content (including operator
    comments) is preserved.  The whole file (merged across its include chain)
    is re-parsed afterwards, and the caller should only rely on it once that
    validation passes.  A duplicate client id is refused.

    Duplicate and target checks consult the merged include graph, not only the
    entry file: after targets moved into included files, an entry file with
    ``include = [...]`` can reference a target while defining no
    ``[targets.*]`` table of its own.  Only the entry file is ever written.
    """
    validate_target_name(client_id)
    path = Path(config_path)
    merged, _ordered = _load_toml_files(path)
    if client_id in merged.get("clients", {}):
        raise ConfigError(f"client {client_id!r} already exists in {path}")

    known = set(merged.get("targets", {}))
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

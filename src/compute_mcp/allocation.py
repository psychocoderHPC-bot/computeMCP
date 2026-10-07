# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Slurm allocation planning and argument rendering.

Implements the "Node description and allocation" and "Gateway-to-provisioner
interface" sections of the computeMCP Slurm design document:

- ``validate_conflicts`` refuses a stage whose enabled mapping collides with a
  conflicting manual option in the *same* stage, and re-checks the
  one-task-per-node precondition of ``cpus-per-node -> cpus-per-task`` from
  the stage's own manual options.
- ``compute_plan`` resolves a :class:`ResolvedPlan` from the node description,
  the allocation policy and connect-time ``--set`` overrides.  Overrides are
  resolved *before* mapping: they feed the mappings, never the other way
  around.
- ``render_args`` converts the manual options plus the mapped calculated values
  of each stage into two independent argv-style argument tuples for
  ``COMPUTEMCP_SBATCH_ARGS`` / ``COMPUTEMCP_SRUN_ARGS``.
- ``plan_summary`` exposes the whole resolution (intent, manual settings,
  per-stage rendered args, un-emitted calculated fields) as a plain dict for
  the ``--dry-run`` preview.  It must stay serializable.

Memory unit semantics (see :func:`parse_memory_mib` for the exact rules):
K/KiB = 1024 bytes, M/MiB = 1 MiB, G/GiB = 1024 MiB, T/TiB = 1024^2 MiB.
``i`` marks the binary interpretation of the magnitude; M is always MiB in
this context, so ``M`` and ``MiB`` are equivalent.  ``node.memory`` and
``--set mem-per-node`` accept strings with one of these units; bare integers
are taken as MiB.  A quantity that would resolve to fewer than one MiB is
rejected.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from .config import (
    ALLOCATION_MODES,
    MULTI_NODE_MODES,
    ConfigError,
    SlurmConfig,
    SlurmStageConfig,
    TargetConfig,
)

#: Option names that belong to the provisioning helper, not to resource
#: allocation.  ``render_args`` rejects them in manual options instead of
#: emitting them (the helper supplies its own ``sbatch --parsable`` etc.).
PROTOCOL_OPTIONS = ("parsable", "quiet", "wrap")

#: ``--set`` keys accepted by :func:`compute_plan`.
OVERRIDE_KEYS = ("nodes", "gpus-per-node", "cpus-per-node", "mem-per-node", "mode")

# Digit sequence followed by a unit: one of K/M/G/T (case-insensitive), with
# an optional 'i' (binary magnification) and optional trailing 'B'.  Slurm
# treats a bare letter equivalently to its binary form (M == MiB == 1 MiB,
# G == GiB == 1024 MiB, etc.), so 'i' is presentational; the unit table
# below gives the multiplier relative to MiB.
# Unit grammar (standard Slurm memory units, case-insensitive):
#   bare letter K/M/G/T               -> 1 KiB / 1 MiB / 1 GiB / 1 TiB
#   letter + i                        -> same as bare (e.g. K == Ki)
#   letter + i + B                    -> same as bare (e.g. KiB == K)
#   bare M + B (MB)                   -> 1 MiB (= M)
# The 'i' is a binary-interpretation marker; a bare letter is equivalent to
# its binary form in Slurm's memory units. The multiplier table below is
# indexed by the first character lower-cased; the unit never changes the
# magnitude, only the label.
_MEMORY_RE = re.compile(r"^([0-9]+(?:\.[0-9]+)?)([KkGgTt][iI]?(?:[Bb])?|[Mm][iI]?[Bb]?)$")
_MEMORY_MULT = {
    "k": 1 / 1024,
    "m": 1.0,
    "g": 1024.0,
    "t": 1024.0 * 1024.0,
}

#: Manual option names that conflict with an enabled ``gpus-per-node`` mapping
#: in the same stage: the total/other GPU request forms plus the per-node
#: spellings, which duplicate the same resource family.  (A mixed GRES value
#: such as ``gres=a:1,gpu:2`` is still a total GRES request and therefore
#: covers it.)
_GPU_CONFLICT_KEYS = ("gres", "gpus", "gpus-per-task", "gpus-per-node")

#: Manual option names conflicting with an enabled ``memory-per-node`` mapping
#: (the design's resource-family rule, including the alternative form).
_MEMORY_CONFLICT_KEYS = ("mem", "mem-per-cpu")

#: Manual option names conflicting with an enabled ``nodes`` mapping.
_NODES_CONFLICT_KEYS = ("nodes", "n")

#: Task-layout option names that pin the effective task count of a stage.
_TASK_OPTION_NAMES = ("ntasks-per-node", "ntasks")


@dataclass(frozen=True)
class ResolvedPlan:
    """The calculated per-node resource request for one allocation.

    ``nodes`` is the number of nodes.  The remaining per-node quantities use
    ``None`` for "no capacity description / not available".  ``exclusive`` is
    the calculated exclusivity *intent* only: site Slurm remains authoritative
    and an enabled ``exclusive`` mapping decides what is actually emitted.
    ``defaults_used`` is true when any field was derived from the allocation
    defaults or node capacities rather than an explicit override.
    """

    nodes: int
    cpus_per_node: int | None = None
    gpus_per_node: int | None = None
    memory_per_node_mib: int | None = None
    exclusive: bool = False
    mode: str = ""
    defaults_used: bool = False
    overrides: Mapping[str, Any] = field(default_factory=dict)
    not_emitted: tuple[str, ...] = ()

    def calculated_fields(self) -> tuple[tuple[str, Any], ...]:
        """Calculated fields with a concrete value, in canonical order."""
        fields: list[tuple[str, Any]] = [("nodes", self.nodes)]
        for key in ("cpus_per_node", "gpus_per_node", "memory_per_node_mib"):
            value = getattr(self, key)
            if value is not None:
                fields.append((key, value))
        return tuple(fields)


def parse_memory_mib(value: str | int, *, where: str = "memory") -> int:
    """Parse a memory quantity into whole MiB.

    See the module docstring for the unit semantics.  ``int`` inputs are taken
    as plain MiB (TOML has no unit literals, so a bare integer is MiB by
    convention); strings require an explicit unit from K/M/G/T with optional
    ``i``.
    """
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ConfigError(
            f"{where} must be a memory quantity string (e.g. '378000M')"
        )
    if isinstance(value, int):
        if value <= 0:
            raise ConfigError(f"{where} must be a positive memory quantity")
        return value
    match = _MEMORY_RE.match(value.strip())
    if match is None:
        raise ConfigError(
            f"{where} must look like '<number><unit>' with unit K/M/G/T and "
            f"optional i, e.g. '100G', '378000M', '200GiB' (got {value!r})"
        )
    number, unit = match.groups()
    mult = _MEMORY_MULT[unit[0].lower()]
    # Exact integer arithmetic for whole numbers (Python ints are arbitrary
    # precision, so 378000 * 1024 ** 2 is exact for any practical value);
    # floats only for the optional fractional part.
    if "." in number:
        scaled = int(round(float(number) * mult))
    else:
        whole = int(number)
        if mult == 1.0:
            scaled = whole
        elif mult == 1024.0:
            scaled = whole * 1024
        elif mult == 1024.0 * 1024.0:
            scaled = whole * (1024 * 1024)
        else:  # 1/1024 (K)
            scaled = whole // 1024  # truncates: no fractional MiB below 1 MiB
    if scaled <= 0:
        raise ConfigError(
            f"{where} must resolve to at least 1 MiB (got {value!r} = {scaled} MiB)"
        )
    return scaled


def _positive_int(value: Any, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{where} must be a positive integer")
    return value


def _effective_tasks_per_node(options: Mapping[str, Any]) -> int:
    """Effective task count of a stage from its manual options (0 = unset).

    ``ntasks-per-node`` wins; the total ``ntasks`` count equals the per-node
    count in the one-task-per-node layout and is a valid pinning.  Returns 0
    when neither is set (the mapping precondition then fails with an
    actionable hint).
    """
    for key in _TASK_OPTION_NAMES:
        if key in options:
            raw = options[key]
            if isinstance(raw, (list, tuple)):
                raw = raw[0] if raw else None
            if not isinstance(raw, int) or isinstance(raw, bool) or raw <= 0:
                raise ConfigError(f"slurm {key} must be a positive integer when set")
            return raw
    return 0


def validate_conflicts(target: TargetConfig) -> None:
    """Refuse enabled mappings that collide with manual options in the stage.

    An enabled mapping and a conflicting manual option is always an error,
    never a silent precedence rule: the error names the target, the calculated
    field and the manual key.  ``cpus-per-node -> cpus-per-task`` additionally
    requires a one-task-per-node layout (``ntasks-per-node`` or ``ntasks`` ==
    1), re-checked from the stage's manual options (T1 validates it at load
    time; this re-run keeps plan-time and config-time semantics locked).
    Stages are checked independently: only a manual option in the *same* stage
    conflicts, which is also why the two stages can map the same calculated
    value differently.
    """
    slurm = target.slurm
    if slurm is None:
        return
    for stage_name, stage in (("sbatch", slurm.sbatch), ("srun", slurm.srun)):
        if stage is None:
            continue
        _check_stage_conflicts(target, stage_name, stage)


def _check_stage_conflicts(
    target: TargetConfig, stage_name: str, stage: SlurmStageConfig
) -> None:
    options = stage.options
    prefix = f"target {target.name!r}, stage {stage_name!r}"

    families = (
        ("nodes", _NODES_CONFLICT_KEYS),
        ("gpus-per-node", _GPU_CONFLICT_KEYS),
        ("memory-per-node", _MEMORY_CONFLICT_KEYS),
        ("exclusive", ("exclusive",)),
    )
    for calculated, keys in families:
        if calculated not in stage.mapping:
            continue
        for key in keys:
            if key in options:
                raise _conflict(prefix, calculated, key)

    # cpus-per-task is a translation, not a free-form option name: it is only
    # safe with exactly one task per node (T1 load-time rule, re-checked here).
    if "cpus-per-node" in stage.mapping:
        effective = _effective_tasks_per_node(options)
        if effective != 1:
            raise ConfigError(
                f"{prefix}: mapping cpus-per-node -> cpus-per-task is only "
                "valid with exactly one task per node; set "
                f"ntasks-per-node = 1 (or ntasks = 1) in the {stage_name!r} "
                f"stage (found an effective task count of {effective})"
            )


def _conflict(prefix: str, calculated: str, key: str) -> ConfigError:
    return ConfigError(
        f"{prefix}: manual option {key!r} conflicts with the enabled mapping "
        f"for {calculated!r} (the mapping would emit a value for {calculated!r}); "
        "remove either the mapping or the manual option"
    )


def _parse_overrides(target: TargetConfig, overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    """Validate and normalize ``--set`` overrides; unknown keys/bad values
    raise :class:`ConfigError` before the plan is built (overrides resolve
    BEFORE mapping)."""
    parsed: dict[str, Any] = {}
    for key, value in (overrides or {}).items():
        if key not in OVERRIDE_KEYS:
            raise ConfigError(
                f"unknown --set key {key!r}; valid keys: {', '.join(OVERRIDE_KEYS)}"
            )
        if key == "nodes":
            parsed[key] = _positive_int(value, "--set nodes")
        elif key in ("gpus-per-node", "cpus-per-node"):
            parsed[key] = _positive_int(value, f"--set {key}")
        elif key == "mem-per-node":
            parse_memory_mib(value, where="--set mem-per-node")
            parsed[key] = value
        else:  # mode
            if not isinstance(value, str) or value not in ALLOCATION_MODES:
                raise ConfigError(
                    f"--set mode must be one of {', '.join(ALLOCATION_MODES)}"
                )
            parsed[key] = value
    return parsed


def compute_plan(
    target: TargetConfig,
    overrides: Mapping[str, Any] | None = None,
) -> ResolvedPlan:
    """Resolve the allocation plan for a connect/preview.

    ``--set`` overrides are parsed and validated first (resolved before
    mapping); then the node count and policy; then the per-node quantities:

    - One node is the default; a GPU system defaults to one GPU, a CPU-only
      system can use ``default-cpus``.
    - Single node uses the ``single-node`` policy (``mode`` override allowed);
      multi-node uses the ``multi-node`` policy, restricted to ``full`` and
      ``exclusive`` (an explicit partial GPU/CPU override conflicts with it).
    - ``gpu-proportional``: an integer per-GPU CPU share and a whole-MiB
      per-GPU memory share are derived from node capacities first, then
      multiplied by the requested GPU count.
    - ``cpu-proportional``: the memory share scales with the requested CPU
      count (initially CPU-only targets).
    - ``full``/``exclusive``: the complete configured per-node capacities, with
      the exclusivity intent only for ``exclusive``.
    - ``max-nodes`` bounds the requested node count.
    """
    allocation = target.allocation
    node = target.node
    parsed = _parse_overrides(target, overrides)
    validate_conflicts(target)

    nodes: int | None
    if "nodes" in parsed:
        nodes = parsed["nodes"]
    else:
        nodes = 1  # one node is the default
    max_nodes = allocation.max_nodes if allocation is not None else None
    if max_nodes is not None and nodes > max_nodes:
        raise ConfigError(
            f"target {target.name!r}: requested {nodes} nodes exceed "
            f"max-nodes = {max_nodes}"
        )

    defaults_used = False
    if "nodes" not in parsed:
        defaults_used = True

    multi = nodes > 1
    if multi:
        # Explicit --set mode overrides the configured multi-node policy and
        # is honoured, but only one of the allowed multi-node modes; a shared
        # / proportional mode explicitly conflicts with the full-resource
        # multi-node contract and must be rejected, not silently replaced.
        if "mode" in parsed:
            policy = parsed["mode"]
            if policy not in MULTI_NODE_MODES:
                raise ConfigError(
                    f"target {target.name!r}: requested multi-node mode "
                    f"{policy!r} is not allowed for multiple nodes; use one "
                    f"of {', '.join(MULTI_NODE_MODES)}"
                )
            defaults_used = False
        else:
            policy = (
                allocation.multi_node
                if allocation is not None and allocation.multi_node
                else "full"
            )
            if policy not in MULTI_NODE_MODES:
                raise ConfigError(
                    f"target {target.name!r}: multi-node mode {policy!r} is not "
                    f"allowed; use one of {', '.join(MULTI_NODE_MODES)}"
                )
    else:
        policy = parsed.get("mode") or (
            allocation.single_node
            if allocation is not None and allocation.single_node
            else "gpu-proportional"
        )
        if policy not in ALLOCATION_MODES:
            raise ConfigError(
                f"target {target.name!r}: single-node mode {policy!r} is "
                f"not allowed; use one of {', '.join(ALLOCATION_MODES)}"
            )

    gpus: int | None = parsed.get("gpus-per-node")
    cpus: int | None = parsed.get("cpus-per-node")
    memory: int | None = (
        parse_memory_mib(parsed["mem-per-node"], where="--set mem-per-node")
        if "mem-per-node" in parsed
        else None
    )

    if policy in ("full", "exclusive") and multi and (
        gpus is not None or cpus is not None
    ):
        field_name = "GPU" if gpus is not None else "CPU"
        raise ConfigError(
            f"target {target.name!r}: a {field_name} override "
            f"({gpus if gpus is not None else cpus}) conflicts with the "
            f"multi-node {policy!r} policy, which uses the complete "
            "configured per-node resources; remove the override"
        )

    capacity_gpus = node.gpus if node is not None else None
    capacity_cpus = node.cpus if node is not None else None
    capacity_mib: int | None = (
        parse_memory_mib(node.memory, where="node.memory")
        if node is not None and node.memory
        else None
    )
    if capacity_gpus is not None and capacity_gpus == 0:
        capacity_gpus = None

    if policy == "gpu-proportional":
        # Resolve the requested GPU count per node (default: one GPU).
        if gpus is None:
            gpus = 1  # GPU systems default to one GPU
            defaults_used = True
        elif capacity_gpus is not None and gpus > capacity_gpus:
            raise ConfigError(
                f"target {target.name!r}: requested {gpus} GPUs per node "
                f"exceed the node capacity of {capacity_gpus}"
            )
        if cpus is None:
            if capacity_gpus is None or capacity_gpus <= 0 or capacity_cpus is None:
                cpus = capacity_cpus  # no usable per-GPU share: full per-node capacity
            else:
                per_gpu = capacity_cpus // capacity_gpus
                if per_gpu < 1:
                    raise ConfigError(
                        f"target {target.name!r}: GPU-proportional mode needs "
                        f"at least one CPU per GPU, but the node has only "
                        f"{capacity_cpus} CPUs for {capacity_gpus} GPUs"
                    )
                cpus = per_gpu * gpus
            defaults_used = True
        elif capacity_cpus is not None and cpus > capacity_cpus:
            raise ConfigError(
                f"target {target.name!r}: requested {cpus} CPUs per node "
                f"exceed the node capacity of {capacity_cpus}"
            )
        if memory is None:
            if capacity_mib is None or capacity_gpus is None or capacity_gpus <= 0:
                memory = capacity_mib
            else:
                per_gpu = capacity_mib // capacity_gpus
                if per_gpu < 1:
                    raise ConfigError(
                        f"target {target.name!r}: GPU-proportional mode needs "
                        f"at least 1 MiB per GPU, but the node has only "
                        f"{capacity_mib} MiB for {capacity_gpus} GPUs"
                    )
                memory = per_gpu * gpus
            defaults_used = True
        elif capacity_mib is not None and memory > capacity_mib:
            # An explicit --set mem-per-node is checked against the node
            # capacity (as the CPU ceiling above and cpu-proportional / full /
            # exclusive do); a computed per-GPU share never exceeds capacity and
            # is left untouched.
            raise ConfigError(
                f"target {target.name!r}: requested {memory} MiB per node "
                f"exceed the node capacity of {capacity_mib} MiB"
            )
    elif policy == "cpu-proportional":
        if cpus is None:
            if capacity_cpus is None:
                cpus = (
                    allocation.default_cpus
                    if allocation is not None and allocation.default_cpus
                    else 1
                )
            else:
                cpus = capacity_cpus
            defaults_used = True
        elif capacity_cpus is not None and cpus > capacity_cpus:
            raise ConfigError(
                f"target {target.name!r}: requested {cpus} CPUs per node "
                f"exceed the node capacity of {capacity_cpus}"
            )
        if gpus is None:
            gpus = capacity_gpus or 0
        if memory is None:
            if capacity_mib is None:
                memory = 0
            elif capacity_cpus is None or cpus is None:
                memory = capacity_mib
            else:
                memory = (capacity_mib * cpus) // capacity_cpus
                if memory < 1:
                    memory = 1  # a CPU allocation keeps at least one MiB
            defaults_used = True
        if memory is not None and capacity_mib is not None and memory > capacity_mib:
            raise ConfigError(
                f"target {target.name!r}: requested {memory} MiB per node "
                f"exceed the node capacity of {capacity_mib} MiB"
            )
    elif policy in ("full", "exclusive"):
        if gpus is None:
            gpus = capacity_gpus or 0
            defaults_used = True
        elif capacity_gpus is not None and gpus > capacity_gpus:
            raise ConfigError(
                f"target {target.name!r}: requested {gpus} GPUs per node "
                f"exceed the node capacity of {capacity_gpus}"
            )
        if cpus is None:
            cpus = capacity_cpus
            defaults_used = True
        elif capacity_cpus is not None and cpus > capacity_cpus:
            raise ConfigError(
                f"target {target.name!r}: requested {cpus} CPUs per node "
                f"exceed the node capacity of {capacity_cpus}"
            )
        if memory is None:
            memory = capacity_mib
            defaults_used = True
        elif capacity_mib is not None and memory > capacity_mib:
            raise ConfigError(
                f"target {target.name!r}: requested {memory} MiB per node "
                f"exceed the node capacity of {capacity_mib}"
            )
        if cpus is not None and cpus < 1:
            raise ConfigError(
                f"target {target.name!r}: allocation with fewer than one CPU "
                f"per node (cpus-per-node = {cpus}); raise the request or the "
                "node capacity"
            )

    exclusive = policy == "exclusive"
    return ResolvedPlan(
        nodes=nodes,
        cpus_per_node=cpus,
        gpus_per_node=gpus,
        memory_per_node_mib=memory,
        exclusive=exclusive,
        mode=policy,
        defaults_used=defaults_used,
        overrides=dict(parsed),
    )


def _ctx(target: TargetConfig | None, stage_name: str) -> str:
    name = target.name if target is not None else "?"
    return f"target {name!r}, stage {stage_name!r}"


def _validate_option_name(key: str, stage_name: str, target: TargetConfig | None) -> None:
    """Validate a manual option name: plain token, no leading ``--`` injection."""
    if not key or key != key.strip():
        raise ConfigError(f"{_ctx(target, stage_name)}: invalid option name {key!r}")
    if any(ch.isspace() or ord(ch) < 0x20 for ch in key):
        raise ConfigError(
            f"{_ctx(target, stage_name)}: option name {key!r} must not contain "
            "whitespace or control characters"
        )
    if key.startswith(("-", ",", "\\", '"', "'")):
        raise ConfigError(
            f"{_ctx(target, stage_name)}: option name {key!r} is not a "
            "plain option name"
        )


def _check_value_text(value: Any, key: str, stage_name: str, target: TargetConfig | None) -> str:
    """String form of an option value, rejecting \\n, \\r and NUL."""
    text = str(value)
    for marker in ("\n", "\r", "\x00"):
        if marker in text:
            raise ConfigError(
                f"{_ctx(target, stage_name)}: value for option {key!r} "
                "must not contain newlines, carriage returns or NUL"
            )
    return text


def _render_manual(
    target: TargetConfig, stage_name: str, options: Mapping[str, Any]
) -> list[str]:
    args: list[str] = []
    for key, value in options.items():
        _validate_option_name(key, stage_name, target)
        if key.lower() in PROTOCOL_OPTIONS:
            raise ConfigError(
                f"{_ctx(target, stage_name)}: option {key!r} is a launcher "
                f"protocol option owned by the provisioning helper; do not "
                f"set it under slurm.{stage_name}"
            )
        if isinstance(value, bool):
            if value:
                args.append(f"--{key}")
            # bool False: omit
            continue
        if isinstance(value, (list, tuple)):
            if value:  # repeated options (arrays); empty arrays are dropped
                for item in value:
                    if isinstance(item, bool) or not isinstance(item, (str, int)):
                        raise ConfigError(
                            f"{_ctx(target, stage_name)}: option {key!r} must "
                            "contain only strings or integers"
                        )
                    text = _check_value_text(item, key, stage_name, target)
                    args.append(f"--{key}={text}" if text else f"--{key}")
            continue
        if value is None or isinstance(value, (dict, set, bytes)):
            raise ConfigError(
                f"{_ctx(target, stage_name)}: option {key!r} must be a "
                f"string, an integer or an array, not {type(value).__name__}"
            )
        text = _check_value_text(value, key, stage_name, target)
        args.append(f"--{key}={text}" if text else f"--{key}")
    return args


def _render_mapped(
    target: TargetConfig,
    stage_name: str,
    stage: SlurmStageConfig | None,
    plan: ResolvedPlan,
) -> tuple[list[str], tuple[str, ...]]:
    """Render the stage's calculated-value mappings; return (args, emitted).

    A mapping for a field whose value is ``None`` (or 0 where 0 is "none")
    emits nothing and is not reported as emitted.  Missing mapping tables
    yield only the manual arguments.
    """
    emitted: list[str] = []
    values: list[str] = []
    if stage is None or not stage.mapping:
        return values, tuple(emitted)
    for key, representation in stage.mapping.items():
        if key == "nodes":
            if plan.nodes is None or plan.nodes <= 0:
                continue
            values.append(f"--nodes={plan.nodes}")
        elif key == "gpus-per-node":
            if plan.gpus_per_node is None or plan.gpus_per_node <= 0:
                continue
            if representation == "gres":
                values.append(f"--gres=gpu:{plan.gpus_per_node}")
            else:  # gpus-per-node
                values.append(f"--gpus-per-node={plan.gpus_per_node}")
        elif key == "cpus-per-node":
            if plan.cpus_per_node is None or plan.cpus_per_node < 1:
                continue
            values.append(f"--cpus-per-task={plan.cpus_per_node}")
        elif key == "memory-per-node":
            if plan.memory_per_node_mib is None or plan.memory_per_node_mib <= 0:
                continue
            # Whole MiB with explicit M unit: the guaranteed-supported unit.
            values.append(f"--mem={plan.memory_per_node_mib}M")
        elif key == "exclusive":
            if not plan.exclusive:
                continue  # never implied: calculated false emits nothing
            values.append("--exclusive")
        else:
            raise AssertionError(
                f"unhandled mapping key {key!r} (guarded by the "
                "MAPPING_VOCABULARY validation at config load time)"
            )
        emitted.append(key)
    return values, tuple(emitted)


def render_args(
    target: TargetConfig, plan: ResolvedPlan
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return ``(sbatch_args, srun_args)``: complete option strings per stage.

    The two stages are rendered independently from their own manual options
    and own mapping tables; nothing is copied between them.  Every element of
    a returned tuple is one complete argument (e.g. ``--nodes=2``,
    ``--gres=gpu:2``): joining them with newlines fills
    ``COMPUTEMCP_SBATCH_ARGS`` / ``COMPUTEMCP_SRUN_ARGS`` for the provisioning
    helper, which supplies its own protocol options.  Order is stable and
    deterministic: manual options in configuration order first, then the
    mapped options in the stage's mapping order.
    """
    if target.slurm is None:
        return (), ()
    return (
        _render_stage(target, "sbatch", target.slurm.sbatch, plan),
        _render_stage(target, "srun", target.slurm.srun, plan),
    )


def _render_stage(
    target: TargetConfig,
    stage_name: str,
    stage: SlurmStageConfig | None,
    plan: ResolvedPlan,
) -> tuple[str, ...]:
    if stage is None:
        return ()  # absent stage: no arguments at all
    args = _render_manual(target, stage_name, stage.options)
    mapped, _ = _render_mapped(target, stage_name, stage, plan)
    combined = args + mapped
    # Verify no stage setting appears twice: a manual option and a mapped
    # value of the same family must have been caught by validate_conflicts,
    # so a duplicate is a bug, not something to silently dedupe.
    seen: set[str] = set()
    excess: list[str] = []
    for entry in combined:
        if entry in seen:
            excess.append(entry)
        else:
            seen.add(entry)
    if excess:
        raise AssertionError(
            f"{_ctx(target, stage_name)}: settings {sorted(set(excess))} "
            "appeared twice; a conflicting manual option and a mapping must "
            "not both emit"
        )
    return tuple(combined)


def plan_summary(target: TargetConfig, plan: ResolvedPlan) -> dict:
    """Serializable preview of the plan, per-stage emitted args and un-emitted
    calculated fields for the ``--dry-run`` output.

    ``args`` is the per-stage rendered argument list; ``emitted`` names the
    calculated fields each stage's mapping turned into options; ``not_emitted``
    lists the calculated fields no stage emitted (e.g. a calculated memory
    share when only a manual ``mem`` option is configured, or an exclusivity
    intent with no ``exclusive`` mapping).  ``manual`` carries the verbatim
    manual options per stage.  The preview must keep the *calculated* intent,
    the *emitted* request and the possibly-different manual memory apart.
    """
    slurm = target.slurm
    emitted: dict[str, list[str]] = {"sbatch": [], "srun": []}
    manual: dict[str, dict[str, Any]] = {"sbatch": {}, "srun": {}}
    if slurm is not None:
        for stage_name, stage in (("sbatch", slurm.sbatch), ("srun", slurm.srun)):
            if stage is None:
                continue
            _, fields = _render_mapped(target, stage_name, stage, plan)
            emitted[stage_name] = list(fields)
            manual[stage_name] = {str(k): v for k, v in stage.options.items()}
    # Map each plan field to its canonical mapping key so that "emitted"
    # (named per mapping key, hyphens) and "not_emitted" compare like-for-like.
    field_to_mapping_key = {
        "nodes": "nodes",
        "cpus_per_node": "cpus-per-node",
        "gpus_per_node": "gpus-per-node",
        "memory_per_node_mib": "memory-per-node",
    }
    all_calculated = [
        (field, field_to_mapping_key[field])
        for field, value in plan.calculated_fields()
        if value is not None
    ]
    emitted_keys = {f for fields in emitted.values() for f in fields}
    # Report each un-emitted field by its canonical mapping key (same naming
    # convention as "emitted") so the preview can cross-reference cleanly.
    not_emitted = [mk for field, mk in all_calculated if mk not in emitted_keys]
    sbatch, srun = render_args(target, plan)
    return {
        "target": target.name,
        "plan": {
            "nodes": plan.nodes,
            "cpus_per_node": plan.cpus_per_node,
            "gpus_per_node": plan.gpus_per_node,
            "memory_per_node_mib": plan.memory_per_node_mib,
            "exclusive": plan.exclusive,
            "mode": plan.mode,
        },
        "defaults_used": plan.defaults_used,
        "overrides": dict(plan.overrides),
        "manual": manual,
        "args": {"sbatch": list(sbatch), "srun": list(srun)},
        "emitted": emitted,
        "not_emitted": not_emitted,
    }

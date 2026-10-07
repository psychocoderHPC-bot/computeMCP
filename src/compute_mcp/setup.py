# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Interactive setup wizard for the computeMCP gateway.

``computeMCP-gateway --bootstrap`` creates the initial configuration (server,
auth, one client, and optionally one or more targets) and writes a hashed client
token. ``computeMCP-gatewayctl --add-target`` appends a target to an existing
configuration.

The wizard is deliberately dependency-free: it writes the same TOML the loader
validates, re-reads and validates it before declaring success, and rolls back an
append that would not load.  Every question prints a short description, and
fixed-answer questions list their options.  Prompt I/O is injectable so tests can
drive the flow without a terminal.
"""

from __future__ import annotations

import contextlib
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from .auth import hash_token, new_token
from .config import (
    ALLOCATION_MODES,
    MULTI_NODE_MODES,
    OPERATOR_TOKEN_NAME,
    ConfigError,
    _atomic_write,
    append_include,
    append_token_hash,
    default_token_path,
    load_config,
    validate_target_name,
)

GPU_VENDORS = ("nvidia", "amd", "intel")
CONTAINER_RUNTIMES = ("apptainer", "docker")
TRANSPORTS = ("tunnel", "direct")
# OpenSSH Host aliases are arbitrary tokens; reject whitespace and glob/negation
# metacharacters that `ssh -G` would interpret specially.
_SSH_ALIAS_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class WizardAbort(SystemExit):
    """Raised when the operator interrupts the wizard."""


@dataclass
class TargetAnswers:
    """Collected answers for one ``[targets.X]`` block."""

    name: str
    transport: str = "tunnel"
    ssh_targets: tuple[str, ...] = ()
    direct_host: str | None = None
    direct_port: int = 2222
    user: str = ""
    client_key: str = ""
    host_key_sha256: str | None = None
    host_key_check: str = "on"
    host_key_algorithms: tuple[str, ...] = ()
    known_hosts: str | None = None
    proxy_jump: str | None = None
    interactive_auth: bool = False
    auto_connect: bool = True
    container_runtime: str | None = None
    container_storage_root: str | None = None
    container_image: str | None = None
    container_gpus: tuple[str, ...] = ()
    container_host_home: str | None = None
    bundle: bool = False
    bundle_deploy_dir: str | None = None
    bundle_provision_env: tuple[str, ...] = ()
    # Whether the target sits behind a Slurm scheduler.  Independent of the
    # bundle: a plain Docker host uses the same generic provisioner without a
    # node/allocation/slurm block.
    use_slurm: bool = False
    node_cpus: int | None = None
    node_gpus: int | None = None
    node_memory: str | None = None
    allocation_single: str | None = None
    allocation_multi: str | None = None
    allocation_max_nodes: int | None = None
    sbatch_partition: str | None = None
    sbatch_time: str | None = None
    srun_cpu_bind: str | None = None
    extra: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Prompt layer
# ---------------------------------------------------------------------------
class Wizard:
    """Prompt helper with injectable I/O and per-question descriptions."""

    def __init__(self, *, input_fn=input, print_fn=print, terminal: bool | None = None):
        self._input = input_fn
        self._print = print_fn
        self.terminal = sys.stdin.isatty() if terminal is None else terminal

    def _require_terminal(self) -> None:
        if not self.terminal:
            raise WizardAbort(
                "the setup wizard needs an interactive terminal; run it without "
                "--non-interactive, or edit the configuration by hand"
            )

    def say(self, message: str = "") -> None:
        self._print(message)

    def section(self, title: str) -> None:
        self._print("")
        self._print(f"== {title} ==")

    def ask(
        self,
        question: str,
        *,
        description: str | None = None,
        default: str | None = None,
        required: bool = True,
        validator=None,
        choices: tuple[str, ...] | None = None,
    ) -> str:
        self._require_terminal()
        if description:
            self._print(f"  {description}")
        if choices:
            self._print(f"  options: {', '.join(choices)}")
        suffix = f" [{default}]" if default not in (None, "") else ""
        while True:
            try:
                raw = self._input(f"{question}{suffix}: ").strip()
            except EOFError as exc:
                raise WizardAbort("input closed; aborting setup") from exc
            except KeyboardInterrupt as exc:
                raise WizardAbort("setup aborted") from exc
            value = raw or (default or "")
            if not value:
                if required:
                    self._print("  a value is required")
                    continue
                return ""
            # Answers become TOML values; a newline or other control character
            # would yield invalid TOML that only fails later at validation.
            if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
                self._print("  no control characters or newlines")
                continue
            if choices and value not in choices:
                self._print(f"  choose one of: {', '.join(choices)}")
                continue
            if validator is not None:
                message = validator(value)
                if message:
                    self._print(f"  {message}")
                    continue
            return value

    def confirm(
        self,
        question: str,
        *,
        default: bool = True,
        description: str | None = None,
    ) -> bool:
        if description:
            self._print(f"  {description}")
        answer = self.ask(
            question,
            default=("y" if default else "n"),
            choices=("y", "n", "yes", "no"),
            required=False,
        )
        answer = answer.lower()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        return default

    def ask_int(
        self,
        question: str,
        *,
        description: str | None = None,
        default: int | None = None,
        minimum: int = 0,
    ) -> int | None:
        def check(value: str) -> str | None:
            try:
                number = int(value)
            except ValueError:
                return "enter a whole number"
            if number < minimum:
                return f"must be >= {minimum}"
            return None

        result = self.ask(
            question,
            description=description,
            default=str(default) if default is not None else None,
            required=default is not None,
            validator=check,
        )
        return int(result) if result else None


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------
def _validate_port(value: str) -> str | None:
    try:
        port = int(value)
    except ValueError:
        return "enter a port number"
    if not 1 <= port <= 65535:
        return "port must be between 1 and 65535"
    return None


def _validate_target_name(value: str) -> str | None:
    try:
        validate_target_name(value)
    except ConfigError as exc:
        return str(exc)
    return None


def _split_list(value: str) -> tuple[str, ...]:
    """Split a comma/whitespace separated answer into ordered unique items."""
    items = [item.strip() for item in value.replace(" ", ",").split(",")]
    return tuple(dict.fromkeys(item for item in items if item))


def _normalize_user(value: str) -> str:
    """Map the explicit "unset" sentinel to an empty user string."""
    return "" if value.strip() in ("-", "none", "unset") else value.strip()


def _validate_aliases(value: str) -> str | None:
    aliases = _split_list(value)
    if not aliases:
        return "enter at least one alias"
    for alias in aliases:
        if not _SSH_ALIAS_RE.match(alias):
            return f"invalid alias {alias!r}"
    return None


def _validate_sha256(value: str) -> str | None:
    if not value.startswith("SHA256:") or len(value) < 12:
        return "expected a value like 'SHA256:...'"
    return None


def _validate_absolute_path(value: str) -> str | None:
    if not value.startswith("/"):
        return "enter an absolute path"
    return None


def _validate_remote_path(value: str) -> str | None:
    """Accept an absolute path or a leading ``$HOME``/``~`` reference.

    The scripts expand these in the remote shell, so the operator does not have
    to know the remote home directory.
    """
    if value.startswith("/") or value.startswith("$HOME") or value.startswith("~"):
        return None
    return "enter an absolute path or a $HOME/... reference"


def _expand_local_home(value: str) -> str:
    """Expand a leading ``~`` or ``$HOME`` using the gateway user's home.

    Used only for a local probe default; remote values keep the placeholder.
    """
    home = str(Path.home())
    if value == "~":
        return home
    if value.startswith("~/"):
        return home + value[1:]
    if value.startswith("$HOME"):
        return home + value[len("$HOME") :]
    return value


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def _toml_str(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _toml_array(values: tuple[str, ...]) -> str:
    return "[" + ", ".join(_toml_str(v) for v in values) + "]"


def render_target_block(answers: TargetAnswers) -> str:
    """Render one target as a commented TOML block ending in a newline."""
    lines: list[str] = ["", f"[targets.{answers.name}]"]
    if answers.transport == "direct":
        lines.append('transport = "direct"')
        lines.append(f"remote_host = {_toml_str(answers.direct_host or '127.0.0.1')}")
        lines.append(f"remote_port = {answers.direct_port}")
    elif answers.ssh_targets:
        lines.append(f"ssh_targets = {_toml_array(answers.ssh_targets)}")
    if answers.user:
        lines.append(f"user = {_toml_str(answers.user)}")
    if answers.client_key:
        lines.append(f"client_key = {_toml_str(answers.client_key)}")
    if answers.host_key_sha256:
        lines.append(f"host_key_sha256 = {_toml_str(answers.host_key_sha256)}")
    if answers.host_key_check != "on":
        lines.append(f'host_key_check = {_toml_str(answers.host_key_check)}')
    if answers.host_key_algorithms:
        lines.append(
            f"host_key_algorithms = {_toml_array(answers.host_key_algorithms)}"
        )
    if answers.known_hosts:
        lines.append(f"known_hosts = {_toml_str(answers.known_hosts)}")
    if answers.proxy_jump:
        lines.append(f"proxy_jump = {_toml_str(answers.proxy_jump)}")
    if answers.interactive_auth:
        lines.append("interactive_auth = true")
    # The config loader defaults auto_connect to false, so the wizard default
    # (true) must be rendered explicitly to round-trip.  A target configured
    # for auto-connect needs the key present as true; omitting it (or writing
    # false) would fall back to the loader default and never auto-connect.
    if answers.auto_connect:
        lines.append("auto_connect = true")
    if answers.bundle:
        lines.append("")
        lines.append(f"[targets.{answers.name}.bundle]")
        lines.append('source = "computemcp-container"')
        if answers.bundle_deploy_dir:
            lines.append(f"deploy-dir = {_toml_str(answers.bundle_deploy_dir)}")
        if answers.bundle_provision_env:
            lines.append(
                f"provision-env = {_toml_array(answers.bundle_provision_env)}"
            )
    if answers.container_runtime:
        lines.append("")
        lines.append(f"[targets.{answers.name}.container]")
        lines.append(f"runtime = {_toml_str(answers.container_runtime)}")
        if answers.container_storage_root:
            lines.append(f"storage-root = {_toml_str(answers.container_storage_root)}")
        if answers.container_image:
            lines.append(f"image = {_toml_str(answers.container_image)}")
        if answers.container_gpus:
            lines.append(f"gpus = {_toml_array(answers.container_gpus)}")
        if answers.container_host_home:
            lines.append(f"host-home = {_toml_str(answers.container_host_home)}")
    if answers.node_cpus is not None or answers.node_gpus is not None or answers.node_memory:
        lines.append("")
        lines.append(f"[targets.{answers.name}.node]")
        if answers.node_cpus is not None:
            lines.append(f"cpus = {answers.node_cpus}")
        if answers.node_gpus is not None:
            lines.append(f"gpus = {answers.node_gpus}")
        if answers.node_memory:
            lines.append(f"memory = {_toml_str(answers.node_memory)}")
    if (
        answers.allocation_single
        or answers.allocation_multi
        or answers.allocation_max_nodes is not None
    ):
        lines.append("")
        lines.append(f"[targets.{answers.name}.allocation]")
        if answers.allocation_single:
            lines.append(f"single-node = {_toml_str(answers.allocation_single)}")
        if answers.allocation_multi:
            lines.append(f"multi-node = {_toml_str(answers.allocation_multi)}")
        if answers.allocation_max_nodes is not None:
            lines.append(f"max-nodes = {answers.allocation_max_nodes}")
    if answers.sbatch_partition or answers.sbatch_time:
        lines.append("")
        lines.append(f"[targets.{answers.name}.slurm.sbatch]")
        if answers.sbatch_partition:
            lines.append(f"partition = {_toml_str(answers.sbatch_partition)}")
        if answers.sbatch_time:
            lines.append(f"time = {_toml_str(answers.sbatch_time)}")
        lines.append("ntasks-per-node = 1")
    if answers.srun_cpu_bind:
        lines.append("")
        lines.append(f"[targets.{answers.name}.slurm.srun]")
        lines.append("ntasks-per-node = 1")
        lines.append(f"cpu-bind = {_toml_str(answers.srun_cpu_bind)}")
    return "\n".join(lines) + "\n"


def render_gateway_config(
    *,
    listen: str,
    port: int,
    allow_enrollment: bool,
    client_id: str,
    client_targets: tuple[str, ...],
    client_label: str | None,
    token_file: str | None,
    include: list[str] | None = None,
) -> str:
    """Render the whole gateway configuration as commented TOML.

    Targets live in their own include files (see :func:`target_relative_path`);
    the main file only lists them under ``include``.
    """
    lines = [
        "# computeMCP gateway configuration.",
        "#",
        "# Generated by `computeMCP-gateway --bootstrap`.  Each target is a",
        "# separate file under systems/, included below.  Edit freely; the",
        "# gateway validates the file on start and on reload.",
    ]
    if include:
        lines.append("")
        lines.append("include = [")
        for entry in include:
            lines.append(f"    {_toml_str(entry)},")
        lines.append("]")
    lines += [
        "",
        "[server]",
        f"listen = {_toml_str(listen)}",
        f"port = {port}",
        f"allow_enrollment = {'true' if allow_enrollment else 'false'}",
        "",
        "[ssh]",
        "",
        "[sessions]",
        "",
        "[auth]",
    ]
    if token_file:
        lines.append(f"token_file = {_toml_str(token_file)}")
    lines.append("")
    lines.append("[clients." + client_id + "]")
    lines.append(f"targets = {_toml_array(client_targets)}")
    if client_label:
        lines.append(f"label = {_toml_str(client_label)}")
    lines.append("")
    return "\n".join(lines).rstrip("\n") + "\n"


def render_tokens_file(entries: list[tuple[str, str]]) -> str:
    lines = [
        "# Generated by computeMCP setup; contains sha256 hashes only.",
        "# Keep plaintext tokens out of this file.",
        "",
        "[tokens]",
    ]
    for client_id, token in entries:
        lines.append(f"{_toml_str(client_id)} = {_toml_str(hash_token(token))}")
    return "\n".join(lines) + "\n"


SYSTEMS_DIRNAME = "systems"


def target_relative_path(name: str) -> str:
    """Path of a target's include file relative to the config directory."""
    return f"{SYSTEMS_DIRNAME}/{name}.toml"


def render_target_file(answers: TargetAnswers) -> str:
    """Render one target as a standalone, includable TOML file."""
    header = (
        f"# computeMCP target {answers.name!r}.\n"
        f"# Included by the main gateway configuration; edit freely.\n"
    )
    return header + render_target_block(answers).strip("\n") + "\n"


def write_target_file(config_dir: Path, answers: TargetAnswers) -> Path:
    """Write a target's include file and return its path.

    The file lives in a ``systems/`` subdirectory so a target name can never
    collide with the main config or the token file.
    """
    systems_dir = config_dir / SYSTEMS_DIRNAME
    systems_dir.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        systems_dir.chmod(0o700)
    target_path = systems_dir / f"{answers.name}.toml"
    _atomic_write(target_path, render_target_file(answers), mode=0o600)
    return target_path


# ---------------------------------------------------------------------------
# SSH alias probing
# ---------------------------------------------------------------------------
def probe_ssh_alias(alias: str) -> dict | None:
    """Best-effort ``ssh -G <alias>`` lookup; returns None on any failure."""
    try:
        result = subprocess.run(
            ["ssh", "-G", alias],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    info: dict = {"identityfiles": []}
    for line in result.stdout.splitlines():
        key, _, value = line.partition(" ")
        value = value.strip()
        if key == "user" and value:
            info["user"] = value
        elif key == "hostname" and value:
            info["hostname"] = value
        elif key == "port" and value:
            info["port"] = value
        elif key == "identityfile" and value:
            info["identityfiles"].append(value)
    return info


# ---------------------------------------------------------------------------
# Target wizard
# ---------------------------------------------------------------------------
def collect_target(wizard: Wizard, existing: set[str]) -> TargetAnswers:
    """Ask every question for one target; optional block when applicable."""
    existing_lower = {name.lower() for name in existing}

    def unique_name(value: str) -> str | None:
        message = _validate_target_name(value)
        if message:
            return message
        if value.lower() in existing_lower:
            return f"target {value!r} already exists"
        return None

    wizard.section("Target")
    wizard.say(
        "  A target is one remote system the gateway connects to.  The name is"
    )
    wizard.say("  only used inside computeMCP.")
    name = wizard.ask(
        "Target name", description="short label, e.g. hal or rosi", validator=unique_name
    )

    wizard.say("")
    wizard.say("  The gateway reaches the target through an SSH alias from")
    wizard.say("  ~/.ssh/config (recommended) or a direct address.")
    transport = wizard.ask(
        "Transport",
        description="tunnel uses an SSH alias; direct dials host:port",
        default="tunnel",
        choices=TRANSPORTS,
    )

    answers = TargetAnswers(name=name, transport=transport)
    session_user = os.environ.get("USER") or ""
    if transport == "tunnel":
        alias = wizard.ask(
            "SSH alias",
            description="one or more ~/.ssh/config Host names, tried in order; "
            "comma-separate for failover, e.g. hal,ex_hal",
            validator=_validate_aliases,
        )
        aliases = _split_list(alias)
        answers.ssh_targets = aliases
        probed = probe_ssh_alias(aliases[0])
        default_user = (probed or {}).get("user") or session_user
        remote_user = wizard.ask(
            "Remote user",
            description="login user for the container/route connection; press "
            "Enter for the current user, or type - to leave it unset and let "
            "the SSH config decide",
            default=default_user,
            required=False,
        )
        answers.user = _normalize_user(remote_user)
        default_key = str(Path.home() / ".ssh" / "computemcp_container")
        answers.client_key = wizard.ask(
            "Private key path",
            description="dedicated gateway key; never your personal key",
            default=default_key,
        )
    else:
        host = wizard.ask("Remote host", description="IP or hostname")
        port = wizard.ask_int(
            "Remote port", description="container sshd port", default=2222, minimum=1
        )
        answers.ssh_targets = ()
        answers.direct_host = host
        answers.direct_port = port or 2222
        remote_user = wizard.ask(
            "Remote user",
            description="container login user; press Enter for the current "
            f"user ({session_user or 'unset'}), or - to leave it unset",
            default=session_user,
            required=False,
        )
        answers.user = _normalize_user(remote_user)

    answers.host_key_sha256 = wizard.ask(
        "Container host-key fingerprint",
        description="pin the container sshd key as SHA256:...; leave blank to "
        "disable host-key verification",
        default=None,
        required=False,
        validator=_validate_sha256,
    ) or None
    if answers.host_key_sha256 is None:
        answers.host_key_check = "off"
        wizard.say(
            "  Blank fingerprint: host-key verification is disabled "
            '(host_key_check = "off").'
        )
    else:
        algorithms = wizard.ask(
            "Host-key algorithms",
            description="comma list of ssh-keygen key types to accept",
            default="ssh-ed25519",
        )
        answers.host_key_algorithms = _split_list(algorithms)

    answers.interactive_auth = wizard.confirm(
        "Does the login node require a second factor (password/OTP)?",
        default=False,
        description="2FA targets connect only on an explicit connect/refresh",
    )
    if answers.interactive_auth:
        # A second factor cannot be supplied while the gateway connects on its
        # own, so auto-connect is forced off and the question is skipped.
        answers.auto_connect = False
        wizard.say(
            "  Auto-connect is unavailable for 2FA targets; the gateway "
            "connects this target only on an explicit connect/refresh."
        )
    else:
        answers.auto_connect = wizard.confirm(
            "Automatically connect this target on gateway start?",
            default=True,
            description="disabled targets connect only on an explicit "
            "connect/refresh",
        )

    # -- container ---------------------------------------------------------
    wizard.section("Container")
    if wizard.confirm(
        "Configure the development container?",
        default=True,
        description="runtime, storage, base image and GPU vendors",
    ):
        answers.container_runtime = wizard.ask(
            "Container runtime",
            description="how the container is built and started on the target",
            default="apptainer",
            choices=CONTAINER_RUNTIMES,
        )
        answers.container_storage_root = wizard.ask(
            "Storage root",
            description="remote directory for sandbox, home and state; must be "
            "visible to login and compute nodes. Use $HOME or ~ for the remote "
            "home, e.g. $HOME/computemcp",
            default="$HOME/computemcp",
            validator=_validate_remote_path,
        )
        default_image = (
            "docker://ubuntu:24.04"
            if answers.container_runtime == "apptainer"
            else "ubuntu:24.04"
        )
        answers.container_image = wizard.ask(
            "Base image",
            description="docker:// ref for Apptainer, plain ref for Docker",
            default=default_image,
        )
        vendors = wizard.ask(
            "GPU vendors",
            description="comma list, any of nvidia, amd, intel; blank for "
            "CPU-only",
            default="",
            required=False,
        )
        answers.container_gpus = tuple(
            v.strip() for v in vendors.replace(" ", "").split(",") if v.strip()
        )
        unknown = [v for v in answers.container_gpus if v not in GPU_VENDORS]
        if unknown:
            wizard.say(f"  ignoring unknown vendors: {', '.join(unknown)}")
            answers.container_gpus = tuple(
                v for v in answers.container_gpus if v in GPU_VENDORS
            )

    # A bundle needs tunnel transport and a client key (validated by the
    # loader); only offer it where it can actually work.  The shipped bundle is
    # the generic container provisioner, not Slurm-specific, so the provisioning
    # question is independent of the scheduler question below.
    if (
        answers.container_runtime
        and answers.transport == "tunnel"
        and answers.client_key
    ):
        if wizard.confirm(
            "Should the gateway build and start this container?",
            default=True,
            description="the gateway deploys the generic provisioning bundle "
            "over the route connection and runs it on the login node",
        ):
            answers.bundle = True
            if not answers.container_storage_root:
                # A bundle needs a deploy directory; otherwise the loader rejects it.
                answers.bundle_deploy_dir = wizard.ask(
                    "Bundle deploy directory",
                    description="remote directory on storage shared by login and "
                    "compute nodes; $HOME or ~ is expanded on the target",
                    default="$HOME/computemcp/bundle",
                    validator=_validate_remote_path,
                )
            raw_provision_env = wizard.ask(
                "Pre-provision environment (comma-separated shell lines)",
                description="lines run on the remote before the container runtime "
                "is used, e.g. 'module load apptainer'; empty is a no-op",
                default=None,
                required=False,
            )
            answers.bundle_provision_env = tuple(
                line.strip() for line in raw_provision_env.split(",") if line.strip()
            )

    # -- Slurm allocation (only for a Slurm target) ------------------------
    # These keys describe the cluster scheduler and affect only the plan.  The
    # question is asked only when a bundle is configured (a scheduler matters
    # only there), and the section itself is gated on the explicit answer so a
    # plain Docker host gets no node/allocation/slurm block.
    if answers.bundle:
        answers.use_slurm = wizard.confirm(
            "Is this target behind a Slurm scheduler?",
            default=False,
            description="enables the node, allocation and sbatch/srun plan "
            "questions; a plain container host answers no",
        )
    if answers.use_slurm:
        wizard.section("Slurm allocation")
        if wizard.confirm(
            "Configure the Slurm node capacities and allocation policy?",
            default=True,
            description="node sizes, the default allocation mode and the "
            "sbatch partition; needed for --set overrides and dry-run previews",
        ):
            answers.node_cpus = wizard.ask_int(
                "CPUs per node",
                description="allocatable Slurm CPUs",
                default=None,
                minimum=1,
            )
            answers.node_gpus = wizard.ask_int(
                "GPUs per node",
                description="scheduler-visible GPU units",
                default=None,
                minimum=0,
            )
            answers.node_memory = wizard.ask(
                "Memory per node",
                description="allocatable host memory with a unit, e.g. 378000M",
                default=None,
                required=False,
            ) or None
            answers.allocation_single = wizard.ask(
                "Single-node allocation mode",
                description="how one node is sized by default",
                default="gpu-proportional" if (answers.node_gpus or 0) else "cpu-proportional",
                choices=ALLOCATION_MODES,
            )
            answers.allocation_multi = wizard.ask(
                "Multi-node allocation mode",
                description="used when nodes > 1",
                default="exclusive",
                choices=MULTI_NODE_MODES,
            )
            answers.allocation_max_nodes = wizard.ask_int(
                "Maximum nodes",
                description="upper bound for --set nodes=",
                default=1,
                minimum=1,
            )
            answers.sbatch_partition = wizard.ask(
                "Slurm partition",
                description="partition name for sbatch",
                default=None,
                required=False,
            ) or None
            answers.sbatch_time = wizard.ask(
                "Time limit",
                description="wall time for sbatch, e.g. 02:00:00",
                default="02:00:00",
            )
            answers.srun_cpu_bind = wizard.ask(
                "srun cpu-bind",
                description="step CPU binding; 'none' keeps scheduler defaults",
                default="none",
            )
    return answers


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------
def _run_targets(wizard: Wizard) -> list[TargetAnswers]:
    targets: list[TargetAnswers] = []
    wanted = wizard.confirm(
        "Set up a target now?",
        default=True,
        description="you can add more later with "
        "'computeMCP-gatewayctl --add-target'",
    )
    while wanted:
        targets.append(collect_target(wizard, {t.name for t in targets}))
        wanted = wizard.confirm(
            "Add another target?", default=False, description="repeat the target questions"
        )
    return targets


def run_bootstrap(
    config_path: str | Path,
    *,
    force: bool = False,
    wizard: Wizard | None = None,
) -> int:
    """Create a new gateway configuration interactively."""
    wizard = wizard or Wizard()
    path = Path(config_path)
    token_path = path.parent / Path(default_token_path()).name

    wizard.say("computeMCP gateway setup")
    wizard.say("  This writes a configuration and one hashed operator token for")
    wizard.say("  computeMCP-gatewayctl.  Every question has a default in")
    wizard.say("  brackets; press Enter to accept it.")
    if path.exists() and not force:
        raise WizardAbort(
            f"{path} already exists; pass --force to overwrite or use "
            "'computeMCP-gatewayctl --add-target' to add a target"
        )

    wizard.section("Server")
    listen = wizard.ask(
        "Listen address",
        description="host interface for the gateway HTTP API",
        default="127.0.0.1",
    )
    port = wizard.ask_int(
        "Port", description="gateway HTTP port", default=2222, minimum=1
    )
    allow_enrollment = wizard.confirm(
        "Allow interactive enrollment (handshake)?",
        default=True,
        description="lets a Terok task request access; approval stays manual. "
        "Written to the config so you can disable it later.",
    )

    # The operator client for computeMCP-gatewayctl.  A Terok task receives its
    # own token through the handshake, so bootstrap mints no project token.
    client_id = "admin"
    client_label = "operator"
    client_targets = ("*",)

    targets = _run_targets(wizard)

    token = new_token()
    include = [target_relative_path(t.name) for t in targets]
    text = render_gateway_config(
        listen=listen,
        port=port,
        allow_enrollment=allow_enrollment,
        client_id=client_id,
        client_targets=client_targets,
        client_label=client_label,
        token_file=str(token_path),
        include=include,
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        path.parent.chmod(0o700)
    operator_token_path = path.parent / OPERATOR_TOKEN_NAME
    written_targets: list[Path] = []
    try:
        # Write the token hash first: the config names token_file explicitly,
        # and an explicit token file must exist when the config is validated.
        append_token_hash(token_path, client_id, token)
        with contextlib.suppress(OSError):
            token_path.chmod(0o600)
        # A host-local plaintext operator token so computeMCP-gatewayctl can
        # authenticate from the config alone, without an exported env token.
        _atomic_write(operator_token_path, token + "\n", mode=0o600)
        for target in targets:
            written_targets.append(write_target_file(path.parent, target))
        _atomic_write(path, text, mode=0o600)
        load_config(path, token_file=None)
    except (ConfigError, OSError) as exc:
        # Remove everything a failed bootstrap wrote, so no partial state stays.
        for cleanup in (path, token_path, operator_token_path, *written_targets):
            with contextlib.suppress(OSError):
                Path(cleanup).unlink()
        reason = (
            f"generated configuration did not validate: {exc}"
            if isinstance(exc, ConfigError)
            else f"could not write the configuration: {exc}"
        )
        raise WizardAbort(reason) from exc

    wizard.section("Done")
    wizard.say(f"  config: {path}")
    wizard.say(f"  tokens: {token_path} (hashes only)")
    wizard.say(f"  operator token: {operator_token_path} (0600, for gatewayctl)")
    if written_targets:
        wizard.say("  target files:")
        for target_path in written_targets:
            wizard.say(f"    {target_path}")
    wizard.say("")
    wizard.say(f"  operator client {client_id!r} token (also stored on the host):")
    wizard.say(f"    {token}")
    wizard.say("")
    wizard.say("  Next steps:")
    wizard.say(f"    1. start the gateway:  computeMCP-gateway --config {path}")
    wizard.say("    2. operate it; the CLI reads the operator token from the config:")
    wizard.say(f"         computeMCP-gatewayctl --config {path} status")
    wizard.say("       (or export COMPUTEMCP_TOKEN=<token> to override)")
    wizard.say("    3. inside each Terok task run the handshake, then approve it:")
    wizard.say(f"         computeMCP-handshake <project-id> --port {port}")
    wizard.say("         computeMCP-gatewayctl approve <request-id>   # on the host")
    return 0


def run_add_target(config_path: str | Path, *, wizard: Wizard | None = None) -> int:
    """Add one target to an existing configuration as its own include file."""
    wizard = wizard or Wizard()
    path = Path(config_path)
    if not path.exists():
        raise WizardAbort(
            f"{path} does not exist; run 'computeMCP-gateway --bootstrap' first"
        )
    config = load_config(path, token_file=None)
    answers = collect_target(wizard, set(config.targets))

    target_path = write_target_file(path.parent, answers)
    relative = target_relative_path(answers.name)
    before = path.read_text()
    try:
        append_include(path, relative)
        load_config(path, token_file=None)
    except ConfigError as exc:
        # Leave the main config as it was and drop the orphaned target file.
        _atomic_write(path, before, mode=0o600)
        with contextlib.suppress(OSError):
            target_path.unlink()
        raise WizardAbort(f"adding the target did not validate: {exc}") from exc

    wizard.section("Done")
    wizard.say(f"  wrote target {answers.name!r} to {target_path}")
    wizard.say(f"  included it from {path}")
    wizard.say("  reload a running gateway:  computeMCP-gatewayctl reload")
    return 0

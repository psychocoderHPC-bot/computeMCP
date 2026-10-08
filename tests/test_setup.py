# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Tests for the interactive setup wizard.

Prompt I/O is injected, so every flow runs without a terminal.  The tests assert
that generated TOML actually loads through the real config loader, that the
token is stored hashed, and that a bad append rolls back.
"""

from __future__ import annotations

import re
import tomllib

import pytest

from compute_mcp.auth import hash_token
from compute_mcp.config import ConfigError, load_config
from compute_mcp.setup import (
    TargetAnswers,
    Wizard,
    WizardAbort,
    collect_target,
    render_gateway_config,
    render_target_block,
    render_target_file,
    render_tokens_file,
    run_add_target,
    run_bootstrap,
    target_relative_path,
    write_target_file,
)


def _wizard(values, *, terminal=True):
    it = iter(values)

    def _input(prompt=""):
        try:
            return next(it)
        except StopIteration as exc:
            raise AssertionError(f"wizard asked more questions than provided: {prompt}") from exc

    return Wizard(input_fn=_input, print_fn=lambda *a, **k: None, terminal=terminal)


# ---------------------------------------------------------------------------
# prompt helpers
# ---------------------------------------------------------------------------

def test_ask_uses_default_on_blank():
    w = _wizard(["", "value"])
    assert w.ask("q", default="fallback") == "fallback"
    assert w.ask("q") == "value"


def test_ask_requires_value_when_no_default():
    w = _wizard(["", "", "given"])
    assert w.ask("q") == "given"


def test_ask_choice_rejects_and_lists():
    w = _wizard(["podman", "docker"])
    assert w.ask("runtime", choices=("apptainer", "docker")) == "docker"


def test_ask_validator_reprompts():
    def valid_port(value):
        if not value.isdigit() or int(value) > 65535 or int(value) == 0:
            return "bad port"
        return None

    w = _wizard(["0", "70000", "2222"])
    assert w.ask("port", validator=valid_port) == "2222"


def test_confirm_defaults():
    assert _wizard([""]).confirm("q", default=True) is True
    assert _wizard([""]).confirm("q", default=False) is False
    assert _wizard(["n"]).confirm("q", default=True) is False
    assert _wizard(["yes"]).confirm("q", default=False) is True


def test_non_terminal_aborts():
    w = _wizard([], terminal=False)
    with pytest.raises(WizardAbort):
        w.ask("q")


# ---------------------------------------------------------------------------
# rendering round-trips through the loader
# ---------------------------------------------------------------------------

def _full_target():
    return TargetAnswers(
        name="rosi",
        ssh_targets=("rosi",),
        user="agent",
        client_key="/home/u/.ssh/computemcp_container",
        host_key_sha256="SHA256:" + "a" * 40,
        bundle=True,
        use_slurm=True,
        container_runtime="apptainer",
        container_storage_root="/scratch/u/computemcp",
        container_image="docker://ubuntu:24.04",
        container_gpus=("nvidia", "amd"),
        node_cpus=24,
        node_gpus=4,
        node_memory="378000M",
        allocation_single="gpu-proportional",
        allocation_multi="exclusive",
        allocation_max_nodes=4,
        sbatch_partition="gpu",
        sbatch_time="02:00:00",
        srun_cpu_bind="none",
    )


def test_render_config_loads(tmp_path):
    target = _full_target()
    tokens = tmp_path / "tokens.toml"
    tokens.write_text('[tokens]\n"alpaka" = "sha256:' + "a" * 64 + '"\n')
    write_target_file(tmp_path, target)
    text = render_gateway_config(
        listen="127.0.0.1",
        port=2222,
        allow_enrollment=True,
        client_id="alpaka",
        client_targets=("*",),
        client_label=None,
        token_file=str(tokens),
        include=[target_relative_path("rosi")],
    )
    config_path = tmp_path / "config.toml"
    config_path.write_text(text)
    cfg = load_config(config_path)
    t = cfg.targets["rosi"]
    assert t.node.cpus == 24
    assert t.node.gpus == 4
    assert t.container.runtime == "apptainer"
    assert t.container.gpus == ("nvidia", "amd")
    assert t.bundle.source == "computemcp-container"
    assert t.allocation.single_node == "gpu-proportional"
    assert cfg.clients["alpaka"].allow_all is True


def test_render_direct_target_loads(tmp_path):
    target = TargetAnswers(
        name="hal",
        transport="direct",
        direct_host="10.0.0.5",
        direct_port=2222,
        user="agent",
        client_key="/k",
    )
    tokens = tmp_path / "tokens.toml"
    tokens.write_text('[tokens]\n"alpaka" = "sha256:' + "a" * 64 + '"\n')
    write_target_file(tmp_path, target)
    text = render_gateway_config(
        listen="127.0.0.1",
        port=2222,
        allow_enrollment=False,
        client_id="alpaka",
        client_targets=(),
        client_label="dev",
        token_file=str(tokens),
        include=[target_relative_path("hal")],
    )
    config_path = tmp_path / "config.toml"
    config_path.write_text(text)
    cfg = load_config(config_path)
    assert cfg.targets["hal"].transport.kind == "direct"
    assert cfg.targets["hal"].transport.remote_host == "10.0.0.5"


def test_render_tokens_file_is_hashed():
    text = render_tokens_file([("alpaka", "plain-secret")])
    table = tomllib.loads(text)["tokens"]
    assert table["alpaka"].startswith("sha256:")
    assert "plain-secret" not in text


def test_render_target_block_is_parseable():
    block = render_target_block(_full_target())
    parsed = tomllib.loads(block)
    assert parsed["targets"]["rosi"]["node"]["cpus"] == 24
    assert parsed["targets"]["rosi"]["bundle"]["source"] == "computemcp-container"


# ---------------------------------------------------------------------------
# commented discovery defaults
# ---------------------------------------------------------------------------

def _discovery_target():
    """A tunnel target that leaves the commented-default options unset."""
    return TargetAnswers(
        name="rosi",
        ssh_targets=("rosi",),
        user="agent",
        client_key="/home/u/.ssh/k",
        host_key_check="off",  # blank fingerprint wizard path
        container_runtime="apptainer",
        bundle=True,
        bundle_deploy_dir="$HOME/computemcp/bundle",
    )


def _write_and_render(tmp_path, answers):
    """Write one target file plus its gateway config; return (config, text)."""
    token_path = tmp_path / "tokens.toml"
    token_path.write_text('[tokens]\n"alpaka" = "sha256:' + "a" * 64 + '"\n')
    config_path = tmp_path / "config.toml"
    text = render_gateway_config(
        listen="127.0.0.1",
        port=2222,
        allow_enrollment=True,
        client_id="alpaka",
        client_targets=("*",),
        client_label=None,
        token_file=str(token_path),
        include=[target_relative_path(answers.name)],
    )
    config_path.write_text(text)
    write_target_file(tmp_path, answers)
    return config_path, text


def test_target_block_commented_defaults_present():
    block = render_target_block(_discovery_target())
    assert '# connect_command_mode = "on_failure"' in block
    assert "# provision_timeout = 900.0" in block
    assert "# sandbox = false" in block
    assert "# auto-deploy = true" in block
    assert "# gpus = []" in block


def test_target_block_no_duplicate_key_across_comment_and_active():
    block = render_target_block(_discovery_target())
    table = None
    seen: set[tuple[str | None, str]] = set()
    for raw in block.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("["):
            table = line.strip("[]")
            continue
        body = line
        if body.startswith("# "):
            body = body[2:]
        elif body.startswith("#"):
            continue
        if "=" not in body:
            continue
        key = body.split("=", 1)[0].strip()
        ident = (table, key)
        assert ident not in seen, f"duplicate key {ident} in rendered block"
        seen.add(ident)


# Representative answer sets: plain tunnel, tunnel+container (apptainer and
# docker) with bundle, 2FA, and a full Slurm target.  Each must render into a
# config that loads, and every commented option line in it must keep the
# config loadable when that single line is uncommented.
def _exhaustive_targets():
    return [
        # (a) plain tunnel
        TargetAnswers(
            name="plain",
            ssh_targets=("plain",),
            user="agent",
            client_key="/home/u/.ssh/k",
        ),
        # (b) tunnel + apptainer container + bundle, fingerprint so
        # host_key_check stays "on"
        TargetAnswers(
            name="rosa",
            ssh_targets=("rosa",),
            user="agent",
            client_key="/home/u/.ssh/k",
            host_key_sha256="SHA256:" + "a" * 40,
            auto_connect=False,
            container_runtime="apptainer",
            container_storage_root="$HOME/computemcp",
            bundle=True,
        ),
        # (c) tunnel + docker container + bundle
        TargetAnswers(
            name="dock",
            ssh_targets=("dock",),
            user="agent",
            client_key="/home/u/.ssh/k",
            host_key_sha256=None,
            host_key_check="off",
            container_runtime="docker",
            container_storage_root="$HOME/computemcp",
            bundle=True,
        ),
        # (d) direct
        TargetAnswers(
            name="dial",
            transport="direct",
            direct_host="10.0.0.5",
            direct_port=2222,
            user="agent",
            client_key="/k",
        ),
        # (e) 2FA tunnel target
        TargetAnswers(
            name="otp",
            ssh_targets=("otp",),
            user="agent",
            client_key="/k",
            interactive_auth=True,
            auto_connect=False,
        ),
        # (f) full Slurm target (node, allocation, sbatch, srun)
        TargetAnswers(
            name="rosi",
            ssh_targets=("rosi",),
            user="agent",
            client_key="/home/u/.ssh/k",
            host_key_sha256="SHA256:" + "a" * 40,
            auto_connect=False,
            container_runtime="apptainer",
            container_storage_root="$HOME/computemcp",
            bundle=True,
            node_cpus=24,
            node_gpus=4,
            node_memory="378000M",
            allocation_single="gpu-proportional",
            allocation_multi="exclusive",
            allocation_max_nodes=4,
            sbatch_partition="gpu",
            sbatch_time="02:00:00",
            srun_cpu_bind="none",
        ),
    ]


def _target_option_lines(text):
    """Commented option lines of a rendered target, counting occurrences.

    A prose comment line (file prologue, inline notes) is not ``# key = value``
    and is skipped; an option line uncommented from within the file that
    defines it belongs to the table open at its position, so each table key is
    only exercised once.
    """
    lines: list[tuple[str, tuple[str, str]]] = []
    table = ""
    seen: set[tuple[str, str]] = set()
    for line in text.splitlines():
        if line.startswith("# "):
            body = line[2:]
            key, sep, _value = body.partition(" = ")
            if sep and re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]*", key):
                if (table, key) not in seen:
                    seen.add((table, key))
                    lines.append((line, (table, key)))
        elif line.startswith("[") and line.endswith("]"):
            table = line[1:-1]
    return lines


def _main_config_option_lines(text):
    """Commented option lines of the main gateway config.

    Prose lines (the file prologue starts with uppercase) are not of the shape
    ``# key = `` and are skipped; the option lines carry the defaults the
    [server] table would pick up when uncommented.
    """
    return [
        line
        for line in text.splitlines()
        if re.match(r"^# [a-z][a-z_]* = ", line)
    ]


def test_every_commented_default_loads_when_uncommented(tmp_path):
    """Exhaustive sweep: every commented option line must load uncommented.

    Each target of the matrix renders into a real include-based config; then
    every commented option line (prose skipped) is uncommented one at a time
    and ``load_config`` must succeed -- with zero exceptions, including
    ``route_host_key_sha256`` whose placeholder now passes the fingerprint
    format check (see
    :func:`test_route_fingerprint_placeholder_is_valid_toml`).
    """
    exercised = 0
    failures: list[str] = []
    targets = _exhaustive_targets()
    for answers in targets:
        config_path = tmp_path / answers.name / "config.toml"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        token_path = config_path.parent / "tokens.toml"
        token_path.write_text('[tokens]\n"alpaka" = "sha256:' + "a" * 64 + '"\n')
        config_path.write_text(
            render_gateway_config(
                listen="127.0.0.1",
                port=2222,
                allow_enrollment=True,
                client_id="alpaka",
                client_targets=("*",),
                client_label=None,
                token_file=str(token_path),
                include=[target_relative_path(answers.name)],
            )
        )
        target_path = write_target_file(config_path.parent, answers)
        # Main config: only the root ``key = `` option lines (prose is
        # uppercase and therefore not matched).
        main_text = config_path.read_text()
        try:
            load_config(config_path)
        except ConfigError as exc:
            failures.append(f"target {answers.name}: base config does not load: {exc}")
        for line in _main_config_option_lines(main_text):
            exercised += 1
            variant = main_text.replace(line, line[2:], 1)
            config_path.write_text(variant)
            try:
                load_config(config_path)
            except ConfigError as exc:
                failures.append(
                    f"main config ({answers.name}): {line!r}\n"
                    f"    {_classify(exc) if _looks_like_toml_error(exc) else 'rejected: ' + str(exc)}"
                )
            finally:
                config_path.write_text(main_text)
        # Target file: every table-scoped option line exactly once.
        original_text = target_path.read_text()
        for line, where in _target_option_lines(original_text):
            exercised += 1
            variant = original_text.replace(line, line[2:], 1)
            target_path.write_text(variant)
            try:
                load_config(config_path)
            except ConfigError as exc:
                failures.append(
                    f"target {answers.name} [{where[0] or 'root'}] {line!r}\n"
                    f"    {_classify(exc) if _looks_like_toml_error(exc) else 'rejected: ' + str(exc)}"
                )
            finally:
                target_path.write_text(original_text)
    assert exercised, "no commented option lines were exercised at all"
    assert not failures, "uncommentable commented defaults:\n" + "\n".join(failures)
    # Report what the sweep covered.
    print(f"exercised {exercised} commented option lines across {len(targets)} targets; zero fail")


def _looks_like_toml_error(exc: ConfigError) -> bool:
    text = str(exc)
    return "malformed TOML" in text or "TOMLDecodeError" in text


def _classify(exc: ConfigError) -> str:
    text = str(exc)
    if "malformed TOML" in text or "TOMLDecodeError" in text:
        return f"invalid TOML: {text}"
    return f"loaded as TOML but rejected by the loader: {text}"


def test_route_fingerprint_placeholder_is_valid_toml(tmp_path):
    """The route fingerprint placeholder is valid TOML and loads uncommented.

    ``SHA256:REPLACE_WITH_FINGERPRINT`` is a format-complete placeholder
    that passes the loader's route host-key check, so uncommenting the
    rendered line keeps the config loadable; a real fingerprint on the same
    line loads as well.
    """
    token_path = tmp_path / "tokens.toml"
    token_path.write_text('[tokens]\n"alpaka" = "sha256:' + "a" * 64 + '"\n')
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        render_gateway_config(
            listen="127.0.0.1", port=2222, allow_enrollment=False,
            client_id="alpaka", client_targets=("*",), client_label=None,
            token_file=str(token_path),
            include=[target_relative_path("rosi")],
        )
    )
    path = write_target_file(tmp_path, _discovery_target())
    base_text = path.read_text()
    placeholder = '# route_host_key_sha256 = "SHA256:REPLACE_WITH_FINGERPRINT"'
    assert base_text.count(placeholder) == 1

    # The placeholder itself loads when uncommented.
    candidate = base_text.replace(placeholder, placeholder[2:], 1)
    tomllib.loads(candidate)
    path.write_text(candidate)
    try:
        load_config(config_path)
    finally:
        path.write_text(base_text)

    # A full fingerprint on the same line loads as well.
    fingerprint = "SHA256:" + "a" * 40
    full_line = f'# route_host_key_sha256 = "{fingerprint}"'
    candidate = base_text.replace(placeholder, full_line, 1)
    tomllib.loads(candidate)
    candidate = candidate.replace(full_line, full_line[2:], 1)
    tomllib.loads(candidate)
    path.write_text(candidate)
    try:
        load_config(config_path)
    finally:
        path.write_text(base_text)


def test_commented_bundle_deploy_dir_default():
    # A bundle target without an explicit deploy dir shows the documented
    # default location, which loads when uncommented.
    answers = TargetAnswers(
        name="rosi",
        ssh_targets=("rosi",),
        user="agent",
        client_key="/k",
        container_runtime="apptainer",
        bundle=True,
    )
    assert '# deploy-dir = "$HOME/computemcp/bundle"' in render_target_block(answers)


def test_commented_default_values_are_valid():
    # The apptainer+bundle discovery target carries the corrected values.
    block = render_target_block(_discovery_target())
    assert '# container_user = "agent"' in block
    assert '# route_host_key_sha256 = "SHA256:REPLACE_WITH_FINGERPRINT"' in block
    assert '# host-home = "$HOME/computemcp/rosi/home"' in block
    # Runtime-appropriate image per target: docker:// for Apptainer (which
    # cannot pull docker tags), plain tag for Docker.
    apptainer_block = render_target_block(
        TargetAnswers(
            name="hal",
            ssh_targets=("hal",),
            user="agent",
            client_key="/k",
            container_runtime="apptainer",
        )
    )
    docker_block = render_target_block(
        TargetAnswers(
            name="hal",
            ssh_targets=("hal",),
            user="agent",
            client_key="/k",
            container_runtime="docker",
        )
    )
    assert '# image = "docker://ubuntu:24.04"' in apptainer_block
    assert '# image = "ubuntu:24.04"' in docker_block
    # host-home substitutes the target name and is non-empty; it also shows
    # for the docker target above.
    assert f'# host-home = "$HOME/computemcp/hal/home"' in docker_block
    assert f'# host-home = "$HOME/computemcp/hal/home"' in apptainer_block
    assert '# route_host_key_sha256 = "SHA256:REPLACE_WITH_FINGERPRINT"' in docker_block
    assert '# route_host_key_sha256 = "SHA256:REPLACE_WITH_FINGERPRINT"' in apptainer_block
    assert '# container_user = "agent"' in docker_block
    assert '# container_user = "agent"' in apptainer_block


def test_bootstrap_main_config_commented_defaults_parse_and_load(tmp_path):
    config_path, text = _write_and_render(tmp_path, _discovery_target())
    assert "# enroll_ttl = 600.0" in text
    assert "# enroll_max_pending = 32" in text
    tomllib.loads(text)
    load_config(config_path)
    # Uncommenting one enrollment default keeps the file valid TOML.
    uncommented = text.replace("# enroll_ttl = 600.0", "enroll_ttl = 600.0", 1)
    assert tomllib.loads(uncommented)["server"]["enroll_ttl"] == 600.0


def test_render_direct_target_omits_tunnel_only_defaults():
    target = TargetAnswers(
        name="hal",
        transport="direct",
        direct_host="10.0.0.5",
        direct_port=2222,
        user="agent",
        client_key="/k",
    )
    block = render_target_block(target)
    assert "proxy_jump" not in block


# ---------------------------------------------------------------------------
# bootstrap flow
# ---------------------------------------------------------------------------

def _bootstrap_answers():
    return [
        "127.0.0.1", "2222", "y",              # server + enrollment
        "y",                                   # set up a target
        "rosi", "tunnel", "rosi", "agent", "/home/u/.ssh/k",
        "SHA256:" + "a" * 40,                  # fingerprint pins verification
        "ssh-ed25519",                         # host-key algorithms
        "n",                                   # no 2FA
        "y",                                   # auto-connect on gateway start
        "y", "apptainer", "/scratch/u/computemcp", "docker://ubuntu:24.04", "nvidia",
        "y",                                   # build/start the container? yes
        "",                                    # pre-provision environment (none)
        "y",                                   # behind a Slurm scheduler? yes
        "y",                                   # slurm description
        "24", "4", "378000M", "gpu-proportional", "exclusive", "4", "gpu", "02:00:00", "none",
        "n",                                   # no more targets
    ]


def test_bootstrap_writes_config_and_hashed_token(tmp_path):
    config_path = tmp_path / "config.toml"
    rc = run_bootstrap(config_path, wizard=_wizard(_bootstrap_answers()))
    assert rc == 0
    assert config_path.exists()
    token_path = tmp_path / "tokens.toml"
    assert token_path.exists()
    assert (config_path.stat().st_mode & 0o777) == 0o600
    assert (token_path.stat().st_mode & 0o777) == 0o600

    cfg = load_config(config_path)
    assert set(cfg.targets) == {"rosi"}
    # Bootstrap mints only the operator token, not a per-project one.
    assert set(cfg.clients) == {"admin"}
    assert cfg.clients["admin"].allow_all


def test_bootstrap_enables_enrollment_by_default(tmp_path):
    config_path = tmp_path / "config.toml"
    run_bootstrap(config_path, wizard=_wizard(_bootstrap_answers()))
    cfg = load_config(config_path)
    assert cfg.server.allow_enrollment is True
    assert "allow_enrollment = true" in config_path.read_text()


def test_bootstrap_can_disable_enrollment(tmp_path):
    answers = list(_bootstrap_answers())
    answers[2] = "n"  # enrollment off
    config_path = tmp_path / "config.toml"
    run_bootstrap(config_path, wizard=_wizard(answers))
    cfg = load_config(config_path)
    assert cfg.server.allow_enrollment is False


def test_bootstrap_refuses_overwrite_without_force(tmp_path):
    config_path = tmp_path / "config.toml"
    run_bootstrap(config_path, wizard=_wizard(_bootstrap_answers()))
    with pytest.raises(WizardAbort):
        run_bootstrap(config_path, wizard=_wizard([]))


def test_bootstrap_force_overwrites(tmp_path):
    config_path = tmp_path / "config.toml"
    run_bootstrap(config_path, wizard=_wizard(_bootstrap_answers()))
    # A minimal second run with --force: no target.
    minimal = ["127.0.0.1", "2222", "n", "n"]
    rc = run_bootstrap(config_path, force=True, wizard=_wizard(minimal))
    assert rc == 0
    cfg = load_config(config_path)
    assert set(cfg.clients) == {"admin"}
    assert cfg.targets == {}


def test_bootstrap_stores_only_the_hash(tmp_path, capsys):
    config_path = tmp_path / "config.toml"
    run_bootstrap(config_path, wizard=_wizard(_bootstrap_answers()))
    tokens_text = (tmp_path / "tokens.toml").read_text()
    table = tomllib.loads(tokens_text)["tokens"]
    stored = table["admin"]
    assert stored.startswith("sha256:")
    # The config itself must not contain a plaintext token.
    assert "token =" not in config_path.read_text()


def test_bootstrap_non_terminal_aborts(tmp_path):
    with pytest.raises(WizardAbort):
        run_bootstrap(tmp_path / "config.toml", wizard=_wizard([], terminal=False))


def test_bootstrap_no_orphan_token_when_validation_fails(tmp_path, monkeypatch):
    """A failed bootstrap must not leave a hashed token behind."""
    import compute_mcp.setup as setup_module

    # Force the post-write validation to fail.
    def bad_load(path, token_file=None):
        raise ConfigError("injected failure")

    monkeypatch.setattr(setup_module, "load_config", bad_load)
    with pytest.raises(WizardAbort):
        run_bootstrap(tmp_path / "config.toml", wizard=_wizard(_bootstrap_answers()))
    assert not (tmp_path / "config.toml").exists()
    assert not (tmp_path / "tokens.toml").exists()


def test_ask_rejects_control_characters():
    w = _wizard(["bad\tvalue", "good"])
    assert w.ask("q") == "good"


# ---------------------------------------------------------------------------
# add-target flow
# ---------------------------------------------------------------------------

def _minimal_bootstrap(tmp_path):
    config_path = tmp_path / "config.toml"
    run_bootstrap(
        config_path,
        wizard=_wizard(["127.0.0.1", "2222", "n", "n"]),
    )
    return config_path


def test_add_target_appends_and_validates(tmp_path):
    config_path = _minimal_bootstrap(tmp_path)
    # name, transport, host, port, user, fingerprint, 2FA, auto-connect, container
    rc = run_add_target(
        config_path,
        wizard=_wizard(["hal", "direct", "10.0.0.9", "2222", "agent", "", "n", "y", "n"]),
    )
    assert rc == 0
    # The target lives in its own include file, listed from the main config.
    target_file = tmp_path / "systems" / "hal.toml"
    assert target_file.exists()
    assert "systems/hal.toml" in config_path.read_text()
    cfg = load_config(config_path)
    assert "hal" in cfg.targets
    assert cfg.targets["hal"].transport.remote_host == "10.0.0.9"


def test_add_target_requires_existing_config(tmp_path):
    with pytest.raises(WizardAbort):
        run_add_target(tmp_path / "missing.toml", wizard=_wizard([]))


def test_add_target_rejects_duplicate_name(tmp_path):
    config_path = _minimal_bootstrap(tmp_path)
    run_add_target(
        config_path,
        wizard=_wizard(["hal", "direct", "10.0.0.9", "2222", "agent", "", "n", "y", "n"]),
    )
    # Second attempt reuses the name; the validator re-asks, so supply it twice.
    before = config_path.read_text()
    with pytest.raises(AssertionError):
        # The validator rejects the duplicate and asks again; our finite input
        # runs out, proving the duplicate was refused rather than accepted.
        run_add_target(
            config_path,
            wizard=_wizard(["hal", "hal", "direct", "h", "2222", "agent", "", "n", "y", "n", "n"]),
        )
    assert config_path.read_text() == before


def test_add_target_rolls_back_target_file_on_invalid_append(tmp_path, monkeypatch):
    config_path = _minimal_bootstrap(tmp_path)
    before = config_path.read_text()
    import compute_mcp.setup as setup_module

    calls = {"n": 0}
    real_load = setup_module.load_config

    def flaky_load(path, token_file=None):
        calls["n"] += 1
        if calls["n"] > 1:
            raise ConfigError("injected validation failure")
        return real_load(path, token_file=token_file)

    monkeypatch.setattr(setup_module, "load_config", flaky_load)
    with pytest.raises(WizardAbort):
        run_add_target(
            config_path,
            wizard=_wizard(["hal", "direct", "10.0.0.9", "2222", "agent", "", "n", "y", "n"]),
        )
    # The orphaned target file is removed; append_include reverts the main file.
    assert not (tmp_path / "systems" / "hal.toml").exists()
    assert config_path.read_text() == before


def test_bootstrap_writes_per_target_files(tmp_path):
    config_path = tmp_path / "config.toml"
    run_bootstrap(config_path, wizard=_wizard(_bootstrap_answers()))
    text = config_path.read_text()
    assert 'include = [' in text
    assert 'systems/rosi.toml' in text
    # The target is not inline in the main file.
    assert "[targets.rosi]" not in text
    assert (tmp_path / "systems" / "rosi.toml").exists()
    cfg = load_config(config_path)
    assert "rosi" in cfg.targets


def test_render_target_file_is_includable(tmp_path):
    text = render_target_file(_full_target())
    assert text.startswith("#")
    assert "[targets.rosi]" in text
    target = tmp_path / "rosi.toml"
    target.write_text(text)
    parsed = tomllib.loads(text)
    assert parsed["targets"]["rosi"]["node"]["cpus"] == 24


def test_target_relative_path_isolated_under_systems():
    assert target_relative_path("config") == "systems/config.toml"
    assert target_relative_path("tokens") == "systems/tokens.toml"


def test_collect_target_rejects_duplicate_existing_name():
    # The duplicate 'hal' is refused, so the next input is consumed as the name.
    w = _wizard(
        ["hal", "hal2", "direct", "h", "2222", "agent", "", "n", "y", "n", "n", "n", "n"]
    )
    answers = collect_target(w, {"hal"})
    assert answers.name == "hal2"
    assert answers.transport == "direct"


# -- SSH alias list ----------------------------------------------------------

def test_split_list_variants():
    from compute_mcp.setup import _split_list

    assert _split_list("hal") == ("hal",)
    assert _split_list("hal, ex_hal") == ("hal", "ex_hal")
    assert _split_list("hal ex_hal") == ("hal", "ex_hal")
    assert _split_list("hal,hal") == ("hal",)  # deduplicated, order kept


def test_validate_aliases():
    from compute_mcp.setup import _validate_aliases

    assert _validate_aliases("hal,ex_hal") is None
    assert _validate_aliases("") is not None
    assert _validate_aliases("bad alias!") is not None


def test_collect_target_accepts_alias_list():
    from compute_mcp.setup import collect_target

    # name, transport, aliases, user, key, fingerprint, 2FA, auto-connect,
    # container, bundle, slurm
    w = _wizard(
        [
            "rosi", "tunnel", "rosi,ex_rosi", "agent", "/home/u/.ssh/k", "",
            "n", "y", "n", "n", "n",
        ]
    )
    answers = collect_target(w, set())
    assert answers.ssh_targets == ("rosi", "ex_rosi")


def test_render_target_block_multi_alias():
    target = TargetAnswers(
        name="rosi",
        ssh_targets=("rosi", "ex_rosi"),
        user="agent",
        client_key="/k",
    )
    block = render_target_block(target)
    parsed = tomllib.loads(block)
    assert parsed["targets"]["rosi"]["ssh_targets"] == ["rosi", "ex_rosi"]


# -- host-key + remote-path refinements --------------------------------------

def test_validate_remote_path_accepts_home_and_absolute():
    from compute_mcp.setup import _validate_remote_path

    assert _validate_remote_path("/scratch/x") is None
    assert _validate_remote_path("$HOME/computemcp") is None
    assert _validate_remote_path("~/computemcp") is None
    assert _validate_remote_path("relative/path") is not None


def test_collect_target_blank_fingerprint_disables_verification():
    from compute_mcp.setup import collect_target

    # name, transport, aliases, user, key, blank fingerprint, 2FA, auto-connect,
    # container n, bundle n
    w = _wizard(
        ["rosi", "tunnel", "rosi", "agent", "/home/u/.ssh/k", "", "n", "y", "n", "n"]
    )
    answers = collect_target(w, set())
    assert answers.host_key_sha256 is None
    assert answers.host_key_check == "off"


def test_collect_target_pin_sets_algorithms():
    from compute_mcp.setup import collect_target

    w = _wizard(
        [
            "rosi", "tunnel", "rosi", "agent", "/home/u/.ssh/k",
            "SHA256:" + "a" * 40, "ssh-ed25519,rsa-sha2-512", "n", "y", "n", "n",
        ]
    )
    answers = collect_target(w, set())
    assert answers.host_key_check == "on"
    assert answers.host_key_algorithms == ("ssh-ed25519", "rsa-sha2-512")


def test_collect_target_empty_user_is_omitted():
    from compute_mcp.setup import collect_target

    # "-" asks for no explicit user: the SSH config or local account decides.
    w = _wizard(
        ["rosi", "tunnel", "rosi", "-", "/home/u/.ssh/k", "", "n", "y", "n", "n"]
    )
    answers = collect_target(w, set())
    assert answers.user == ""
    block = render_target_block(answers)
    active = [line for line in block.splitlines() if not line.startswith("#")]
    assert "user = " not in "\n".join(active)


def test_no_slurm_question_without_bundle():
    from compute_mcp.setup import collect_target

    # container yes, provision no -> no bundle, no Slurm questions; the wizard
    # must not ask for them, so the answer list ends right after the container
    # answers.
    w = _wizard(
        [
            "rosi", "tunnel", "rosi", "agent", "/home/u/.ssh/k", "", "n",
            "y",  # auto-connect on gateway start
            "y", "apptainer", "$HOME/computemcp", "docker://ubuntu:24.04", "",
            "n",  # build/start the container? no
        ]
    )
    answers = collect_target(w, set())
    assert answers.bundle is False
    assert answers.node_cpus is None
    assert answers.container_storage_root == "$HOME/computemcp"


def test_wizard_container_target_without_slurm_gets_bundle():
    from compute_mcp.setup import collect_target

    # A plain Docker host: container yes, provision yes, slurm no.  The bundle
    # block must be written (the generic provisioner) and the container block
    # too, with no node/allocation/slurm block.
    w = _wizard(
        [
            "hal", "tunnel", "hal", "agent", "/home/u/.ssh/k", "", "n",
            "y",  # auto-connect on gateway start
            "y", "docker", "$HOME/computemcp", "ubuntu:24.04", "nvidia",
            "y",   # build and start this container? yes
            "",    # pre-provision environment (none)
            "n",   # behind a Slurm scheduler? no
        ]
    )
    answers = collect_target(w, set())
    assert answers.bundle is True
    assert answers.use_slurm is False
    block = render_target_block(answers)
    assert "[targets.hal.bundle]" in block
    assert "[targets.hal.container]" in block
    assert "[targets.hal.node]" not in block
    assert "[targets.hal.allocation]" not in block
    assert "[targets.hal.slurm" not in block
    parsed = tomllib.loads(block)
    assert parsed["targets"]["hal"]["bundle"]["source"] == "computemcp-container"
    # The generated block must load through the real loader.
    import tempfile
    from pathlib import Path

    from compute_mcp.config import load_config

    with tempfile.TemporaryDirectory() as tmp:
        write_target_file(Path(tmp), answers)
        token_path = Path(tmp) / "tokens.toml"
        token_path.write_text('[tokens]\n"alpaka" = "sha256:' + "a" * 64 + '"\n')
        config_path = Path(tmp) / "config.toml"
        config_path.write_text(
            render_gateway_config(
                listen="127.0.0.1",
                port=2222,
                allow_enrollment=False,
                client_id="alpaka",
                client_targets=(),
                client_label=None,
                token_file=str(token_path),
                include=[target_relative_path("hal")],
            )
        )
        cfg = load_config(config_path)
    assert cfg.targets["hal"].bundle is not None
    assert cfg.targets["hal"].node is None


def test_collect_target_bundle_provision_env_comma_separated():
    from compute_mcp.setup import collect_target

    # name, transport, aliases, user, key, fingerprint, 2FA, auto-connect,
    # container yes, provision yes, provision-env, slurm no
    w = _wizard(
        [
            "rosi", "tunnel", "rosi", "agent", "/home/u/.ssh/k", "", "n",
            "y",  # auto-connect on gateway start
            "y", "apptainer", "$HOME/computemcp", "docker://ubuntu:24.04", "",
            "y",  # build/start the container? yes
            "module load apptainer, source /etc/profile.d/spack.sh",
            "n",  # no Slurm questions
        ]
    )
    answers = collect_target(w, set())
    assert answers.bundle_provision_env == (
        "module load apptainer",
        "source /etc/profile.d/spack.sh",
    )
    parsed = tomllib.loads(render_target_block(answers))
    assert parsed["targets"]["rosi"]["bundle"]["provision-env"] == [
        "module load apptainer",
        "source /etc/profile.d/spack.sh",
    ]


# -- auto-connect / 2FA coupling -------------------------------------------

def _write_and_load(toml_dir, answers):
    """Write one wizard target as a file + a minimal gateway config, then load."""
    import tempfile
    from pathlib import Path

    token_path = toml_dir / "tokens.toml"
    token_path.write_text('[tokens]\n"alpaka" = "sha256:' + "a" * 64 + '"\n')
    config_path = toml_dir / "config.toml"
    config_path.write_text(
        render_gateway_config(
            listen="127.0.0.1",
            port=2222,
            allow_enrollment=False,
            client_id="alpaka",
            client_targets=(),
            client_label=None,
            token_file=str(token_path),
            include=[target_relative_path(answers.name)],
        )
    )
    write_target_file(toml_dir, answers)
    return load_config(config_path), toml_dir / "systems" / f"{answers.name}.toml"


def test_wizard_auto_connect_yes_renders_and_loads_true(tmp_path):
    from compute_mcp.setup import collect_target

    # name, transport, aliases, user, key, blank fingerprint, 2FA no,
    # auto-connect yes, container no
    w = _wizard(
        ["rosi", "tunnel", "rosi", "agent", "/home/u/.ssh/k", "", "n", "y", "n"]
    )
    answers = collect_target(w, set())
    assert answers.interactive_auth is False
    assert answers.auto_connect is True
    # The key must be rendered explicitly: the loader default is false, so an
    # omitted key would not auto-connect after reload.
    assert "auto_connect = true" in render_target_block(answers)
    cfg, _ = _write_and_load(tmp_path, answers)
    assert cfg.targets["rosi"].auto_connect is True


def test_wizard_auto_connect_no_loads_false(tmp_path):
    from compute_mcp.setup import collect_target

    # name, transport, aliases, user, key, blank fingerprint, 2FA no,
    # auto-connect no, container no
    w = _wizard(
        ["rosi", "tunnel", "rosi", "agent", "/home/u/.ssh/k", "", "n", "n", "n"]
    )
    answers = collect_target(w, set())
    assert answers.auto_connect is False
    # Omit the key entirely (loader default false); never write "true".
    block = render_target_block(answers)
    active = [line for line in block.splitlines() if not line.startswith("#")]
    assert "auto_connect = true" not in active
    assert "auto_connect = false" not in active
    cfg, _ = _write_and_load(tmp_path, answers)
    assert cfg.targets["rosi"].auto_connect is False


def test_wizard_2fa_skips_auto_connect_and_loads_false(tmp_path):
    from compute_mcp.setup import collect_target

    # name, transport, aliases, user, key, blank fingerprint, 2FA yes, container
    # no.  The auto-connect question is never asked, so no input is consumed
    # for it: after "y" (2FA) the wizard moves straight to the container.
    w = _wizard(
        ["rosi", "tunnel", "rosi", "agent", "/home/u/.ssh/k", "", "y", "n"]
    )
    answers = collect_target(w, set())
    assert answers.interactive_auth is True
    assert answers.auto_connect is False
    # 2FA forces auto-connect off; the block must not claim auto-connect.
    assert "auto_connect = true" not in render_target_block(answers)
    assert "interactive_auth = true" in render_target_block(answers)
    cfg, _ = _write_and_load(tmp_path, answers)
    assert cfg.targets["rosi"].auto_connect is False
    assert cfg.targets["rosi"].interactive_auth is True
    # If the wizard wrongly asked for auto-connect it would have advanced the
    # container answer into that slot, producing a runtime mismatch.  The
    # container must be unconfigured here.
    assert answers.container_runtime is None


def test_wizard_2fa_container_yes_still_valid(tmp_path):
    from compute_mcp.setup import collect_target

    # 2FA yes, then container block: auto-connect is skipped, so the container
    # answers follow immediately after the 2FA "y".
    w = _wizard(
        [
            "rosi", "tunnel", "rosi", "agent", "/home/u/.ssh/k", "", "y",
            "y", "apptainer", "$HOME/computemcp", "docker://ubuntu:24.04", "",
            "n",  # build/start the container? no
        ]
    )
    answers = collect_target(w, set())
    assert answers.interactive_auth is True
    assert answers.auto_connect is False
    assert answers.container_runtime == "apptainer"
    assert "auto_connect = true" not in render_target_block(answers)


def test_bootstrap_writes_readable_operator_token(tmp_path):
    from compute_mcp.config import OPERATOR_TOKEN_NAME
    from compute_mcp.control import _resolve_token

    config_path = tmp_path / "config.toml"
    run_bootstrap(config_path, wizard=_wizard(_bootstrap_answers()))
    operator = tmp_path / OPERATOR_TOKEN_NAME
    assert operator.exists()
    assert (operator.stat().st_mode & 0o777) == 0o600
    token = operator.read_text().strip()
    assert token
    # The operator CLI can resolve it straight from the config directory.
    assert _resolve_token(str(config_path), None, "admin", None) == token


def test_bootstrap_failure_removes_operator_token(tmp_path, monkeypatch):
    import compute_mcp.setup as setup_module
    from compute_mcp.config import OPERATOR_TOKEN_NAME

    def bad_load(path, token_file=None):
        raise ConfigError("injected failure")

    monkeypatch.setattr(setup_module, "load_config", bad_load)
    with pytest.raises(WizardAbort):
        run_bootstrap(tmp_path / "config.toml", wizard=_wizard(_bootstrap_answers()))
    assert not (tmp_path / OPERATOR_TOKEN_NAME).exists()
    assert not (tmp_path / "tokens.toml").exists()
    assert not (tmp_path / "config.toml").exists()

# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Tests for the interactive setup wizard.

Prompt I/O is injected, so every flow runs without a terminal.  The tests assert
that generated TOML actually loads through the real config loader, that the
token is stored hashed, and that a bad append rolls back.
"""

from __future__ import annotations

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
    assert t.bundle.source == "computemcp-slurm"
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
    assert parsed["targets"]["rosi"]["bundle"]["source"] == "computemcp-slurm"


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
        "y", "apptainer", "/scratch/u/computemcp", "docker://ubuntu:24.04", "nvidia",
        "y",                                   # bundle
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
    # name, transport, host, port, user, fingerprint, 2FA, container, bundle, slurm
    rc = run_add_target(
        config_path,
        wizard=_wizard(["hal", "direct", "10.0.0.9", "2222", "agent", "", "n", "n", "n", "n"]),
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
        wizard=_wizard(["hal", "direct", "10.0.0.9", "2222", "agent", "", "n", "n", "n", "n"]),
    )
    # Second attempt reuses the name; the validator re-asks, so supply it twice.
    before = config_path.read_text()
    with pytest.raises(AssertionError):
        # The validator rejects the duplicate and asks again; our finite input
        # runs out, proving the duplicate was refused rather than accepted.
        run_add_target(
            config_path,
            wizard=_wizard(["hal", "hal", "direct", "h", "2222", "agent", "", "n", "n", "n", "n"]),
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
            wizard=_wizard(["hal", "direct", "10.0.0.9", "2222", "agent", "", "n", "n", "n", "n"]),
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
        ["hal", "hal2", "direct", "h", "2222", "agent", "", "n", "n", "n", "n"]
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

    # name, transport, aliases, user, key, fingerprint, 2FA, container, bundle, slurm
    w = _wizard(
        [
            "rosi", "tunnel", "rosi,ex_rosi", "agent", "/home/u/.ssh/k", "",
            "n", "n", "n", "n",
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

    # name, transport, aliases, user, key, blank fingerprint, 2FA, container n,
    # bundle n
    w = _wizard(
        ["rosi", "tunnel", "rosi", "agent", "/home/u/.ssh/k", "", "n", "n", "n"]
    )
    answers = collect_target(w, set())
    assert answers.host_key_sha256 is None
    assert answers.host_key_check == "off"


def test_collect_target_pin_sets_algorithms():
    from compute_mcp.setup import collect_target

    w = _wizard(
        [
            "rosi", "tunnel", "rosi", "agent", "/home/u/.ssh/k",
            "SHA256:" + "a" * 40, "ssh-ed25519,rsa-sha2-512", "n", "n", "n",
        ]
    )
    answers = collect_target(w, set())
    assert answers.host_key_check == "on"
    assert answers.host_key_algorithms == ("ssh-ed25519", "rsa-sha2-512")


def test_collect_target_empty_user_is_omitted():
    from compute_mcp.setup import collect_target

    # "-" asks for no explicit user: the SSH config or local account decides.
    w = _wizard(
        ["rosi", "tunnel", "rosi", "-", "/home/u/.ssh/k", "", "n", "n", "n"]
    )
    answers = collect_target(w, set())
    assert answers.user == ""
    block = render_target_block(answers)
    assert "user = " not in block


def test_no_slurm_question_without_bundle():
    from compute_mcp.setup import collect_target

    # container yes, bundle no -> no Slurm questions; the wizard must not ask
    # for them, so the answer list ends right after the container/bundle answers.
    w = _wizard(
        [
            "rosi", "tunnel", "rosi", "agent", "/home/u/.ssh/k", "", "n",
            "y", "apptainer", "$HOME/computemcp", "docker://ubuntu:24.04", "",
            "n",  # bundle no
        ]
    )
    answers = collect_target(w, set())
    assert answers.node_cpus is None
    assert answers.container_storage_root == "$HOME/computemcp"

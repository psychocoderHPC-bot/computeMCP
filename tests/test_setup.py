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
    render_tokens_file,
    run_add_target,
    run_bootstrap,
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
    text = render_gateway_config(
        listen="127.0.0.1",
        port=2222,
        allow_enrollment=True,
        client_id="alpaka",
        client_targets=("*",),
        client_label=None,
        token_file=str(tokens),
        targets=[target],
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
    text = render_gateway_config(
        listen="127.0.0.1",
        port=2222,
        allow_enrollment=False,
        client_id="alpaka",
        client_targets=(),
        client_label="dev",
        token_file=str(tokens),
        targets=[target],
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
        "127.0.0.1", "2222", "y", "alpaka", "",
        "y",                                   # set up a target
        "rosi", "tunnel", "rosi", "agent", "/home/u/.ssh/k", "SHA256:" + "a" * 40, "n",
        "y", "apptainer", "/scratch/u/computemcp", "docker://ubuntu:24.04", "nvidia",
        "y",                                   # bundle
        "y",                                   # slurm description
        "24", "4", "378000M", "gpu-proportional", "exclusive", "4", "gpu", "02:00:00", "none",
        "n",                                   # no more targets
        "*",                                   # client targets
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
    assert cfg.clients["alpaka"].allow_all


def test_bootstrap_refuses_overwrite_without_force(tmp_path):
    config_path = tmp_path / "config.toml"
    run_bootstrap(config_path, wizard=_wizard(_bootstrap_answers()))
    with pytest.raises(WizardAbort):
        run_bootstrap(config_path, wizard=_wizard([]))


def test_bootstrap_force_overwrites(tmp_path):
    config_path = tmp_path / "config.toml"
    run_bootstrap(config_path, wizard=_wizard(_bootstrap_answers()))
    # A minimal second run with --force: no target.
    minimal = ["127.0.0.1", "2222", "n", "other", "", "n"]
    rc = run_bootstrap(config_path, force=True, wizard=_wizard(minimal))
    assert rc == 0
    cfg = load_config(config_path)
    assert set(cfg.clients) == {"other"}
    assert cfg.targets == {}


def test_bootstrap_stores_only_the_hash(tmp_path, capsys):
    config_path = tmp_path / "config.toml"
    run_bootstrap(config_path, wizard=_wizard(_bootstrap_answers()))
    tokens_text = (tmp_path / "tokens.toml").read_text()
    table = tomllib.loads(tokens_text)["tokens"]
    stored = table["alpaka"]
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
        wizard=_wizard(["127.0.0.1", "2222", "n", "alpaka", "", "n"]),
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


def test_add_target_rolls_back_on_invalid_append(tmp_path, monkeypatch):
    config_path = _minimal_bootstrap(tmp_path)
    before = config_path.read_text()
    # Force the loader to reject the appended content.
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
    assert config_path.read_text() == before


def test_collect_target_rejects_duplicate_existing_name():
    # The duplicate 'hal' is refused, so the next input is consumed as the name.
    w = _wizard(
        ["hal", "hal2", "direct", "h", "2222", "agent", "", "n", "n", "n", "n"]
    )
    answers = collect_target(w, {"hal"})
    assert answers.name == "hal2"
    assert answers.transport == "direct"

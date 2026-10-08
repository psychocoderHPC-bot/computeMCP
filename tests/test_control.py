# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import pytest

from compute_mcp.config import parse_config
from compute_mcp.control import (
    DEFAULT_TIMEOUT,
    PROVISION_HANDSHAKE_MARGIN,
    _resolve_gateway,
    _resolve_timeout,
    build_parser,
)


def make_config(targets=None, **server):
    raw = {
        "server": {"listen": "127.0.0.1", "port": 2222, **server},
        "clients": {"admin": {"token": "x", "targets": ["*"]}},
        "targets": targets or {},
    }
    return parse_config(raw)


def make_config_with_target(provision_timeout=900.0):
    return make_config(
        targets={
            "hal": {
                "transport": "direct",
                "remote_host": "127.0.0.1",
                "remote_port": 2222,
                "user": "agent",
                "provision_timeout": provision_timeout,
            }
        }
    )


def test_resolve_gateway_from_config(monkeypatch):
    # Hermetic: the ambient COMPUTEMCP_GATEWAY must not override the config value.
    monkeypatch.delenv("COMPUTEMCP_GATEWAY", raising=False)
    cfg = make_config()
    assert _resolve_gateway(cfg, None) == "http://127.0.0.1:2222"


def test_resolve_gateway_wildcard_binds_loopback(monkeypatch):
    monkeypatch.delenv("COMPUTEMCP_GATEWAY", raising=False)
    cfg = make_config(listen="0.0.0.0")
    assert _resolve_gateway(cfg, None) == "http://127.0.0.1:2222"


def test_resolve_gateway_override_and_env(monkeypatch):
    cfg = make_config()
    assert _resolve_gateway(cfg, "http://example:9/").rstrip("/") == "http://example:9"
    monkeypatch.setenv("COMPUTEMCP_GATEWAY", "http://env:1")
    assert _resolve_gateway(cfg, None) == "http://env:1"


def test_parser_accepts_all_commands():
    parser = build_parser()
    base = ["--config", "x.toml"]
    for argv in (
        ["status"], ["targets"], ["clients"], ["sessions"], ["reload"],
        ["client", "alpaka"],
        ["target-connect", "hal"], ["target-refresh", "hal"], ["target-stop", "hal"],
        ["client-connect", "alpaka"], ["client-refresh", "alpaka"],
        ["client-stop", "alpaka"], ["client-kill", "alpaka"],
    ):
        args = parser.parse_args(base + argv)
        assert args.command == argv[0]


# -- CLI: --2fa factor threading and warning rendering -----------------------

def test_parser_parses_2fa_factor():
    parser = build_parser()
    args = parser.parse_args(
        ["--config", "x.toml", "target-connect", "hal", "--2fa", "SECRET"]
    )
    assert args.command == "target-connect"
    assert args.factor == "SECRET"
    args = parser.parse_args(
        ["--config", "x.toml", "target-refresh", "--2fa", "SECRET", "hal"]
    )
    assert args.command == "target-refresh"
    assert args.factor == "SECRET"


async def _run_control(monkeypatch, argv, response, cfg=None):
    """Drive control._run with a fake Control that records requests."""
    import compute_mcp.control as control_mod

    calls = []
    timeouts = []

    cfg = cfg if cfg is not None else make_config()

    class FakeControl:
        def __init__(self, base, token, timeout):
            self.base = base
            self.token = token

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def request(self, method, path, json_body=None, timeout=None):
            calls.append((method, path, json_body))
            timeouts.append(timeout)
            return response

    monkeypatch.setattr(control_mod, "load_config", lambda *a, **k: cfg)
    monkeypatch.setattr(control_mod, "_resolve_gateway", lambda *a, **k: "http://x")
    monkeypatch.setattr(control_mod, "_resolve_token", lambda *a, **k: "tok")
    monkeypatch.setattr(control_mod, "Control", FakeControl)

    args = build_parser().parse_args(["--config", "x.toml"] + argv)
    rc = await control_mod._run(args)
    assert rc == 0
    return calls, timeouts


async def test_control_target_connect_sends_factor(monkeypatch, capsys):
    calls, _ = await _run_control(
        monkeypatch,
        ["target-connect", "hal", "--2fa", "SECRET"],
        {"name": "hal", "state": "connected", "active_route": "hal"},
    )
    assert calls == [
        ("POST", "/v1/targets/hal/connect", {"factor": "SECRET"}),
    ]


async def test_control_dry_run_posts_preview_and_prints_labeled_block(monkeypatch, capsys):
    preview_body = {
        "target": "hal",
        "connected": False,
        "planned": {
            "plan": {
                "nodes": 1,
                "cpus_per_node": 16,
                "gpus_per_node": 1,
                "memory_per_node_mib": 189000,
                "exclusive": False,
                "mode": "gpu-proportional",
            },
            "defaults_used": True,
            "manual": {"sbatch": {"ntasks-per-node": 1}, "srun": {}},
        },
        "sbatch_args": ["--ntasks-per-node=1", "--gres=gpu:1"],
        "srun_args": [],
        "provision_env": {
            "COMPUTEMCP_SBATCH_ARGS": "--ntasks-per-node=1\n--gres=gpu:1",
            "COMPUTEMCP_SRUN_ARGS": "",
        },
        "would_emit": {"sbatch": [], "srun": [], "not_emitted": ["memory-per-node"]},
    }
    calls, _ = await _run_control(
        monkeypatch,
        ["target-connect", "hal", "--dry-run", "--set", "gpus-per-node=1"],
        preview_body,
    )
    # Dry-run hits /preview, never /connect, and keeps the {"set": ...} body.
    assert calls == [
        (
            "POST",
            "/v1/targets/hal/preview",
            {"set": {"gpus-per-node": 1}},
        ),
    ]


async def test_control_set_body_and_back_compat(monkeypatch):
    # With --set, the body carries "set": overrides
    calls, _ = await _run_control(
        monkeypatch,
        ["target-connect", "hal", "--set", "nodes=2", "--set", "gpus-per-node=1"],
        {"name": "hal", "state": "connected", "active_route": "hal"},
    )
    assert calls == [
        (
            "POST",
            "/v1/targets/hal/connect",
            {"set": {"nodes": 2, "gpus-per-node": 1}},
        ),
    ]

    # Back-compat: no body when no --set and no --2fa
    calls, _ = await _run_control(
        monkeypatch,
        ["target-connect", "hal"],
        {"name": "hal", "state": "connected", "active_route": "hal"},
    )
    assert calls == [("POST", "/v1/targets/hal/connect", None)]


async def test_control_set_with_factor_combined_body(monkeypatch):
    calls, _ = await _run_control(
        monkeypatch,
        ["target-connect", "hal", "--2fa", "SECRET", "--set", "nodes=2"],
        {"name": "hal", "state": "connected", "active_route": "hal"},
    )
    assert calls == [
        (
            "POST",
            "/v1/targets/hal/connect",
            {"factor": "SECRET", "set": {"nodes": 2}},
        ),
    ]
    import io, sys
    # factor must never be echoed on stdout — just check the call body
    body_captured = calls[0][2]
    assert body_captured == {"factor": "SECRET", "set": {"nodes": 2}}


def test_parse_overrides_none_and_malformed():
    from compute_mcp.control import _parse_overrides

    assert _parse_overrides(None) is None
    assert _parse_overrides([]) is None
    with pytest.raises(SystemExit):
        _parse_overrides(["no-equals"])
    with pytest.raises(SystemExit):
        _parse_overrides(["=value"])
    with pytest.raises(SystemExit):
        _parse_overrides([ '  =value'])


def test_parse_overrides_repeated_and_int_coercion():
    from compute_mcp.control import _parse_overrides

    result = _parse_overrides([
        "gpus-per-node=2", "nodes=1", "mem-per-node=100G", "mode=full",
    ])
    assert result == {
        "gpus-per-node": 2,
        "nodes": 1,
        "mem-per-node": "100G",
        "mode": "full",
    }
    assert isinstance(result["gpus-per-node"], int)
    assert isinstance(result["nodes"], int)
    assert isinstance(result["mem-per-node"], str)


def test_parser_accepts_set_and_dry_run():
    for argv in (
        ["target-connect", "hal", "--set", "gpus-per-node=2"],
        ["target-refresh", "hal", "--set", "mode=full", "--set", "nodes=2"],
        ["target-connect", "hal", "--dry-run"],
        ["target-refresh", "hal", "--dry-run", "--set", "nodes=2"],
    ):
        args = build_parser().parse_args(["--config", "x.toml"] + argv)
        assert args.command


def test_parser_set_without_value_is_usage_error():
    with pytest.raises(SystemExit) as excinfo:
        build_parser().parse_args(
            ["--config", "x.toml", "target-connect", "hal", "--set"]
        )
    assert excinfo.value.code == 2


async def test_control_target_connect_without_factor_sends_no_body(monkeypatch):
    calls, _ = await _run_control(
        monkeypatch,
        ["target-connect", "hal"],
        {"name": "hal", "state": "connected", "active_route": "hal"},
    )
    assert calls == [("POST", "/v1/targets/hal/connect", None)]

async def test_control_target_refresh_sends_factor(monkeypatch):
    calls, _ = await _run_control(
        monkeypatch,
        ["target-refresh", "hal", "--2fa", "SECRET"],
        {"name": "hal", "state": "connected", "active_route": "hal"},
    )
    assert calls == [
        ("POST", "/v1/targets/hal/refresh", {"factor": "SECRET"}),
    ]

async def test_control_prints_gateway_warning(monkeypatch, capsys):
    await _run_control(
        monkeypatch,
        ["target-connect", "hal"],
        {
            "name": "hal",
            "state": "disconnected",
            "active_route": None,
            "warning": "target requires interactive authentication",
        },
    )
    captured = capsys.readouterr()
    assert "warning:" in captured.err
    assert "requires interactive authentication" in captured.err

def test_parser_2fa_without_value_is_usage_error():
    """`target-connect hal --2fa` (no value) must exit 2, not silently accept."""
    parser = build_parser()
    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(["--config", "x.toml", "target-connect", "hal", "--2fa"])
    assert excinfo.value.code == 2

def test_parser_refresh_2fa_without_value_is_usage_error():
    parser = build_parser()
    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(["--config", "x.toml", "target-refresh", "hal", "--2fa"])
    assert excinfo.value.code == 2

def _parse(argv):
    return build_parser().parse_args(["--config", "x.toml"] + argv)

def test_resolve_timeout_uses_provision_timeout_plus_margin():
    cfg = make_config_with_target(provision_timeout=900.0)
    args = _parse(["target-connect", "hal"])
    assert _resolve_timeout(args, cfg, ["hal"]) == 900.0 + PROVISION_HANDSHAKE_MARGIN

def test_resolve_timeout_global_flag_overrides_provision_timeout():
    cfg = make_config_with_target(provision_timeout=900.0)
    args = _parse(["--timeout", "5", "target-connect", "hal"])
    assert _resolve_timeout(args, cfg, ["hal"]) == 5.0

def test_resolve_timeout_subcommand_flag_overrides_global():
    cfg = make_config_with_target(provision_timeout=900.0)
    args = _parse(["--timeout", "5", "target-connect", "--timeout", "7", "hal"])
    assert _resolve_timeout(args, cfg, ["hal"]) == 7.0

def test_resolve_timeout_refresh_subcommand_flag_without_global():
    cfg = make_config_with_target(provision_timeout=900.0)
    args = _parse(["target-refresh", "--timeout", "7", "hal"])
    assert _resolve_timeout(args, cfg, ["hal"]) == 7.0

def test_resolve_timeout_unknown_target_falls_back_to_default():
    cfg = make_config_with_target(provision_timeout=900.0)
    args = _parse(["target-connect", "unknown"])
    assert _resolve_timeout(args, cfg, ["unknown"]) == DEFAULT_TIMEOUT

def test_resolve_timeout_non_connect_command_is_default():
    cfg = make_config_with_target(provision_timeout=900.0)
    args = _parse(["status"])
    assert _resolve_timeout(args, cfg, []) == DEFAULT_TIMEOUT
    assert not hasattr(args, "action_timeout")

def test_resolve_timeout_picks_max_over_multiple_targets():
    cfg = make_config_with_target(provision_timeout=900.0)
    args = _parse(["client-connect", "alpaka"])
    assert _resolve_timeout(args, cfg, ["hal", "other"]) == (
        900.0 + PROVISION_HANDSHAKE_MARGIN
    )

async def test_target_connect_sends_provision_timeout_to_request(monkeypatch):
    cfg = make_config_with_target(provision_timeout=900.0)
    _, timeouts = await _run_control(
        monkeypatch,
        ["target-connect", "hal"],
        {"name": "hal", "state": "connected", "active_route": "hal"},
        cfg=cfg,
    )
    assert timeouts == [900.0 + PROVISION_HANDSHAKE_MARGIN]

async def test_target_connect_subcommand_timeout_reaches_request(monkeypatch):
    cfg = make_config_with_target(provision_timeout=900.0)
    _, timeouts = await _run_control(
        monkeypatch,
        ["target-connect", "--timeout", "7", "hal"],
        {"name": "hal", "state": "connected", "active_route": "hal"},
        cfg=cfg,
    )
    assert timeouts == [7.0]



# -- --add-target dispatch (no gateway connection) ---------------------------

def test_parser_accepts_add_target():
    parser = build_parser()
    args = parser.parse_args(["--config", "x.toml", "--add-target"])
    assert args.add_target is True
    assert args.command is None


def test_add_target_dispatches_to_setup_without_gateway(monkeypatch):
    import compute_mcp.control as control_mod
    import compute_mcp.setup as setup_mod

    seen = {}

    def fake_add_target(config_path, *, wizard=None):
        seen["config"] = str(config_path)
        seen["wizard"] = wizard
        return 0

    def explode(*a, **k):  # aiohttp must never be reached
        raise AssertionError("gateway must not be contacted for --add-target")

    monkeypatch.setattr(setup_mod, "run_add_target", fake_add_target)
    monkeypatch.setattr(control_mod.aiohttp, "ClientSession", explode)

    rc = control_mod.main(["--config", "x.toml", "--add-target"])
    assert rc == 0
    assert seen["config"] == "x.toml"
    assert seen["wizard"] is not None


def test_add_target_wizard_abort_returns_2(monkeypatch):
    import compute_mcp.control as control_mod
    import compute_mcp.setup as setup_mod

    def fake_add_target(config_path, *, wizard=None):
        raise setup_mod.WizardAbort("boom")

    monkeypatch.setattr(setup_mod, "run_add_target", fake_add_target)
    rc = control_mod.main(["--config", "x.toml", "--add-target"])
    assert rc == 2


def test_missing_command_is_usage_error():
    import compute_mcp.control as control_mod

    with pytest.raises(SystemExit):
        control_mod.main(["--config", "x.toml"])


# -- operator token resolution -----------------------------------------------

def test_resolve_token_prefers_operator_token_over_env(tmp_path, monkeypatch):
    from compute_mcp.config import OPERATOR_TOKEN_NAME
    from compute_mcp.control import _resolve_token

    config = tmp_path / "config.toml"
    config.write_text("")
    (tmp_path / OPERATOR_TOKEN_NAME).write_text("operator-secret\n")
    monkeypatch.setenv("COMPUTEMCP_TOKEN", "stale-env-token")
    # The config-local operator token must win over a stray ambient token.
    assert _resolve_token(str(config), None, "admin", None) == "operator-secret"


def test_resolve_token_flag_beats_operator_token(tmp_path, monkeypatch):
    from compute_mcp.config import OPERATOR_TOKEN_NAME
    from compute_mcp.control import _resolve_token

    config = tmp_path / "config.toml"
    config.write_text("")
    (tmp_path / OPERATOR_TOKEN_NAME).write_text("operator-secret\n")
    monkeypatch.setenv("COMPUTEMCP_TOKEN", "stale-env-token")
    assert _resolve_token(str(config), None, "admin", "flag-token") == "flag-token"


def test_resolve_token_falls_back_to_env_without_operator_token(tmp_path, monkeypatch):
    from compute_mcp.control import _resolve_token

    config = tmp_path / "config.toml"
    config.write_text("")
    monkeypatch.setenv("COMPUTEMCP_TOKEN", "env-token")
    assert _resolve_token(str(config), None, "admin", None) == "env-token"


def test_resolve_token_logs_operator_token_source(tmp_path, monkeypatch, caplog):
    from compute_mcp.config import OPERATOR_TOKEN_NAME
    from compute_mcp.control import _resolve_token

    config = tmp_path / "config.toml"
    config.write_text("")
    (tmp_path / OPERATOR_TOKEN_NAME).write_text("operator-secret\n")
    monkeypatch.setenv("COMPUTEMCP_TOKEN", "stale-env-token")
    with caplog.at_level("DEBUG", logger="compute_mcp.control"):
        token = _resolve_token(str(config), None, "admin", None)
    assert token == "operator-secret"
    assert "operator-token" in caplog.text

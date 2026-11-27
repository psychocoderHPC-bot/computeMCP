# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import json

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

def test_parser_parses_2fa_flag():
    parser = build_parser()
    args = parser.parse_args(
        ["--config", "x.toml", "target-connect", "hal", "--2fa"]
    )
    assert args.command == "target-connect"
    assert args.use_2fa is True
    assert not hasattr(args, "factor")
    args = parser.parse_args(
        ["--config", "x.toml", "target-refresh", "--2fa", "hal"]
    )
    assert args.command == "target-refresh"
    assert args.use_2fa is True


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

        async def job_request(self, method, path, json_body=None, timeout=None):
            return await self.request(
                method, path, json_body=json_body, timeout=timeout
            )

    monkeypatch.setattr(control_mod, "load_config", lambda *a, **k: cfg)
    monkeypatch.setattr(control_mod, "_resolve_gateway", lambda *a, **k: "http://x")
    monkeypatch.setattr(control_mod, "_resolve_token", lambda *a, **k: "tok")
    monkeypatch.setattr(control_mod, "Control", FakeControl)
    monkeypatch.setattr(control_mod, "prompt_factor", lambda: "SECRET")

    args = build_parser().parse_args(["--config", "x.toml"] + argv)
    rc = await control_mod._run(args)
    assert rc == 0
    return calls, timeouts


async def test_control_target_connect_sends_factor(monkeypatch, capsys):
    calls, _ = await _run_control(
        monkeypatch,
        ["target-connect", "hal", "--2fa"],
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
        ["target-connect", "hal", "--2fa", "--set", "nodes=2"],
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
        ["target-refresh", "hal", "--2fa"],
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

def test_parser_2fa_is_value_less_flag():
    parser = build_parser()
    args = parser.parse_args(
        ["--config", "x.toml", "target-connect", "hal", "--2fa"]
    )
    assert args.command == "target-connect"
    assert args.use_2fa is True


def test_parser_2fa_rejects_value():
    parser = build_parser()
    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(
            ["--config", "x.toml", "target-connect", "hal", "--2fa", "SECRET"]
        )
    assert excinfo.value.code == 2


def test_prompt_factor_uses_hidden_getpass(monkeypatch):
    import compute_mcp.factor as factor_mod
    from types import SimpleNamespace

    monkeypatch.setattr(
        factor_mod.sys, "stdin", SimpleNamespace(isatty=lambda: True)
    )
    seen = []

    def fake_getpass(prompt=""):
        seen.append(prompt)
        return "SECRET"

    monkeypatch.setattr(factor_mod.getpass, "getpass", fake_getpass)
    assert factor_mod.prompt_factor() == "SECRET"
    assert seen == ["2FA token: "]


def test_prompt_factor_requires_a_tty(monkeypatch):
    import compute_mcp.factor as factor_mod
    from types import SimpleNamespace

    monkeypatch.setattr(
        factor_mod.sys, "stdin", SimpleNamespace(isatty=lambda: False)
    )
    with pytest.raises(factor_mod.FactorPromptError):
        factor_mod.prompt_factor()


async def test_control_2fa_prompt_error_exits(monkeypatch):
    import compute_mcp.control as control_mod
    from compute_mcp.factor import FactorPromptError

    cfg = make_config()
    monkeypatch.setattr(control_mod, "load_config", lambda *a, **k: cfg)
    monkeypatch.setattr(control_mod, "_resolve_gateway", lambda *a, **k: "http://x")
    monkeypatch.setattr(control_mod, "_resolve_token", lambda *a, **k: "tok")

    def fail():
        raise FactorPromptError("no tty")

    monkeypatch.setattr(control_mod, "prompt_factor", fail)
    args = build_parser().parse_args(
        ["--config", "x.toml", "target-connect", "hal", "--2fa"]
    )
    with pytest.raises(SystemExit) as excinfo:
        await control_mod._run(args)
    assert "no tty" in str(excinfo.value)

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


# -- public job surfaces: target-open/shared/sbatch and job-* ----------------

JOB = {
    "job_id": "J7",
    "target": "hal",
    "kind": "own",
    "state": "pending",
    "slurm_job_id": "413872",
    "cluster": "frontier",
    "scheduler_state": "PENDING",
    "root_mode": "clean",
    "home": "benchmarks/run-42",
    "node": None,
    "terminal_reason": None,
}


def test_parser_accepts_job_surface_commands():
    parser = build_parser()
    base = ["--config", "x.toml"]
    for argv in (
        ["target-open", "hal", "--2fa"],
        ["target-shared", "hal", "--2fa"],
        ["target-sbatch", "hal", "--2fa", "--set", "nodes=2",
         "--root-mode", "clean", "--clean-home", "/benchmarks/run-42",
         "--request-id", "r1", "--dry-run"],
        ["job-list"],
        ["job-list", "--target", "hal"],
        ["job-status", "J7"],
        ["job-wait", "J7", "--timeout", "5"],
        ["job-stop", "J7"],
        ["approve", "req", "--job-limit", "frontier=2"],
    ):
        args = parser.parse_args(base + argv)
        assert args.command == argv[0]
    args = parser.parse_args(
        base + ["target-sbatch", "hal", "--root-mode", "clean"]
    )
    assert args.root_mode == "clean"
    args = parser.parse_args(base + ["job-wait", "J7"])
    assert args.action_timeout == 30.0


def test_parser_rejects_unknown_root_mode():
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["--config", "x.toml", "target-sbatch", "hal", "--root-mode", "root"]
        )


async def test_control_target_open_posts_open_with_factor(monkeypatch):
    calls, _ = await _run_control(
        monkeypatch,
        ["target-open", "hal", "--2fa"],
        {"name": "hal", "state": "connected", "active_route": "hal"},
    )
    assert calls == [("POST", "/v1/targets/hal/open", {"factor": "SECRET"})]


async def test_control_target_open_without_factor_sends_no_body(monkeypatch):
    calls, _ = await _run_control(
        monkeypatch,
        ["target-open", "hal"],
        {"name": "hal", "state": "disconnected"},
    )
    assert calls == [("POST", "/v1/targets/hal/open", None)]


async def test_control_target_shared_reports_route_and_job(monkeypatch, capsys):
    calls, _ = await _run_control(
        monkeypatch,
        ["target-shared", "hal"],
        {
            "name": "hal",
            "state": "connected",
            "active_route": "hal",
            "job": {"job_id": "S1", "state": "pending"},
            "job_id": "S1",
        },
    )
    assert calls == [("POST", "/v1/targets/hal/shared", None)]
    out = capsys.readouterr().out
    assert "route: open" in out
    assert "route_state: connected" in out
    assert "job: S1" in out
    assert "state: pending" in out


async def test_control_target_shared_pending_job_is_not_ready(monkeypatch, capsys):
    await _run_control(
        monkeypatch,
        ["target-shared", "hal"],
        {
            "name": "hal",
            "state": "connected",
            "job": {"job_id": "S1", "state": "pending"},
        },
    )
    out = capsys.readouterr().out
    assert "job: S1" in out
    assert "state: pending" in out
    assert "state: ready" not in out


def test_parser_accepts_target_shared_set_and_dry_run():
    args = build_parser().parse_args(
        [
            "--config", "x.toml", "target-shared", "hal",
            "--set", "nodes=2", "--set", "gpus-per-node=1",
            "--root-mode", "clean", "--clean-home", "/benchmarks/run-42",
            "--dry-run",
        ]
    )
    assert args.command == "target-shared"
    assert args.overrides == ["nodes=2", "gpus-per-node=1"]
    assert args.root_mode == "clean"
    assert args.clean_home == "/benchmarks/run-42"
    assert args.dry_run is True


async def test_control_target_shared_sends_set_and_factor(monkeypatch):
    calls, _ = await _run_control(
        monkeypatch,
        ["target-shared", "hal", "--2fa", "--set", "nodes=2"],
        {
            "name": "hal",
            "state": "connected",
            "job": {"job_id": "S1", "state": "pending"},
        },
    )
    assert calls == [
        (
            "POST",
            "/v1/targets/hal/shared",
            {"factor": "SECRET", "set": {"nodes": 2}},
        )
    ]


async def test_control_target_shared_dry_run_uses_preview_without_factor(
    monkeypatch, capsys
):
    calls, _ = await _run_control(
        monkeypatch,
        ["target-shared", "hal", "--2fa", "--dry-run", "--set", "gpus-per-node=2"],
        {"target": "hal", "connected": False, "planned": {"plan": {}}},
    )
    # Dry-run uses the preview route and never prompts for or sends a factor.
    assert calls == [
        ("POST", "/v1/targets/hal/preview", {"set": {"gpus-per-node": 2}})
    ]
    out = capsys.readouterr().out
    assert "planned:" in out
    assert "shared_reuse:" in out


async def test_control_target_shared_dry_run_forwards_root_home_and_set(
    monkeypatch, capsys
):
    calls, _ = await _run_control(
        monkeypatch,
        [
            "target-shared", "hal", "--2fa", "--dry-run",
            "--set", "nodes=2",
            "--root-mode", "clean", "--clean-home", "/benchmarks/run-42",
        ],
        {"target": "hal", "connected": False, "planned": {"plan": {}}},
    )
    # The requested target-level overrides reach the preview body; no factor
    # is prompted for or sent on a dry-run.
    assert calls == [
        (
            "POST",
            "/v1/targets/hal/preview",
            {
                "set": {"nodes": 2},
                "root_mode": "clean",
                "clean_home": "/benchmarks/run-42",
            },
        )
    ]
    capsys.readouterr()


@pytest.mark.parametrize(
    "argv,path",
    [
        (["target-connect", "hal", "--2fa"], "/v1/targets/hal/connect"),
        (["target-open", "hal", "--2fa"], "/v1/targets/hal/open"),
        (["target-shared", "hal", "--2fa"], "/v1/targets/hal/shared"),
        (["target-sbatch", "hal", "--2fa"], "/v1/targets/hal/sbatch"),
    ],
)
async def test_control_2fa_forwarded_on_target_commands(monkeypatch, argv, path):
    calls, _ = await _run_control(
        monkeypatch,
        argv,
        {"name": "hal", "state": "connected", "active_route": "hal"},
    )
    assert calls == [("POST", path, {"factor": "SECRET"})]


async def test_control_target_open_dry_run_uses_preview(monkeypatch):
    calls, _ = await _run_control(
        monkeypatch,
        ["target-open", "hal", "--dry-run"],
        {"target": "hal", "connected": False, "planned": {"plan": {}}},
    )
    assert calls == [("POST", "/v1/targets/hal/preview", None)]


async def test_control_preview_renders_design_fields(monkeypatch, capsys):
    preview = {
        "target": "hal",
        "connected": False,
        "root_mode": "clean",
        "home": "agents/alpaka/benchmarks/run-42",
        "shared_reuse": {"reuse": True, "job_id": "S1"},
        "settings_conflict": False,
        "quota_available": {"available": 1, "limit": 2},
        "planned": {"plan": {"mode": "full"}},
    }
    await _run_control(
        monkeypatch, ["target-sbatch", "hal", "--dry-run"], preview
    )
    out = capsys.readouterr().out
    assert "root_mode: clean" in out
    assert "home: agents/alpaka/benchmarks/run-42" in out
    assert 'shared_reuse: {"job_id": "S1", "reuse": true}' in out
    assert "settings_conflict: false" in out
    assert 'quota_available: {"available": 1, "limit": 2}' in out


async def test_control_preview_missing_design_fields_is_not_invented(
    monkeypatch, capsys
):
    preview = {"target": "hal", "connected": False, "planned": {"plan": {}}}
    await _run_control(
        monkeypatch, ["target-sbatch", "hal", "--dry-run"], preview
    )
    out = capsys.readouterr().out
    assert "root_mode: -" in out
    assert "home: -" in out
    assert "shared_reuse: -" in out
    assert "settings_conflict: -" in out
    assert "quota_available: -" in out


async def test_control_preview_json_keeps_design_fields(monkeypatch, capsys):
    preview = {
        "target": "hal",
        "connected": False,
        "root_mode": "fakeroot",
        "home": "shared",
        "settings_conflict": True,
        "quota_available": {"available": 0},
    }
    await _run_control(
        monkeypatch, ["--json", "target-sbatch", "hal", "--dry-run"], preview
    )
    data = json.loads(capsys.readouterr().out)
    assert data["root_mode"] == "fakeroot"
    assert data["home"] == "shared"
    assert data["settings_conflict"] is True
    assert data["quota_available"] == {"available": 0}


async def test_control_target_sbatch_body(monkeypatch):
    calls, _ = await _run_control(
        monkeypatch,
        [
            "target-sbatch", "hal", "--2fa",
            "--set", "gpus-per-node=1", "--set", "nodes=2",
            "--root-mode", "clean", "--clean-home", "/benchmarks/run-42",
            "--request-id", "r1",
        ],
        JOB,
    )
    assert calls == [
        (
            "POST",
            "/v1/targets/hal/sbatch",
            {
                "factor": "SECRET",
                "set": {"gpus-per-node": 1, "nodes": 2},
                "root_mode": "clean",
                "clean_home": "/benchmarks/run-42",
                "request_id": "r1",
            },
        ),
    ]


async def test_control_target_sbatch_minimal_body(monkeypatch):
    calls, _ = await _run_control(monkeypatch, ["target-sbatch", "hal"], JOB)
    assert calls == [("POST", "/v1/targets/hal/sbatch", None)]


async def test_control_target_sbatch_dry_run_uses_preview(monkeypatch):
    preview = {"target": "hal", "planned": {"plan": {}}}
    calls, _ = await _run_control(
        monkeypatch,
        ["target-sbatch", "hal", "--dry-run", "--set", "nodes=2"],
        preview,
    )
    assert calls == [
        ("POST", "/v1/targets/hal/preview", {"set": {"nodes": 2}})
    ]


async def test_control_target_sbatch_dry_run_forwards_root_home_and_set(
    monkeypatch,
):
    preview = {"target": "hal", "planned": {"plan": {}}}
    calls, _ = await _run_control(
        monkeypatch,
        [
            "target-sbatch", "hal", "--2fa", "--dry-run",
            "--set", "gpus-per-node=1",
            "--root-mode", "clean", "--clean-home", "/benchmarks/run-42",
            "--request-id", "r1",
        ],
        preview,
    )
    # The preview carries the requested allocation fields exactly as the real
    # submit would, but never the 2FA factor or the submit idempotency id.
    assert calls == [
        (
            "POST",
            "/v1/targets/hal/preview",
            {
                "set": {"gpus-per-node": 1},
                "root_mode": "clean",
                "clean_home": "/benchmarks/run-42",
            },
        )
    ]


async def test_control_job_list_and_target_filter(monkeypatch):
    calls, _ = await _run_control(
        monkeypatch, ["job-list"], {"jobs": [JOB]}
    )
    assert calls == [("GET", "/v1/jobs", None)]
    calls, _ = await _run_control(
        monkeypatch, ["job-list", "--target", "hal"], {"jobs": [JOB]}
    )
    assert calls == [("GET", "/v1/jobs?target=hal", None)]


async def test_control_job_status_wait_stop_paths(monkeypatch):
    calls, _ = await _run_control(monkeypatch, ["job-status", "J7"], JOB)
    assert calls == [("GET", "/v1/jobs/J7", None)]

    calls, timeouts = await _run_control(
        monkeypatch, ["job-wait", "J7", "--timeout", "5"], JOB
    )
    assert calls == [("GET", "/v1/jobs/J7?wait=true&timeout=5", None)]
    assert timeouts == [20.0]

    calls, _ = await _run_control(monkeypatch, ["job-stop", "J7"], JOB)
    assert calls == [("POST", "/v1/jobs/J7/cancel", None)]


async def test_control_json_output_is_raw(monkeypatch, capsys):
    await _run_control(monkeypatch, ["--json", "job-status", "J7"], JOB)
    out = capsys.readouterr().out
    assert json.loads(out)["job_id"] == "J7"
    assert "job: J7" not in out


async def test_control_job_list_human_table(monkeypatch, capsys):
    await _run_control(monkeypatch, ["job-list"], {"jobs": [JOB]})
    out = capsys.readouterr().out
    assert "JOB_ID" in out
    assert "J7" in out
    assert "pending" in out


async def test_control_approve_sends_job_limits(monkeypatch):
    calls, _ = await _run_control(
        monkeypatch,
        ["approve", "req", "--job-limit", "frontier=2", "--job-limit", "juwels=0"],
        {"request_id": "req", "client_id": "alpaka"},
    )
    assert calls == [
        (
            "POST",
            "/v1/enroll-requests/req/approve",
            {"job_limits": {"frontier": 2, "juwels": 0}},
        ),
    ]


async def test_control_approve_without_limits_sends_no_body(monkeypatch):
    calls, _ = await _run_control(
        monkeypatch,
        ["approve", "req"],
        {"request_id": "req", "client_id": "alpaka"},
    )
    assert calls == [("POST", "/v1/enroll-requests/req/approve", None)]


async def test_control_approve_invalid_job_limit_exits(monkeypatch):
    with pytest.raises(SystemExit):
        await _run_control(
            monkeypatch,
            ["approve", "req", "--job-limit", "frontier=-1"],
            {"request_id": "req", "client_id": "alpaka"},
        )


def test_normalize_gateway_error_collapses_job_not_found():
    from compute_mcp.control import JOB_NOT_FOUND_TEXT, _normalize_gateway_error

    # A foreign id and an unknown id must produce the identical message.
    assert _normalize_gateway_error("gateway error 404: job not found") == (
        JOB_NOT_FOUND_TEXT
    )
    assert _normalize_gateway_error("gateway error 404: nope") == (
        JOB_NOT_FOUND_TEXT
    )
    quota = "gateway error 429: private job quota exhausted"
    assert _normalize_gateway_error(quota) == quota

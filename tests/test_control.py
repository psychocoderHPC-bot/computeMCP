# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import pytest

from compute_mcp.config import parse_config
from compute_mcp.control import _resolve_gateway, build_parser


def make_config(**server):
    raw = {
        "server": {"listen": "127.0.0.1", "port": 2222, **server},
        "clients": {"admin": {"token": "x", "targets": ["*"]}},
        "targets": {},
    }
    return parse_config(raw)


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


async def _run_control(monkeypatch, argv, response):
    """Drive control._run with a fake Control that records requests."""
    import compute_mcp.control as control_mod

    calls = []

    cfg = make_config()

    class FakeControl:
        def __init__(self, base, token, timeout):
            self.base = base
            self.token = token

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def request(self, method, path, json_body=None):
            calls.append((method, path, json_body))
            return response

    monkeypatch.setattr(control_mod, "load_config", lambda *a, **k: cfg)
    monkeypatch.setattr(control_mod, "_resolve_gateway", lambda *a, **k: "http://x")
    monkeypatch.setattr(control_mod, "_resolve_token", lambda *a, **k: "tok")
    monkeypatch.setattr(control_mod, "Control", FakeControl)

    args = build_parser().parse_args(["--config", "x.toml"] + argv)
    rc = await control_mod._run(args)
    assert rc == 0
    return calls


async def test_control_target_connect_sends_factor(monkeypatch, capsys):
    calls = await _run_control(
        monkeypatch,
        ["target-connect", "hal", "--2fa", "SECRET"],
        {"name": "hal", "state": "connected", "active_route": "hal"},
    )
    assert calls == [
        ("POST", "/v1/targets/hal/connect", {"factor": "SECRET"}),
    ]
    # the factor must never be echoed to stdout/stderr
    captured = capsys.readouterr()
    assert "SECRET" not in captured.out + captured.err


async def test_control_target_connect_without_factor_sends_no_body(monkeypatch):
    calls = await _run_control(
        monkeypatch,
        ["target-connect", "hal"],
        {"name": "hal", "state": "connected", "active_route": "hal"},
    )
    assert calls == [("POST", "/v1/targets/hal/connect", None)]


async def test_control_target_refresh_sends_factor(monkeypatch):
    calls = await _run_control(
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

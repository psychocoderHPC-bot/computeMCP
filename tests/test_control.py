# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
from terok_compute.config import parse_config
from terok_compute.control import _resolve_gateway, build_parser


def make_config(**server):
    raw = {
        "server": {"listen": "127.0.0.1", "port": 2222, **server},
        "clients": {"admin": {"token": "x", "targets": ["*"]}},
        "targets": {},
    }
    return parse_config(raw)


def test_resolve_gateway_from_config():
    cfg = make_config()
    assert _resolve_gateway(cfg, None) == "http://127.0.0.1:2222"


def test_resolve_gateway_wildcard_binds_loopback():
    cfg = make_config(listen="0.0.0.0")
    assert _resolve_gateway(cfg, None) == "http://127.0.0.1:2222"


def test_resolve_gateway_override_and_env(monkeypatch):
    cfg = make_config()
    assert _resolve_gateway(cfg, "http://example:9/").rstrip("/") == "http://example:9"
    monkeypatch.setenv("TEROK_COMPUTE_GATEWAY", "http://env:1")
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

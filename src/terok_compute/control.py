# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Operator CLI for a running Terok Compute Gateway.

Talks to the gateway's authenticated HTTP API, so it works against a gateway
managed by systemd where the interactive console is not available.

Examples::

    terok-compute-gatewayctl status
    terok-compute-gatewayctl clients
    terok-compute-gatewayctl client alpaka
    terok-compute-gatewayctl target-refresh hal
    terok-compute-gatewayctl client-refresh alpaka
    terok-compute-gatewayctl client-connect alpaka
    terok-compute-gatewayctl client-stop alpaka
    terok-compute-gatewayctl client-kill alpaka
    terok-compute-gatewayctl reload
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import aiohttp

from .config import (
    ConfigError,
    GatewayConfig,
    default_config_path,
    load_config,
)


def _resolve_gateway(config: GatewayConfig, override: str | None) -> str:
    if override:
        return override.rstrip("/")
    env = os.environ.get("TEROK_COMPUTE_GATEWAY")
    if env:
        return env.rstrip("/")
    host = config.server.listen
    if host in ("0.0.0.0", "::", ""):
        host = "127.0.0.1"
    return f"http://{host}:{config.server.port}"


def _resolve_token(config_path: str, token_file: str | None,
                   client: str, token: str | None) -> str:
    if token:
        return token
    env = os.environ.get("TEROK_COMPUTE_TOKEN")
    if env:
        return env
    # Fall back to reading the plaintext token from a tokens file, if present.
    path = token_file
    if path is None:
        try:
            cfg = load_config(config_path, token_file=None)
            path = cfg.token_file
        except ConfigError:
            path = None
    if path and os.path.exists(path):
        import tomllib

        with open(path, "rb") as handle:
            table = tomllib.load(handle).get("tokens", {})
        for key, value in table.items():
            if not str(key).startswith("sha256:") and str(value).startswith("sha256:"):
                continue  # hashed entries can't be reversed to a plaintext token
            if key == client:
                return str(value)
    raise SystemExit(
        "no token available: set TEROK_COMPUTE_TOKEN, pass --token, or keep a "
        "plaintext token in the tokens file (hashes cannot be used by the CLI)"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="terok-compute-gatewayctl")
    parser.add_argument(
        "--config",
        default=str(default_config_path()),
        help="gateway config.toml (default: "
        "~/.config/terok-compute-gateway/config.toml)",
    )
    parser.add_argument("--token-file", help="override [auth] token_file")
    parser.add_argument("--gateway", help="gateway base URL (default from config/env)")
    parser.add_argument("--token", help="admin/operator bearer token")
    parser.add_argument("--client", default="admin",
                        help="client id whose token to use (default: admin)")
    parser.add_argument("--json", action="store_true", help="print raw JSON")
    parser.add_argument("--timeout", type=float, default=60.0)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="show targets and their state")
    sub.add_parser("targets", help="list target names")
    sub.add_parser("clients", help="list clients, ACLs and live sessions")
    sub.add_parser("sessions", help="list live sessions (all clients)")
    sub.add_parser("reload", help="reload the gateway configuration")

    p = sub.add_parser("client", help="show one client")
    p.add_argument("name")

    p = sub.add_parser("target-connect", help="connect a target")
    p.add_argument("target")
    p = sub.add_parser("target-refresh", help="refresh a target")
    p.add_argument("target")
    p = sub.add_parser("target-stop", help="stop a target")
    p.add_argument("target")

    for name, help_ in (
        ("client-connect", "connect every target a client may access"),
        ("client-refresh", "refresh every target a client may access"),
        ("client-stop", "stop every target a client may access"),
        ("client-kill", "close every live session of a client"),
    ):
        p = sub.add_parser(name, help=help_)
        p.add_argument("name")
        p.add_argument("target", nargs="?", help="limit to one target")

    return parser


class Control:
    def __init__(self, base: str, token: str, timeout: float) -> None:
        self.base = base.rstrip("/")
        self.token = token
        self.timeout = timeout
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self.token}"}
        )
        return self

    async def __aexit__(self, *exc):
        if self._session:
            await self._session.close()

    async def request(self, method: str, path: str) -> dict:
        assert self._session is not None
        async with self._session.request(
            method, f"{self.base}{path}",
            timeout=aiohttp.ClientTimeout(total=self.timeout),
        ) as response:
            text = await response.text()
            if response.status >= 400:
                raise SystemExit(f"gateway error {response.status}: {text[:500]}")
            return json.loads(text) if text else {}


async def _run(args: argparse.Namespace) -> int:
    config = load_config(args.config, token_file=args.token_file)
    base = _resolve_gateway(config, args.gateway)
    token = _resolve_token(args.config, args.token_file, args.client, args.token)

    async with Control(base, token, args.timeout) as control:
        cmd = args.command
        if cmd == "targets":
            body = await control.request("GET", "/v1/targets")
            _emit(args, body, lambda b: "\n".join(t["name"] for t in b["targets"]))
        elif cmd == "status":
            body = await control.request("GET", "/v1/targets")
            _emit_status(args, body)
        elif cmd == "clients":
            body = await control.request("GET", "/v1/clients")
            _emit_clients(args, body)
        elif cmd == "client":
            body = await control.request("GET", f"/v1/clients/{args.name}")
            _emit_client(args, body)
        elif cmd == "sessions":
            body = await control.request("GET", "/v1/sessions?all=true")
            _emit_sessions(args, body)
        elif cmd == "reload":
            body = await control.request("POST", "/v1/reload")
            _emit(args, body, lambda b: f"reloaded: {b['report']}")
        elif cmd == "target-connect":
            body = await control.request("POST", f"/v1/targets/{args.target}/connect")
            _emit(args, body, lambda b: f"{b['name']} -> {b['state']} ({b['active_route']})")
        elif cmd == "target-refresh":
            body = await control.request("POST", f"/v1/targets/{args.target}/refresh")
            _emit(args, body, lambda b: f"{b['name']} -> {b['state']} ({b['active_route']})")
        elif cmd == "target-stop":
            body = await control.request("POST", f"/v1/targets/{args.target}/stop")
            _emit(args, body, lambda b: f"{b['name']} -> {b['state']}")
        elif cmd.startswith("client-"):
            await _client_action(args, control, cmd[len("client-"):])
        else:
            raise SystemExit(f"unknown command: {cmd}")
    return 0


async def _client_action(args, control: Control, action: str) -> None:
    name = args.name
    body = await control.request("GET", f"/v1/clients/{name}")
    if action == "kill":
        result = await control.request("DELETE", f"/v1/clients/{name}/sessions")
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print(f"closed {result['closed']} session(s) for client {name}")
        return
    if action == "sessions":
        _emit_sessions(args, body)
        return
    targets = args.target and [args.target] or body["targets"]
    for target in targets:
        path = f"/v1/targets/{target}/{action}"
        result = await control.request("POST", path)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print(f"{name}: {target} -> {result['state']} ({result.get('active_route')})")


def _emit(args, body: dict, render) -> None:
    if args.json:
        print(json.dumps(body, indent=2))
    else:
        print(render(body))


def _emit_status(args, body: dict) -> None:
    if args.json:
        print(json.dumps(body, indent=2))
        return
    print(f"{'TARGET':<18}{'STATE':<14}{'ROUTE':<14}{'LOCAL':<8}{'SHARING':<11}CLIENTS")
    for t in body["targets"]:
        print(
            f"{t['name']:<18}{t['state']:<14}{t.get('active_route') or '-':<14}"
            f"{t.get('local_port') or '-':<8}{t.get('sharing', 'unknown'):<11}"
            f"{t.get('clients', 0)}"
        )


def _emit_clients(args, body: dict) -> None:
    if args.json:
        print(json.dumps(body, indent=2))
        return
    print(f"{'CLIENT':<18}{'LABEL':<22}{'TARGETS':<40}{'SESSIONS'}")
    for c in body["clients"]:
        targets = "*" if c["allow_all"] else ",".join(c["targets"])
        print(
            f"{c['name']:<18}{(c.get('label') or '-'):<22}{targets:<40}"
            f"{c['session_count']}"
        )


def _emit_client(args, body: dict) -> None:
    if args.json:
        print(json.dumps(body, indent=2))
        return
    print(f"client:    {body['name']}")
    print(f"label:     {body.get('label') or '-'}")
    print(f"token fp:  {body['token_fingerprint']}")
    print(f"allow_all: {body['allow_all']}")
    print(f"targets:   {', '.join(body['targets']) or '-'}")
    print(f"sessions:  {body['session_count']}")
    for s in body["sessions"]:
        print(f"  {s['session_id']}  target={s['target']}  idle={s['idle']}")


def _emit_sessions(args, body: dict) -> None:
    if args.json:
        print(json.dumps(body, indent=2))
        return
    for s in body["sessions"]:
        print(
            f"{s['session_id']}  client={s.get('client', '?')}  target={s['target']}  "
            f"idle={s['idle']}  {s.get('connection', '')}"
        )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    import asyncio

    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())

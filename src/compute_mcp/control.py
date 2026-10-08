# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Operator CLI for a running computeMCP gateway.

Talks to the gateway's authenticated HTTP API, so it works against a gateway
managed by systemd where the interactive console is not available.

Examples::

    computeMCP-gatewayctl status
    computeMCP-gatewayctl clients
    computeMCP-gatewayctl client alpaka
    computeMCP-gatewayctl target-refresh hal
    computeMCP-gatewayctl client-refresh alpaka
    computeMCP-gatewayctl client-connect alpaka
    computeMCP-gatewayctl client-stop alpaka
    computeMCP-gatewayctl client-kill alpaka
    computeMCP-gatewayctl reload
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import aiohttp

from .config import (
    OPERATOR_TOKEN_NAME,
    ConfigError,
    GatewayConfig,
    default_config_path,
    load_config,
)

log = logging.getLogger("compute_mcp.control")

# Fallback HTTP client timeout for commands that do not know a provisioning
# deadline (status, clients, reload, ...) and when no flag overrides it.
DEFAULT_TIMEOUT = 60.0
# Extra slack on top of a target's provision_timeout so the HTTP client waits
# past the provisioning command's own deadline for the connect handshake.
PROVISION_HANDSHAKE_MARGIN = 60.0


def _resolve_gateway(config: GatewayConfig, override: str | None) -> str:
    if override:
        return override.rstrip("/")
    env = os.environ.get("COMPUTEMCP_GATEWAY")
    if env:
        return env.rstrip("/")
    host = config.server.listen
    if host in ("0.0.0.0", "::", ""):
        host = "127.0.0.1"
    return f"http://{host}:{config.server.port}"


def _resolve_token(config_path: str, token_file: str | None,
                   client: str, token: str | None) -> str:
    # Explicit flag wins; then a plaintext operator token written by --bootstrap
    # next to the config; then COMPUTEMCP_TOKEN; then a plaintext token in the
    # tokens file.  The config-local token outranks the environment so a stray
    # ambient token (e.g. one exported for a MCP client) cannot shadow the
    # gateway this command was pointed at.
    if token:
        log.debug("token source: flag")
        return token
    operator_token = Path(config_path).parent / OPERATOR_TOKEN_NAME
    if operator_token.is_file():
        text = operator_token.read_text().strip()
        if text:
            log.debug("token source: operator-token (%s)", operator_token)
            return text
    env = os.environ.get("COMPUTEMCP_TOKEN")
    if env:
        log.debug("token source: environment (COMPUTEMCP_TOKEN)")
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
                log.debug("token source: tokens-file (%s)", path)
                return str(value)
    raise SystemExit(
        "no token available: run --bootstrap (writes an operator token next to "
        f"the config), set COMPUTEMCP_TOKEN, pass --token, or keep a plaintext "
        "token in the tokens file (hashes cannot be used by the CLI)"
    )


def _resolve_timeout(
    args: argparse.Namespace,
    config: GatewayConfig,
    target_names: list[str],
) -> float:
    """Pick the HTTP client timeout for a connect/refresh request.

    Precedence: the subcommand ``--timeout`` wins, then the global ``--timeout``,
    then the longest ``provision_timeout`` among the named known targets plus a
    handshake margin, and finally :data:`DEFAULT_TIMEOUT`.
    """
    action_timeout = getattr(args, "action_timeout", None)
    if action_timeout is not None:
        return action_timeout
    if getattr(args, "timeout", None) is not None:
        return args.timeout
    provisions = [
        config.targets[name].provision_timeout
        for name in target_names
        if name in config.targets and config.targets[name].provision_timeout > 0
    ]
    if provisions:
        return max(provisions) + PROVISION_HANDSHAKE_MARGIN
    return DEFAULT_TIMEOUT


def _parse_overrides(entries: list[str] | None) -> dict | None:
    """Parse repeated ``--set key=value`` entries into an override dict.

    Malformed entries (no ``=``, empty key) are rejected here; unknown keys and
    invalid values are rejected by the allocation layer on the gateway, so the
    CLI does not duplicate that vocabulary.  Returns ``None`` when no entry was
    given, keeping the request body byte-for-byte as before.
    """
    if not entries:
        return None
    overrides: dict = {}
    for entry in entries:
        key, sep, value = entry.partition("=")
        key = key.strip()
        if not sep or not key:
            raise SystemExit(
                f"malformed --set {entry!r}: expected KEY=VALUE "
                "(e.g. --set gpus-per-node=2)"
            )
        # A shell/argparse value is always a string; numeric-looking values are
        # handed to the gateway as integers so the allocation layer's type
        # checks accept them.  Memory quantities (e.g. 100G) and mode names stay
        # strings.  Unknown keys still fail in the allocation layer.
        overrides[key] = int(value) if value.isdigit() else value
    return overrides


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="computeMCP-gatewayctl")
    parser.add_argument(
        "--config",
        default=str(default_config_path()),
        help="gateway config.toml (default: "
        "~/.config/computeMCP-gateway/config.toml)",
    )
    parser.add_argument("--token-file", help="override [auth] token_file")
    parser.add_argument("--gateway", help="gateway base URL (default from config/env)")
    parser.add_argument("--token", help="admin/operator bearer token")
    parser.add_argument("--client", default="admin",
                        help="client id whose token to use (default: admin)")
    parser.add_argument("--json", action="store_true", help="print raw JSON")
    parser.add_argument("--timeout", type=float, default=None)
    parser.add_argument(
        "--add-target",
        action="store_true",
        help="interactively append a [targets.X] block to the config and exit "
        "(no running gateway required)",
    )
    sub = parser.add_subparsers(dest="command", required=False)

    sub.add_parser("status", help="show targets and their state")
    sub.add_parser("targets", help="list target names")
    sub.add_parser("clients", help="list clients, ACLs and live sessions")
    sub.add_parser("sessions", help="list live sessions (all clients)")
    sub.add_parser("reload", help="reload the gateway configuration")
    sub.add_parser("enrollments", help="list pending enrollment requests (admin)")

    p = sub.add_parser("approve", help="approve an enrollment request (admin)")
    p.add_argument("request_id")
    p = sub.add_parser("deny", help="deny an enrollment request (admin)")
    p.add_argument("request_id")

    p = sub.add_parser("client", help="show one client")
    p.add_argument("name")

    for name, help_ in (
        ("target-connect", "connect a target"),
        ("target-refresh", "refresh a target"),
    ):
        p = sub.add_parser(name, help=help_)
        p.add_argument("target")
        p.add_argument("--2fa", dest="factor", metavar="SECRET",
                       help="second factor (password/OTP) for interactive_auth "
                       "targets")
        p.add_argument(
            "--set", dest="overrides", action="append", default=None,
            metavar="KEY=VALUE",
            help="allocation override (repeatable), e.g. --set gpus-per-node=2",
        )
        p.add_argument("--timeout", dest="action_timeout", type=float,
                       default=argparse.SUPPRESS, metavar="SECONDS",
                       help="override the request timeout; default is the "
                       "target's provision_timeout plus a margin")
        p.add_argument(
            "--dry-run", action="store_true",
            help="preview the allocation without connecting (no allocation)",
        )
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

    async def request(
        self,
        method: str,
        path: str,
        json_body: dict | None = None,
        timeout: float | None = None,
    ) -> dict:
        assert self._session is not None
        effective_timeout = self.timeout if timeout is None else timeout
        async with self._session.request(
            method, f"{self.base}{path}",
            json=json_body,
            timeout=aiohttp.ClientTimeout(total=effective_timeout),
        ) as response:
            text = await response.text()
            if response.status >= 400:
                raise SystemExit(f"gateway error {response.status}: {text[:500]}")
            return json.loads(text) if text else {}


async def _run(args: argparse.Namespace) -> int:
    config = load_config(args.config, token_file=args.token_file)
    base = _resolve_gateway(config, args.gateway)
    token = _resolve_token(args.config, args.token_file, args.client, args.token)

    default_timeout = (
        args.timeout if args.timeout is not None else DEFAULT_TIMEOUT
    )
    async with Control(base, token, default_timeout) as control:
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
        elif cmd == "enrollments":
            body = await control.request("GET", "/v1/enroll-requests")
            _emit(args, body, _render_enrollments)
        elif cmd == "approve":
            body = await control.request(
                "POST", f"/v1/enroll-requests/{args.request_id}/approve"
            )
            _emit(
                args,
                body,
                lambda b: f"approved {b['request_id']}: client {b['client_id']}",
            )
        elif cmd == "deny":
            body = await control.request(
                "POST", f"/v1/enroll-requests/{args.request_id}/deny"
            )
            _emit(args, body, lambda b: f"denied {b['request_id']}")
        elif cmd == "target-connect":
            overrides = _parse_overrides(args.overrides)
            if args.dry_run:
                payload = _target_body(args.factor, overrides)
                body = await control.request(
                    "POST", f"/v1/targets/{args.target}/preview",
                    json_body=payload,
                    timeout=_resolve_timeout(args, config, [args.target]),
                )
                _emit_preview(args, body)
            else:
                body = await control.request(
                    "POST", f"/v1/targets/{args.target}/connect",
                    json_body=_target_body(args.factor, overrides),
                    timeout=_resolve_timeout(args, config, [args.target]),
                )
                _emit_target(args, body)
        elif cmd == "target-refresh":
            overrides = _parse_overrides(args.overrides)
            if getattr(args, "dry_run", False):
                payload = _target_body(args.factor, overrides)
                body = await control.request(
                    "POST", f"/v1/targets/{args.target}/preview",
                    json_body=payload,
                    timeout=_resolve_timeout(args, config, [args.target]),
                )
                _emit_preview(args, body)
            else:
                body = await control.request(
                    "POST", f"/v1/targets/{args.target}/refresh",
                    json_body=_target_body(args.factor, overrides),
                    timeout=_resolve_timeout(args, config, [args.target]),
                )
                _emit_target(args, body)
        elif cmd == "target-stop":
            body = await control.request("POST", f"/v1/targets/{args.target}/stop")
            _emit(args, body, lambda b: f"{b['name']} -> {b['state']}")
        elif cmd.startswith("client-"):
            await _client_action(args, control, config, cmd[len("client-"):])
        else:
            raise SystemExit(f"unknown command: {cmd}")
    return 0


async def _client_action(
    args, control: Control, config: GatewayConfig, action: str
) -> None:
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
    request_timeout = (
        _resolve_timeout(args, config, targets)
        if action in ("connect", "refresh")
        else None
    )
    for target in targets:
        path = f"/v1/targets/{target}/{action}"
        result = await control.request("POST", path, timeout=request_timeout)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print(f"{name}: {target} -> {result['state']} ({result.get('active_route')})")


def _emit(args, body: dict, render) -> None:
    if args.json:
        print(json.dumps(body, indent=2))
    else:
        print(render(body))


def _target_body(factor: str | None, overrides: dict | None) -> dict | None:
    """Build the optional connect/refresh/preview request body.

    Returns ``None`` when neither a factor nor overrides are present, preserving
    the exact pre-existing request shape (no body).  The factor is never echoed.
    """
    body: dict = {}
    if factor is not None:
        body["factor"] = factor
    if overrides is not None:
        body["set"] = overrides
    return body or None


def _emit_target(args, body: dict) -> None:
    """Print a target state line plus any gateway warning (to stderr)."""
    if args.json:
        print(json.dumps(body, indent=2))
        return
    print(f"{body['name']} -> {body['state']} ({body.get('active_route')})")
    warning = body.get("warning")
    if warning:
        print(f"warning: {warning}", file=sys.stderr)


def _emit_preview(args, body: dict) -> None:
    """Print a stable, labeled dry-run block.

    Labels sit on their own lines so the output is easy to scan (and easy to
    parse with ``grep``/``cut``).  Map-like sections are indented consistently.
    """
    if args.json:
        print(json.dumps(body, indent=2))
        return
    planned = body.get("planned") or {}
    plan = planned.get("plan") or {}
    provision_env = body.get("provision_env") or {}

    print(f"target: {body.get('target')}")
    print(f"connected: {body.get('connected')}")
    if body.get("needs_refresh"):
        print("needs_refresh: true")

    print("planned:")
    print(f"  mode: {plan.get('mode')}")
    print(f"  nodes: {plan.get('nodes')}")
    print(f"  cpus_per_node: {plan.get('cpus_per_node')}")
    print(f"  gpus_per_node: {plan.get('gpus_per_node')}")
    print(f"  memory_per_node_mib: {plan.get('memory_per_node_mib')}")
    print(f"  exclusive: {plan.get('exclusive')}")
    print(f"  defaults_used: {planned.get('defaults_used')}")

    print("manual:")
    for stage in ("sbatch", "srun"):
        stage_options = (planned.get("manual") or {}).get(stage) or {}
        print(f"  {stage}: {json.dumps(stage_options, sort_keys=True)}")

    print("args:")
    print(f"  sbatch: {json.dumps(body.get('sbatch_args') or [])}")
    print(f"  srun: {json.dumps(body.get('srun_args') or [])}")

    print("not_emitted:")
    for field in (body.get("would_emit") or {}).get("not_emitted") or []:
        print(f"  {field}")

    print("COMPUTEMCP_SBATCH_ARGS:")
    print(provision_env.get("COMPUTEMCP_SBATCH_ARGS", ""))
    print("COMPUTEMCP_SRUN_ARGS:")
    print(provision_env.get("COMPUTEMCP_SRUN_ARGS", ""))


def _emit_status(args, body: dict) -> None:
    if args.json:
        print(json.dumps(body, indent=2))
        return
    print(
        f"{'TARGET':<18}{'STATE':<14}{'ROUTE':<14}{'LOCAL':<8}"
        f"{'SHARING':<11}{'NODE_INFO':<42}{'AGENT':<30}CLIENTS"
    )
    for t in body["targets"]:
        node_info = ", ".join(t.get("node_info") or []) or "-"
        if len(node_info) > 40:
            node_info = node_info[:39] + "\u2026"
        agent = ", ".join(
            f"{a.get('agent', '?')}@{a.get('model', '?')}"
            for a in (t.get("agent") or [])
        )
        if len(agent) > 28:
            agent = agent[:27] + "\u2026"
        print(
            f"{t['name']:<18}{t['state']:<14}{t.get('active_route') or '-':<14}"
            f"{t.get('local_port') or '-':<8}{t.get('sharing', 'unknown'):<11}"
            f"{node_info:<42}{agent or '-':<30}{t.get('clients', 0)}"
        )


def _render_enrollments(body: dict) -> str:
    pending = body["pending"]
    if not pending:
        return "no pending enrollment requests"
    lines = [f"{'REQUEST':<14}{'CLIENT':<22}{'TARGETS':<28}SOURCE"]
    for r in pending:
        lines.append(
            f"{r['request_id']:<14}{r['client_id']:<22}"
            f"{','.join(r['targets']) or '-':<28}{r.get('source') or '-'}"
        )
    return "\n".join(lines)


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
    if args.add_target:
        # Setup wizard: no gateway connection, no token needed.
        from .setup import Wizard, WizardAbort, run_add_target

        try:
            return run_add_target(
                args.config, wizard=Wizard(terminal=None)
            )
        except WizardAbort as exc:
            print(f"add-target: {exc}", file=sys.stderr)
            return 2
    if not getattr(args, "command", None):
        build_parser().error("a command is required (or use --add-target)")
    import asyncio

    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())

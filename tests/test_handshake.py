# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import json
from pathlib import Path

from compute_mcp.handshake import (
    BEGIN_MARK,
    END_MARK,
    _mcp_snippet,
    _parse_system,
    _render_env_file,
    _update_bashrc,
)


def test_parse_system_variants():
    assert _parse_system(None) == []
    assert _parse_system("") == []
    assert _parse_system("hal") == ["hal"]
    assert _parse_system("hal,fwk394") == ["hal", "fwk394"]
    assert _parse_system(" hal , fwk394 ") == ["hal", "fwk394"]


def test_update_bashrc_is_idempotent(tmp_path: Path):
    rc = tmp_path / ".bashrc"
    rc.write_text("export KEEP=1\n")
    _update_bashrc(rc, source_file=None, url="http://gw:2223", token="tok1")
    _update_bashrc(rc, source_file=None, url="http://gw:2223", token="tok2")
    text = rc.read_text()
    assert text.count(BEGIN_MARK) == 1
    assert text.count(END_MARK) == 1
    assert "export KEEP=1" in text
    assert "tok2" in text and "tok1" not in text


def test_update_bashrc_source_file(tmp_path: Path):
    rc = tmp_path / ".bashrc"
    env = tmp_path / "env"
    env.write_text(_render_env_file("http://gw:2223", "tok"))
    _update_bashrc(rc, source_file=env, url="http://gw:2223", token="tok")
    text = rc.read_text()
    assert f'. "{env}"' in text
    assert "export COMPUTEMCP_TOKEN" not in text


def test_mcp_snippet_is_valid_json_fragment():
    snippet = "{" + _mcp_snippet("http://gw:2223", "tok") + "}"
    parsed = json.loads(snippet)
    assert parsed["compute"]["environment"]["COMPUTEMCP_TOKEN"] == "tok"


async def test_handshake_end_to_end(tmp_path):
    """Drive request -> approve -> poll -> .bashrc against a live gateway."""
    from aiohttp import web

    from compute_mcp.config import load_config
    from compute_mcp.gateway import Gateway
    from compute_mcp.handshake import (
        _poll,
        _request_enrollment,
        _update_bashrc,
    )

    cfg = tmp_path / "config.toml"
    cfg.write_text(
        """
        [server]
        allow_enrollment = true

        [clients.admin]
        token = "admin-token"
        targets = ["*"]

        [targets.hal]
        transport = "direct"
        remote_host = "127.0.0.1"
        remote_port = 9
        user = "agent"
        host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
        """
    )
    gw = Gateway(load_config(cfg))
    app = gw.create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    try:
        created = await _request_enrollment(base, "newci", ["hal"], "CI", 5.0)
        rid, secret = created["request_id"], created["poll_secret"]

        # Approve directly through the gateway API as admin.
        import aiohttp

        async with aiohttp.ClientSession(
            headers={"Authorization": "Bearer admin-token"}
        ) as s:
            async with s.post(f"{base}/v1/enroll-requests/{rid}/approve") as r:
                assert r.status == 200

        result = await _poll(base, rid, secret, timeout=5.0, interval=0.05)
        assert result["status"] == "approved"
        assert result["token"]

        rc = tmp_path / ".bashrc"
        _update_bashrc(rc, source_file=None, url=base, token=result["token"])
        assert "COMPUTEMCP_TOKEN" in rc.read_text()

        # The delivered token authenticates.
        async with aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {result['token']}"}
        ) as s:
            async with s.get(f"{base}/v1/targets") as r:
                assert r.status == 200
    finally:
        await runner.cleanup()

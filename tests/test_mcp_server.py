# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import json
import os

import pytest

from compute_mcp.files import parse_mode
from compute_mcp.ssh_backend import SSHError


def test_parse_mode_octal_strings():
    assert parse_mode("644") == 0o644
    assert parse_mode("755") == 0o755
    assert parse_mode("0o644") == 0o644
    assert parse_mode("0755") == 0o755


def test_parse_mode_int_passthrough():
    assert parse_mode(0o600) == 0o600


def test_parse_mode_invalid():
    with pytest.raises(SSHError):
        parse_mode("99z")
    with pytest.raises(SSHError):
        parse_mode("99999")


def test_matches_globs():
    from compute_mcp.mcp_server import _matches

    assert _matches("src/a.c", [], []) is True
    assert _matches("src/a.c", ["src/*"], []) is True
    assert _matches("src/a.o", ["src/*"], ["*.o"]) is False
    assert _matches("build/x.o", [], ["build/*"]) is False


class FakeGatewayClient:
    """In-memory stand-in for the gateway used by the tree helpers."""

    def __init__(self):
        self.dirs = {"/remote"}
        self.files = {}
        self.uploaded = []

    async def request(self, method, path, params=None, json=None):
        if path == "/v1/files/stat":
            p = params["path"]
            if p in self.dirs:
                return {"path": p, "type": "directory"}
            if p in self.files:
                return {"path": p, "type": "file", "size": len(self.files[p])}
            raise RuntimeError("not found")
        if path == "/v1/files/mkdir":
            self.dirs.add(json["path"])
            return {"path": json["path"], "created": True}
        if path == "/v1/files/list":
            p = params["path"]
            entries = []
            for d in self.dirs:
                if os.path.dirname(d) == p and d != p:
                    entries.append({"name": os.path.basename(d), "path": d, "type": "directory"})
            for f in self.files:
                if os.path.dirname(f) == p:
                    entries.append({"name": os.path.basename(f), "path": f, "type": "file"})
            return {"path": p, "entries": entries}
        raise RuntimeError(f"unexpected {path}")

    async def upload(
        self, target, remote_path, data, length,
        append=False, parents=False, job_id=None,
    ):
        payload = data.read()
        self.files[remote_path] = payload
        self.uploaded.append((remote_path, payload))
        return {"path": remote_path, "written": len(payload)}

    async def download(self, target, remote_path, sink, job_id=None):
        sink.write(self.files[remote_path])
        return {"path": remote_path, "written": len(self.files[remote_path])}


async def test_upload_tree_mirrors_and_filters(tmp_path):
    from compute_mcp.mcp_server import _upload_tree

    root = tmp_path / "src"
    (root / "sub").mkdir(parents=True)
    (root / "a.txt").write_text("A")
    (root / "sub" / "b.txt").write_text("B")
    (root / "sub" / "c.o").write_text("C")

    client = FakeGatewayClient()
    report = await _upload_tree(
        client, "hal", str(root), "/remote/dst",
        append=False, parents=True, overwrite=True, skip_existing=False,
        max_files=0, include=[], exclude=["*.o"],
    )
    assert report["uploaded"] == 2
    assert report["skipped"] == 1
    assert report["failed"] == []
    assert client.files["/remote/dst/a.txt"] == b"A"
    assert client.files["/remote/dst/sub/b.txt"] == b"B"
    assert not any(p.endswith("c.o") for p in client.files)


async def test_upload_tree_skip_existing(tmp_path):
    from compute_mcp.mcp_server import _upload_tree

    root = tmp_path / "src"
    root.mkdir()
    (root / "a.txt").write_text("A")
    (root / "b.txt").write_text("B")

    client = FakeGatewayClient()
    client.files["/remote/dst/a.txt"] = b"old"
    report = await _upload_tree(
        client, "hal", str(root), "/remote/dst",
        append=False, parents=True, overwrite=True, skip_existing=True,
        max_files=0, include=[], exclude=[],
    )
    assert report["uploaded"] == 1 and report["skipped"] == 1
    assert client.files["/remote/dst/a.txt"] == b"old"  # untouched
    assert client.files["/remote/dst/b.txt"] == b"B"


async def test_download_tree_recursive(tmp_path):
    from compute_mcp.mcp_server import _download_tree

    client = FakeGatewayClient()
    client.dirs.update({"/remote/dst", "/remote/dst/sub"})
    client.files["/remote/dst/a.txt"] = b"A"
    client.files["/remote/dst/sub/b.bin"] = b"\x00\x01\x02"

    out = tmp_path / "out"
    report = await _download_tree(client, "hal", "/remote/dst", str(out))
    assert report["downloaded"] == 2
    assert (out / "a.txt").read_bytes() == b"A"
    assert (out / "sub" / "b.bin").read_bytes() == b"\x00\x01\x02"


async def test_target_status_tool_docs_cover_container_login_user_and_endpoint():
    """The target/status descriptions document the container fields.

    Agents learn the tool surface from the docstrings, so they must explain the
    in-container login account (`container_user`) and the resolved
    `provisioned_endpoint`; this locks the added documentation in.
    """
    from compute_mcp.mcp_server import build_server

    server = build_server(None)
    tools = {tool.name: tool for tool in await server.list_tools()}

    for name in ("computeMCP_targets", "computeMCP_status"):
        description = tools[name].description or ""
        assert "container_user" in description, name
        assert "provisioned_endpoint" in description, name
        assert "container" in description, name


# -- public job tools and additive job_id forwarding ------------------------

class RecordingClient:
    """Gateway stand-in that records every request and returns canned data."""

    def __init__(self, response=None):
        self.response = response if response is not None else {"ok": True}
        self.calls = []

    async def request(self, method, path, *, json=None, params=None, timeout=None):
        self.calls.append((method, path, json, params, timeout))
        return self.response

    async def job_request(self, method, path, *, json=None, params=None, timeout=None):
        return await self.request(
            method, path, json=json, params=params, timeout=timeout
        )


def _content(result):
    return json.loads(result.content[0].text)


async def test_job_tools_registered_with_schemas():
    from compute_mcp.mcp_server import build_server

    server = build_server(None)
    tools = {tool.name: tool for tool in await server.list_tools()}
    for name in (
        "computeMCP_job_start",
        "computeMCP_job_list",
        "computeMCP_job_status",
        "computeMCP_job_wait",
        "computeMCP_job_stop",
    ):
        assert name in tools, name
    start = tools["computeMCP_job_start"].input_schema["properties"]
    assert "target" in start
    assert start["kind"]["default"] == "private"
    wait = tools["computeMCP_job_wait"].input_schema["properties"]
    assert "job_id" in wait
    assert wait["timeout"]["default"] == 30.0


async def test_job_id_is_additive_on_existing_tools():
    from compute_mcp.mcp_server import build_server

    tools = {tool.name: tool for tool in await build_server(None).list_tools()}
    for name in (
        "computeMCP_exec",
        "computeMCP_session_create",
        "computeMCP_session_write",
        "computeMCP_session_read",
        "computeMCP_session_resize",
        "computeMCP_session_close",
        "computeMCP_sessions",
        "computeMCP_file_read",
        "computeMCP_file_read_base64",
        "computeMCP_file_write",
        "computeMCP_file_list",
        "computeMCP_file_stat",
        "computeMCP_file_mkdir",
        "computeMCP_file_remove",
        "computeMCP_file_rename",
        "computeMCP_file_chmod",
        "computeMCP_file_upload",
        "computeMCP_file_upload_tree",
        "computeMCP_file_download",
    ):
        assert "job_id" in tools[name].input_schema["properties"], name


async def test_job_start_shared_reports_route_and_job():
    from compute_mcp.mcp_server import build_server

    client = RecordingClient({
        "name": "hal",
        "state": "connected",
        "active_route": "hal",
        "job": {"job_id": "S1", "state": "pending"},
        "job_id": "S1",
    })
    server = build_server(client)
    result = await server.call_tool(
        "computeMCP_job_start", {"target": "hal", "kind": "shared"}
    )
    body = _content(result)
    assert body["route"] == "open"
    assert body["route_state"] == "connected"
    assert body["job_state"] == "pending"
    assert body["job"]["job_id"] == "S1"
    assert client.calls[0][:2] == ("POST", "/v1/targets/hal/shared")


async def test_job_start_private_forwards_request_fields():
    from compute_mcp.mcp_server import build_server

    client = RecordingClient({"job_id": "J7", "state": "pending"})
    server = build_server(client)
    result = await server.call_tool(
        "computeMCP_job_start",
        {
            "target": "hal",
            "kind": "private",
            "overrides": {"gpus-per-node": 1},
            "root_mode": "clean",
            "clean_home": "/benchmarks/run-42",
            "request_id": "r1",
        },
    )
    assert _content(result)["job_id"] == "J7"
    method, path, body, _, _ = client.calls[0]
    assert (method, path) == ("POST", "/v1/targets/hal/sbatch")
    assert body == {
        "set": {"gpus-per-node": 1},
        "root_mode": "clean",
        "clean_home": "/benchmarks/run-42",
        "request_id": "r1",
    }


async def test_job_start_rejects_unknown_kind():
    from compute_mcp.mcp_server import build_server

    server = build_server(RecordingClient())
    with pytest.raises(Exception):
        await server.call_tool(
            "computeMCP_job_start", {"target": "hal", "kind": "batch"}
        )


async def test_job_list_and_status_forward_target_and_job_id():
    from compute_mcp.mcp_server import build_server

    client = RecordingClient({"jobs": []})
    server = build_server(client)
    await server.call_tool("computeMCP_job_list", {"target": "hal"})
    assert client.calls[-1][:4] == (
        "GET", "/v1/jobs", None, {"target": "hal"},
    )
    client.response = {"job_id": "J7", "state": "ready"}
    result = await server.call_tool(
        "computeMCP_job_status", {"job_id": "J7"}
    )
    assert _content(result)["job_id"] == "J7"
    assert client.calls[-1][:4] == ("GET", "/v1/jobs/J7", None, None)


async def test_job_wait_sets_wait_params_and_client_timeout():
    from compute_mcp.mcp_server import build_server

    client = RecordingClient({"job_id": "J7", "state": "pending"})
    server = build_server(client)
    await server.call_tool(
        "computeMCP_job_wait", {"job_id": "J7", "timeout": 5}
    )
    method, path, _, params, timeout = client.calls[-1]
    assert (method, path) == ("GET", "/v1/jobs/J7")
    assert params == {"wait": "true", "timeout": 5}
    assert timeout == 20.0


async def test_job_stop_uses_cancel_endpoint():
    from compute_mcp.mcp_server import build_server

    client = RecordingClient({"job_id": "J7", "state": "terminal"})
    server = build_server(client)
    await server.call_tool("computeMCP_job_stop", {"job_id": "J7"})
    assert client.calls[-1][:2] == ("POST", "/v1/jobs/J7/cancel")


async def test_exec_forwards_job_id():
    from compute_mcp.mcp_server import build_server

    client = RecordingClient({"exit_status": 0})
    server = build_server(client)
    await server.call_tool(
        "computeMCP_exec",
        {"target": "hal", "command": "true", "job_id": "J7"},
    )
    body = client.calls[-1][2]
    assert body["job_id"] == "J7"


async def test_session_create_and_file_tools_forward_job_id():
    from compute_mcp.mcp_server import build_server

    client = RecordingClient({"created": True})
    server = build_server(client)
    await server.call_tool(
        "computeMCP_session_create", {"target": "hal", "job_id": "J7"}
    )
    assert client.calls[-1][2]["job_id"] == "J7"

    client.response = {"entries": []}
    await server.call_tool(
        "computeMCP_file_list",
        {"target": "hal", "path": "/tmp", "job_id": "J7"},
    )
    assert client.calls[-1][3]["job_id"] == "J7"


async def test_gateway_job_request_normalizes_404_only():
    from compute_mcp.mcp_server import GatewayClient, JOB_NOT_FOUND_TEXT

    class NotFound(GatewayClient):
        async def request(self, *a, **k):
            raise RuntimeError("gateway GET /v1/jobs/x failed with 404: job not found")

    class Quota(GatewayClient):
        async def request(self, *a, **k):
            raise RuntimeError("gateway POST /v1/x failed with 429: quota")

    with pytest.raises(RuntimeError) as excinfo:
        await NotFound("http://x", "t").job_request("GET", "/v1/jobs/x")
    assert str(excinfo.value) == JOB_NOT_FOUND_TEXT

    with pytest.raises(RuntimeError) as excinfo:
        await Quota("http://x", "t").job_request("POST", "/v1/x")
    assert "quota" in str(excinfo.value)


async def test_upload_tree_forwards_job_id(tmp_path):
    from compute_mcp.mcp_server import _upload_tree

    root = tmp_path / "src"
    root.mkdir()
    (root / "a.txt").write_text("A")

    seen = []

    class Client(FakeGatewayClient):
        async def request(self, method, path, params=None, json=None):
            seen.append(("request", params))
            return await super().request(
                method, path, params=params, json=json
            )

        async def upload(self, target, remote_path, data, length,
                         append=False, parents=False, job_id=None):
            seen.append(("upload", job_id))
            return await super().upload(
                target, remote_path, data, length,
                append=append, parents=parents, job_id=job_id,
            )

    report = await _upload_tree(
        Client(), "hal", str(root), "/remote/dst",
        append=False, parents=True, overwrite=True, skip_existing=False,
        max_files=0, include=[], exclude=[], job_id="J7",
    )
    assert report["uploaded"] == 1
    assert ("upload", "J7") in seen
    assert any(
        params and params.get("job_id") == "J7"
        for kind, params in seen if kind == "request"
    )

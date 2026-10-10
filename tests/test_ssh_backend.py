# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import asyncio

import pytest

import asyncssh

from compute_mcp.ssh_backend import InteractiveSSHClient


class FakeKey:
    def get_fingerprint(self, _alg):
        return "SHA256:abc123"


async def test_no_prompter_returns_none_for_password():
    client = InteractiveSSHClient(pin=None, prompter=None)
    assert await client.password_auth_requested() is None
    assert await client.kbdint_challenge_received("n", "", "", [("Password: ", False)]) is None


async def test_kbdint_advertised_even_without_prompter():
    client = InteractiveSSHClient(pin=None, prompter=None)
    # Empty string advertises keyboard-interactive but supplies no secret itself.
    assert await client.kbdint_auth_requested() == ""


async def test_prompter_supplies_password():
    seen = []

    async def prompter(prompt, echo):
        seen.append((prompt, echo))
        return "s3cret"

    client = InteractiveSSHClient(pin=None, prompter=prompter)
    assert await client.password_auth_requested() == "s3cret"
    assert seen == [("Password: ", False)]


async def test_prompter_answers_otp_challenge_in_order():
    answers = iter(["otp-code"])

    async def prompter(prompt, echo):
        return next(answers)

    client = InteractiveSSHClient(pin=None, prompter=prompter)
    result = await client.kbdint_challenge_received(
        "OTP", "Enter your one-time code", "", [("Verification code: ", True)]
    )
    assert result == ["otp-code"]


async def test_prompter_empty_answer_cancels():
    async def prompter(prompt, echo):
        return None

    client = InteractiveSSHClient(pin=None, prompter=prompter)
    assert await client.password_auth_requested() is None


async def test_prompter_multiple_prompts():
    responses = iter(["user", "pin"])

    async def prompter(prompt, echo):
        return next(responses)

    client = InteractiveSSHClient(pin=None, prompter=prompter)
    result = await client.kbdint_challenge_received(
        "two", "", "", [("User: ", True), ("PIN: ", False)]
    )
    assert result == ["user", "pin"]


def test_host_key_pin_still_enforced():
    client = InteractiveSSHClient(pin="SHA256:abc123", prompter=None)
    assert client.validate_host_public_key("h", "1.2.3.4", 22, FakeKey()) is True
    assert client.validate_host_public_key("h", "1.2.3.4", 22, FakeKey()) is True
    bad = InteractiveSSHClient(pin="SHA256:zzz", prompter=None)
    assert bad.validate_host_public_key("h", "1.2.3.4", 22, FakeKey()) is False


def test_no_pin_means_no_host_key_accepted():
    client = InteractiveSSHClient(pin=None, prompter=None)
    assert client.validate_host_public_key("h", "1.2.3.4", 22, FakeKey()) is False


def test_accept_any_mode_accepts_any_host_key():
    client = InteractiveSSHClient(pin=None, prompter=None, accept_any=True)
    assert client.validate_host_public_key("h", "1.2.3.4", 22, FakeKey()) is True
    # A pin that would not match is also accepted when verification is off.
    client = InteractiveSSHClient(
        pin="SHA256:does-not-match", prompter=None, accept_any=True
    )
    assert client.validate_host_public_key("h", "1.2.3.4", 22, FakeKey()) is True


def test_sh_identifier_rejects_injection():
    from compute_mcp.ssh_backend import _sh_identifier, SSHError

    assert _sh_identifier("PATH") == "PATH"
    assert _sh_identifier("_X1") == "_X1"
    for bad in ["A B", "A;B", "A$(x)", "1ABC", "", "A-B", "A\nB"]:
        try:
            _sh_identifier(bad)
        except SSHError:
            continue
        raise AssertionError(f"accepted unsafe name {bad!r}")


def test_shquote_escapes():
    from compute_mcp.ssh_backend import _shquote

    assert _shquote("abc") == "'abc'"
    assert _shquote("a'b") == "'a'\\''b'"


# -- route dial: known_hosts selection and prompter threading ---------------

class _FakeSSHClient:
    def is_closed(self):
        return False


async def _capture_dial_route(monkeypatch, **kwargs):
    seen = {}

    async def fake_connect(host, **connect_kwargs):
        seen["host"] = host
        seen.update(connect_kwargs)
        return _FakeSSHClient()

    monkeypatch.setattr(
        "compute_mcp.ssh_backend.asyncssh.connect", fake_connect
    )
    from compute_mcp.ssh_backend import dial_route

    defaults = dict(
        name="route",
        host="127.0.0.1",
        port=22,
        username="agent",
        client_keys=None,
        passphrase=None,
        prompter=None,
        host_key_sha256=None,
        known_hosts=None,
        host_key_algorithms=(),
        host_key_check="on",
    )
    defaults.update(kwargs)
    await dial_route(**defaults)
    return seen


async def test_dial_route_pin_uses_empty_trusted_set(monkeypatch):
    from compute_mcp.ssh_backend import _empty_known_hosts

    async def prompter(prompt, echo):
        return "SECRET"

    seen = await _capture_dial_route(
        monkeypatch,
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
        prompter=prompter,
        passphrase="PASSPHRASE",
    )
    # A pin must not silently fall back to ~/.ssh/known_hosts: an empty trusted
    # set is passed so asyncssh still drives the client validation hook.
    assert seen["known_hosts"] is _empty_known_hosts
    assert seen["passphrase"] == "PASSPHRASE"
    # The prompter is threaded into the client factory, not dropped.
    client = seen["client_factory"]()
    assert await client.password_auth_requested() == "SECRET"


async def test_dial_route_without_pin_defers_known_hosts(monkeypatch):
    seen = await _capture_dial_route(monkeypatch, host_key_sha256=None)
    # No pin and no explicit known_hosts -> asyncssh default (None).
    assert seen["known_hosts"] is None


async def test_dial_route_explicit_known_hosts_wins(monkeypatch):
    sentinel = lambda *a: ((), (), (), (), (), (), ())
    seen = await _capture_dial_route(
        monkeypatch,
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
        known_hosts=sentinel,
    )
    assert seen["known_hosts"] is sentinel


async def test_dial_route_accept_any_uses_empty_trusted_set(monkeypatch):
    from compute_mcp.ssh_backend import _empty_known_hosts

    seen = await _capture_dial_route(
        monkeypatch, host_key_check="off", host_key_sha256=None
    )
    assert seen["known_hosts"] is _empty_known_hosts


# -- make_factor_prompter ----------------------------------------------------

async def test_factor_prompter_answers_password_and_kbdint():
    from compute_mcp.ssh_backend import make_factor_prompter

    prompter = make_factor_prompter("SECRET")
    assert await prompter("Password: ", False) == "SECRET"
    assert await prompter("OTP code: ", True) == "SECRET"


async def test_factor_prompter_caps_at_three():
    from compute_mcp.ssh_backend import make_factor_prompter

    prompter = make_factor_prompter("SECRET")
    answers = [await prompter("p", False) for _ in range(5)]
    assert answers[:3] == ["SECRET", "SECRET", "SECRET"]
    assert answers[3:] == [None, None]


async def test_factor_prompter_never_leaks_secret_in_repr(caplog):
    from compute_mcp.ssh_backend import make_factor_prompter

    prompter = make_factor_prompter("TOP-SECRET")
    with caplog.at_level("DEBUG"):
        for _ in range(4):
            await prompter("Password: ", False)
    assert "TOP-SECRET" not in repr(prompter)
    assert "TOP-SECRET" not in caplog.text


async def test_empty_route_user_uses_asyncssh_sentinel(monkeypatch):
    """An empty user must be passed as (), not None.

    asyncssh's "unset" sentinel is ``()`` (fall back to the SSH config alias /
    local account); passing ``None`` raises TypeError inside saslprep.
    """
    from compute_mcp.ssh_backend import dial_route

    seen = {}

    async def fake_connect(host, **connect_kwargs):
        seen.update(connect_kwargs)
        return _FakeSSHClient()

    monkeypatch.setattr("compute_mcp.ssh_backend.asyncssh.connect", fake_connect)
    await dial_route(
        name="route",
        host="127.0.0.1",
        port=22,
        username=(),
        client_keys=None,
        passphrase=None,
        prompter=None,
        host_key_sha256=None,
        known_hosts=None,
        host_key_algorithms=(),
        host_key_check="on",
    )
    assert seen["username"] == ()
    assert seen["username"] is not None


# -- container hop uses the container login user, not the route user ---------

def _container_target(**overrides):
    from compute_mcp.config import TargetConfig, TransportConfig

    kwargs = dict(
        name="hal",
        user="rwidera",
        transport=TransportConfig(
            kind="direct", remote_host="127.0.0.1", remote_port=2222
        ),
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    )
    kwargs.update(overrides)
    return TargetConfig(**kwargs)


async def _capture_container_dial(monkeypatch, target):
    seen = {}

    async def fake_connect(host, **connect_kwargs):
        seen["host"] = host
        seen.update(connect_kwargs)
        return _FakeSSHClient()

    monkeypatch.setattr("compute_mcp.ssh_backend.asyncssh.connect", fake_connect)
    from compute_mcp.ssh_backend import SSHBackend

    await SSHBackend()._dial(target, "127.0.0.1", 2222)
    return seen


async def test_container_dial_defaults_to_ubuntu(monkeypatch):
    """The container hop dials ``ubuntu`` by default, not the route login user.

    This is the bug fix: the route account (``target.user``, here ``rwidera``)
    authenticates the gateway -> login hop, but the container sshd's
    ``AllowUsers`` only accepts the container account.  Dialing ``target.user``
    inside the container failed with ``Permission denied``.
    """
    seen = await _capture_container_dial(monkeypatch, _container_target())
    assert seen["username"] == "ubuntu"
    assert seen["username"] != "rwidera"


async def test_container_dial_uses_container_user_override(monkeypatch):
    """An explicit ``container_user`` is dialed inside the container."""
    seen = await _capture_container_dial(
        monkeypatch, _container_target(container_user="dev")
    )
    assert seen["username"] == "dev"


async def test_container_dial_honors_env_override(monkeypatch):
    """``COMPUTEMCP_SSH_USER`` is honored when ``container_user`` is unset."""
    monkeypatch.setenv("COMPUTEMCP_SSH_USER", "siteagent")
    seen = await _capture_container_dial(monkeypatch, _container_target())
    assert seen["username"] == "siteagent"


# -- in-process container acceptance: the wrong account fails, the container
#    account succeeds.  This stands up a real asyncssh server whose
#    ``validate_public_key`` mirrors the container sshd's ``AllowUsers``.
_container_server_support = hasattr(asyncssh, "listen") and hasattr(
    asyncssh, "SSHServer"
)


class _ContainerServer(asyncssh.SSHServer):
    def __init__(self, allowed_user: str, key) -> None:
        self.allowed_user = allowed_user
        self.key = key

    def public_key_auth_supported(self) -> bool:
        return True

    async def validate_public_key(self, username, key) -> bool:
        # Mirror ``AllowUsers $SSH_USER``: only the container account is allowed.
        return username == self.allowed_user


# -- connection cache: endpoint-keyed invalidation --------------------------
#
# The container's published SSH port can change between container restarts
# (docker/podman re-publish a fresh ephemeral port on `-p/tcp:2222`).  A
# gateway-level refresh re-provisions to the new endpoint; the SSHBackend's
# per-target cache must follow the endpoint, not just the target name, or
# exec/files/sessions keep dialing the stale port (502).

class _FakeSessionConn:
    """Minimal stand-in for an orchestrator-level asyncssh.SSHClientConnection."""

    def __init__(self) -> None:
        self._closed = False

    def is_closed(self):
        return self._closed

    def close(self):
        self._closed = True

    async def wait_closed(self):
        return None


def _tunnel_target_for_cache(name="hal", endpoint=("127.0.0.1", 2222)):
    from compute_mcp.config import TargetConfig, TransportConfig

    host, port = endpoint
    return TargetConfig(
        name=name,
        user="agent",
        transport=TransportConfig(
            kind="direct", remote_host=host, remote_port=port
        ),
    )


async def test_connection_cache_reuses_same_endpoint(monkeypatch):
    from compute_mcp.ssh_backend import SSHBackend

    target = _tunnel_target_for_cache(endpoint=("127.0.0.1", 2222))
    dialed = []
    conns = []

    async def fake_dial(self, tgt, host, port, prompter=None, passphrase=None, username=None):
        dialed.append((host, port))
        conn = _FakeSessionConn()
        conns.append(conn)
        return conn

    monkeypatch.setattr(SSHBackend, "_dial", fake_dial)

    backend = SSHBackend()
    c1 = await backend.connection(target, "127.0.0.1", 2222)
    assert c1 is conns[0]

    # Same endpoint: reuse the cached connection (no redial).
    c1b = await backend.connection(target, "127.0.0.1", 2222)
    assert c1b is c1
    assert dialed == [("127.0.0.1", 2222)]


async def test_connection_cache_invalidated_on_endpoint_change(monkeypatch):
    from compute_mcp.ssh_backend import SSHBackend

    target = _tunnel_target_for_cache(endpoint=("127.0.0.1", 2222))
    dialed = []
    conns = []

    async def fake_dial(self, tgt, host, port, prompter=None, passphrase=None, username=None):
        dialed.append((host, port))
        conn = _FakeSessionConn()
        conns.append(conn)
        return conn

    monkeypatch.setattr(SSHBackend, "_dial", fake_dial)

    backend = SSHBackend()
    c0 = await backend.connection(target, "127.0.0.1", 2222)
    assert c0 is conns[0]

    # Re-provision changed the published port to 3001: MUST NOT reuse.
    c1 = await backend.connection(target, "127.0.0.1", 3001)
    assert c1 is not c0
    assert c1 is conns[1]
    assert dialed == [("127.0.0.1", 2222), ("127.0.0.1", 3001)]

    # Route change (host, port unchanged) also invalidates: the connection
    # was opened toward a specific (host, port) on the old route.
    c2 = await backend.connection(target, "10.0.0.5", 3001)
    assert c2 is not c1
    assert c2 is conns[2]
    assert dialed == [
        ("127.0.0.1", 2222),
        ("127.0.0.1", 3001),
        ("10.0.0.5", 3001),
    ]

    # Restore the second endpoint: the top-of-stack cache is the 10.0.0.5 one,
    # so this still redials (no entry is indexed by a separate key).
    c3 = await backend.connection(target, "127.0.0.1", 3001)
    assert c3 is not c2
    assert c3 is conns[3]
    assert len(dialed) == 4


async def test_disconnect_drops_endpoint_keyed_cache(monkeypatch):
    """Even when the endpoint did not change, an explicit `disconnect` from the
    gateway's refresh path must empty the cache so the redial happens."""
    from compute_mcp.ssh_backend import SSHBackend

    target = _tunnel_target_for_cache(endpoint=("127.0.0.1", 2222))
    dialed = []
    conns = []

    async def fake_dial(self, tgt, host, port, prompter=None, passphrase=None, username=None):
        dialed.append((host, port))
        conn = _FakeSessionConn()
        conns.append(conn)
        return conn

    monkeypatch.setattr(SSHBackend, "_dial", fake_dial)

    backend = SSHBackend()
    c0 = await backend.connection(target, "127.0.0.1", 2222)
    assert c0 is conns[0]

    await backend.disconnect("hal")
    assert backend.known_targets() == []

    # Redial, even for the same endpoint we just disconnected from.
    c1 = await backend.connection(target, "127.0.0.1", 2222)
    assert c1 is conns[1]
    assert c1 is not c0
    assert dialed == [("127.0.0.1", 2222), ("127.0.0.1", 2222)]


@pytest.mark.skipif(
    not _container_server_support, reason="asyncssh server support unavailable"
)
async def test_container_dial_wrong_account_fails_but_container_account_ok(    tmp_path_factory,
):
    import asyncio
    import os

    from compute_mcp.config import TargetConfig, TransportConfig
    from compute_mcp.ssh_backend import SSHBackend, SSHError
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    client_key = asyncssh.generate_private_key("ssh-ed25519")
    tmp = tmp_path_factory.mktemp("container")
    host_path = os.path.join(str(tmp), "host_key")
    client_path = os.path.join(str(tmp), "client_key")
    host_key.write_private_key(host_path)
    client_key.write_private_key(client_path)

    server = await asyncssh.listen(
        "127.0.0.1",
        0,
        server_host_keys=[host_path],
        server_factory=lambda: _ContainerServer("agent", client_key),
        encoding=None,
    )
    port = server.get_port()
    pin = host_key.get_fingerprint("sha256")

    def target(container_user):
        return TargetConfig(
            name="c",
            user="rwidera",
            container_user=container_user,
            transport=TransportConfig(
                kind="direct", remote_host="127.0.0.1", remote_port=port
            ),
            client_key=client_path,
            host_key_sha256=pin,
        )

    backend = SSHBackend()
    try:
        # The container sshd allows only ``agent``; dialing the route account
        # (container_user unset would still resolve to ``ubuntu``, so use an
        # explicit wrong account) is refused -> the 502 root cause.
        with pytest.raises(SSHError):
            await asyncio.wait_for(
                backend.open_connection(target("dev"), "127.0.0.1", port), 10.0
            )
        # The container account succeeds.
        conn = await asyncio.wait_for(
            backend.open_connection(target("agent"), "127.0.0.1", port), 10.0
        )
        assert not conn.is_closed()
        await SSHBackend.close_connection(conn)
    finally:
        server.close()
        import contextlib

        with contextlib.suppress(Exception):
            await server.wait_closed()


# -- ChannelOpenError: sshd refused a new session channel (HTTP 500 fix) -----


class _ChannelOpenConn:
    """Connection whose channel-open always fails like a MaxSessions-exhausted sshd."""

    def __init__(self, exc) -> None:
        self._exc = exc
        self.closed = False

    def is_closed(self):
        return self.closed

    def close(self):
        self.closed = True

    async def wait_closed(self):
        return None

    async def run(self, *args, **kwargs):
        raise self._exc

    async def create_process(self, *args, **kwargs):
        raise self._exc


def _channel_open_error():
    return asyncssh.ChannelOpenError(asyncssh.OPEN_CONNECT_FAILED, "open failed")


async def test_run_maps_channel_open_error_to_ssherror():
    from compute_mcp.ssh_backend import SSHBackend, SSHError

    conn = _ChannelOpenConn(_channel_open_error())
    with pytest.raises(SSHError) as excinfo:
        await SSHBackend().run(conn, "echo hi")
    msg = str(excinfo.value)
    assert "session channel" in msg
    assert "MaxSessions" in msg
    assert "open failed" in msg


async def test_create_session_maps_channel_open_error_to_ssherror():
    from compute_mcp.ssh_backend import SSHBackend, SSHError

    conn = _ChannelOpenConn(_channel_open_error())
    with pytest.raises(SSHError) as excinfo:
        await SSHBackend().create_session(conn, None, 80, 24)
    msg = str(excinfo.value)
    assert "session channel" in msg
    assert "MaxSessions" in msg
    assert "open failed" in msg


async def test_channel_open_error_discards_cached_connection(monkeypatch):
    """A cached SHARED connection that cannot open a channel is dropped so the
    next request redials instead of failing the same way forever."""
    from compute_mcp.config import TargetConfig, TransportConfig
    from compute_mcp.ssh_backend import SSHBackend, SSHError

    target = TargetConfig(
        name="hal",
        user="agent",
        transport=TransportConfig(kind="direct", remote_host="127.0.0.1", remote_port=2222),
    )
    dialed = []
    conns = []

    async def fake_dial(self, tgt, host, port, prompter=None, passphrase=None, username=None):
        dialed.append((host, port))
        conn = _ChannelOpenConn(_channel_open_error())
        conns.append(conn)
        return conn

    monkeypatch.setattr(SSHBackend, "_dial", fake_dial)

    backend = SSHBackend()
    conn = await backend.connection(target, "127.0.0.1", 2222)
    assert backend.known_targets() == ["hal"]

    with pytest.raises(SSHError) as excinfo:
        await backend.run(conn, "echo hi")

    assert "session channel" in str(excinfo.value)
    assert backend.known_targets() == []
    assert conns[0].closed is True

    await backend.connection(target, "127.0.0.1", 2222)
    assert dialed == [("127.0.0.1", 2222), ("127.0.0.1", 2222)]

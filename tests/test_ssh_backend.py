# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import asyncio

import pytest

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

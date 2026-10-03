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

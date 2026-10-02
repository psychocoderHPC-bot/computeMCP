# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC

import pytest

from terok_compute.auth import (
    AuthError,
    Authenticator,
    ForbiddenTarget,
    hash_token,
    new_token,
)
from terok_compute.config import ClientConfig


def make_auth():
    return Authenticator(
        {
            "alpaka": ClientConfig(
                client_id="alpaka",
                token_sha256=hash_token("alpaka-token"),
                targets=("hal", "fwk394"),
            ),
            "picongpu": ClientConfig(
                client_id="picongpu",
                token_sha256=hash_token("picongpu-token"),
                targets=("hal", "gpu03"),
            ),
            "admin": ClientConfig(
                client_id="admin",
                token_sha256=hash_token("admin-token"),
                targets=(),
                allow_all=True,
            ),
        }
    )


def test_missing_token_rejected():
    auth = make_auth()
    with pytest.raises(AuthError):
        auth.authenticate_bearer(None)
    with pytest.raises(AuthError):
        auth.authenticate_bearer("")


def test_wrong_scheme_rejected():
    auth = make_auth()
    with pytest.raises(AuthError):
        auth.authenticate_bearer("Basic abc")


def test_wrong_token_rejected():
    auth = make_auth()
    with pytest.raises(AuthError):
        auth.authenticate_bearer("Bearer nope")


def test_correct_token_accepted():
    auth = make_auth()
    client = auth.authenticate_bearer("Bearer alpaka-token")
    assert client.client_id == "alpaka"


def test_acl_enforced():
    auth = make_auth()
    client = auth.authenticate_bearer("Bearer alpaka-token")
    assert client.may_access("hal")
    assert not client.may_access("gpu03")
    with pytest.raises(ForbiddenTarget):
        client.require_target("gpu03")


def test_admin_wildcard():
    auth = make_auth()
    client = auth.authenticate_bearer("Bearer admin-token")
    assert client.may_access("anything")


def test_hash_is_deterministic_and_prefixed():
    assert hash_token("x") == hash_token("x")
    assert hash_token("x").startswith("sha256:")


def test_new_token_has_entropy():
    tokens = {new_token() for _ in range(100)}
    assert len(tokens) == 100
    assert all(len(t) >= 32 for t in tokens)

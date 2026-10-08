# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Constant-time bearer-token authentication and per-client ACLs."""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass

from .config import ClientConfig, ConfigError

TOKEN_PREFIX = "sha256:"


def hash_token(token: str) -> str:
    return TOKEN_PREFIX + hashlib.sha256(token.encode("utf-8")).hexdigest()


def new_token(nbytes: int = 32) -> str:
    return secrets.token_urlsafe(nbytes)


@dataclass(frozen=True)
class Client:
    client_id: str
    allow_all: bool
    targets: frozenset[str]
    label: str | None = None

    @property
    def is_admin(self) -> bool:
        return self.allow_all

    def require_admin(self) -> None:
        if not self.allow_all:
            raise ForbiddenTarget("*")

    def may_access(self, target: str) -> bool:
        return self.allow_all or target in self.targets

    def require_target(self, target: str) -> None:
        if not self.may_access(target):
            raise ForbiddenTarget(target)


class AuthError(Exception):
    """Missing or invalid credentials.  Maps to HTTP 401."""


class ForbiddenTarget(Exception):
    """Authenticated but not allowed to use the requested target.  Maps to 403."""

    def __init__(self, target: str) -> None:
        super().__init__(target)
        self.target = target


class Authenticator:
    """Authenticates bearer tokens against configured client token hashes.

    Comparison is constant-time with respect to the token value.  The client is
    identified before ACL checks, but the error returned for an unknown token
    and the error returned for a known token are deliberately indistinguishable
    to the caller.
    """

    def __init__(self, clients: dict[str, ClientConfig]) -> None:
        if not clients:
            raise ConfigError("authenticator requires at least one client")
        self._clients = dict(clients)
        self._hashes = {cid: c.token_sha256 for cid, c in clients.items()}

    def _lookup(self, token: str) -> ClientConfig | None:
        """Resolve the client for ``token``, comparing in constant time.

        If more than one configured client carries the candidate hash the
        lookup fails closed with :class:`AuthError` instead of returning an
        order-dependent match.  Config validation rejects duplicate tokens, so
        this guards any path that bypasses it (e.g. a directly built
        ``Authenticator``).
        """
        if not token:
            return None
        candidate = hash_token(token)
        match: ClientConfig | None = None
        for client in self._clients.values():
            if hmac.compare_digest(candidate, client.token_sha256):
                if match is not None:
                    raise AuthError("multiple clients share the same token")
                match = client
        return match

    def authenticate_bearer(self, header: str | None) -> Client:
        if not header:
            raise AuthError("missing Authorization header")
        parts = header.split(None, 1)
        if len(parts) != 2 or parts[0].lower() != "bearer":
            raise AuthError("expected 'Bearer <token>'")
        client = self._lookup(parts[1].strip())
        if client is None:
            raise AuthError("invalid token")
        return Client(
            client_id=client.client_id,
            allow_all=client.allow_all,
            targets=frozenset(client.targets),
            label=client.label,
        )

    def known_client(self, token: str) -> Client | None:
        client = self._lookup(token)
        if client is None:
            return None
        return Client(
            client_id=client.client_id,
            allow_all=client.allow_all,
            targets=frozenset(client.targets),
            label=client.label,
        )

# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Out-of-band client enrollment with operator approval.

A Terok container that has never been configured can ask the gateway for
access.  The request is **unauthenticated** but grants nothing: it only places a
pending entry in an in-memory queue.  An operator must approve it on the gateway
console (or admin CLI); only then is a token minted, the client appended to the
gateway configuration, and the plaintext token handed back to the requester.

Security properties:

- The pending queue is bounded and entries expire, so an attacker who can reach
  the gateway port cannot grow state without limit.
- A requester proves ownership of its own request with a high-entropy poll
  secret; another client cannot read or hijack the token.
- The plaintext token is held in memory only between approval and the first
  successful poll, then dropped.  The config never stores plaintext.
- Approval is always an explicit human action; there is no path from an
  unauthenticated request to access without it.
"""

from __future__ import annotations

import hmac
import time
from dataclasses import dataclass, field

from .auth import hash_token, new_token

PENDING = "pending"
APPROVED = "approved"
CONSUMED = "consumed"
DENIED = "denied"
EXPIRED = "expired"


class EnrollmentError(Exception):
    """A malformed or rejected enrollment request.  Maps to HTTP 400/404."""


class EnrollmentQueueFull(EnrollmentError):
    """Too many pending requests; maps to HTTP 429."""


@dataclass
class PendingEnrollment:
    request_id: str
    client_id: str
    targets: tuple[str, ...]
    label: str | None
    source: str | None
    created_at: float
    expires_at: float
    secret_hash: str
    status: str = PENDING
    # Plaintext token is populated on approval and cleared after first delivery.
    token: str | None = field(default=None, repr=False)

    def is_expired(self, now: float | None = None) -> bool:
        return (now if now is not None else time.time()) >= self.expires_at


class EnrollmentManager:
    """In-memory, bounded, expiring queue of pending enrollment requests."""

    def __init__(self, *, ttl: float = 600.0, max_pending: int = 32) -> None:
        self._ttl = ttl
        self._max_pending = max_pending
        self._requests: dict[str, PendingEnrollment] = {}

    # -- request side ------------------------------------------------------
    def create(
        self,
        client_id: str,
        targets: tuple[str, ...],
        label: str | None,
        source: str | None,
    ) -> tuple[PendingEnrollment, str]:
        """Register a pending request; return it and the plaintext poll secret."""
        self._expire()
        pending_count = sum(
            1 for r in self._requests.values() if r.status == PENDING
        )
        if pending_count >= self._max_pending:
            raise EnrollmentQueueFull(
                "too many pending enrollment requests; try again later"
            )
        # A client id already queued (or approved but not yet collected) cannot
        # be queued a second time; ids are validated upstream, so collisions
        # here are honest duplicates.
        if any(
            r.client_id == client_id and r.status in (PENDING, APPROVED)
            for r in self._requests.values()
        ):
            raise EnrollmentError(
                f"an enrollment for client {client_id!r} is already pending"
            )
        now = time.time()
        secret = new_token()
        request = PendingEnrollment(
            request_id=new_token(9),
            client_id=client_id,
            targets=targets,
            label=label,
            source=source,
            created_at=now,
            expires_at=now + self._ttl,
            secret_hash=hash_token(secret),
        )
        self._requests[request.request_id] = request
        return request, secret

    def get(self, request_id: str, secret: str) -> PendingEnrollment:
        """Return a request if the poll secret matches, else raise."""
        self._expire()
        request = self._requests.get(request_id)
        # Compare in constant time so a bad request id and a bad secret are
        # indistinguishable, and neither can be probed by timing.
        supplied = hash_token(secret) if secret else ""
        expected = request.secret_hash if request is not None else ""
        if request is None or not hmac.compare_digest(supplied, expected):
            raise EnrollmentError("unknown enrollment request")
        return request

    # -- operator side -----------------------------------------------------
    def list_pending(self) -> list[PendingEnrollment]:
        self._expire()
        return sorted(self._requests.values(), key=lambda r: r.created_at)

    def get_by_id(self, request_id: str) -> PendingEnrollment:
        self._expire()
        request = self._requests.get(request_id)
        if request is None:
            raise EnrollmentError("unknown enrollment request")
        return request

    def approve(self, request_id: str) -> tuple[PendingEnrollment, str]:
        """Mark a pending request approved and mint its token."""
        request = self.get_by_id(request_id)
        if request.status == PENDING:
            request.status = APPROVED
            request.token = new_token()
        elif request.status not in (APPROVED, CONSUMED):
            raise EnrollmentError(
                f"request {request_id!r} is {request.status}, cannot approve"
            )
        return request, request.token or ""

    def deny(self, request_id: str) -> PendingEnrollment:
        request = self.get_by_id(request_id)
        if request.status == PENDING:
            request.status = DENIED
        return request

    def consume(self, request: PendingEnrollment) -> str | None:
        """Return the token exactly once, then clear it from memory."""
        token = request.token
        request.token = None
        request.status = CONSUMED
        return token

    def drop(self, request_id: str) -> None:
        self._requests.pop(request_id, None)

    def _expire(self) -> None:
        now = time.time()
        for request in self._requests.values():
            if request.status == PENDING and request.is_expired(now):
                request.status = EXPIRED
        for request_id in [
            r.request_id for r in self._requests.values() if r.status == EXPIRED
        ]:
            del self._requests[request_id]

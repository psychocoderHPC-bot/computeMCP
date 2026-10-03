# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import time

import pytest

from compute_mcp.enrollment import (
    APPROVED,
    DENIED,
    EnrollmentError,
    EnrollmentManager,
    EnrollmentQueueFull,
)


def test_create_and_poll_roundtrip():
    mgr = EnrollmentManager()
    pending, secret = mgr.create("ci", ("hal",), "label", "10.0.0.1")
    assert pending.status == "pending"
    got = mgr.get(pending.request_id, secret)
    assert got is pending
    with pytest.raises(EnrollmentError):
        mgr.get(pending.request_id, "wrong-secret")


def test_approve_delivers_token_once():
    mgr = EnrollmentManager()
    pending, secret = mgr.create("ci", (), None, None)
    mgr.approve(pending.request_id)
    assert pending.status == APPROVED
    token = mgr.consume(pending)
    assert token
    # Second consume returns nothing -- the token is not re-readable.
    assert mgr.consume(pending) is None
    assert pending.status == "consumed"


def test_deny_is_terminal():
    mgr = EnrollmentManager()
    pending, _ = mgr.create("ci", (), None, None)
    mgr.deny(pending.request_id)
    assert pending.status == DENIED


def test_duplicate_client_id_rejected():
    mgr = EnrollmentManager()
    mgr.create("ci", (), None, None)
    with pytest.raises(EnrollmentError):
        mgr.create("ci", (), None, None)


def test_queue_is_bounded():
    mgr = EnrollmentManager(max_pending=2)
    mgr.create("a", (), None, None)
    mgr.create("b", (), None, None)
    with pytest.raises(EnrollmentQueueFull):
        mgr.create("c", (), None, None)


def test_expiry_drops_pending(monkeypatch):
    mgr = EnrollmentManager(ttl=0.01)
    pending, _ = mgr.create("ci", (), None, None)
    time.sleep(0.02)
    assert pending.request_id not in {r.request_id for r in mgr.list_pending()}

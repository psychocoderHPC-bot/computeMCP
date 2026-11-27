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


def test_request_cannot_carry_job_limits():
    mgr = EnrollmentManager()
    pending, _ = mgr.create("ci", ("hal",), None, None)
    # The unauthenticated request path never grants quota.
    assert pending.job_limits == {}


def test_approve_records_operator_job_limits():
    mgr = EnrollmentManager()
    pending, _ = mgr.create("ci", ("hal",), None, None)
    mgr.approve(pending.request_id, job_limits={"hal": 0})
    assert pending.job_limits == {"hal": 0}


def test_approve_without_job_limits_keeps_default():
    mgr = EnrollmentManager()
    pending, _ = mgr.create("ci", ("hal",), None, None)
    mgr.approve(pending.request_id)
    assert pending.job_limits == {}


def test_approve_rejects_invalid_job_limits():
    mgr = EnrollmentManager()
    pending, _ = mgr.create("ci", ("hal",), None, None)
    for bad in ({"hal": -1}, {"hal": True}, {"hal": "2"}, {"hal": 1.5}):
        with pytest.raises(EnrollmentError):
            mgr.approve(pending.request_id, job_limits=bad)
    assert pending.job_limits == {}
    assert pending.status == "pending"

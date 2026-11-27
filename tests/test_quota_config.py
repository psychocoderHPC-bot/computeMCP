# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Per-client, per-target private-job quota configuration.

These tests cover the configuration foundation only: parsing ``job_limits``,
the effective-limit accessor, the approval helper, and writing limits through
``append_client``.  Quota enforcement and reservation are out of scope.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from compute_mcp.auth import hash_token
from compute_mcp.config import (
    DEFAULT_JOB_LIMIT,
    ClientConfig,
    ConfigError,
    append_client,
    append_token_hash,
    load_config,
    parse_config,
    parse_job_limits,
)

TARGET_BLOCK = """
[targets.hal]
transport = "direct"
remote_host = "127.0.0.1"
remote_port = 2222
user = "agent"
host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
"""


def _raw(job_limits=None, targets=("hal",), allow_all=False):
    client = {
        "token": "agent-token",
        "targets": ["*"] if allow_all else list(targets),
    }
    if job_limits is not None:
        client["job_limits"] = job_limits
    return {
        "server": {"listen": "127.0.0.1", "port": 2222},
        "auth": {},
        "clients": {"agent": client},
        "targets": {
            "hal": {
                "transport": "direct",
                "remote_host": "127.0.0.1",
                "remote_port": 2222,
                "user": "agent",
                "host_key_sha256": "SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
            }
        },
    }


# -- effective limit ---------------------------------------------------------

def test_client_config_default_limit_is_one():
    client = ClientConfig(client_id="a", token_sha256="sha256:x", targets=("hal",))
    assert client.job_limit("hal") == DEFAULT_JOB_LIMIT == 1
    assert client.job_limit("anything-else") == 1


def test_client_config_configured_limit_and_zero():
    client = ClientConfig(
        client_id="a",
        token_sha256="sha256:x",
        targets=("hal",),
        job_limits={"hal": 0},
    )
    assert client.job_limit("hal") == 0


# -- config parsing ----------------------------------------------------------

def test_job_limits_inline_table_parsed():
    cfg = parse_config(_raw(job_limits={"hal": 3}))
    assert cfg.clients["agent"].job_limits == {"hal": 3}
    assert cfg.clients["agent"].job_limit("hal") == 3


def test_job_limits_nested_table_parsed(config_loader):
    cfg = config_loader(
        f"""
        [clients.agent]
        token = "agent-token"
        targets = ["hal"]

        [clients.agent.job_limits]
        hal = 2
        """ + TARGET_BLOCK
    )
    assert cfg.clients["agent"].job_limits == {"hal": 2}
    assert cfg.clients["agent"].job_limit("hal") == 2


def test_job_limits_absent_defaults_to_one():
    cfg = parse_config(_raw())
    assert cfg.clients["agent"].job_limits == {}
    assert cfg.clients["agent"].job_limit("hal") == 1


def test_job_limits_unknown_target_rejected():
    with pytest.raises(ConfigError, match="unknown target"):
        parse_config(_raw(job_limits={"nope": 1}))


def test_job_limits_unauthorized_target_rejected():
    raw = _raw(job_limits={"other": 1})
    raw["targets"]["other"] = dict(raw["targets"]["hal"])
    with pytest.raises(ConfigError, match="not in the client's targets list"):
        parse_config(raw)


def test_job_limits_admin_wildcard_may_name_any_configured_target():
    raw = _raw(job_limits={"hal": 0}, allow_all=True)
    cfg = parse_config(raw)
    assert cfg.clients["agent"].job_limit("hal") == 0


@pytest.mark.parametrize(
    "value", [-1, True, False, 1.5, "2", [1], {"n": 1}, None]
)
def test_job_limits_invalid_value_rejected(value):
    with pytest.raises(ConfigError, match="non-negative integer"):
        parse_config(_raw(job_limits={"hal": value}))


def test_job_limits_must_be_a_table():
    with pytest.raises(ConfigError, match="job_limits must be a table"):
        parse_config(_raw(job_limits=3))


# -- CLI-style parser --------------------------------------------------------

def test_parse_job_limits_pairs():
    assert parse_job_limits(["hal=2", "fwk394=0"]) == {"hal": 2, "fwk394": 0}


def test_parse_job_limits_last_duplicate_wins():
    assert parse_job_limits(["hal=2", "hal=0"]) == {"hal": 0}


def test_parse_job_limits_absent_is_empty():
    assert parse_job_limits(None) == {}
    assert parse_job_limits([]) == {}


@pytest.mark.parametrize("entry", ["hal", "=2", "hal=", "hal=two", "hal=-1"])
def test_parse_job_limits_invalid_entry_rejected(entry):
    with pytest.raises(ConfigError):
        parse_job_limits([entry])


def test_parse_job_limits_invalid_target_name_rejected():
    with pytest.raises(ConfigError):
        parse_job_limits(["bad target=1"])


# -- append_client -----------------------------------------------------------

def _seed_config(tmp_path: Path) -> tuple[Path, Path]:
    cfg = tmp_path / "config.toml"
    tokens = tmp_path / "tokens.toml"
    cfg.write_text(
        textwrap.dedent(
            """
            # operator comment, must be preserved
            [clients.ci]
            token = "ci-secret"
            targets = ["hal"]
            """
        )
        + TARGET_BLOCK
    )
    return cfg, tokens


def test_append_client_writes_job_limits_and_reloads(tmp_path):
    cfg, tokens = _seed_config(tmp_path)
    append_token_hash(tokens, "newci", "plain", create=True)
    append_client(cfg, "newci", ("hal",), job_limits={"hal": 2})
    text = cfg.read_text()
    assert "# operator comment, must be preserved" in text
    assert 'job_limits = { "hal" = 2 }' in text
    loaded = load_config(cfg, token_file=str(tokens))
    assert loaded.clients["newci"].job_limit("hal") == 2


def test_append_client_multi_target_job_limits_are_sorted(tmp_path):
    cfg, tokens = _seed_config(tmp_path)
    cfg.write_text(
        cfg.read_text()
        + '\n[targets.secondTarget]\ntransport = "direct"\nuser = "agent"\n'
        'host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"\n'
    )
    append_token_hash(tokens, "newci", "plain", create=True)
    append_client(
        cfg,
        "newci",
        ("hal", "secondTarget"),
        job_limits={"secondTarget": 0, "hal": 3},
    )
    text = cfg.read_text()
    assert 'job_limits = { "hal" = 3, "secondTarget" = 0 }' in text
    loaded = load_config(cfg, token_file=str(tokens))
    assert loaded.clients["newci"].job_limits == {"hal": 3, "secondTarget": 0}


def test_append_client_job_limits_unknown_target_rejected(tmp_path):
    cfg, _ = _seed_config(tmp_path)
    before = cfg.read_text()
    with pytest.raises(ConfigError, match="unknown target"):
        append_client(cfg, "newci", ("hal",), job_limits={"nope": 1})
    assert cfg.read_text() == before


def test_append_client_job_limits_unauthorized_target_rejected(tmp_path):
    cfg, _ = _seed_config(tmp_path)
    cfg.write_text(
        cfg.read_text()
        + '\n[targets.other]\ntransport = "direct"\nuser = "agent"\n'
        'host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"\n'
    )
    before = cfg.read_text()
    with pytest.raises(ConfigError, match="not in client"):
        append_client(cfg, "newci", ("hal",), job_limits={"other": 1})
    assert cfg.read_text() == before


@pytest.mark.parametrize("value", [-1, True, 1.5, "2"])
def test_append_client_job_limits_invalid_value_rejected(tmp_path, value):
    cfg, _ = _seed_config(tmp_path)
    before = cfg.read_text()
    with pytest.raises(ConfigError, match="non-negative integer"):
        append_client(cfg, "newci", ("hal",), job_limits={"hal": value})
    assert cfg.read_text() == before

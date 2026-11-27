# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Shared pytest fixtures for the computeMCP test suite.

The fixtures here are deliberately small and non-autouse so existing test
modules keep their own local helpers.  They provide the building blocks the
multi-job tests need: a temporary state root, a config loader, an admin and an
agent identity with their bearer tokens, and one minimal target.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from compute_mcp.auth import Authenticator, hash_token
from compute_mcp.config import ClientConfig, load_config, parse_config

# Plaintext tokens used by the shared identity fixtures.  They never reach a
# real config file; ``ClientConfig`` stores only the hash.
AGENT_TOKEN = "agent-test-token"
ADMIN_TOKEN = "admin-test-token"

# A format-complete container host-key fingerprint that passes the loader.
TEST_FINGERPRINT = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"


@pytest.fixture
def state_root(tmp_path: Path) -> Path:
    """A temporary, writable state root (sibling-style, not a home)."""
    root = tmp_path / "state"
    root.mkdir()
    return root


@pytest.fixture
def minimal_target() -> dict:
    """One direct-transport target that loads without SSH or a container."""
    return {
        "transport": "direct",
        "remote_host": "127.0.0.1",
        "remote_port": 2222,
        "user": "agent",
        "host_key_sha256": TEST_FINGERPRINT,
    }


@pytest.fixture
def minimal_raw(minimal_target: dict) -> dict:
    """A complete, valid raw config table with the minimal target."""
    return {
        "server": {"listen": "127.0.0.1", "port": 2222},
        "auth": {},
        "clients": {
            "agent": {"token": AGENT_TOKEN, "targets": ["hal"]},
            "admin": {"token": ADMIN_TOKEN, "targets": ["*"]},
        },
        "targets": {"hal": dict(minimal_target)},
    }


@pytest.fixture
def config_loader(tmp_path: Path):
    """Load a config from either raw tables or TOML source.

    ``config_loader(dict)`` parses tables through the real validator;
    ``config_loader("toml text")`` writes a temp file and uses the real file
    loader (including ``include`` handling).  The written file is
    ``config.toml`` inside ``tmp_path`` unless ``name`` overrides it.
    """

    def _load(raw: dict | str, name: str = "config.toml") -> GatewayConfig:
        if isinstance(raw, str):
            path = tmp_path / name
            path.write_text(textwrap.dedent(raw))
            return load_config(path)
        return parse_config(raw)

    return _load


@pytest.fixture
def client_configs() -> dict[str, ClientConfig]:
    """Admin and agent identities with hashed tokens and no quota overrides."""
    return {
        "agent": ClientConfig(
            client_id="agent",
            token_sha256=hash_token(AGENT_TOKEN),
            targets=("hal",),
        ),
        "admin": ClientConfig(
            client_id="admin",
            token_sha256=hash_token(ADMIN_TOKEN),
            allow_all=True,
        ),
    }


@pytest.fixture
def authenticator(client_configs: dict[str, ClientConfig]) -> Authenticator:
    return Authenticator(client_configs)


@pytest.fixture
def agent_token() -> str:
    return AGENT_TOKEN


@pytest.fixture
def admin_token() -> str:
    return ADMIN_TOKEN


@pytest.fixture
def agent_client(authenticator: Authenticator):
    return authenticator.authenticate_bearer(f"Bearer {AGENT_TOKEN}")


@pytest.fixture
def admin_client(authenticator: Authenticator):
    return authenticator.authenticate_bearer(f"Bearer {ADMIN_TOKEN}")

# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import textwrap
from pathlib import Path

import pytest

from terok_compute.config import (
    ConfigError,
    load_config,
    parse_config,
)


def base_raw(**target_overrides):
    target = {
        "ssh_targets": ["hal"],
        "remote_host": "127.0.0.1",
        "remote_port": 2222,
        "user": "agent",
        "host_key_sha256": "SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    }
    target.update(target_overrides)
    return {
        "server": {"listen": "127.0.0.1", "port": 2222},
        "auth": {},
        "clients": {"alpaka": {"token": "secret", "targets": ["hal"]}},
        "targets": {"hal": target},
    }


def test_valid_config_parses():
    cfg = parse_config(base_raw())
    assert cfg.server.port == 2222
    assert "hal" in cfg.targets
    assert cfg.targets["hal"].transport.kind == "tunnel"
    assert cfg.targets["hal"].transport.ssh_targets == ("hal",)
    assert cfg.clients["alpaka"].may_access("hal")
    assert not cfg.clients["alpaka"].allow_all


def test_empty_ssh_targets_rejected_for_tunnel():
    raw = base_raw(ssh_targets=[])
    with pytest.raises(ConfigError):
        parse_config(raw)


def test_explicit_tunnel_without_routes_rejected():
    raw = base_raw(transport="tunnel", ssh_targets=[])
    with pytest.raises(ConfigError):
        parse_config(raw)


def test_direct_transport_allowed_without_routes():
    raw = base_raw(transport="direct", ssh_targets=[])
    cfg = parse_config(raw)
    assert cfg.targets["hal"].transport.kind == "direct"


def test_connect_mode_defaults_to_shared():
    cfg = parse_config(base_raw())
    assert cfg.targets["hal"].connect_mode == "shared"


def test_connect_mode_dedicated_accepted():
    cfg = parse_config(base_raw(connect_mode="dedicated"))
    assert cfg.targets["hal"].connect_mode == "dedicated"


def test_invalid_connect_mode_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(connect_mode="everyone"))


def test_interactive_auth_default_and_true():
    assert parse_config(base_raw()).targets["hal"].interactive_auth is False
    assert parse_config(base_raw(interactive_auth=True)).targets["hal"].interactive_auth is True


def test_sharing_default_and_values():
    assert parse_config(base_raw()).targets["hal"].sharing == "unknown"
    assert parse_config(base_raw(sharing="exclusive")).targets["hal"].sharing == "exclusive"
    assert parse_config(base_raw(sharing="shared")).targets["hal"].sharing == "shared"


def test_invalid_sharing_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(sharing="sometimes"))


def test_proxy_jump_parsed_for_tunnel():
    cfg = parse_config(base_raw(proxy_jump="rosi5"))
    assert cfg.targets["hal"].transport.proxy_jump == "rosi5"


def test_proxy_jump_rejected_for_direct():
    raw = base_raw(transport="direct", ssh_targets=[], proxy_jump="rosi5")
    with pytest.raises(ConfigError):
        parse_config(raw)


def test_provision_command_parsed():
    cfg = parse_config(base_raw(provision_command=["/bin/prov.sh", "--x"]))
    assert cfg.targets["hal"].provision_command == ("/bin/prov.sh", "--x")


def test_provision_command_rejected_for_direct():
    raw = base_raw(transport="direct", ssh_targets=[], provision_command=["/bin/x"])
    with pytest.raises(ConfigError):
        parse_config(raw)


def test_invalid_target_name_rejected():
    raw = base_raw()
    raw["targets"]["bad name!"] = raw["targets"].pop("hal")
    raw["clients"]["alpaka"]["targets"] = ["bad name!"]
    with pytest.raises(ConfigError):
        parse_config(raw)


def test_unknown_client_target_rejected():
    raw = base_raw()
    raw["clients"]["alpaka"]["targets"] = ["does-not-exist"]
    with pytest.raises(ConfigError):
        parse_config(raw)


def test_bad_host_key_fingerprint_rejected():
    raw = base_raw(host_key_sha256="not-a-fingerprint")
    with pytest.raises(ConfigError):
        parse_config(raw)


def test_client_requires_credentials():
    raw = base_raw()
    raw["clients"] = {"alpaka": {"targets": ["hal"]}}
    with pytest.raises(ConfigError):
        parse_config(raw)


def test_malformed_toml_is_rejected(tmp_path: Path):
    bad = tmp_path / "config.toml"
    bad.write_text("this is = = not toml")
    with pytest.raises(ConfigError):
        load_config(bad)


def test_missing_config_is_rejected(tmp_path: Path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "nope.toml")


def test_load_config_roundtrip(tmp_path: Path):
    good = tmp_path / "config.toml"
    good.write_text(
        textwrap.dedent(
            """
            [server]
            listen = "127.0.0.1"
            port = 2223

            [clients.ci]
            token = "abc"
            targets = ["hal"]

            [targets.hal]
            ssh_targets = ["hal"]
            user = "agent"
            host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
            """
        )
    )
    cfg = load_config(good)
    assert cfg.config_path == str(good)
    assert cfg.server.port == 2223


def test_token_file_supplies_client_hashes(tmp_path):
    from terok_compute.auth import hash_token

    tokens = tmp_path / "tokens.toml"
    tokens.write_text(f'[tokens]\nalpaka = "{hash_token("sekret")}"\n')
    raw = base_raw()
    raw["clients"] = {"alpaka": {"targets": ["hal"]}}
    cfg = parse_config(raw, token_file=str(tokens))
    assert cfg.clients["alpaka"].token_sha256 == hash_token("sekret")
    assert cfg.token_file == str(tokens)


def test_token_file_plaintext_is_hashed(tmp_path):
    from terok_compute.auth import hash_token

    tokens = tmp_path / "tokens.toml"
    tokens.write_text('[tokens]\nalpaka = "plain-value"\n')
    raw = base_raw()
    raw["clients"] = {"alpaka": {"targets": ["hal"]}}
    cfg = parse_config(raw, token_file=str(tokens))
    assert cfg.clients["alpaka"].token_sha256 == hash_token("plain-value")


def test_missing_token_file_is_rejected(tmp_path):
    raw = base_raw()
    raw["clients"] = {"alpaka": {"targets": ["hal"]}}
    with pytest.raises(FileNotFoundError):
        parse_config(raw, token_file=str(tmp_path / "nope.toml"))

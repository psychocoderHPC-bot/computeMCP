# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import textwrap
from pathlib import Path

import pytest

from compute_mcp.config import (
    ConfigError,
    TargetConfig,
    TransportConfig,
    load_config,
    load_tokens,
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


def test_node_info_default_is_empty():
    assert parse_config(base_raw()).targets["hal"].node_info == ()


def test_node_info_parsed():
    cfg = parse_config(base_raw(node_info=["GPU nvidia", "x86 CPU"]))
    assert cfg.targets["hal"].node_info == ("GPU nvidia", "x86 CPU")


def test_node_info_bare_string_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(node_info="GPU"))


def test_node_info_non_string_entry_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(node_info=["GPU", 3]))


def _direct_target(**overrides):
    kwargs = dict(
        name="hal",
        user="agent",
        transport=TransportConfig(kind="direct", remote_host="127.0.0.1", remote_port=2222),
        host_key_sha256="SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    )
    kwargs.update(overrides)
    return TargetConfig(**kwargs)


def test_node_info_direct_bare_string_rejected():
    with pytest.raises(ConfigError):
        _direct_target(node_info="GPU")


def test_node_info_direct_list_normalized_to_tuple_and_hashable():
    target = _direct_target(node_info=["GPU nvidia", "x86 CPU"])
    assert target.node_info == ("GPU nvidia", "x86 CPU")
    assert isinstance(target.node_info, tuple)
    assert hash(target) is not None


def test_agent_default_is_empty():
    assert parse_config(base_raw()).targets["hal"].agent == ()


def test_agent_inline_tables_parsed_to_tuple_of_pairs():
    raw = base_raw(
        agent=[
            {"agent": "opencode", "model": "GWen 3.5"},
            {"agent": "codex", "model": "Sole"},
        ]
    )
    assert parse_config(raw).targets["hal"].agent == (
        ("opencode", "GWen 3.5"),
        ("codex", "Sole"),
    )


def test_agent_spaces_preserved():
    raw = base_raw(agent=[{"agent": "my agent", "model": "big model v2"}])
    assert parse_config(raw).targets["hal"].agent == (("my agent", "big model v2"),)


def test_agent_non_list_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(agent="opencode"))


def test_agent_bare_string_entry_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(agent=["opencode@GWen 3.5"]))


def test_agent_unknown_key_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(agent=[{"agent": "opencode", "model": "x", "extra": "y"}]))


def test_agent_missing_agent_key_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(agent=[{"model": "GWen 3.5"}]))


def test_agent_missing_model_key_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(agent=[{"agent": "opencode"}]))


def test_agent_empty_string_value_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(agent=[{"agent": "", "model": "GWen 3.5"}]))


def test_agent_non_string_value_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(agent=[{"agent": "opencode", "model": 3}]))


def test_agent_empty_model_value_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(agent=[{"agent": "opencode", "model": ""}]))


def test_agent_non_string_agent_value_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(agent=[{"agent": 3, "model": "x"}]))


def test_agent_normalized_tuple_entry_accepted():
    raw = base_raw(agent=[("opencode", "GWen 3.5")])
    assert parse_config(raw).targets["hal"].agent == (("opencode", "GWen 3.5"),)


def test_agent_two_element_array_entry_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(agent=[["a", "b"]]))


def test_agent_direct_normalized_to_tuple_and_hashable():
    target = _direct_target(
        agent=[("opencode", "GWen 3.5"), ("codex", "Sole")],
    )
    assert target.agent == (("opencode", "GWen 3.5"), ("codex", "Sole"))
    assert isinstance(target.agent, tuple)
    assert hash(target) is not None


def test_agent_direct_dict_normalized_to_tuple_and_hashable():
    target = _direct_target(agent=[{"agent": "opencode", "model": "GWen 3.5"}])
    assert target.agent == (("opencode", "GWen 3.5"),)
    assert hash(target) is not None


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


def test_close_command_parsed_with_timeout():
    cfg = parse_config(
        base_raw(close_command=["scancel", "--name", "job"], close_command_timeout=45)
    )
    t = cfg.targets["hal"]
    assert t.close_command == ("scancel", "--name", "job")
    assert t.close_command_timeout == 45.0


def test_close_command_defaults_empty_and_timeout_120():
    t = parse_config(base_raw()).targets["hal"]
    assert t.close_command == ()
    assert t.close_command_timeout == 120.0


def test_close_command_rejected_for_direct():
    raw = base_raw(transport="direct", ssh_targets=[], close_command=["scancel"])
    with pytest.raises(ConfigError, match="close_command requires tunnel transport"):
        parse_config(raw)


def test_connect_command_parsed():
    cfg = parse_config(base_raw(connect_command=["/bin/up.sh"], connect_command_timeout=30))
    t = cfg.targets["hal"]
    assert t.connect_command == ("/bin/up.sh",)
    assert t.connect_command_timeout == 30.0
    assert t.connect_command_mode == "on_failure"


def test_connect_command_mode_always():
    cfg = parse_config(base_raw(connect_command=["/bin/up.sh"], connect_command_mode="always"))
    assert cfg.targets["hal"].connect_command_mode == "always"


def test_bad_connect_command_mode_rejected():
    raw = base_raw(connect_command=["/bin/x"], connect_command_mode="sometimes")
    with pytest.raises(ConfigError):
        parse_config(raw)


def test_connect_command_rejected_for_direct():
    raw = base_raw(transport="direct", ssh_targets=[], connect_command=["/bin/x"])
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


def test_host_key_check_defaults_on():
    cfg = parse_config(base_raw())
    assert cfg.targets["hal"].host_key_check == "on"


def test_host_key_check_off_without_pin_is_valid():
    raw = base_raw()
    target = raw["targets"]["hal"]
    target.pop("host_key_sha256")
    target["host_key_check"] = "off"
    cfg = parse_config(raw)
    assert cfg.targets["hal"].host_key_check == "off"
    assert cfg.targets["hal"].host_key_sha256 is None


def test_bad_host_key_check_rejected():
    raw = base_raw(host_key_check="maybe")
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
    from compute_mcp.auth import hash_token

    tokens = tmp_path / "tokens.toml"
    tokens.write_text(f'[tokens]\nalpaka = "{hash_token("sekret")}"\n')
    raw = base_raw()
    raw["clients"] = {"alpaka": {"targets": ["hal"]}}
    cfg = parse_config(raw, token_file=str(tokens))
    assert cfg.clients["alpaka"].token_sha256 == hash_token("sekret")
    assert cfg.token_file == str(tokens)


def test_token_file_plaintext_is_hashed(tmp_path):
    from compute_mcp.auth import hash_token

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


def test_default_paths_use_xdg_config_home(tmp_path, monkeypatch):
    from compute_mcp.config import default_config_path, default_token_path

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    assert default_config_path() == tmp_path / "computeMCP-gateway" / "config.toml"
    assert default_token_path() == tmp_path / "computeMCP-gateway" / "tokens.toml"


def test_load_config_defaults_to_xdg_config_home(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    cfgdir = tmp_path / "computeMCP-gateway"
    cfgdir.mkdir(parents=True)
    (cfgdir / "config.toml").write_text(
        textwrap.dedent(
            """
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
    cfg = load_config()  # no path -> conventional location
    assert cfg.server.port == 2222
    assert "hal" in cfg.targets


def test_load_config_picks_up_sibling_default_token_file(tmp_path, monkeypatch):
    from compute_mcp.auth import hash_token

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    cfgdir = tmp_path / "computeMCP-gateway"
    cfgdir.mkdir(parents=True)
    (cfgdir / "config.toml").write_text(
        textwrap.dedent(
            """
            [clients.ci]
            targets = ["hal"]

            [targets.hal]
            ssh_targets = ["hal"]
            user = "agent"
            host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
            """
        )
    )
    (cfgdir / "tokens.toml").write_text(f'[tokens]\nci = "{hash_token("sekret")}"\n')
    cfg = load_config()
    assert cfg.clients["ci"].token_sha256 == hash_token("sekret")
    assert cfg.token_file == str(cfgdir / "tokens.toml")


def test_explicit_token_file_still_required_when_named(tmp_path):
    raw = base_raw()
    raw["clients"] = {"alpaka": {"targets": ["hal"]}}
    with pytest.raises(FileNotFoundError):
        parse_config(raw, token_file=str(tmp_path / "explicit-missing.toml"))


def _write_base_config(path: Path, extra: str = "") -> None:
    path.write_text(
        textwrap.dedent(
            """
            # operator comment, must be preserved
            [clients.ci]
            token = "ci-secret"
            targets = ["hal"]

            [targets.hal]
            ssh_targets = ["hal"]
            user = "agent"
            host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
            """
        )
        + extra
    )


def test_append_client_preserves_existing_and_loads(tmp_path):
    from compute_mcp.config import append_client, append_token_hash

    cfg = tmp_path / "config.toml"
    tok = tmp_path / "tokens.toml"
    _write_base_config(cfg)
    append_token_hash(tok, "newci", "plain", create=True)
    append_client(cfg, "newci", ("hal",), "New CI")
    text = cfg.read_text()
    assert "# operator comment, must be preserved" in text
    assert "[clients.newci]" in text
    loaded = load_config(cfg, token_file=str(tok))
    assert set(loaded.clients) == {"ci", "newci"}
    assert loaded.clients["newci"].may_access("hal")
    assert not loaded.clients["newci"].allow_all


def test_append_client_unknown_target_is_rejected(tmp_path):
    from compute_mcp.config import append_client, append_token_hash

    cfg = tmp_path / "config.toml"
    tok = tmp_path / "tokens.toml"
    _write_base_config(cfg)
    append_token_hash(tok, "newci", "plain")
    with pytest.raises(ConfigError):
        append_client(cfg, "newci", ("nope",))
    # File is left untouched on rejection.
    assert "[clients.newci]" not in cfg.read_text()


def test_append_client_duplicate_rejected(tmp_path):
    from compute_mcp.config import append_client

    cfg = tmp_path / "config.toml"
    _write_base_config(cfg)
    with pytest.raises(ConfigError):
        append_client(cfg, "ci", ("hal",))


def _include_entry_with_target_included(tmp_path, target_file="systems/hal.toml"):
    """Entry file whose targets live in an included file, like after the
    include refactor (no [targets.*] table in the entry itself)."""
    halo = tmp_path / target_file
    halo.parent.mkdir(parents=True, exist_ok=True)
    halo.write_text(
        """
        [targets.hal]
        transport = "direct"
        remote_host = "127.0.0.1"
        remote_port = 9
        user = "agent"
        host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
        """
    )
    entry = tmp_path / "gateway.toml"
    entry.write_text(
        f'include = ["{target_file}"]\n\n[clients.ci]\ntoken = "ci-secret"\ntargets = ["hal"]\n'
    )
    return entry, halo


def test_append_client_accepts_target_defined_in_include(tmp_path):
    from compute_mcp.config import append_client, append_token_hash

    entry, halo = _include_entry_with_target_included(tmp_path)
    tok = tmp_path / "tokens.toml"
    append_token_hash(tok, "newci", "plain", create=True)
    before = halo.read_text()
    append_client(entry, "newci", ("hal",))
    assert "[clients.newci]" in entry.read_text()
    assert halo.read_text() == before
    loaded = load_config(entry, token_file=str(tok))
    assert set(loaded.targets) == {"hal"}
    assert loaded.clients["newci"].may_access("hal")


def test_append_client_rejects_target_not_in_any_include(tmp_path):
    from compute_mcp.config import append_client

    entry, _ = _include_entry_with_target_included(tmp_path)
    with pytest.raises(ConfigError, match="known targets") as excinfo:
        append_client(entry, "newci", ("nope",))
    assert "hal" in str(excinfo.value)
    assert "[clients.newci]" not in entry.read_text()


def test_append_client_rejects_duplicate_client_defined_in_include(tmp_path):
    from compute_mcp.config import append_client

    _include_entry_with_target_included(tmp_path)
    included = tmp_path / "systems" / "hal.toml"
    included.write_text(
        """
        [targets.hal]
        transport = "direct"
        remote_host = "127.0.0.1"
        remote_port = 9
        user = "agent"
        host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"

        [clients.ci]
        token = "ci-secret"
        targets = ["hal"]
        """
    )
    entry = tmp_path / "gateway.toml"
    entry.write_text('include = ["systems/hal.toml"]\n')
    before = entry.read_text()
    with pytest.raises(ConfigError, match="already exists"):
        append_client(entry, "ci", ("hal",))
    assert entry.read_text() == before


def test_append_client_empty_acl(tmp_path):
    from compute_mcp.config import append_client, append_token_hash

    cfg = tmp_path / "config.toml"
    tok = tmp_path / "tokens.toml"
    _write_base_config(cfg)
    append_token_hash(tok, "newci", "plain")
    append_client(cfg, "newci", ())
    loaded = load_config(cfg, token_file=str(tok))
    assert loaded.clients["newci"].targets == ()
    assert not loaded.clients["newci"].allow_all


def test_append_token_hash_replaces_existing(tmp_path):
    from compute_mcp.auth import hash_token
    from compute_mcp.config import append_token_hash

    tok = tmp_path / "tokens.toml"
    append_token_hash(tok, "ci", "first")
    append_token_hash(tok, "ci", "second")
    assert load_tokens(tok)["ci"] == hash_token("second")


# -- route_host_key_sha256 ---------------------------------------------------

def test_route_host_key_sha256_absent_by_default():
    cfg = parse_config(base_raw())
    assert cfg.targets["hal"].route_host_key_sha256 is None


def test_route_host_key_sha256_parsed_and_stripped():
    raw = base_raw(route_host_key_sha256="  SHA256:abcdefghijklmnopqrstuvwxyz0123456789  ")
    cfg = parse_config(raw)
    assert (
        cfg.targets["hal"].route_host_key_sha256
        == "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
    )


def test_route_host_key_sha256_malformed_rejected():
    with pytest.raises(ConfigError):
        parse_config(base_raw(route_host_key_sha256="md5:abc"))
    with pytest.raises(ConfigError):
        parse_config(base_raw(route_host_key_sha256="SHA256:x"))


def test_route_host_key_sha256_normalized_like_container_pin():
    # Both pins must apply the same validation/normalization rule.
    good = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"
    for field in ("host_key_sha256", "route_host_key_sha256"):
        cfg = parse_config(base_raw(**{field: f"  {good}  "}))
        assert getattr(cfg.targets["hal"], field) == good
        with pytest.raises(ConfigError):
            parse_config(base_raw(**{field: "not-a-pin"}))


# -- include: merge semantics -------------------------------------------------

def test_include_relative_path_resolves_against_including_file(tmp_path: Path, monkeypatch):
    root = tmp_path / "cfgroot"
    shared = root / "shared"
    shared.mkdir(parents=True)
    (shared / "targets.toml").write_text(
        '[clients.ci]\ntoken = "abc"\ntargets = ["hal"]\n'
        '\n'
        "[targets.hal]\n"
        'ssh_targets = ["hal"]\n'
        'user = "agent"\n'
        'host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"\n'
        "node.cpus = 24\n"
        "node.gpus = 4\n"
        'node.memory = "378000M"\n'
    )
    entry = root / "gateway.toml"
    entry.write_text('include = ["shared/targets.toml"]\n')
    # Chdir away from the config directory: the relative include is resolved
    # against the entry file's directory, never the process CWD.
    monkeypatch.chdir(tmp_path)
    cfg = load_config(entry)
    assert cfg.targets["hal"].node.cpus == 24
    assert cfg.targets["hal"].node.gpus == 4
    assert cfg.targets["hal"].node.memory == "378000M"
    assert cfg.include_paths == (str(shared / "targets.toml"),)
    assert cfg.config_path == str(entry)


def test_include_absolute_path_accepted(tmp_path: Path):
    other = tmp_path / "elsewhere"
    other.mkdir()
    shared = other / "targets.toml"
    shared.write_text(
        "[targets.hal]\n"
        'transport = "direct"\n'
        'user = "agent"\n'
        'host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"\n'
        "[clients.ci]\n"
        'token = "abc"\n'
        'targets = ["hal"]\n'
    )
    entry = tmp_path / "gateway.toml"
    entry.write_text(f'include = ["{shared}"]\n')
    cfg = load_config(entry)
    assert "hal" in cfg.targets
    assert cfg.include_paths == (str(shared),)



def _write_include_tree(base, entry_text, **files):
    for rel, content in files.items():
        path = base / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    entry = base / "gateway.toml"
    entry.write_text(entry_text)
    return entry


def test_include_duplicate_leaf_names_both_files(tmp_path: Path):
    entry = _write_include_tree(
        tmp_path,
        'include = ["a.toml", "b.toml"]\n[clients.ci]\ntargets = []\n',
        **{
            "a.toml": "node.cpus = 1\n[t]\nx = 1\n",
            "b.toml": "node.cpus = 2\n[s]\ny = 2\n",
        },
    )
    with pytest.raises(ConfigError) as excinfo:
        load_config(entry)
    msg = str(excinfo.value)
    assert "node.cpus" in msg
    assert str(tmp_path / "a.toml") in msg
    assert str(tmp_path / "b.toml") in msg


def test_include_missing_file_rejected(tmp_path: Path):
    entry = _write_include_tree(
        tmp_path,
        'include = ["missing.toml"]\n[t]\nx = 1\n',
    )
    with pytest.raises(ConfigError, match="configuration file not found"):
        load_config(entry)


def test_include_cycle_rejected(tmp_path: Path):
    entry = _write_include_tree(
        tmp_path,
        'include = ["b.toml"]\n[t]\nx = 1\n',
        **{
            "a.toml": 'include = ["b.toml"]\n[a]\ny = 1\n',
            "b.toml": 'include = ["a.toml"]\n[b]\nz = 1\n',
        },
    )
    with pytest.raises(ConfigError, match="include cycle detected"):
        load_config(entry)


def test_include_diamond_is_applied_once(tmp_path: Path):
    entry = _write_include_tree(
        tmp_path,
        'include = ["a.toml", "b.toml"]\n',
        **{
            "a.toml": '[targets.hal]\ninclude = ["common.toml"]\n[a]\nx = 1\n[clients.ci]\ntoken = "abc"\ntargets = ["hal"]\n',
            "b.toml": 'include = ["common.toml"]\n[b]\ny = 2\n',
            "common.toml": 'transport = "direct"\nuser = "agent"\nhost_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"\n',
        },
    )
    cfg = load_config(entry)
    # First-seen preorder: a, common, b (a diamond is not a duplicate).
    assert cfg.include_paths == (
        str(tmp_path / "a.toml"),
        str(tmp_path / "common.toml"),
        str(tmp_path / "b.toml"),
    )
    assert cfg.include_paths.count(str(tmp_path / "common.toml")) == 1


def test_no_include_yields_empty_include_paths(tmp_path: Path):
    entry = _write_include_tree(
        tmp_path,
        "[clients.ci]\ntoken = \"abc\"\ntargets = []\n"
        "[targets.hal]\ntransport = \"direct\"\nuser = \"agent\"\n"
        'host_key_sha256 = "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"\n',
    )
    cfg = load_config(entry)
    assert cfg.include_paths == ()


def test_include_must_be_a_list_of_strings(tmp_path: Path):
    entry = _write_include_tree(tmp_path, "include = \"b.toml\"\n[t]\nx = 1\n")
    with pytest.raises(ConfigError, match="must be a list of file path strings"):
        load_config(entry)


# -- node allocation tables ---------------------------------------------------

def test_node_table_parsed_and_validated():
    cfg = parse_config(
        base_raw(node={"cpus": 32, "gpus": 2, "memory": "378000M"})
    )
    node = cfg.targets["hal"].node
    assert node.cpus == 32
    assert node.gpus == 2
    assert node.memory == "378000M"


def test_node_table_missing_is_none():
    assert parse_config(base_raw()).targets["hal"].node is None


def test_node_table_unknown_key_rejected():
    with pytest.raises(ConfigError, match="unknown key"):
        parse_config(base_raw(node={"cpus": 4, "accelerators": 1}))




def test_node_table_negative_cpus_rejected():
    with pytest.raises(ConfigError, match="positive integer"):
        parse_config(base_raw(node={"cpus": -4}))
    with pytest.raises(ConfigError, match="positive integer"):
        parse_config(base_raw(node={"gpus": 0}))


def test_node_table_empty_memory_rejected():
    with pytest.raises(ConfigError, match="non-empty string"):
        parse_config(base_raw(node={"memory": ""}))


def test_node_table_memory_must_be_string():
    with pytest.raises(ConfigError, match="non-empty string"):
        parse_config(base_raw(node={"memory": 378000}))


def test_allocation_table_parsed_and_validated():
    cfg = parse_config(
        base_raw(
            allocation={
                "default-gpus": 1,
                "default-cpus": 8,
                "single-node": "gpu-proportional",
                "multi-node": "full",
                "max-nodes": 4,
            }
        )
    )
    alloc = cfg.targets["hal"].allocation
    assert alloc.default_gpus == 1
    assert alloc.default_cpus == 8
    assert alloc.single_node == "gpu-proportional"
    assert alloc.multi_node == "full"
    assert alloc.max_nodes == 4


def test_allocation_missing_is_none():
    assert parse_config(base_raw()).targets["hal"].allocation is None


def test_allocation_unknown_mode_rejected():
    for field, bad in (("single-node", "sometimes"), ("multi-node", "gpu-proportional")):
        with pytest.raises(ConfigError, match="must be one of"):
            parse_config(base_raw(allocation={field: bad}))


def test_allocation_multi_node_rejects_partial_modes():
    for bad in ("gpu-proportional", "cpu-proportional"):
        with pytest.raises(ConfigError, match="must be one of"):
            parse_config(base_raw(allocation={"multi-node": bad}))


def test_allocation_max_nodes_must_be_positive():
    with pytest.raises(ConfigError, match="positive integer"):
        parse_config(base_raw(allocation={"max-nodes": 0}))
    with pytest.raises(ConfigError, match="positive integer"):
        parse_config(base_raw(allocation={"default-gpus": True}))


def test_allocation_unknown_key_rejected():
    with pytest.raises(ConfigError, match="unknown key"):
        parse_config(base_raw(allocation={"single-node": "full", "warp": "dmesg"}))


def test_slurm_stage_tables_parsed_and_stages_independent():
    cfg = parse_config(
        base_raw(
            slurm={
                "sbatch": {"partition": "gpu", "time": "02:00:00"},
                "sbatch-map": {
                    "nodes": "nodes",
                    "gpus-per-node": "gres",
                    "memory-per-node": "mem",
                },
                "srun": {"ntasks-per-node": 1, "cpu-bind": "none"},
                "srun-map": {"cpus-per-node": "cpus-per-task"},
            }
        )
    )
    slurm = cfg.targets["hal"].slurm
    assert slurm.sbatch.options == {"partition": "gpu", "time": "02:00:00"}
    assert slurm.sbatch.mapping == {
        "nodes": "nodes",
        "gpus-per-node": "gres",
        "memory-per-node": "mem",
    }
    assert slurm.srun.options == {"ntasks-per-node": 1, "cpu-bind": "none"}
    assert slurm.srun.mapping == {"cpus-per-node": "cpus-per-task"}


def test_slurm_unknown_key_rejected():
    with pytest.raises(ConfigError, match="unknown key"):
        parse_config(base_raw(slurm={"sbatch": {}, "wat": {}}))


def test_slurm_empty_table_is_equivalent_to_absent():
    cfg = parse_config(base_raw(slurm={}))
    assert cfg.targets["hal"].slurm is None


def test_mapping_unknown_key_rejected():
    with pytest.raises(ConfigError, match="unknown mapping key"):
        parse_config(
            base_raw(
                slurm={
                    "sbatch": {"ntasks-per-node": 1},
                    "sbatch-map": {"ntasks": "1"},
                }
            )
        )


def test_mapping_unknown_representation_rejected():
    for key, bad in (
        ("nodes", "node"),
        ("gpus-per-node", "gpus-per"),
        ("cpus-per-node", "cpus-per-node"),
        ("memory-per-node", "mem-per-cpu"),
        ("exclusive", "x"),
    ):
        options = {"ntasks-per-node": 1} if key == "cpus-per-node" else None
        with pytest.raises(ConfigError, match="must be one of"):
            parse_config(
                base_raw(
                    slurm={
                        "sbatch": options or {},
                        "sbatch-map": {key: bad},
                    }
                )
            )


def test_mapping_cpus_per_node_requires_one_task_per_node():
    # Precondition: the stage needs ntasks-per-node or ntasks of exactly 1.
    for options in ({}, {"ntasks-per-node": 2}, {"ntasks": 4}):
        with pytest.raises(ConfigError, match="one task per node"):
            parse_config(
                base_raw(
                    slurm={
                        "sbatch": options,
                        "sbatch-map": {"cpus-per-node": "cpus-per-task"},
                    }
                )
            )
    # Both accepted spellings pass.
    for options in ({"ntasks-per-node": 1}, {"ntasks": 1}):
        cfg = parse_config(
            base_raw(
                slurm={
                    "sbatch": options,
                    "sbatch-map": {"cpus-per-node": "cpus-per-task"},
                }
            )
        )
        assert cfg.targets["hal"].slurm.sbatch.mapping == {
            "cpus-per-node": "cpus-per-task"
        }


def test_container_block_parsed():
    cfg = parse_config(
        base_raw(
            container={
                "runtime": "apptainer",
                "storage-root": "/scratch/x",
                "image": "ubuntu:24.04",
                "gpus": ["nvidia", "amd"],
                "host-home": "/home/agent",
                "sandbox": True,
            }
        )
    )
    ctr = cfg.targets["hal"].container
    assert ctr.runtime == "apptainer"
    assert ctr.storage_root == "/scratch/x"
    assert ctr.image == "ubuntu:24.04"
    assert ctr.gpus == ("nvidia", "amd")
    assert ctr.host_home == "/home/agent"
    assert ctr.sandbox is True


def test_container_missing_is_none():
    assert parse_config(base_raw()).targets["hal"].container is None


def test_container_requires_runtime():
    with pytest.raises(ConfigError, match="requires"):
        parse_config(base_raw(container={"storage-root": "/x"}))


def test_container_runtime_enum():
    with pytest.raises(ConfigError, match="apptainer"):
        parse_config(base_raw(container={"runtime": "podman"}))
    assert (
        parse_config(base_raw(container={"runtime": "docker"}))
        .targets["hal"]
        .container.runtime
        == "docker"
    )


def test_container_gpu_vendor_enum():
    with pytest.raises(ConfigError, match="unknown vendor"):
        parse_config(
            base_raw(container={"runtime": "docker", "gpus": ["nvidia", "intel", "zotac"]})
        )
    with pytest.raises(ConfigError, match="unknown vendor"):
        # A whitespace entry is not a vendor: rejected by the enum, not the
        # separate non-empty check (whose sorted() set renders it invisibly).
        parse_config(
            base_raw(
                container={"runtime": "docker", "gpus": ["nvidia", " "]}
            )
        )
    vendors = (
        parse_config(
            base_raw(
                container={"runtime": "docker", "gpus": ["nvidia", "amd", "nvidia"]}
            )
        )
        .targets["hal"]
        .container.gpus
    )
    assert vendors == ("nvidia", "amd")  # deduplicated on first-seen


def test_container_unknown_key_rejected():
    with pytest.raises(ConfigError, match="unknown key"):
        parse_config(base_raw(container={"runtime": "docker", "extra": 1}))


def test_back_compat_config_without_new_tables_loads():
    """Existing configs without [node]/[allocation]/[slurm]/[container] load unchanged."""
    cfg = parse_config(base_raw())
    target = cfg.targets["hal"]
    assert target.node is None
    assert target.allocation is None
    assert target.slurm is None
    assert target.container is None

    raw = {
        "server": {"listen": "127.0.0.1", "port": 2222},
        "clients": {"alpaka": {"token": "secret", "targets": ["x"]}},
        "targets": {"x": {"transport": "direct", "user": "agent"}},
    }
    cfg = parse_config(raw)
    assert cfg.targets["x"].node is None
    assert cfg.include_paths == ()


def test_bundle_block_defaults():
    cfg = parse_config(
        base_raw(
            client_key="/home/user/.ssh/key",
            container={"runtime": "apptainer", "storage-root": "/scratch/x"},
            bundle={"source": "computemcp-container"},
        )
    )
    bundle = cfg.targets["hal"].bundle
    assert bundle.source == "computemcp-container"
    assert bundle.deploy_dir is None
    assert bundle.auto_deploy is True


def test_bundle_legacy_slurm_source_still_parses():
    # Back-compat: existing configs keep loading and resolving.
    cfg = parse_config(
        base_raw(
            client_key="/home/user/.ssh/key",
            container={"runtime": "apptainer", "storage-root": "/scratch/x"},
            bundle={"source": "computemcp-slurm"},
        )
    )
    assert cfg.targets["hal"].bundle.source == "computemcp-slurm"


def test_bundle_block_parsed():
    cfg = parse_config(
        base_raw(
            client_key="/home/user/.ssh/key",
            bundle={
                "source": "computemcp-slurm",
                "deploy-dir": "/scratch/agent/bundle",
                "auto-deploy": False,
            },
        )
    )
    bundle = cfg.targets["hal"].bundle
    assert bundle.deploy_dir == "/scratch/agent/bundle"
    assert bundle.auto_deploy is False


def test_bundle_missing_is_none():
    assert parse_config(base_raw()).targets["hal"].bundle is None


def test_bundle_requires_source():
    with pytest.raises(ConfigError, match="requires 'source'"):
        parse_config(base_raw(bundle={"deploy-dir": "/scratch/x"}))


def test_bundle_unknown_source_rejected():
    with pytest.raises(ConfigError, match="bundle.source"):
        parse_config(base_raw(bundle={"source": "nope"}))


def test_bundle_deploy_dir_must_be_absolute():
    with pytest.raises(ConfigError, match="absolute"):
        parse_config(base_raw(bundle={"source": "computemcp-slurm", "deploy-dir": "rel"}))


def test_bundle_needs_deploy_dir_or_storage_root():
    with pytest.raises(ConfigError, match="deploy-dir"):
        parse_config(base_raw(bundle={"source": "computemcp-slurm"}))


def test_bundle_requires_client_key():
    with pytest.raises(ConfigError, match="client_key"):
        parse_config(
            base_raw(
                container={"runtime": "apptainer", "storage-root": "/scratch/x"},
                bundle={"source": "computemcp-slurm"},
            )
        )


def test_bundle_auto_deploy_must_be_bool():
    with pytest.raises(ConfigError, match="auto_deploy"):
        parse_config(
            base_raw(bundle={"source": "computemcp-slurm", "auto-deploy": "nope"})
        )


def test_bundle_unknown_key_rejected():
    with pytest.raises(ConfigError, match="unknown key"):
        parse_config(base_raw(bundle={"source": "computemcp-slurm", "extra": 1}))


def test_bundle_requires_tunnel_transport():
    raw = {
        "server": {"listen": "127.0.0.1", "port": 2222},
        "clients": {"alpaka": {"token": "secret", "targets": ["h"]}},
        "targets": {
            "h": {
                "transport": "direct",
                "user": "agent",
                "bundle": {"source": "computemcp-slurm"},
            }
        },
    }
    with pytest.raises(ConfigError, match="bundle requires tunnel"):
        parse_config(raw)


def test_bundle_provision_env_parse():
    default = parse_config(
        base_raw(
            client_key="/home/user/.ssh/key",
            container={"runtime": "apptainer", "storage-root": "/scratch/x"},
            bundle={"source": "computemcp-slurm"},
        )
    ).targets["hal"].bundle
    assert default.provision_env == ()
    parsed = parse_config(
        base_raw(
            client_key="/home/user/.ssh/key",
            container={"runtime": "apptainer", "storage-root": "/scratch/x"},
            bundle={
                "source": "computemcp-slurm",
                "provision-env": ["module load apptainer", "source /etc/profile"],
            },
        )
    ).targets["hal"].bundle
    assert parsed.provision_env == (
        "module load apptainer",
        "source /etc/profile",
    )


@pytest.mark.parametrize(
    "bad",
    [
        [""],
        ["   "],
        ["ok", ""],
        ["a\0b"],
        ["a\rb"],
        [1],
    ],
)
def test_bundle_provision_env_reject_bad_line(bad):
    with pytest.raises(ConfigError, match="provision-env"):
        parse_config(
            base_raw(
                client_key="/home/user/.ssh/key",
                container={"runtime": "apptainer", "storage-root": "/scratch/x"},
                bundle={"source": "computemcp-slurm", "provision-env": bad},
            )
        )


def test_bundle_provision_env_not_array_rejected():
    with pytest.raises(ConfigError, match="provision-env"):
        parse_config(
            base_raw(
                client_key="/home/user/.ssh/key",
                container={"runtime": "apptainer", "storage-root": "/scratch/x"},
                bundle={"source": "computemcp-slurm", "provision-env": "x"},
            )
        )


def test_bundle_provision_env_requires_tunnel():
    raw = {
        "server": {"listen": "127.0.0.1", "port": 2222},
        "clients": {"alpaka": {"token": "secret", "targets": ["h"]}},
        "targets": {
            "h": {
                "transport": "direct",
                "user": "agent",
                "bundle": {
                    "source": "computemcp-slurm",
                    "provision-env": ["module load apptainer"],
                },
            }
        },
    }
    with pytest.raises(ConfigError, match="provision-env requires tunnel"):
        parse_config(raw)


def test_bundle_without_provision_command_loads():
    cfg = parse_config(
        base_raw(
            client_key="/home/user/.ssh/key",
            container={"runtime": "apptainer", "storage-root": "/scratch/x"},
            bundle={"source": "computemcp-slurm"},
        )
    )
    assert cfg.targets["hal"].bundle is not None
    assert cfg.targets["hal"].provision_command == ()


# -- append_include ----------------------------------------------------------

def _include_base(tmp_path, include_text=""):
    from compute_mcp.config import append_include  # noqa: F401

    tokens = tmp_path / "tokens.toml"
    tokens.write_text('[tokens]\n"a" = "sha256:' + "a" * 64 + '"\n')
    body = include_text + (
        "[server]\nport = 2222\n\n"
        f'[auth]\ntoken_file = "{tokens}"\n\n'
        '[clients.a]\ntargets = ["*"]\n'
    )
    path = tmp_path / "config.toml"
    path.write_text(body)
    (tmp_path / "systems").mkdir(exist_ok=True)
    for name in ("a", "b"):
        (tmp_path / "systems" / f"{name}.toml").write_text(
            f'[targets.{name}]\nssh_targets = ["{name}"]\nuser = "agent"\n'
        )
    return path


def test_append_include_inserts_when_absent(tmp_path):
    from compute_mcp.config import append_include, load_config

    path = _include_base(tmp_path)
    append_include(path, "systems/a.toml")
    assert "systems/a.toml" in path.read_text()
    assert "a" in load_config(path).targets


def test_append_include_is_idempotent(tmp_path):
    from compute_mcp.config import append_include

    path = _include_base(tmp_path)
    append_include(path, "systems/a.toml")
    append_include(path, "systems/a.toml")
    assert path.read_text().count("systems/a.toml") == 1


def test_append_include_extends_single_line(tmp_path):
    from compute_mcp.config import append_include, load_config

    path = _include_base(tmp_path, 'include = ["systems/a.toml"]\n\n')
    append_include(path, "systems/b.toml")
    assert set(load_config(path).targets) == {"a", "b"}


def test_append_include_extends_multi_line(tmp_path):
    from compute_mcp.config import append_include, load_config

    path = _include_base(tmp_path, 'include = [\n    "systems/a.toml",\n]\n\n')
    append_include(path, "systems/b.toml")
    assert set(load_config(path).targets) == {"a", "b"}


def test_append_include_reverts_on_invalid(tmp_path):
    from compute_mcp.config import ConfigError, append_include

    path = _include_base(tmp_path)
    before = path.read_text()
    with pytest.raises(ConfigError):
        append_include(path, "systems/missing.toml")
    assert path.read_text() == before


def test_append_include_handles_bracket_on_last_entry_line(tmp_path):
    from compute_mcp.config import append_include, load_config

    path = _include_base(tmp_path, 'include = [\n    "systems/a.toml"]\n\n')
    append_include(path, "systems/b.toml")
    assert set(load_config(path).targets) == {"a", "b"}


def test_append_include_ignores_bracket_in_comment(tmp_path):
    from compute_mcp.config import append_include, load_config

    path = _include_base(
        tmp_path, 'include = [\n    "systems/a.toml",  # see ]\n]\n\n'
    )
    append_include(path, "systems/b.toml")
    assert set(load_config(path).targets) == {"a", "b"}


def test_append_include_ignores_nested_include_like_key(tmp_path):
    from compute_mcp.config import ConfigError, append_include

    tokens = tmp_path / "tokens.toml"
    tokens.write_text('[tokens]\n"c" = "sha256:' + "a" * 64 + '"\n')
    path = tmp_path / "config.toml"
    # A nested key that starts with "include" must not be mistaken for the
    # root-level array; the edit fails cleanly and the file is left untouched.
    path.write_text(
        "[server]\n"
        'include = "nested-not-the-array"\n'
        "port = 2222\n\n"
        f'[auth]\ntoken_file = "{tokens}"\n\n'
        '[clients.c]\ntargets = ["*"]\n'
    )
    before = path.read_text()
    with pytest.raises(ConfigError):
        append_include(path, "systems/a.toml")
    assert path.read_text() == before

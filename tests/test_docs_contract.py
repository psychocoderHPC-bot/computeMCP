# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Documentation/skill contract: names match the shipped implementation.

The multi-job design requires the operator docs and the agent skill to describe
the operations that actually exist, and the bundle to ship every helper it
references.  These checks are intentionally name-based: they catch a renamed CLI
subcommand or MCP tool that the docs still spell the old way.
"""

from __future__ import annotations

import importlib.resources
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
BUNDLE = REPO / "src" / "compute_mcp" / "bundles" / "computemcp-slurm"
CLI_COMMANDS = (
    "target-open",
    "target-shared",
    "target-sbatch",
    "job-list",
    "job-status",
    "job-wait",
    "job-stop",
)
MCP_JOBS = (
    "computeMCP_job_start",
    "computeMCP_job_list",
    "computeMCP_job_status",
    "computeMCP_job_wait",
    "computeMCP_job_stop",
)
HELPERS = (
    "computemcp-provision.sh",
    "computemcp-container.sh",
    "computemcp-job.sh",
    "computemcp-relay.py",
    "computemcp-mux.py",
)


def test_bundle_helpers_exist_and_are_packaged():
    assert sorted(p.name for p in BUNDLE.iterdir() if p.is_file() and p.name in HELPERS) == sorted(HELPERS)
    packaged = importlib.resources.files("compute_mcp.bundles").joinpath(
        "computemcp-slurm"
    )
    for name in HELPERS:
        assert (packaged / name).is_file(), name
    pyproject = (REPO / "pyproject.toml").read_text()
    assert '"computemcp-slurm/*.sh"' in pyproject
    assert '"computemcp-slurm/*.py"' in pyproject


def test_docs_describe_every_cli_command():
    operations = (REPO / "docs" / "operations.md").read_text()
    for command in CLI_COMMANDS:
        assert command in operations, f"docs/operations.md omits {command}"
    mention = re.compile("|".join(re.escape(c) for c in CLI_COMMANDS))
    assert mention.search((REPO / "docs" / "architecture.md").read_text())
    assert mention.search((REPO / "docs" / "troubleshooting.md").read_text())


def test_skill_names_match_mcp_tools():
    server = (REPO / "src" / "compute_mcp" / "mcp_server.py").read_text()
    skill = (REPO / "skills" / "computeMCP" / "SKILL.md").read_text()
    for tool in MCP_JOBS:
        assert f"async def {tool}(" in server, f"mcp_server.py missing {tool}"
        assert tool in skill, f"skill omits {tool}"
    # The skill must teach the non-submitting wait loop and job_id selection.
    assert "Do not submit again" in skill
    assert "job_id" in skill

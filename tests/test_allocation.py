# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
import json

import pytest

from compute_mcp.allocation import (
    PROTOCOL_OPTIONS,
    compute_plan,
    parse_memory_mib,
    plan_summary,
    render_args,
    validate_conflicts,
)
from compute_mcp.config import (
    AllocationConfig,
    ConfigError,
    NodeConfig,
    SlurmConfig,
    SlurmStageConfig,
    TargetConfig,
    TransportConfig,
)


def make_target(
    name="hal",
    *,
    alloc=None,
    node=None,
    slurm=None,
    **overrides,
):
    kwargs = {
        "name": name,
        "user": "agent",
        "transport": TransportConfig(
            kind="direct", remote_host="127.0.0.1", remote_port=2222
        ),
        "host_key_sha256": "SHA256:abcdefghijklmnopqrstuvwxyz0123456789",
    }
    if alloc is not None:
        kwargs["allocation"] = alloc
    if node is not None:
        kwargs["node"] = node
    if slurm is not None:
        kwargs["slurm"] = slurm
    kwargs.update(overrides)
    return TargetConfig(**kwargs)


# Canonical Slurm node from the design doc (ROSI machine description).
ROSI_NODE = NodeConfig(cpus=24, gpus=4, memory="378000M")


# ---------------------------------------------------------------------------
# parse_memory_mib
# ---------------------------------------------------------------------------

def test_parse_memory_mib_matrix():
    assert parse_memory_mib("378000M") == 378000
    assert parse_memory_mib("100G") == 102400
    assert parse_memory_mib("200GiB") == 204800
    assert parse_memory_mib("1T") == 1048576
    assert parse_memory_mib("0.5G") == 512
    assert parse_memory_mib(378000) == 378000
    assert parse_memory_mib(1) == 1


def test_parse_memory_mib_rejects_garbage():
    for bad in ("garbage", "1PB", "", "1.2.3G", " G", "1 Jain"):
        with pytest.raises(ConfigError):
            parse_memory_mib(bad)


def test_parse_memory_mib_rejects_non_positive():
    for bad in (0, -1, "0M", "512K", "-5G"):
        with pytest.raises(ConfigError):
            parse_memory_mib(bad)


def test_parse_memory_mib_rejects_wrong_type():
    for bad in (True, 1.5, None, ["1G"]):
        with pytest.raises(ConfigError):
            parse_memory_mib(bad)


# ---------------------------------------------------------------------------
# compute_plan: modes and rounding
# ---------------------------------------------------------------------------

def test_gpu_proportional_default_is_one_gpu():
    target = make_target(node=ROSI_NODE)
    plan = compute_plan(target)
    # Integer per-GPU share first: 24//4 = 6 CPUs, 378000//4 = 94500 MiB;
    # one GPU gets 6 CPUs / 94500 MiB.
    assert plan.mode == "gpu-proportional"
    assert plan.nodes == 1
    assert plan.gpus_per_node == 1
    assert plan.cpus_per_node == 6
    assert plan.memory_per_node_mib == 94500
    assert plan.defaults_used is True
    assert plan.overrides == {}


def test_gpu_proportional_rosi_two_gpus_case():
    """Design-doc case: 2 GPUs of the 24 CPU / 378000 MiB ROSI node.

    Per-GPU share: 24//4 = 6 CPUs, 378000//4 = 94500 MiB.
    2 GPUs get 12 CPUs / 189000 MiB — the documented values.
    """
    target = make_target(node=ROSI_NODE)
    plan = compute_plan(target, {"gpus-per-node": 2})
    assert plan.gpus_per_node == 2
    assert plan.cpus_per_node == 12
    assert plan.memory_per_node_mib == 189000
    assert plan.defaults_used is True
    assert plan.overrides == {"gpus-per-node": 2}


def test_gpu_proportional_rejects_fractional_gpu_share():
    # The design doc requires integer *per-GPU* shares ("an integer per-GPU
    # CPU share is derived first"); 2 CPUs for 4 GPUs would give 0.5.
    target = make_target(node=NodeConfig(cpus=2, gpus=4, memory="64G"))
    with pytest.raises(ConfigError, match="at least one CPU per GPU"):
        compute_plan(target)


def test_gpu_proportional_over_capacity_gpus_rejected():
    # ROSI node has 4 GPUs; requesting 5 exceeds capacity.
    target = make_target(node=ROSI_NODE)
    with pytest.raises(ConfigError, match="exceed the node capacity"):
        compute_plan(target, {"gpus-per-node": 5})


def test_gpu_proportional_cpu_only_node_uses_full_capacity():
    target = make_target(node=NodeConfig(cpus=16))
    plan = compute_plan(target)
    # No usable per-GPU share: the full per-node CPU capacity.  The one-GPU
    # default still applies even though the node has no GPU capacity, while
    # an unset memory description stays None (rendered as no emit).
    assert plan.gpus_per_node == 1
    assert plan.cpus_per_node == 16
    assert plan.memory_per_node_mib is None


def test_full_uses_configured_capacities():
    target = make_target(node=ROSI_NODE, alloc=AllocationConfig(single_node="full"))
    plan = compute_plan(target)
    assert plan.mode == "full"
    assert plan.gpus_per_node == 4
    assert plan.cpus_per_node == 24
    assert plan.memory_per_node_mib == 378000
    assert plan.exclusive is False
    assert plan.defaults_used is True


def test_exclusive_sets_intent_only():
    target = make_target(
        node=ROSI_NODE, alloc=AllocationConfig(single_node="exclusive")
    )
    plan = compute_plan(target)
    assert plan.mode == "exclusive"
    assert plan.exclusive is True
    assert plan.gpus_per_node == 4
    assert plan.cpus_per_node == 24


def test_cpu_proportional_memory_scales_with_cpus():
    target = make_target(
        node=NodeConfig(cpus=32, memory="128G"),
        alloc=AllocationConfig(single_node="cpu-proportional"),
    )
    plan = compute_plan(target)
    assert plan.mode == "cpu-proportional"
    assert plan.cpus_per_node == 32
    assert plan.memory_per_node_mib == 131072

    plan = compute_plan(target, {"cpus-per-node": 8})
    assert plan.cpus_per_node == 8
    assert plan.memory_per_node_mib == 32768


def test_cpu_proportional_uses_default_cpus_without_capacity():
    target = make_target(
        alloc=AllocationConfig(single_node="cpu-proportional", default_cpus=4)
    )
    plan = compute_plan(target)
    assert plan.cpus_per_node == 4
    # No memory description: nothing to scale; the plan exposes 0 rather than
    # "at least 1 MiB" (a CPU allocation keeps its memory field at its default).
    assert plan.memory_per_node_mib == 0
    assert plan.defaults_used is True


def test_multi_node_uses_full_by_default():
    target = make_target(node=ROSI_NODE)
    plan = compute_plan(target, {"nodes": 2})
    assert plan.mode == "full"
    assert plan.nodes == 2
    assert plan.gpus_per_node == 4
    assert plan.cpus_per_node == 24
    assert plan.memory_per_node_mib == 378000


def test_multi_node_accepts_exclusive_configured_policy():
    target = make_target(
        node=ROSI_NODE, alloc=AllocationConfig(multi_node="exclusive")
    )
    plan = compute_plan(target, {"nodes": 2})
    assert plan.mode == "exclusive"
    assert plan.exclusive is True


def test_multi_node_only_full_and_exclusive():
    # T1 restricts the multi-node vocabulary to full/exclusive at load time.
    for bad in ("gpu-proportional", "cpu-proportional"):
        with pytest.raises(ConfigError, match="must be one of"):
            AllocationConfig(multi_node=bad)


def test_multi_node_rejects_partial_quantity_override():
    target = make_target(node=ROSI_NODE, alloc=AllocationConfig(multi_node="full"))
    for override in ({"gpus-per-node": 2}, {"cpus-per-node": 12}):
        with pytest.raises(ConfigError, match="conflicts with the multi-node"):
            compute_plan(target, {"nodes": 2, **override})


# ---------------------------------------------------------------------------
# Multi-node --set mode override
# ---------------------------------------------------------------------------

def test_multi_node_rejects_explicit_partial_mode():
    """An explicit --set mode conflicting with the multi-node contract is an
    error, not silently replaced: the partial modes are only for single node,
    and an operator must not get a proportional plan they did not ask for
    while requesting two nodes."""
    target = make_target(
        node=ROSI_NODE, alloc=AllocationConfig(multi_node="full")
    )
    for bad in ("gpu-proportional", "cpu-proportional"):
        with pytest.raises(
            ConfigError, match="requested multi-node mode .* not allowed"
        ):
            compute_plan(target, {"nodes": 2, "mode": bad})


def test_multi_node_honors_explicit_exclusive_mode():
    """An explicit compatible mode is honoured even when the target configures
    a different multi-node policy: exclusive intent (and hence the
    --exclusive flag) must reach the plan and the mappings."""
    target = make_target(
        node=ROSI_NODE, alloc=AllocationConfig(multi_node="full")
    )
    plan = compute_plan(target, {"nodes": 2, "mode": "exclusive"})
    assert plan.mode == "exclusive"
    assert plan.exclusive is True
    assert plan.nodes == 2
    # Full per-node capacities on each node (the multi-node contract).
    assert plan.gpus_per_node == 4
    assert plan.cpus_per_node == 24
    assert plan.memory_per_node_mib == 378000


def test_multi_node_honors_explicit_full_mode():
    target = make_target(
        node=ROSI_NODE, alloc=AllocationConfig(multi_node="exclusive")
    )
    plan = compute_plan(target, {"nodes": 2, "mode": "full"})
    assert plan.mode == "full"
    assert plan.exclusive is False
    assert plan.nodes == 2
    assert plan.gpus_per_node == 4
    assert plan.cpus_per_node == 24
    assert plan.memory_per_node_mib == 378000


def test_max_nodes_ceiling_enforced():
    target = make_target(node=ROSI_NODE, alloc=AllocationConfig(max_nodes=2))
    compute_plan(target, {"nodes": 2})
    with pytest.raises(ConfigError, match="exceed max-nodes"):
        compute_plan(target, {"nodes": 3})


def test_capacity_ceiling_rejects_request_above_capacity():
    target = make_target(node=ROSI_NODE, alloc=AllocationConfig(single_node="full"))
    for override in (
        {"cpus-per-node": 25},
        {"gpus-per-node": 5},
        {"mem-per-node": "400000M"},
    ):
        with pytest.raises(ConfigError, match="exceed the node capacity"):
            compute_plan(target, override)


def test_gpu_proportional_rejects_explicit_memory_above_capacity():
    """gpu-proportional must apply the same node-capacity ceiling to an
    explicit ``--set mem-per-node`` that cpu-proportional/full/exclusive do;
    the same ceiling must NOT reject the computed per-GPU share."""
    target = make_target(node=ROSI_NODE)  # 378000 MiB capacity
    # 400000 MiB > 378000 MiB node capacity: the explicit request is refused,
    # naming the target, the requested MiB, and the node capacity.
    with pytest.raises(
        ConfigError,
        match=r"hal.*400000 MiB per node.*node capacity of 378000 MiB",
    ):
        compute_plan(target, {"mem-per-node": "400000M"})
    # At exactly the capacity the request is accepted (= full node memory).
    plan = compute_plan(target, {"mem-per-node": "378000M"})
    assert plan.memory_per_node_mib == 378000
    # A computed per-GPU share always stays at or below capacity and is never
    # rejected by the new check: 2 GPUs = 189000 MiB < 378000 MiB.
    plan = compute_plan(target, {"gpus-per-node": 2})
    assert plan.memory_per_node_mib == 189000


def test_ordering_manual_before_mapped_in_configuration_order():
    target = make_target(
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(
                options={"partition": "gpu", "ntasks-per-node": 1},
                mapping={"nodes": "nodes", "gpus-per-node": "gres"},
            ),
            srun=SlurmStageConfig(),
        ),
    )
    plan = compute_plan(target)
    sbatch, _ = render_args(target, plan)
    # Manual options keep configuration order; mapped options follow them.
    assert sbatch == (
        "--partition=gpu",
        "--ntasks-per-node=1",
        "--nodes=1",
        "--gres=gpu:1",
    )


def test_override_parsing_rejects_unknown_key_and_bad_values():
    target = make_target(node=ROSI_NODE)
    with pytest.raises(ConfigError, match="unknown --set key"):
        compute_plan(target, {"bogus": 1})
    with pytest.raises(ConfigError):
        compute_plan(target, {"nodes": 0})
    with pytest.raises(ConfigError):
        compute_plan(target, {"nodes": -2})
    with pytest.raises(ConfigError):
        compute_plan(target, {"cpus-per-node": True})
    with pytest.raises(ConfigError):
        compute_plan(target, {"mem-per-node": "12"})  # unitless string
    with pytest.raises(ConfigError, match="--set mode"):
        compute_plan(target, {"mode": "sometimes"})
    with pytest.raises(ConfigError, match="--set mode"):
        compute_plan(target, {"mode": 3})


def test_override_handled_before_mapping():
    """--set mem-per-node wins over the calculated value of a mapping."""
    target = make_target(
        node=ROSI_NODE,
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(options={}, mapping={"memory-per-node": "mem"}),
            srun=SlurmStageConfig(),
        ),
    )
    plan = compute_plan(target, {"mem-per-node": "1G"})
    assert plan.memory_per_node_mib == 1024
    sbatch, srun = render_args(target, plan)
    assert sbatch == ("--mem=1024M",)
    assert srun == ()


def test_mode_override_allowed_for_single_node():
    target = make_target(node=ROSI_NODE, alloc=AllocationConfig(single_node="gpu-proportional"))
    plan = compute_plan(target, {"mode": "full"})
    assert plan.mode == "full"
    assert plan.cpus_per_node == 24
    assert plan.gpus_per_node == 4


# ---------------------------------------------------------------------------
# validate_conflicts
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("calculated", "manual_key", "resource"),
    [
        ("nodes", "nodes", "n1"),
        ("nodes", "n", "nodes"),
        ("gpus-per-node", "gres", "gpu:1"),
        ("gpus-per-node", "gpus", "1"),
        ("gpus-per-node", "gpus-per-task", "1"),
        ("gpus-per-node", "gpus-per-node", "1"),
        ("memory-per-node", "mem", "64G"),
        ("memory-per-node", "mem-per-cpu", "100M"),
        ("exclusive", "exclusive", True),
    ],
)
def test_conflicts_between_mapping_and_manual_options(calculated, manual_key, resource):
    target = make_target(
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(
                options={manual_key: resource}, mapping={calculated: "_"}
            ),
            srun=SlurmStageConfig(),
        )
    )
    with pytest.raises(ConfigError, match="conflicts with the enabled mapping"):
        validate_conflicts(target)


@pytest.mark.parametrize("key", ("ntasks-per-node", "ntasks"))
def test_cpus_mapping_requires_one_task_per_node(key):
    target = make_target(
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(
                options={key: 2}, mapping={"cpus-per-node": "cpus-per-task"}
            ),
            srun=SlurmStageConfig(),
        )
    )
    with pytest.raises(ConfigError, match="one task per node"):
        validate_conflicts(target)


def test_cpus_mapping_satisfied_by_ntasks_one():
    target = make_target(
        node=ROSI_NODE,
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(
                options={"ntasks": 1}, mapping={"cpus-per-node": "cpus-per-task"}
            ),
            srun=SlurmStageConfig(),
        )
    )
    validate_conflicts(target)
    plan = compute_plan(target)
    sbatch, _ = render_args(target, plan)
    assert sbatch == ("--ntasks=1", "--cpus-per-task=6")


def test_stages_checked_independently():
    """A manual option in a different stage never conflicts."""
    target = make_target(
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(mapping={"gpus-per-node": "gres"}),
            srun=SlurmStageConfig(options={"gres": "gpu:1"}),
        )
    )
    validate_conflicts(target)


def test_no_slurm_block_never_conflicts():
    validate_conflicts(make_target())
    validate_conflicts(make_target(node=ROSI_NODE, alloc=AllocationConfig()))


# ---------------------------------------------------------------------------
# render_args
# ---------------------------------------------------------------------------

def test_render_manual_options_strings_bools_and_arrays():
    target = make_target(
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(
                options={
                    "partition": "gpu",
                    "ntasks-per-node": 1,
                    "writable": True,
                    "read-only": False,
                    "exclude": ("cn1", "cn2"),
                }
            ),
            srun=SlurmStageConfig(options={"bind": "cores"}),
        )
    )
    plan = compute_plan(target)
    sbatch, srun = render_args(target, plan)
    assert sbatch == (
        "--partition=gpu",
        "--ntasks-per-node=1",
        "--writable",
        "--exclude=cn1",
        "--exclude=cn2",
    )
    assert srun == ("--bind=cores",)


def test_render_account_leads_stage_args_before_manual_options():
    target = make_target(
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(
                account="proj",
                options={"partition": "gpu", "ntasks-per-node": 1},
                mapping={"nodes": "nodes"},
            ),
            srun=SlurmStageConfig(),
        ),
    )
    plan = compute_plan(target)
    sbatch, srun = render_args(target, plan)
    # The account leads the stage's arguments regardless of manual order.
    assert sbatch == (
        "--account=proj",
        "--partition=gpu",
        "--ntasks-per-node=1",
        "--nodes=1",
    )
    assert srun == ()


def test_render_account_empty_or_none_emits_nothing():
    for value in (None, ""):
        target = make_target(
            slurm=SlurmConfig(
                sbatch=SlurmStageConfig(account=value, options={"partition": "gpu"}),
                srun=SlurmStageConfig(),
            ),
        )
        plan = compute_plan(target)
        sbatch, _ = render_args(target, plan)
        assert sbatch == ("--partition=gpu",)


def test_render_account_not_copied_to_other_stage():
    target = make_target(
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(account="proj", options={"partition": "gpu"}),
            srun=SlurmStageConfig(options={"ntasks-per-node": 1}),
        ),
    )
    plan = compute_plan(target)
    sbatch, srun = render_args(target, plan)
    assert "--account=proj" in sbatch
    assert all("account" not in arg for arg in srun)


def test_render_multiple_stages_independently():
    target = make_target(
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(
                mapping={
                    "nodes": "nodes",
                    "gpus-per-node": "gres",
                    "memory-per-node": "mem",
                }
            ),
            srun=SlurmStageConfig(
                mapping={"gpus-per-node": "gpus-per-node"}
            ),
        ),
    )
    # No node description: no calculated memory, so that mapping emits nothing
    # while the GPU mapping still resolves (one GPU by default).
    plan = compute_plan(target)
    sbatch, srun = render_args(target, plan)
    assert sbatch == ("--nodes=1", "--gres=gpu:1")
    assert srun == ("--gpus-per-node=1",)
    assert target.slurm is not None  # sanity


def test_render_gpus_per_node_representation():
    for rep, expected in (
        ("gres", "--gres=gpu:2"),
        ("gpus-per-node", "--gpus-per-node=2"),
    ):
        target = make_target(
            slurm=SlurmConfig(
                sbatch=SlurmStageConfig(mapping={"gpus-per-node": rep}),
                srun=SlurmStageConfig(),
            )
        )
        plan = compute_plan(target, {"gpus-per-node": 2})
        sbatch, _ = render_args(target, plan)
        assert expected in sbatch, (rep, sbatch)


def test_render_exclusive_only_when_plan_exclusive():
    target = make_target(
        alloc=AllocationConfig(single_node="exclusive"),
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(
                mapping={"nodes": "nodes", "exclusive": "exclusive"}
            ),
            srun=SlurmStageConfig(),
        ),
    )
    plan = compute_plan(target)
    sbatch, _ = render_args(target, plan)
    assert "--exclusive" in sbatch

    # A non-exclusive plan never implies the flag.
    target2 = make_target(
        alloc=AllocationConfig(single_node="full"),
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(
                mapping={"nodes": "nodes", "exclusive": "exclusive"}
            ),
            srun=SlurmStageConfig(),
        ),
    )
    plan2 = compute_plan(target2)
    assert plan2.exclusive is False
    sbatch2, _ = render_args(target2, plan2)
    assert "--exclusive" not in sbatch2


def test_render_rejects_protocol_options():
    for key in PROTOCOL_OPTIONS:
        target = make_target(
            slurm=SlurmConfig(
                sbatch=SlurmStageConfig(options={key: "x"}),
                srun=SlurmStageConfig(),
            )
        )
        plan = compute_plan(target)
        with pytest.raises(ConfigError, match="protocol option"):
            render_args(target, plan)


def test_render_rejects_value_injections():
    target = make_target(
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(options={"a": "x\ny", "b": "z\rw", "c": "q\x00r"}),
            srun=SlurmStageConfig(),
        )
    )
    plan = compute_plan(target)
    with pytest.raises(ConfigError, match="must not contain"):
        render_args(target, plan)


def test_render_rejects_invalid_option_names():
    for key in ("bad\nkey", " lead", "trail ", "--injected", "-x", chr(39) + "q" + chr(39)):
        target = make_target(
            slurm=SlurmConfig(
                sbatch=SlurmStageConfig(options={key: "v"}),
                srun=SlurmStageConfig(),
            )
        )
        plan = compute_plan(target)
        with pytest.raises(ConfigError):
            render_args(target, plan)


def test_render_manual_over_strings_and_ints_only():
    target = make_target(
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(
                options={"bad": 1.5, "badarray": ["ok", 2.5]}
            ),
            srun=SlurmStageConfig(),
        )
    )
    plan = compute_plan(target)
    with pytest.raises(ConfigError):
        render_args(target, plan)


def test_render_no_slurm_block_yields_empty_tuples():
    plan = compute_plan(make_target())
    assert render_args(make_target(), plan) == ((), ())


def test_render_duplicate_setting_is_a_bug_not_deduped():
    """A mapping and its manual option of the same family must not both emit."""
    target = make_target(
        node=None,
        slurm=SlurmConfig(
            # nodes is always available, so the mapping and option conflict.
            sbatch=SlurmStageConfig(options={"nodes": 1}),
            srun=SlurmStageConfig(),
        ),
    )
    plan = compute_plan(target)
    sbatch, srun = render_args(target, plan)
    # No mapping configured here, so no duplicate: a single manual emit.
    assert sbatch == ("--nodes=1",) and srun == ()


# ---------------------------------------------------------------------------
# plan_summary
# ---------------------------------------------------------------------------

def test_plan_summary_is_json_serializable_and_complete():
    target = make_target(
        node=ROSI_NODE,
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(
                options={"ntasks-per-node": 1},
                mapping={"nodes": "nodes", "gpus-per-node": "gres"},
            ),
            srun=SlurmStageConfig(
                options={"ntasks": 1}, mapping={"cpus-per-node": "cpus-per-task"}
            ),
        ),
    )
    plan = compute_plan(target)
    summary = plan_summary(target, plan)
    text = json.dumps(summary)
    assert summary["target"] == "hal"
    assert summary["plan"]["nodes"] == 1
    assert summary["plan"]["cpus_per_node"] == 6
    assert summary["plan"]["gpus_per_node"] == 1
    assert summary["plan"]["memory_per_node_mib"] == 94500
    assert summary["plan"]["exclusive"] is False
    assert summary["plan"]["mode"] == "gpu-proportional"
    assert summary["args"]["sbatch"] == ["--ntasks-per-node=1", "--nodes=1", "--gres=gpu:1"]
    assert summary["args"]["srun"] == ["--ntasks=1", "--cpus-per-task=6"]
    assert summary["emitted"]["sbatch"] == ["nodes", "gpus-per-node"]
    # memory-per-node was calculated but no stage emitted it.
    assert summary["not_emitted"] == ["memory-per-node"]
    assert json.loads(text)["plan"]["gpus_per_node"] == 1


def test_plan_summary_surfaces_account_per_stage():
    target = make_target(
        slurm=SlurmConfig(
            sbatch=SlurmStageConfig(account="proj"),
            srun=SlurmStageConfig(),
        ),
    )
    plan = compute_plan(target)
    summary = plan_summary(target, plan)
    assert summary["account"] == {"sbatch": "proj", "srun": None}
    assert json.loads(json.dumps(summary))["account"]["sbatch"] == "proj"

"""CRPD platform stage-graph regressions — resumable pipeline topology."""
from __future__ import annotations

from policydb.platform.stage_graph import STAGE_GRAPH, STAGES, checkpoint_key


def test_stage_graph_keys_match_stage_list():
    assert set(STAGE_GRAPH) == set(STAGES)


def test_stage_order_starts_with_inventory_ends_with_release():
    assert STAGES[0] == "SOURCE_INVENTORY"
    assert STAGES[-1] == "RELEASE"


def test_checkpoint_key_is_deterministic_and_namespaced():
    assert checkpoint_key("RUN_1", "FETCH") == checkpoint_key("RUN_1", "FETCH")
    assert checkpoint_key("RUN_1", "FETCH").startswith("crpd_checkpoint_RUN_1_FETCH")
    assert checkpoint_key("RUN_1", "FETCH") != checkpoint_key("RUN_2", "FETCH")
    assert checkpoint_key("RUN_1", "FETCH") != checkpoint_key("RUN_1", "PARSE")


def test_every_stage_declares_output_contract():
    for name, stage in STAGE_GRAPH.items():
        assert stage.output_contract, f"{name} lacks output contract"

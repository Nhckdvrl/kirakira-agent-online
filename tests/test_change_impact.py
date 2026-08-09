from scripts.check_change_impact import impacted_tests


def test_context_change_selects_semantic_and_runtime_contracts() -> None:
    selected = impacted_tests(["agent/model_runtime/query_compaction.py"])
    assert "tests/semantic/test_context_history_contract.py" in selected
    assert "tests/test_query_compaction.py" in selected


def test_scheduler_change_stays_targeted_but_keeps_global_boundaries() -> None:
    selected = impacted_tests(["agent/scheduler.py"])
    assert "tests/test_scheduler.py" in selected
    assert "tests/semantic/test_reference_independence_contract.py" in selected
    assert "tests/test_proactive.py" not in selected

from types import SimpleNamespace

from src.agents.research_diff import ResearchDiffAgent
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.services.research_service import ResearchService


def _f(statement, level="medium"):
    return SimpleNamespace(statement=statement, support_level=level)


def test_diff_detects_new_dropped_shifted_and_sources():
    old = [
        _f("Quantum computers use qubits for computation", "weak"),
        _f("The Brazil economy grew strongly last year", "medium"),
    ]
    new = [
        _f("Quantum computers use qubits for computation", "strong"),  # weak -> strong
        _f("A new drug shows promise in clinical trials", "medium"),   # brand new
    ]
    diff = ResearchDiffAgent().diff(new, old, ["nature.com", "newsite.org"], ["nature.com"])
    assert any("drug" in c for c in diff.new_claims)
    assert any("Brazil" in c for c in diff.dropped_claims)
    assert diff.shifted_claims and diff.shifted_claims[0].old_level == "weak"
    assert diff.shifted_claims[0].new_level == "strong"
    assert diff.new_sources == 1 and "newsite.org" in diff.new_domains


def test_diff_no_changes_when_identical():
    items = [_f("A stable claim about quantum computing systems", "medium")]
    diff = ResearchDiffAgent().diff(items, list(items), ["a.com"], ["a.com"])
    assert not diff.new_claims and not diff.dropped_claims and not diff.shifted_claims
    assert diff.new_sources == 0 and diff.has_changes is False


def test_get_research_diff_empty_without_data():
    svc = ResearchService(task_store=InMemoryTaskStore())
    diff = svc.get_research_diff("does-not-exist")
    assert diff.new_claims == [] and diff.has_changes is False

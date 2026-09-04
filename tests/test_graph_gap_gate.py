import time

import pytest

from src.api.schemas import ResearchRequest, SearchDepth
from src.domain import ReplanRecommendation
from src.repositories import InMemoryTaskStore
from src.services.research_service import ResearchService


def _runner_state(mocker, depth: SearchDepth):
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="research topic", depth=depth),
        task_ids=[],
    )
    analyzer = mocker.Mock()
    analyzer.enable_graph_branching = True
    service = ResearchService(task_store=store, analyzer=analyzer)
    state = service.finalize_graph_runner._build_initial_state(
        research.id,
        research.prompt,
        [],
        depth,
    )
    return service, state


@pytest.mark.parametrize(
    ("depth", "attempts", "deadline_offset", "branch_stalled"),
    [
        (SearchDepth.EASY, 0, 60, False),
        (SearchDepth.MEDIUM, 0, 60, False),
        (SearchDepth.HARD, 1, 60, False),
        (SearchDepth.HARD, 0, -1, False),
        (SearchDepth.HARD, 0, 60, True),
    ],
)
def test_collect_context_skips_gap_analysis_when_branch_is_impossible(
    mocker,
    depth,
    attempts,
    deadline_offset,
    branch_stalled,
):
    mocker.patch("src.graph.research_graph.settings.langgraph_replan_max_loops", 1)
    service, state = _runner_state(mocker, depth)
    suggest_follow_up = mocker.patch.object(service.replan_agent, "suggest_follow_up")
    state["replan_attempts"] = attempts
    state["finalize_deadline"] = time.time() + deadline_offset
    state["branch_stalled"] = branch_stalled

    result = service.finalize_graph_runner._collect_context(state)

    suggest_follow_up.assert_not_called()
    assert result["replan_recommendations"] == []
    assert result["should_replan"] is False


def test_collect_context_runs_gap_analysis_when_branch_is_possible(mocker):
    service, state = _runner_state(mocker, SearchDepth.HARD)
    recommendation = ReplanRecommendation(
        reason="Primary evidence is missing",
        suggested_queries=["research topic primary evidence"],
    )
    suggest_follow_up = mocker.patch.object(
        service.replan_agent,
        "suggest_follow_up",
        return_value=[recommendation],
    )

    result = service.finalize_graph_runner._collect_context(state)

    suggest_follow_up.assert_called_once()
    assert result["replan_recommendations"] == [recommendation.model_dump()]
    assert result["should_replan"] is True

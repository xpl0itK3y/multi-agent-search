"""TRAIL-LANGUAGE: progress-trail details follow the research's stored language, so the
execution trail copied into an English report carries no Russian lines, and loop-back
events stay identifiable by their step/action codes rather than by their text."""

import re

from src.agents.search import SearchAgent
from src.agents.trail_text import TRAIL_DETAILS, trail_detail
from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskStatus, TaskUpdate
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

_CYRILLIC = re.compile(r"[А-Яа-яЁё]")

_REPORT = (
    "## Introduction\nThe roundup ranks the films [S1].\n\n"
    "## Conclusion\nThe official source confirms the ranking [S1].\n\n"
    "## Sources\n- [S1] https://editorial.example.com/roundup"
)


class _BranchingAnalyzer:
    """Stands in for AnalyzerAgent in the deep (HARD) loop without an LLM."""

    llm = None
    enable_graph_branching = True

    def run_analysis(self, prompt, tasks, **kwargs):
        return (
            _REPORT,
            [
                {
                    "source_id": "S1",
                    "url": "https://editorial.example.com/roundup",
                    "title": "Roundup",
                    "domain": "editorial.example.com",
                    "source_quality": "medium",
                    "content": "The roundup ranks the films and the official source confirms it. " * 5,
                }
            ],
        )


def _finalize_hard_run_with_replan(mocker, prompt: str, language: str):
    mocker.patch("src.config.settings.langgraph_replan_max_loops", 1)
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt=prompt, depth=SearchDepth.HARD),
        task_ids=["task-1"],
        language=language,
    )
    store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "initial pass",
            "queries": [prompt],
            "status": TaskStatus.COMPLETED,
            "result": [
                {
                    "url": "https://editorial.example.com/roundup",
                    "domain": "editorial.example.com",
                    "title": "Roundup",
                    "content": "Editorial roundup content. " * 20,
                    "source_quality": "medium",
                }
            ],
            "search_metrics": {"selected_source_count": 1},
        }
    )
    service = ResearchService(task_store=store, analyzer=_BranchingAnalyzer())

    def fake_run_search_task(task_id, depth):
        store.update_task(
            task_id,
            TaskUpdate(
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://official.example.com/source",
                        "domain": "official.example.com",
                        "title": "Official Source",
                        "content": "Primary-source style content. " * 30,
                        "source_quality": "high",
                    }
                ],
                log="completed second pass",
            ),
        )

    mocker.patch.object(service, "run_search_task", side_effect=fake_run_search_task)
    finalized = service.finalize_research(research.id)
    assert finalized.status == ResearchStatus.COMPLETED
    return store.get_research(research.id)


def _trail_section(report: str, heading: str) -> str:
    return report[report.index(heading):]


def test_english_hard_report_with_replan_has_no_cyrillic_trail_lines(mocker):
    research = _finalize_hard_run_with_replan(mocker, "best horror films of the decade", "en")

    assert research.graph_state["replan_attempts"] == 1
    trail_section = _trail_section(research.final_report, "## Graph Execution Trail")
    assert "- Step: " in trail_section
    assert not _CYRILLIC.search(trail_section)
    details = [entry.get("detail") or "" for entry in research.graph_trail]
    assert [detail for detail in details if _CYRILLIC.search(detail)] == []
    assert trail_detail("replan", "en") in details
    assert trail_detail("completed", "en") == details[-1]


def test_russian_hard_run_writes_every_trail_detail_in_russian(mocker):
    research = _finalize_hard_run_with_replan(mocker, "лучшие фильмы ужасов десятилетия", "ru")

    trail_section = _trail_section(research.final_report, "## Трасса выполнения графа")
    assert "- Шаг: " in trail_section
    details = [entry.get("detail") or "" for entry in research.graph_trail]
    assert details and all(_CYRILLIC.search(detail) for detail in details)


def test_loop_back_events_carry_stable_step_and_action_codes(mocker):
    research = _finalize_hard_run_with_replan(mocker, "best horror films of the decade", "es")

    replan_events = [entry for entry in research.graph_trail if entry.get("step") == "replan"]
    assert replan_events
    assert {entry["action"] for entry in replan_events} == {"gap_analysis_loop"}
    assert replan_events[0]["detail"] == trail_detail("replan", "es")
    assert "## Traza de ejecución del grafo" in research.final_report


def test_plan_progress_follows_the_stored_language():
    class _Orchestrator:
        def run_decompose(self, prompt, depth):
            return [{"id": "t1", "description": "d", "queries": ["q1"], "status": TaskStatus.PENDING}]

    store = InMemoryTaskStore()
    service = ResearchService(task_store=store, orchestrator=_Orchestrator())
    request = ResearchRequest(prompt="How do solar panels degrade over time?", depth=SearchDepth.EASY)
    _, research_id = service.start_research(request)

    service.decompose_and_enqueue(research_id, request)

    plan_events = {
        entry["step"]: entry["detail"]
        for entry in store.get_research(research_id).graph_trail
        if entry.get("phase") == "plan"
    }
    assert plan_events["plan_start"] == trail_detail("plan_start", "en")
    assert plan_events["decompose"] == trail_detail("decompose", "en", depth="easy")
    assert plan_events["plan_ready"] == trail_detail("plan_ready", "en", count=1)


def test_search_progress_follows_the_research_language(mocker):
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="energía solar en España", depth=SearchDepth.EASY),
        task_ids=["task-1"],
        language="es",
    )
    store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "paneles solares",
            "queries": ["degradación de paneles solares"],
            "status": "pending",
        }
    )
    mocker.patch(
        "src.providers.search.SearchProvider.search",
        return_value=[{"url": "https://docs.python.org/3/tutorial/", "title": "Example"}],
    )
    mocker.patch("src.providers.search.ContentExtractor.extract_content", return_value="Contenido " * 120)

    SearchAgent(task_store=store, max_sources=1).run_task("task-1")

    details = [entry["detail"] for entry in store.get_research(research.id).graph_trail]
    assert details[0] == trail_detail("search_task_start", "es", task="paneles solares")
    assert details[-1] == trail_detail("search_task_complete", "es", task="paneles solares", count=1)
    assert not any(_CYRILLIC.search(detail) for detail in details)


def test_trail_text_table_covers_every_language_and_falls_back_to_english():
    for key, templates in TRAIL_DETAILS.items():
        assert set(templates) == {"en", "ru", "es"}, key
    assert trail_detail("plan_start", "zh") == trail_detail("plan_start", "en")
    assert trail_detail("collect_context_done", "ru", count=3, replan=True).endswith("да")
    assert trail_detail("collect_context_done", "en", count=3, replan=False).endswith("no")

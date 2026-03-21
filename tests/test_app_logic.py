import json

import pytest
from fastapi import HTTPException

from src.agents.analyzer import AnalyzerAgent
from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, SearchTask, TaskStatus, SearchJobStatus
from src.core.llm import LLMProvider
from src.repositories import InMemoryTaskStore
from src.services.research_service import ResearchService
from src.search_depth_profiles import get_depth_profile


class RecordingLLM(LLMProvider):
    def __init__(self, response: str = "ok"):
        self.response = response
        self.calls = []

    def generate(self, system_prompt: str, user_prompt: str, **kwargs) -> str:
        self.calls.append(
            {
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "kwargs": kwargs,
            }
        )
        return self.response


class SequentialLLM(LLMProvider):
    def __init__(self, responses: list[str]):
        self.responses = list(responses)
        self.calls = []

    def generate(self, system_prompt: str, user_prompt: str, **kwargs) -> str:
        self.calls.append(
            {
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "kwargs": kwargs,
            }
        )
        return self.responses.pop(0)


def test_decompose_requires_initialized_orchestrator(mocker):
    service = ResearchService(task_store=InMemoryTaskStore(), orchestrator=None)

    with pytest.raises(HTTPException) as exc_info:
        service.decompose_prompt(
            "test",
            SearchDepth.EASY,
        )

    assert exc_info.value.status_code == 503


def test_analyzer_agent_uses_llm_provider_contract():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            )
        ],
    )

    assert result.endswith("## Sources")
    assert "## Report Notes" in result
    assert len(llm.calls) == 1
    assert llm.calls[0]["system_prompt"] == agent.SYSTEM_PROMPT
    assert "original prompt" in llm.calls[0]["user_prompt"]
    assert llm.calls[0]["kwargs"]["temperature"] == 0.3
    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    assert parsed["gathered_data"][0]["source_id"] == "S1"
    assert parsed["gathered_data"][0]["domain"] == "example.com"
    assert parsed["gathered_data"][0]["source_quality"] == "low"
    assert "Use inline source references like [S1], [S2]" in agent.SYSTEM_PROMPT
    assert "Prefer higher-quality and more authoritative sources when sources conflict" in agent.SYSTEM_PROMPT


def test_analyzer_agent_filters_failed_and_duplicate_sources():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {"url": "https://example.com/a", "title": "Example", "content": "Useful body " * 40},
                    {"url": "https://example.com/b", "title": "Example", "content": "Useful body " * 40},
                    {"url": "https://example.com/c", "title": "Bad", "content": "Failed to extract content"},
                ],
            )
        ],
    )

    assert result.endswith("## Sources")
    assert "## Report Notes" in result
    assert len(llm.calls) == 1
    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    gathered = parsed["gathered_data"]
    assert len(gathered) == 1
    assert gathered[0]["source_id"] == "S1"
    assert gathered[0]["url"] == "https://example.com/a"


def test_analyzer_agent_limits_prepared_source_count():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    tasks = [
        SearchTask(
            id="task-1",
            description="desc",
            queries=["query"],
            status=TaskStatus.COMPLETED,
            result=[
                {
                    "url": f"https://example.com/{index}",
                    "title": f"Title {index}",
                    "content": f"Body {index} " * 100,
                }
                for index in range(30)
            ],
        )
    ]

    agent.run_analysis("original prompt", tasks)

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    gathered = parsed["gathered_data"]
    assert len(gathered) == 20
    assert gathered[0]["source_id"] == "S1"
    assert gathered[-1]["source_id"] == "S20"


def test_analyzer_agent_prefers_trusted_domains_for_similar_sources():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://generic-blog.example/python",
                        "title": "Python Tutorial",
                        "content": "Useful Python tutorial body " * 80,
                    },
                    {
                        "url": "https://docs.python.org/3/tutorial/",
                        "title": "Python Tutorial",
                        "content": "Useful Python tutorial body " * 80,
                    },
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    gathered = parsed["gathered_data"]
    assert len(gathered) == 1
    assert gathered[0]["url"] == "https://docs.python.org/3/tutorial/"
    assert gathered[0]["source_id"] == "S1"


def test_analyzer_agent_preserves_source_quality_metadata_in_payload():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://docs.python.org/3/tutorial/",
                        "domain": "docs.python.org",
                        "source_quality": "high",
                        "title": "Python Tutorial",
                        "content": "Useful Python tutorial body " * 50,
                    }
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    gathered = parsed["gathered_data"]
    assert gathered[0]["domain"] == "docs.python.org"
    assert gathered[0]["source_quality"] == "high"


def test_analyzer_agent_post_processes_sources_heading():
    llm = RecordingLLM(response="Introduction\n\nSources:\n- [S1] https://example.com")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            )
        ],
    )

    assert "## Sources" in result
    assert "Sources:" not in result


def test_analyzer_agent_adds_sources_heading_when_missing():
    llm = RecordingLLM(response="Introduction\n\nConclusion")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            )
        ],
    )

    assert result.endswith("## Sources")


def test_analyzer_agent_rebuilds_sources_from_valid_inline_citations():
    llm = RecordingLLM(
        response=(
            "Introduction [S1] [S9]\n\n"
            "## Sources\n"
            "- [S1] https://wrong.example\n"
            "- [S9] https://wrong.example/two"
        )
    )
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            )
        ],
    )

    assert "[S9]" not in result
    assert "https://wrong.example" not in result
    assert "## Sources\n- [S1] https://example.com" in result


def test_analyzer_agent_retries_once_when_report_language_mismatches_prompt():
    llm = SequentialLLM(
        responses=[
            "## Introduction\nThis report compares APIs. [S1]\n\n## Sources\n- [S1] https://example.com",
            "## Введение\nЭтот отчет сравнивает API. [S1]\n\n## Sources\n- [S1] https://example.com",
        ]
    )
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "Сравни FastAPI и Flask для небольших API",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            )
        ],
    )

    assert len(llm.calls) == 2
    assert "wrong language" in llm.calls[1]["user_prompt"]
    assert "## Введение" in result


def test_analyzer_agent_detects_conflicts_from_overlapping_claims():
    llm = RecordingLLM(response="## Introduction\nComparison body. [S1] [S2]\n\n## Conclusion\nDone.")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "Compare two systems in English",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://example.com/a",
                        "title": "A",
                        "content": "Framework X supports async handlers in production and handles 1000 requests per second reliably.",
                    },
                    {
                        "url": "https://example.com/b",
                        "title": "B",
                        "content": "Framework X does not support async handlers in production and handles 300 requests per second reliably.",
                    },
                ],
            )
        ],
    )

    assert "## Conflicts And Uncertainties" in result
    assert "[S1]" in result
    assert "[S2]" in result


def test_analyzer_agent_passes_detected_conflicts_into_prompt_payload():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "Compare two systems in English",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://example.com/a",
                        "title": "A",
                        "content": "Platform Y has 99 percent uptime in production and supports async jobs.",
                    },
                    {
                        "url": "https://example.com/b",
                        "title": "B",
                        "content": "Platform Y has 92 percent uptime in production and supports async jobs.",
                    },
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    assert parsed["detected_conflicts"]
    assert parsed["detected_conflicts"][0]["source_ids"] == ["S1", "S2"]


def test_analyzer_agent_adds_report_notes_for_missing_structure_and_citations():
    llm = RecordingLLM(response="Plain body without headings or inline citations.")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "Compare systems in English",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            )
        ],
    )

    assert "## Report Notes" in result
    assert "missing a clear introduction heading" in result
    assert "missing a clear conclusion heading" in result
    assert "does not cite any sources inline" in result
    assert "fewer than two usable sources" in result


def test_analyzer_agent_does_not_add_report_notes_for_well_formed_report():
    llm = RecordingLLM(
        response=(
            "## Introduction\nReport intro [S1] [S2]\n\n"
            "## Conclusion\nReport outro [S1].\n\n"
            "## Sources\n- [S1] https://wrong.example\n- [S2] https://wrong.example/two"
        )
    )
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "Compare systems in English",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {"url": "https://example.com/1", "title": "Example 1", "content": "Body one"},
                    {"url": "https://example.com/2", "title": "Example 2", "content": "Body two"},
                ],
            )
        ],
    )

    assert "## Report Notes" not in result


def test_decompose_does_not_schedule_failed_tasks(mocker):
    orchestrator = mocker.Mock()
    orchestrator.run_decompose.return_value = [
        {
            "id": "task-1",
            "description": "bad task",
            "queries": ["query"],
            "status": TaskStatus.FAILED,
        }
    ]
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store, orchestrator=orchestrator)
    response = service.decompose_prompt(
        "test",
        SearchDepth.EASY,
    )

    assert len(response.tasks) == 1
    assert response.tasks[0].status == TaskStatus.FAILED
    assert task_store.get_pending_search_task_jobs() == []


def test_decompose_persists_search_job_for_pending_task(mocker):
    orchestrator = mocker.Mock()
    orchestrator.run_decompose.return_value = [
        {
            "id": "task-1",
            "description": "good task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    ]
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store, orchestrator=orchestrator)

    response = service.decompose_prompt("test", SearchDepth.HARD)

    assert response.tasks[0].status == TaskStatus.PENDING
    job = task_store.get_latest_search_task_job("task-1")
    assert job is not None
    assert job.depth == SearchDepth.HARD


def test_start_research_persists_task_ids_in_task_store(mocker):
    orchestrator = mocker.Mock()
    orchestrator.run_decompose.return_value = [
        {
            "id": "task-1",
            "description": "task one",
            "queries": ["query one"],
            "status": TaskStatus.PENDING,
        },
        {
            "id": "task-2",
            "description": "task two",
            "queries": ["query two"],
            "status": TaskStatus.PENDING,
        },
    ]
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store, orchestrator=orchestrator)

    response = service.start_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
    )

    research = task_store.get_research(response.research_id)
    assert research is not None
    assert research.task_ids == ["task-1", "task-2"]


def test_get_research_status_does_not_rerun_analysis_while_analyzing(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    task_store.update_research_status(research.id, ResearchStatus.ANALYZING)
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    service = ResearchService(task_store=task_store, analyzer=analyzer)

    current = service.get_research_status(research.id)

    assert current.status == ResearchStatus.ANALYZING
    analyzer.run_analysis.assert_not_called()


def test_get_research_status_is_read_only_when_tasks_are_done(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    service = ResearchService(task_store=task_store, analyzer=analyzer)

    current = service.get_research_status(research.id)

    assert current.status == ResearchStatus.PROCESSING
    analyzer.run_analysis.assert_not_called()


def test_finalize_research_runs_analysis_when_tasks_are_complete(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    analyzer.run_analysis.return_value = "final report"
    service = ResearchService(task_store=task_store, analyzer=analyzer)

    finalized = service.finalize_research(research.id)

    assert finalized.status == ResearchStatus.COMPLETED
    assert finalized.final_report == "final report"
    analyzer.run_analysis.assert_called_once()


def test_enqueue_research_finalization_marks_research_analyzing(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    service = ResearchService(task_store=task_store, analyzer=analyzer)

    queued, job = service.enqueue_research_finalization(research.id)

    assert queued.status == ResearchStatus.ANALYZING
    assert job is not None
    assert job.status.value == "pending"
    analyzer.run_analysis.assert_not_called()


def test_enqueue_research_finalization_persists_job_record(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    service = ResearchService(task_store=task_store, analyzer=analyzer)

    queued, job = service.enqueue_research_finalization(research.id)

    assert queued.status == ResearchStatus.ANALYZING
    assert job is not None
    assert job.research_id == research.id
    assert task_store.get_research_finalize_job(job.id) is not None


def test_process_finalize_job_runs_analysis_and_marks_job_completed(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    analyzer.run_analysis.return_value = "background report"
    service = ResearchService(task_store=task_store, analyzer=analyzer)
    _, job = service.enqueue_research_finalization(research.id)

    processed = service.process_finalize_job(job.id)

    assert processed is not None
    assert processed.status.value == "completed"
    assert task_store.get_research(research.id).final_report == "background report"
    analyzer.run_analysis.assert_called_once()


def test_get_research_finalize_job_returns_persisted_job(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    service = ResearchService(task_store=task_store, analyzer=analyzer)
    _, job = service.enqueue_research_finalization(research.id)

    fetched = service.get_research_finalize_job(job.id)

    assert fetched is not None
    assert fetched.id == job.id


def test_get_latest_research_finalize_job_returns_most_recent_job():
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    older = task_store.add_research_finalize_job(research.id)
    newer = task_store.add_research_finalize_job(research.id)

    fetched = ResearchService(task_store=task_store).get_latest_research_finalize_job(research.id)

    assert fetched is not None
    assert fetched.id == newer.id
    assert fetched.id != older.id
    assert fetched.research_id == research.id


def test_process_search_task_job_marks_job_completed(mocker):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    job = task_store.add_search_task_job("task-1", SearchDepth.MEDIUM.value)
    service = ResearchService(task_store=task_store)
    run_search_task = mocker.patch.object(service, "run_search_task")

    processed = service.process_search_task_job(job.id)

    assert processed is not None
    assert processed.status == SearchJobStatus.COMPLETED
    run_search_task.assert_called_once_with("task-1", SearchDepth.MEDIUM)


@pytest.mark.parametrize(
    ("depth", "expected_limit"),
    [
        (SearchDepth.EASY, 5),
        (SearchDepth.MEDIUM, 12),
        (SearchDepth.HARD, 20),
    ],
)
def test_run_search_task_uses_depth_profile_source_limit(mocker, depth, expected_limit):
    service = ResearchService(task_store=InMemoryTaskStore())
    run_task = mocker.patch("src.agents.search.SearchAgent.run_task")
    init = mocker.patch("src.services.research_service.SearchAgent.__init__", return_value=None)

    service.run_search_task("task-1", depth)

    init.assert_called_once_with(task_store=service.task_store, max_sources=expected_limit)
    run_task.assert_called_once_with("task-1")


def test_depth_profiles_keep_medium_as_default_balanced_mode():
    profile = get_depth_profile(SearchDepth.MEDIUM)

    assert profile["label"] == "Balanced"
    assert profile["task_count"] == 4
    assert profile["source_limit"] == 12


def test_process_finalize_job_marks_missing_job_as_none():
    service = ResearchService(task_store=InMemoryTaskStore())

    assert service.process_finalize_job("missing") is None


def test_process_search_task_job_marks_missing_job_as_none():
    service = ResearchService(task_store=InMemoryTaskStore())

    assert service.process_search_task_job("missing") is None


def test_get_latest_search_task_job_returns_persisted_job():
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    job = task_store.add_search_task_job("task-1", SearchDepth.EASY.value)
    service = ResearchService(task_store=task_store)

    fetched = service.get_latest_search_task_job("task-1")

    assert fetched is not None
    assert fetched.id == job.id


def test_get_queue_metrics_reports_dead_letter_counts():
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    search_job = task_store.add_search_task_job("task-1", SearchDepth.EASY.value, max_attempts=1)
    task_store.claim_next_search_task_job()
    task_store.record_search_task_job_failure(search_job.id, "boom")

    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    finalize_job = task_store.add_research_finalize_job(research.id, max_attempts=1)
    task_store.claim_next_research_finalize_job()
    task_store.record_research_finalize_job_failure(finalize_job.id, "boom")

    metrics = ResearchService(task_store=task_store).get_queue_metrics()

    assert metrics.dead_letter_search_jobs == 1
    assert metrics.dead_letter_finalize_jobs == 1


def test_get_worker_heartbeat_returns_latest_value():
    task_store = InMemoryTaskStore()
    task_store.upsert_worker_heartbeat("job-worker", processed_jobs=2, status="busy")

    heartbeat = ResearchService(task_store=task_store).get_worker_heartbeat("job-worker")

    assert heartbeat is not None
    assert heartbeat.worker_name == "job-worker"
    assert heartbeat.processed_jobs == 2


def test_enqueue_research_finalization_fails_immediately_when_all_tasks_failed(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "failed task",
            "queries": ["query"],
            "status": TaskStatus.FAILED,
        }
    )
    analyzer = mocker.Mock()
    service = ResearchService(task_store=task_store, analyzer=analyzer)

    finalized, job = service.enqueue_research_finalization(research.id)

    assert finalized.status == ResearchStatus.FAILED
    assert finalized.final_report == "All tasks failed."
    assert job is None
    analyzer.run_analysis.assert_not_called()


def test_finalize_research_rejects_incomplete_tasks():
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "pending task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    service = ResearchService(task_store=task_store)

    with pytest.raises(HTTPException) as exc_info:
        service.finalize_research(research.id)

    assert exc_info.value.status_code == 409

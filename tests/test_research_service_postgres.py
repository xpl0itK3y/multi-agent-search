import pytest

from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskStatus, TaskUpdate
from src.repositories import SQLAlchemyTaskStore
from src.services.research_service import ResearchService


class StubOrchestrator:
    def run_decompose(self, prompt: str, depth: SearchDepth):
        return [
            {
                "id": "task-1",
                "description": "collect source one",
                "queries": ["query one"],
                "status": TaskStatus.PENDING,
            },
            {
                "id": "task-2",
                "description": "collect source two",
                "queries": ["query two"],
                "status": TaskStatus.PENDING,
            },
        ]


class StubAnalyzer:
    def run_analysis(self, prompt: str, tasks):
        assert prompt == "postgres lifecycle"
        assert len(tasks) == 2
        return "postgres final report"


@pytest.mark.postgres
def test_research_service_full_postgres_lifecycle(postgres_session_factory):
    store = SQLAlchemyTaskStore(postgres_session_factory)

    service = ResearchService(
        task_store=store,
        orchestrator=StubOrchestrator(),
        analyzer=StubAnalyzer(),
    )

    response = service.start_research(
        ResearchRequest(prompt="postgres lifecycle", depth=SearchDepth.EASY),
    )

    tasks = store.get_tasks_by_research(response.research_id)
    assert len(tasks) == 2

    for task in tasks:
        updated = store.update_task(
            task.id,
            TaskUpdate(
                status=TaskStatus.COMPLETED,
                result=[{"url": f"https://{task.id}.example", "title": task.description, "content": "body"}],
                log="completed for lifecycle test",
            ),
        )
        assert updated is not None
        assert updated.status == TaskStatus.COMPLETED

    current = service.get_research_status(response.research_id)
    assert current.status == ResearchStatus.PROCESSING
    assert current.task_ids == ["task-1", "task-2"]

    finalized = service.finalize_research(response.research_id)
    assert finalized.status == ResearchStatus.COMPLETED
    assert finalized.final_report == "postgres final report"
    assert finalized.task_ids == ["task-1", "task-2"]

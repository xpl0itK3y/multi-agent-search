from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskStatus
from src.repositories import InMemoryTaskStore
from src.services import ResearchService
from src.workers import FinalizeWorker


def test_finalize_worker_processes_pending_jobs(mocker):
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
    analyzer.run_analysis.return_value = "worker report"
    service = ResearchService(task_store=task_store, analyzer=analyzer)
    queued, job = service.enqueue_research_finalization(research.id)

    assert queued.status == ResearchStatus.ANALYZING
    assert job is not None

    processed_count = FinalizeWorker(service).run_once()

    assert processed_count == 1
    assert task_store.get_research(research.id).status == ResearchStatus.COMPLETED
    assert task_store.get_research(research.id).final_report == "worker report"

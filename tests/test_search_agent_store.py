from src.agents.search import SearchAgent
from src.api.schemas import TaskStatus
from src.repositories import InMemoryTaskStore


def test_search_agent_updates_through_injected_task_store(mocker):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "test",
            "queries": ["query"],
            "status": "pending",
        }
    )

    mocker.patch("src.providers.search.SearchProvider.search", return_value=[{"url": "http://example.com", "title": "Example"}])
    mocker.patch("src.providers.search.ContentExtractor.extract_content", return_value="Full page content")

    agent = SearchAgent(task_store=task_store, max_sources=1)
    agent.run_task("task-1")

    final_task = task_store.get_task("task-1")
    assert final_task is not None
    assert final_task.status == TaskStatus.COMPLETED
    assert final_task.result[0]["content"] == "Full page content"
    assert "Search completed" in final_task.logs[-1]

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

    mocker.patch(
        "src.providers.search.SearchProvider.search",
        return_value=[{"url": "https://docs.python.org/3/tutorial/", "title": "Example"}],
    )
    mocker.patch("src.providers.search.ContentExtractor.extract_content", return_value="Full page content " * 120)

    agent = SearchAgent(task_store=task_store, max_sources=1)
    agent.run_task("task-1")

    final_task = task_store.get_task("task-1")
    assert final_task is not None
    assert final_task.status == TaskStatus.COMPLETED
    assert final_task.result[0]["content"] == ("Full page content " * 120).strip()
    assert final_task.result[0]["domain"] == "docs.python.org"
    assert final_task.result[0]["content_length"] == len(("Full page content " * 120).strip())
    assert final_task.result[0]["extraction_status"] == "success"
    assert final_task.result[0]["source_quality"] == "high"
    assert "Search completed" in final_task.logs[-1]


def test_search_agent_deduplicates_near_duplicate_content(mocker):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "test",
            "queries": ["query"],
            "status": "pending",
        }
    )

    mocker.patch(
        "src.providers.search.SearchProvider.search",
        return_value=[
            {"url": "https://a.example/article", "title": "Example Article"},
            {"url": "https://b.example/article", "title": "Example Article"},
        ],
    )
    mocker.patch(
        "src.providers.search.ContentExtractor.extract_content",
        side_effect=[
            "Same core content " * 30,
            "Same core content " * 30,
        ],
    )

    agent = SearchAgent(task_store=task_store, max_sources=5)
    agent.run_task("task-1")

    final_task = task_store.get_task("task-1")
    assert final_task is not None
    assert final_task.status == TaskStatus.COMPLETED
    assert len(final_task.result) == 1


def test_search_agent_prefers_extracted_content_over_failed_extract(mocker):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "test",
            "queries": ["query"],
            "status": "pending",
        }
    )

    mocker.patch(
        "src.providers.search.SearchProvider.search",
        return_value=[
            {"url": "http://failed.example", "title": "Weak Source"},
            {"url": "https://good.example", "title": "Strong Source"},
        ],
    )
    mocker.patch(
        "src.providers.search.ContentExtractor.extract_content",
        side_effect=[None, "Useful extracted content " * 20],
    )

    agent = SearchAgent(task_store=task_store, max_sources=1)
    agent.run_task("task-1")

    final_task = task_store.get_task("task-1")
    assert final_task is not None
    assert final_task.status == TaskStatus.COMPLETED
    assert len(final_task.result) == 1
    assert final_task.result[0]["url"] == "https://good.example"
    assert final_task.result[0]["domain"] == "good.example"
    assert final_task.result[0]["extraction_status"] == "success"
    assert "Useful extracted content" in final_task.result[0]["content"]


def test_search_agent_boosts_trusted_domains_when_quality_is_similar(mocker):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "test",
            "queries": ["query"],
            "status": "pending",
        }
    )

    mocker.patch(
        "src.providers.search.SearchProvider.search",
        return_value=[
            {"url": "https://random-blog.example/post", "title": "Python Guide"},
            {"url": "https://docs.python.org/3/tutorial/", "title": "Python Guide"},
        ],
    )
    mocker.patch(
        "src.providers.search.ContentExtractor.extract_content",
        side_effect=[
            "Useful Python tutorial content " * 15,
            "Useful Python tutorial content " * 15,
        ],
    )

    agent = SearchAgent(task_store=task_store, max_sources=1)
    agent.run_task("task-1")

    final_task = task_store.get_task("task-1")
    assert final_task is not None
    assert final_task.status == TaskStatus.COMPLETED
    assert len(final_task.result) == 1
    assert final_task.result[0]["url"] == "https://docs.python.org/3/tutorial/"
    assert final_task.result[0]["domain"] == "docs.python.org"


def test_search_agent_boosts_gov_domains_over_generic_sites(mocker):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "test",
            "queries": ["query"],
            "status": "pending",
        }
    )

    mocker.patch(
        "src.providers.search.SearchProvider.search",
        return_value=[
            {"url": "https://generic-news.example/story", "title": "Travel Advisory"},
            {"url": "https://travel.state.gov/content/travel/en/traveladvisories.html", "title": "Travel Advisory"},
        ],
    )
    mocker.patch(
        "src.providers.search.ContentExtractor.extract_content",
        side_effect=[
            "Current advisory details " * 20,
            "Current advisory details " * 20,
        ],
    )

    agent = SearchAgent(task_store=task_store, max_sources=1)
    agent.run_task("task-1")

    final_task = task_store.get_task("task-1")
    assert final_task is not None
    assert final_task.status == TaskStatus.COMPLETED
    assert len(final_task.result) == 1
    assert final_task.result[0]["url"] == "https://travel.state.gov/content/travel/en/traveladvisories.html"


def test_search_agent_downranks_low_value_social_domains(mocker):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "test",
            "queries": ["query"],
            "status": "pending",
        }
    )

    mocker.patch(
        "src.providers.search.SearchProvider.search",
        return_value=[
            {"url": "https://www.linkedin.com/posts/example", "title": "API Guide"},
            {"url": "https://docs.python.org/3/library/asyncio.html", "title": "Asyncio API Reference"},
        ],
    )
    mocker.patch(
        "src.providers.search.ContentExtractor.extract_content",
        side_effect=[
            "API guide content " * 60,
            "Official documentation and API reference content " * 40,
        ],
    )

    agent = SearchAgent(task_store=task_store, max_sources=1)
    agent.run_task("task-1")

    final_task = task_store.get_task("task-1")
    assert final_task is not None
    assert final_task.result[0]["url"] == "https://docs.python.org/3/library/asyncio.html"
    assert final_task.result[0]["domain"] == "docs.python.org"

from datetime import datetime, timezone

from src.db.models import ResearchORM, SearchResultORM, SearchTaskORM
from src.repositories.mappers import (
    enrich_search_result_dict,
    research_orm_to_record,
    search_result_dicts_to_orm,
    search_task_orm_to_schema,
)


def test_research_orm_to_record_maps_core_fields():
    now = datetime.now(timezone.utc)
    research = ResearchORM(
        id="research-1",
        prompt="prompt",
        depth="easy",
        status="processing",
        task_ids=["task-1"],
        created_at=now,
        updated_at=now,
    )

    record = research_orm_to_record(research)

    assert record.id == "research-1"
    assert record.depth == "easy"
    assert record.task_ids == ["task-1"]


def test_search_task_orm_to_schema_maps_results_and_logs():
    now = datetime.now(timezone.utc)
    task = SearchTaskORM(
        id="task-1",
        research_id="research-1",
        description="desc",
        queries=["query"],
        status="completed",
        logs=["done"],
        created_at=now,
        updated_at=now,
    )
    task.results = [
        SearchResultORM(
            task_id="task-1",
            url="https://example.com",
            title="Example",
            content="Body",
        )
    ]

    schema = search_task_orm_to_schema(task)

    assert schema.id == "task-1"
    assert schema.status == "completed"
    assert schema.logs == ["done"]
    assert schema.result == [
        {
            "url": "https://example.com",
            "title": "Example",
            "content": "Body",
            "domain": "example.com",
            "content_length": 4,
            "snippet": "Body",
            "extraction_status": "success",
        }
    ]


def test_search_result_dicts_to_orm_builds_rows():
    rows = search_result_dicts_to_orm(
        "task-1",
        [{"url": "https://example.com", "title": "Example", "content": "Body"}],
    )

    assert len(rows) == 1
    assert rows[0].task_id == "task-1"
    assert rows[0].url == "https://example.com"


def test_enrich_search_result_dict_adds_derived_metadata():
    enriched = enrich_search_result_dict(
        {
            "url": "https://example.com/path",
            "title": "Example",
            "content": "Body text",
        }
    )

    assert enriched["domain"] == "example.com"
    assert enriched["content_length"] == 9
    assert enriched["snippet"] == "Body text"
    assert enriched["extraction_status"] == "success"

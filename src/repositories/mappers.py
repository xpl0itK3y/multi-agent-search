from src.api.schemas import ResearchRecord, ResearchStatus, SearchTask, TaskStatus
from src.db.models import ResearchORM, SearchResultORM, SearchTaskORM


def research_orm_to_record(research: ResearchORM) -> ResearchRecord:
    return ResearchRecord(
        id=research.id,
        prompt=research.prompt,
        depth=research.depth,
        status=ResearchStatus(research.status),
        task_ids=research.task_ids,
        created_at=research.created_at,
        updated_at=research.updated_at,
        final_report=research.final_report,
    )


def search_task_orm_to_schema(task: SearchTaskORM) -> SearchTask:
    return SearchTask(
        id=task.id,
        research_id=task.research_id,
        description=task.description,
        queries=task.queries,
        status=TaskStatus(task.status),
        created_at=task.created_at,
        updated_at=task.updated_at,
        result=[
            {
                "url": result.url,
                "title": result.title,
                "content": result.content,
            }
            for result in task.results
        ]
        or None,
        logs=task.logs,
    )


def search_result_dicts_to_orm(task_id: str, results: list[dict]) -> list[SearchResultORM]:
    return [
        SearchResultORM(
            task_id=task_id,
            url=result.get("url", ""),
            title=result.get("title"),
            content=result.get("content"),
        )
        for result in results
    ]

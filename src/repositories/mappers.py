from src.api.schemas import (
    FinalizeJobStatus,
    ResearchFinalizeJob,
    SearchJobStatus,
    SearchTaskJob,
    ResearchRecord,
    ResearchStatus,
    SearchTask,
    TaskStatus,
)
from src.db.models import (
    ResearchFinalizeJobORM,
    ResearchORM,
    SearchResultORM,
    SearchTaskJobORM,
    SearchTaskORM,
)


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


def research_finalize_job_orm_to_schema(job: ResearchFinalizeJobORM) -> ResearchFinalizeJob:
    return ResearchFinalizeJob(
        id=job.id,
        research_id=job.research_id,
        status=FinalizeJobStatus(job.status),
        error=job.error,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def search_task_job_orm_to_schema(job: SearchTaskJobORM) -> SearchTaskJob:
    return SearchTaskJob(
        id=job.id,
        task_id=job.task_id,
        depth=job.depth,
        status=SearchJobStatus(job.status),
        error=job.error,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )

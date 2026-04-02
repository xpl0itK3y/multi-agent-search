from urllib.parse import urlparse

from src.api.schemas import (
    ExtractionMetrics,
    FinalizeJobStatus,
    QueueMetrics,
    ResearchFinalizeJob,
    SearchJobStatus,
    SearchTaskJob,
    SearchTaskMetrics,
    WorkerHeartbeat,
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
    WorkerHeartbeatORM,
)
from src.core import rust_accel


TRUSTED_DOMAIN_EXACT_MATCHES = {
    "developer.mozilla.org",
    "docs.python.org",
    "openai.com",
    "platform.openai.com",
    "wikipedia.org",
}
TRUSTED_DOMAIN_SUFFIXES = (
    ".gov",
    ".edu",
    ".readthedocs.io",
)


def _score_domain_quality(domain: str | None) -> int:
    if not domain:
        return 0

    normalized_domain = domain.removeprefix("www.")
    if normalized_domain in TRUSTED_DOMAIN_EXACT_MATCHES:
        return 2
    if any(normalized_domain.endswith(suffix) for suffix in TRUSTED_DOMAIN_SUFFIXES):
        return 2
    if normalized_domain.endswith(".github.io"):
        return 1
    return 0


def _infer_source_quality(
    domain: str | None,
    content_length: int,
    extraction_status: str,
) -> str:
    if extraction_status != "success":
        return "low"

    quality_score = 0
    if content_length >= 1200:
        quality_score += 2
    elif content_length >= 300:
        quality_score += 1

    quality_score += _score_domain_quality(domain)

    if quality_score >= 3:
        return "high"
    if quality_score >= 1:
        return "medium"
    return "low"


def enrich_search_result_dict(result: dict) -> dict:
    url = result.get("url", "")
    title = result.get("title")
    content = result.get("content")
    parsed = urlparse(url)
    normalized_content = rust_accel.clean_extracted_content(content)

    snippet = result.get("snippet")
    if not snippet and normalized_content:
        snippet = rust_accel.build_snippet(normalized_content, 240)

    extraction_status = result.get("extraction_status")
    if not extraction_status:
        extraction_status = "failed" if "failed to extract content" in normalized_content.lower() else "success"

    domain = parsed.netloc.lower() or None
    content_length = len(normalized_content)

    return {
        "url": url,
        "title": title,
        "content": normalized_content,
        "domain": domain,
        "content_length": content_length,
        "snippet": snippet or None,
        "extraction_status": extraction_status,
        "source_quality": _infer_source_quality(domain, content_length, extraction_status),
    }


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
            enrich_search_result_dict(
                {
                    "url": result.url,
                    "title": result.title,
                    "content": result.content,
                }
            )
            for result in task.results
        ]
        or None,
        logs=task.logs,
        search_metrics=SearchTaskMetrics.model_validate(task.search_metrics or {}),
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
        attempt_count=job.attempt_count,
        max_attempts=job.max_attempts,
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
        attempt_count=job.attempt_count,
        max_attempts=job.max_attempts,
        error=job.error,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def worker_heartbeat_orm_to_schema(heartbeat: WorkerHeartbeatORM) -> WorkerHeartbeat:
    return WorkerHeartbeat(
        worker_name=heartbeat.worker_name,
        processed_jobs=heartbeat.processed_jobs,
        status=heartbeat.status,
        last_error=heartbeat.last_error,
        extraction_metrics=ExtractionMetrics.model_validate(heartbeat.extraction_metrics or {}),
        last_seen_at=heartbeat.last_seen_at,
    )

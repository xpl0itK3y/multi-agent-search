"""LEASE-COMPLETED: what runs after the fenced finalize commit.

Once complete_research_finalize_job commits, the job is COMPLETED and its lease can no
longer be renewed, so the post-commit trail event, log and webhook must not re-check it,
and no post-commit failure may be reported as a lost lease or a failed job.
"""
import logging
from unittest import mock

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    TaskStatus,
)
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

WEBHOOK = "https://hooks.example.com/done"


def _service_with_webhook():
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    store.merge_research_graph_state(research.id, {"webhook_url": WEBHOOK})
    store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mock.Mock()
    analyzer.run_analysis.return_value = "background report"
    analyzer.llm = None
    service = ResearchService(task_store=store, analyzer=analyzer)
    service._fire_webhook = mock.Mock()
    return store, research, service


def _lease_or_failure_logs(caplog) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if "lease_lost" in record.getMessage()
        or "finalize_job_failure" in record.getMessage()
        or "finalize_job_failed" in record.getMessage()
    ]


def test_worker_finalize_fires_webhook_and_ends_trail_with_completed(caplog):
    store, research, service = _service_with_webhook()
    _, job = service.enqueue_research_finalization(research.id)

    with caplog.at_level(logging.INFO):
        processed = service.process_finalize_job(job.id)

    assert processed.status == FinalizeJobStatus.COMPLETED
    current = store.get_research(research.id)
    assert current.status == ResearchStatus.COMPLETED
    assert current.final_report == "background report"
    service._fire_webhook.assert_called_once_with(
        WEBHOOK, research.id, {"research_id": research.id, "status": "completed"}
    )
    assert current.graph_trail[-1]["step"] == "completed"
    assert _lease_or_failure_logs(caplog) == []
    messages = [record.getMessage() for record in caplog.records]
    assert "research_finalize_completed" in messages
    assert "finalize_job_completed" in messages


def test_post_commit_failure_is_not_reported_as_a_failed_or_fenced_job(caplog):
    store, research, service = _service_with_webhook()
    service._fire_webhook.side_effect = RuntimeError("webhook exploded")
    _, job = service.enqueue_research_finalization(research.id)

    with caplog.at_level(logging.INFO):
        processed = service.process_finalize_job(job.id)

    assert processed.status == FinalizeJobStatus.COMPLETED
    assert processed.error is None
    assert store.get_research(research.id).status == ResearchStatus.COMPLETED
    assert _lease_or_failure_logs(caplog) == []
    assert any("finalize_post_commit_failed" in record.getMessage() for record in caplog.records)


def test_cancel_landing_at_commit_skips_webhook_and_completed_step():
    store, research, service = _service_with_webhook()
    _, job = service.enqueue_research_finalization(research.id)
    commit = store.complete_research_finalize_job

    def cancel_then_commit(*args, **kwargs):
        store.update_research_status(research.id, ResearchStatus.CANCELLED, "Cancelled by user.")
        return commit(*args, **kwargs)

    store.complete_research_finalize_job = cancel_then_commit
    processed = service.process_finalize_job(job.id)

    assert processed.status == FinalizeJobStatus.COMPLETED
    current = store.get_research(research.id)
    assert current.status == ResearchStatus.CANCELLED
    service._fire_webhook.assert_not_called()
    assert "completed" not in [entry.get("step") for entry in current.graph_trail]

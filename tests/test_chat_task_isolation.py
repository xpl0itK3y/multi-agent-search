"""C7-3: chat follow-up searches are stored as research tasks ('chat-' ids) but are not
part of the report: they must not count as plan items in verification coverage or the
audit trail, nor gate, feed or get redispatched by finalization and retry."""
from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskStatus, TaskUpdate
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

REPORT = (
    "## Summary\nSolid-state battery commercialization is progressing in 2026 [S1].\n\n"
    "## Sources\n- [S1] https://one.example/a"
)


class _RecordingAnalyzer:
    llm = None

    def __init__(self):
        self.task_ids: list[list[str]] = []

    def run_analysis(self, prompt, tasks, **kwargs):
        self.task_ids.append([task.id for task in tasks])
        return "retried report"


def _research(store, *, status=ResearchStatus.COMPLETED, report_task_status=TaskStatus.COMPLETED):
    research = store.add_research(
        ResearchRequest(prompt="solid-state battery commercialization", depth=SearchDepth.EASY),
        task_ids=["plan-1"],
        language="en",
    )
    store.add_task(
        {
            "id": "plan-1",
            "research_id": research.id,
            "description": "Solid-state battery commercialization progress",
            "queries": ["solid-state battery commercialization 2026"],
            "status": report_task_status,
            "result": (
                [{"url": "https://one.example/a", "title": "One", "content": "solid-state battery commercialization progress"}]
                if report_task_status == TaskStatus.COMPLETED
                else []
            ),
            "logs": ["searched"],
        }
    )
    store.update_research_status(research.id, status, REPORT if status == ResearchStatus.COMPLETED else "failed")
    return research


def _chat_search(service, store, research_id, mocker, *, status=TaskStatus.COMPLETED):
    mocker.patch.object(service, "_question_needs_search", return_value=True)

    def fake_run_task(self, task_id):
        store.update_task(
            task_id,
            TaskUpdate(
                status=status,
                result=(
                    [{"url": "https://chat.example/h2", "title": "H2", "content": "hydrogen storage costs fell"}]
                    if status == TaskStatus.COMPLETED
                    else None
                ),
                log="chat search ran",
            ),
        )

    mocker.patch("src.agents.search.SearchAgent.run_task", fake_run_task)
    service.generate_research_answer(research_id, "What about hydrogen storage costs?")
    chat_tasks = [t for t in store.get_tasks_by_research(research_id) if t.id.startswith("chat-")]
    assert len(chat_tasks) == 1
    return chat_tasks[0]


def test_chat_search_leaves_plan_coverage_and_the_audit_plan_unchanged(mocker):
    store = InMemoryTaskStore()
    research = _research(store)
    chat = mocker.Mock()
    chat.answer.return_value = "Costs fell [S2]."
    service = ResearchService(task_store=store, chat_agent=chat)
    before = service.get_research_verification(research.id)
    trail_before = service.get_research_audit_trail(research.id)
    assert before.coverage_ratio == 1.0

    _chat_search(service, store, research.id, mocker)

    after = service.get_research_verification(research.id)
    assert after.coverage_ratio == before.coverage_ratio == 1.0
    assert after.uncovered_questions == []
    assert [item.question for item in after.plan_coverage] == ["Solid-state battery commercialization progress"]
    trail = service.get_research_audit_trail(research.id)
    assert trail.plan == trail_before.plan == ["Solid-state battery commercialization progress"]
    assert [q.task for q in trail.queries] == ["Solid-state battery commercialization progress"]
    assert trail.query_count == trail_before.query_count == 1


def test_a_failed_chat_search_does_not_force_the_search_path_of_a_retry(mocker):
    store = InMemoryTaskStore()
    research = _research(store, status=ResearchStatus.FAILED)
    chat = mocker.Mock()
    chat.answer.return_value = "No new evidence."
    service = ResearchService(task_store=store, chat_agent=chat, analyzer=_RecordingAnalyzer())
    chat_task = _chat_search(service, store, research.id, mocker, status=TaskStatus.FAILED)

    retried = service.retry_research(research.id)

    # Every report task completed, so only finalization is retried; the chat task is not
    # sent to the search workers as if it were a report search.
    assert retried.status == ResearchStatus.ANALYZING
    assert store.get_latest_research_finalize_job(research.id) is not None
    assert store.get_latest_search_task_job(chat_task.id) is None
    assert store.get_task(chat_task.id).status == TaskStatus.FAILED


def test_finalization_is_built_from_the_report_plan_only(mocker):
    store = InMemoryTaskStore()
    research = _research(store, status=ResearchStatus.FAILED)
    chat = mocker.Mock()
    chat.answer.return_value = "Costs fell [S2]."
    analyzer = _RecordingAnalyzer()
    service = ResearchService(task_store=store, chat_agent=chat, analyzer=analyzer)
    _chat_search(service, store, research.id, mocker)

    service.retry_research(research.id)
    job = store.get_latest_research_finalize_job(research.id)
    service.process_finalize_job(job.id)

    assert store.get_research(research.id).status == ResearchStatus.COMPLETED
    assert analyzer.task_ids == [["plan-1"]]
    assert store.get_research(research.id).graph_state["task_ids"] == ["plan-1"]


def test_a_completed_chat_search_does_not_rescue_a_plan_whose_searches_all_failed(mocker):
    store = InMemoryTaskStore()
    research = _research(store, status=ResearchStatus.PROCESSING, report_task_status=TaskStatus.FAILED)
    chat = mocker.Mock()
    chat.answer.return_value = "Costs fell [S1]."
    service = ResearchService(task_store=store, chat_agent=chat, analyzer=_RecordingAnalyzer())
    _chat_search(service, store, research.id, mocker)

    service._maybe_enqueue_finalization(research.id)

    final = store.get_research(research.id)
    assert final.status == ResearchStatus.FAILED
    assert final.final_report == "All tasks failed."

from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskStatus, TaskUpdate
from src.agents.chat import ChatAgent
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


class CanonicalAnalyzer:
    llm = None
    enable_graph_branching = False

    def run_analysis(self, prompt, tasks, **kwargs):
        return (
            "## Summary\nThe selected source supports this report [S1].",
            [
                {
                    "source_id": "S1",
                    "url": "https://selected.example/report",
                    "title": "Selected",
                    "domain": "selected.example",
                    "source_quality": "high",
                    "content": "The selected source supports this report with detailed evidence.",
                }
            ],
        )

def test_finalize_persists_and_reuses_the_analyzers_canonical_source_pool(mocker):
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "collect evidence",
            "queries": ["topic evidence"],
            "status": TaskStatus.COMPLETED,
            "result": [
                {
                    "url": "https://ignored.example/article",
                    "title": "Ignored",
                    "content": "This source was not selected by the analyzer.",
                },
                {
                    "url": "https://selected.example/report",
                    "title": "Selected",
                    "domain": "selected.example",
                    "source_quality": "high",
                    "content": "The selected source supports this report with detailed evidence.",
                },
            ],
        }
    )
    analyzer = CanonicalAnalyzer()
    service = ResearchService(task_store=store, analyzer=analyzer)

    finalized = service.finalize_research(research.id)

    canonical = finalized.graph_state["canonical_sources"]
    assert canonical == [
        {
            "source_id": "S1",
            "url": "https://selected.example/report",
            "title": "Selected",
            "domain": "selected.example",
            "source_quality": "high",
        }
    ]
    assert "content" not in canonical[0]

    sources = service.get_research_sources(research.id)
    assert [(source.source_id, source.url) for source in sources] == [
        ("S1", "https://selected.example/report")
    ]
    assert service.get_research_audit_trail(research.id).sources[0].source_id == "S1"

    evidence_spy = mocker.spy(service.evidence_mapper, "build_evidence_groups")
    service.get_research_verification(research.id)
    evidence_pool = evidence_spy.call_args.args[0]
    assert evidence_pool == [
        {
            "source_id": "S1",
            "content": "The selected source supports this report with detailed evidence.",
        }
    ]


def test_chat_preserves_canonical_ids_after_ranking_and_sends_the_full_report(mocker):
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    long_report = "R" * 5000 + " FULL_REPORT_END"
    store.update_research_status(research.id, ResearchStatus.COMPLETED, long_report)
    results = [
        {
            "url": f"https://source-{index}.example/report",
            "title": f"Source {index}",
            "content": (
                "specific ranking keyword " * 20
                if index == 13
                else f"general background material {index} " * 20
            ),
        }
        for index in range(1, 14)
    ]
    store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "collect evidence",
            "queries": ["topic evidence"],
            "status": TaskStatus.COMPLETED,
            "result": results,
        }
    )
    store.merge_research_graph_state(
        research.id,
        {
            "canonical_sources": [
                {"source_id": f"S{index}", "url": f"https://source-{index}.example/report"}
                for index in range(1, 14)
            ]
        },
    )
    chat = mocker.Mock()
    chat.answer.return_value = "The specific answer is supported [S2]."
    service = ResearchService(task_store=store, chat_agent=chat)

    message = service.generate_research_answer(
        research.id,
        "What does the specific ranking keyword show?",
    )

    passed_report = chat.answer.call_args.args[1]
    passed_sources = chat.answer.call_args.args[2]
    assert passed_report.endswith("FULL_REPORT_END")
    assert passed_sources[0]["source_id"] == "S13"
    assert message.sources[0].source_id == "S13"


def test_chat_agent_does_not_truncate_the_report(mocker):
    llm = mocker.Mock()
    llm.generate.return_value = "answer"
    report = "R" * 5000 + " FULL_REPORT_END"

    ChatAgent(llm).answer("question", report, [], [])

    assert "FULL_REPORT_END" in llm.generate.call_args.kwargs["user_prompt"]


def test_chat_assigns_new_search_sources_after_existing_canonical_ids(mocker):
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    store.update_research_status(research.id, ResearchStatus.COMPLETED, "Report [S4].")
    store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "existing evidence",
            "queries": ["topic"],
            "status": TaskStatus.COMPLETED,
            "result": [
                {
                    "url": "https://existing.example/report",
                    "title": "Existing",
                    "content": "existing evidence",
                }
            ],
        }
    )
    store.merge_research_graph_state(
        research.id,
        {
            "canonical_sources": [
                {"source_id": "S4", "url": "https://existing.example/report"}
            ]
        },
    )
    chat = mocker.Mock()
    chat.answer.return_value = "New evidence [S5]."
    service = ResearchService(task_store=store, chat_agent=chat)
    mocker.patch.object(service, "_question_needs_search", return_value=True)
    mocker.patch.object(
        service,
        "_mini_search_for_chat",
        return_value=[
            {
                "url": "https://new.example/report",
                "title": "New",
                "content": "new evidence",
            }
        ],
    )

    message = service.generate_research_answer(research.id, "new question")

    by_url = {source.url: source.source_id for source in message.sources}
    assert by_url["https://existing.example/report"] == "S4"
    assert by_url["https://new.example/report"] == "S5"
    # The mini-search source belongs to this answer, not to the report's source table.
    stored = store.get_research(research.id).graph_state["canonical_sources"]
    assert [source["source_id"] for source in stored] == ["S4"]
    assert [source.source_id for source in service.get_research_sources(research.id)] == ["S4"]


def _research_with_task_pool(store: InMemoryTaskStore, *, depth=SearchDepth.EASY):
    research = store.add_research(ResearchRequest(prompt="topic", depth=depth), task_ids=["task-1"])
    store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "collect evidence",
            "queries": ["topic evidence"],
            "status": TaskStatus.COMPLETED,
            "result": [
                {"url": "https://one.example/a", "title": "One", "content": "first source text"},
                {"url": "https://two.example/b", "title": "Two", "content": "second source text"},
            ],
        }
    )
    return research


def test_pre_analyze_checkpoint_leaves_the_pool_not_computed(mocker):
    # collect_context used to checkpoint canonical_sources=[], which every reader took as
    # the report's (empty) table, so the Sources tab was blank until analyze finished.
    store = InMemoryTaskStore()
    research = _research_with_task_pool(store)
    service = ResearchService(task_store=store, analyzer=CanonicalAnalyzer())
    runner = service.finalize_graph_runner

    runner._checkpoint(
        {"research_id": research.id, "prompt": "topic", "tasks": store.get_tasks_by_research(research.id)},
        "collect_context",
        "collected",
    )

    assert store.get_research(research.id).graph_state["canonical_sources"] is None
    assert [(s.source_id, s.url) for s in service.get_research_sources(research.id)] == [
        ("S1", "https://one.example/a"),
        ("S2", "https://two.example/b"),
    ]


def test_run_that_failed_in_analyze_still_lists_the_task_pool():
    class FailingAnalyzer(CanonicalAnalyzer):
        def run_analysis(self, prompt, tasks, **kwargs):
            raise RuntimeError("model outage")

    store = InMemoryTaskStore()
    research = _research_with_task_pool(store)
    service = ResearchService(task_store=store, analyzer=FailingAnalyzer())

    try:
        service.finalize_graph_runner.run(
            research.id, "topic", store.get_tasks_by_research(research.id), research.depth
        )
    except RuntimeError:
        pass

    state = store.get_research(research.id).graph_state
    assert state["step"] == "collect_context"
    assert state.get("canonical_sources") is None
    assert len(service.get_research_sources(research.id)) == 2


def test_an_empty_stored_table_is_still_authoritative():
    store = InMemoryTaskStore()
    research = _research_with_task_pool(store)
    store.merge_research_graph_state(research.id, {"canonical_sources": []})
    service = ResearchService(task_store=store)

    assert service.get_research_sources(research.id) == []


def test_chat_follow_up_tasks_never_enter_the_report_source_fallback(mocker):
    store = InMemoryTaskStore()
    research = _research_with_task_pool(store)
    store.update_research_status(research.id, ResearchStatus.COMPLETED, "Report [S1] [S2].")
    chat = mocker.Mock()
    chat.answer.return_value = "Fresh evidence [S3]."
    service = ResearchService(task_store=store, chat_agent=chat)
    mocker.patch.object(service, "_question_needs_search", return_value=True)

    def fake_run_task(self, task_id):
        store.update_task(
            task_id,
            TaskUpdate(
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://chat.example/new", "title": "New", "content": "fresh evidence"}],
            ),
        )

    mocker.patch("src.agents.search.SearchAgent.run_task", fake_run_task)

    message = service.generate_research_answer(research.id, "what is new?")

    assert "https://chat.example/new" in {source.url for source in message.sources}
    assert "canonical_sources" not in (store.get_research(research.id).graph_state or {})
    assert [s.url for s in service.get_research_sources(research.id)] == [
        "https://one.example/a",
        "https://two.example/b",
    ]


def test_chat_links_every_cited_report_id_even_outside_the_ranked_sources(mocker):
    store = InMemoryTaskStore()
    research = store.add_research(ResearchRequest(prompt="topic", depth=SearchDepth.EASY), task_ids=["task-1"])
    store.update_research_status(research.id, ResearchStatus.COMPLETED, "Report citing [S1] to [S13].")
    store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "collect evidence",
            "queries": ["topic evidence"],
            "status": TaskStatus.COMPLETED,
            "result": [
                {
                    "url": f"https://source-{index}.example/report",
                    "title": f"Source {index}",
                    "content": "specific ranking keyword " * 20 if index == 13 else f"background {index} " * 20,
                }
                for index in range(1, 14)
            ],
        }
    )
    store.merge_research_graph_state(
        research.id,
        {
            "canonical_sources": [
                {"source_id": f"S{index}", "url": f"https://source-{index}.example/report", "title": f"Source {index}"}
                for index in range(1, 14)
            ]
        },
    )
    chat = mocker.Mock()
    # S12 is not among the 12 ranked sources the model got, but it is in the report it read.
    chat.answer.return_value = "The keyword matters [S13], as the report noted [S12] and \\[S12\\]."
    service = ResearchService(task_store=store, chat_agent=chat)

    message = service.generate_research_answer(research.id, "What does the specific ranking keyword show?")

    assert "S12" not in {source["source_id"] for source in chat.answer.call_args.args[2]}
    by_id = {source.source_id: source for source in message.sources}
    assert by_id["S12"].url == "https://source-12.example/report"
    assert by_id["S12"].title == "Source 12"
    assert by_id["S12"].snippet is None  # the id table carries no content
    assert [source.source_id for source in message.sources].count("S12") == 1
    assert by_id["S13"].snippet  # ranked sources keep their snippet

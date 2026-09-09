from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskStatus
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
    stored = store.get_research(research.id).graph_state["canonical_sources"]
    assert stored[-1]["source_id"] == "S5"

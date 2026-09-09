from src.api.schemas import ResearchRequest, SearchDepth, TaskStatus
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


class CanonicalAnalyzer:
    llm = None
    enable_graph_branching = False

    def __init__(self):
        self.conflict_inputs: list[list[dict]] = []

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

    def _resolve_depth_profile(self, depth):
        return {"conflict_source_limit": 10}

    def _detect_conflicts(self, sources):
        self.conflict_inputs.append(sources)
        return []


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
    assert analyzer.conflict_inputs[-1][0]["url"] == "https://selected.example/report"

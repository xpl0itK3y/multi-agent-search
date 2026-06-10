"""5.1 — multi-stage writer (outline → sections → stitch) extended to MEDIUM depth."""

from __future__ import annotations

import pytest

from src.agents.analyzer import AnalyzerAgent
from src.api.schemas import SearchDepth, SourceCriticSummary
from src.search_depth_profiles import SEARCH_DEPTH_PROFILES


class RecordingLLM:
    """Records how many section vs synthesis calls the writer makes."""

    def __init__(self):
        self.section_calls = 0
        self.synth_calls = 0

    def generate(self, system_prompt: str = "", user_prompt: str = "", model=None, **kwargs) -> str:
        if system_prompt == AnalyzerAgent.SECTION_SYSTEM_PROMPT:
            self.section_calls += 1
            return f"## Section {self.section_calls}\nDraft body [S1]."
        if system_prompt == AnalyzerAgent.SYNTHESIS_SYSTEM_PROMPT:
            self.synth_calls += 1
            return "## Report\nSynthesized [S1].\n\n## Sources\n[S1] http://d1.com"
        return "fallback"


def _sources(n: int) -> list[dict]:
    return [
        {
            "source_id": f"S{i}",
            "url": f"http://d{i}.com/x",
            "domain": f"d{i}.com",
            "title": f"T{i}",
            "content": f"Sentence {i} about photovoltaic solar capacity and grid demand growth steadily worldwide.",
        }
        for i in range(1, n + 1)
    ]


# ── chunk / section-cap math ─────────────────────────────────────────────────

@pytest.mark.parametrize(
    "total,chunk,cap,expected_sections",
    [
        (16, 8, 2, 2),   # MEDIUM: 16 sources → 2 chunks of 8
        (24, 8, 2, 2),   # MEDIUM: cap forces 2 sections (chunk grows to 12)
        (12, 8, 2, 2),   # MEDIUM at threshold → 2 sections
        (3, 8, 2, 1),    # tiny pool → a single section (cap never pads upward)
        (30, 12, None, 3),  # HARD-style (no cap): 12,12,6 → 3 sections, unchanged
        (50, 12, None, 5),  # HARD-style: no cap → 5 sections, behavior preserved
    ],
)
def test_section_chunking_respects_cap(total, chunk, cap, expected_sections):
    llm = RecordingLLM()
    analyzer = AnalyzerAgent(llm)
    analyzer._run_parallel_section_analysis(
        _sources(total),
        "renewable energy growth",
        "en",
        SearchDepth.MEDIUM if cap else SearchDepth.HARD,
        chunk_size=chunk,
        max_sections=cap,
    )
    assert llm.section_calls == expected_sections
    assert llm.synth_calls == 1


# ── gate: MEDIUM uses the multi-stage writer only with enough sources ─────────

def _build(mocker, analyzer, source_count):
    summary = SourceCriticSummary(total_sources=source_count, high_confidence_sources=source_count // 2)
    mocker.patch.object(analyzer, "_prepare_aggregated_data", return_value=(_sources(source_count), summary))
    parallel = mocker.patch.object(analyzer, "_run_parallel_section_analysis", return_value="## R\nx [S1].")
    single = mocker.patch.object(analyzer, "_generate_report", return_value="## R\nx [S1].")
    return parallel, single


def test_medium_with_enough_sources_uses_parallel_writer(mocker):
    analyzer = AnalyzerAgent(RecordingLLM())
    parallel, single = _build(mocker, analyzer, AnalyzerAgent._PARALLEL_SECTION_MIN_SOURCES_MEDIUM)
    analyzer.run_analysis("q", [], depth=SearchDepth.MEDIUM)
    assert parallel.called
    assert not single.called
    # MEDIUM is capped at two sections.
    assert parallel.call_args.kwargs["max_sections"] == AnalyzerAgent._PARALLEL_SECTION_MEDIUM_MAX_SECTIONS


def test_medium_below_threshold_stays_single_pass(mocker):
    analyzer = AnalyzerAgent(RecordingLLM())
    parallel, single = _build(mocker, analyzer, AnalyzerAgent._PARALLEL_SECTION_MIN_SOURCES_MEDIUM - 1)
    analyzer.run_analysis("q", [], depth=SearchDepth.MEDIUM)
    assert single.called
    assert not parallel.called


def test_easy_never_uses_parallel_writer(mocker):
    analyzer = AnalyzerAgent(RecordingLLM())
    parallel, single = _build(mocker, analyzer, 20)  # plenty of sources, but EASY is excluded
    analyzer.run_analysis("q", [], depth=SearchDepth.EASY)
    assert single.called
    assert not parallel.called


def test_hard_uses_parallel_writer_capped_at_six_sections(mocker):
    analyzer = AnalyzerAgent(RecordingLLM())
    parallel, single = _build(mocker, analyzer, AnalyzerAgent._PARALLEL_SECTION_MIN_SOURCES)
    analyzer.run_analysis("q", [], depth=SearchDepth.HARD)
    assert parallel.called
    assert not single.called
    # HARD keeps the full-size chunk but bounds the deeper pool to six sections.
    assert parallel.call_args.kwargs["chunk_size"] == AnalyzerAgent._PARALLEL_SECTION_CHUNK
    assert parallel.call_args.kwargs["max_sections"] == AnalyzerAgent._PARALLEL_SECTION_HARD_MAX_SECTIONS


@pytest.mark.parametrize(
    "depth,expected_pool",
    [(SearchDepth.EASY, 30), (SearchDepth.MEDIUM, 60), (SearchDepth.HARD, 120)],
)
def test_source_ladder_is_consistent(depth, expected_pool):
    """30 / 60 / 120 ladder, and the search supply + budget actually support it."""
    profile = AnalyzerAgent.DEPTH_ANALYSIS_PROFILES[depth]
    search = SEARCH_DEPTH_PROFILES[depth]
    assert profile["max_sources"] == expected_pool
    # Search must gather at least the analyzer pool (with margin for dedup).
    assert search["task_count"] * search["source_limit"] >= expected_pool
    # Per-task cap × task count must be able to fill the pool.
    assert profile["max_sources_per_task"] * search["task_count"] >= expected_pool
    # Budget keeps a sane per-source content density (the budget is split across sources).
    assert profile["payload_char_budget"] / expected_pool >= 900


def test_deep_hard_pool_is_bounded_to_six_sections():
    # 120 deep-tier sources are bounded to six section calls (chunk grows to ~20),
    # so cost stays at ~6 sections + synthesis no matter how deep the pool.
    llm = RecordingLLM()
    AnalyzerAgent(llm)._run_parallel_section_analysis(
        _sources(120), "q", "en", SearchDepth.HARD,
        chunk_size=AnalyzerAgent._PARALLEL_SECTION_CHUNK,
        max_sections=AnalyzerAgent._PARALLEL_SECTION_HARD_MAX_SECTIONS,
    )
    assert llm.section_calls == 6
    assert llm.synth_calls == 1

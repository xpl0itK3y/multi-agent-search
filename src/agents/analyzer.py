import logging
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Optional
from urllib.parse import urlparse
from src.agents.claim_verifier import ClaimVerifierAgent
from src.agents.report_critic import ReportCriticAgent
from src.agents.evidence_mapper import EvidenceMapperAgent
from src.agents.language_utils import LANGUAGE_HINTS
from src.agents.source_critic import SourceCriticAgent
from src.core.agent import BaseAgent
from src.core import rust_accel
from src.api.schemas import SearchTask, SearchDepth
from src.config import settings
from src.observability import maybe_traceable
from src.source_quality_policy import TOPIC_POLICIES, combined_topics

logger = logging.getLogger(__name__)

class AnalyzerAgent(BaseAgent):
    MAX_ANALYZER_SOURCES = 24
    MAX_SOURCES_PER_DOMAIN = 3
    MAX_SOURCES_PER_TASK = 6
    MAX_SOURCE_CONTENT_CHARS = 1600
    MAX_PREMIUM_SOURCE_CONTENT_CHARS = 1600
    MAX_MEDIUM_SOURCE_CONTENT_CHARS = 1000
    MAX_LOW_SOURCE_CONTENT_CHARS = 700
    CITATION_PATTERN = re.compile(r"\[S(\d+)\]")
    SENTENCE_PATTERN = re.compile(r"(?<=[.!?])\s+")
    SOURCE_HEADING_PATTERN = re.compile(r"(?ims)\n##\s+(Sources|Источники)\s*$.*\Z")
    CONFLICT_HEADING_PATTERN = re.compile(r"(?im)^##\s+(Conflicts And Uncertainties|Противоречия и неопределенности|Противоречия и неопределённости)\s*$")
    REPORT_NOTES_HEADING_PATTERN = re.compile(r"(?im)^##\s+(Report Notes|Примечания к отчету|Примечания к отчёту)\s*$")
    # An opening summary or a closing bottom-line counts — the report now leads with an
    # "Executive summary" and may close with "Conclusion / Bottom line", so accept those
    # (and the ru/es equivalents) and don't require an exact, suffix-free heading.
    INTRODUCTION_HEADING_PATTERN = re.compile(
        r"(?im)^##\s+(Introduction|Введение|Executive\s+Summary|Кратк\w+\s+резюме|Резюме|Resumen(\s+ejecutivo)?|Introducci[oó]n)\b"
    )
    CONCLUSION_HEADING_PATTERN = re.compile(
        r"(?im)^##\s+(Conclusion|Заключение|Итог\w*|Вывод\w*|Bottom\s+Line|Conclusi[oó]n)\b"
    )
    # Single source of truth lives in src/agents/language_utils.py (A-6).
    LANGUAGE_HINTS = LANGUAGE_HINTS
    STOPWORDS = {
        "the", "and", "for", "with", "that", "this", "from", "into", "their", "there", "about",
        "have", "has", "had", "were", "was", "will", "would", "could", "should", "than", "then",
        "into", "over", "under", "using", "used", "uses", "also", "only", "more", "most", "less",
        "very", "some", "many", "much", "when", "where", "while", "which", "what", "your", "they",
        "them", "being", "been", "because", "through", "each", "same", "such", "make", "made",
        "like", "just", "than", "small", "api", "apis", "framework", "frameworks",
    }
    CONFLICT_GENERIC_TOKENS = {
        "django",
        "fastapi",
        "flask",
        "python",
        "backend",
        "production",
        "system",
        "systems",
        "platform",
        "platforms",
        "supports",
        "support",
        "comparison",
        "compare",
    }
    NEGATION_TOKENS = {"no", "not", "never", "without", "lack", "lacks", "cannot", "can't", "doesn't", "don't"}
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
    LOW_VALUE_DOMAIN_EXACT_MATCHES = {
        "linkedin.com",
        "pinterest.com",
        "facebook.com",
        "x.com",
        "twitter.com",
        "eventify.io",
    }
    LOW_VALUE_DOMAIN_SUBSTRINGS = (
        "bookmark",
        "newsnviews",
        "techandgadgetreviews",
        "techspymagazine",
    )
    SPECULATIVE_TITLE_TOKENS = {
        "predictions",
        "prediction",
        "coming soon",
        "coming in",
        "coming to",
        "future of",
        "trends to watch",
        "what to expect",
        "gadgets coming",
        "best gadgets",
        "breakthrough technologies",
    }
    SPECULATIVE_CONTENT_TOKENS = {
        "expected to",
        "may",
        "might",
        "could",
        "rumored",
        "rumoured",
        "is likely to",
        "are likely to",
        "what to expect",
        "predictions for",
    }
    DEPTH_ANALYSIS_PROFILES = {
        SearchDepth.EASY: {
            "max_sources": 15,
            "max_sources_per_domain": 2,
            "max_sources_per_task": 8,
            "payload_char_budget": 15000,
            "conflict_source_limit": 8,
            "evidence_source_limit": 8,
            "report_instruction": (
                "Write a concise but complete report with a limited number of substantial sections. "
                "Prioritize the clearest findings and avoid unnecessary expansion."
            ),
        },
        SearchDepth.MEDIUM: {
            "max_sources": 60,
            "max_sources_per_domain": 4,
            "max_sources_per_task": 16,
            "payload_char_budget": 70000,
            "conflict_source_limit": 18,
            "evidence_source_limit": 18,
            "report_instruction": (
                "Write a substantially more comprehensive report than a brief summary. "
                "Prefer multiple substantial sections or subsections, include more concrete examples, "
                "and cover the topic from several angles when the evidence supports it."
            ),
        },
        SearchDepth.HARD: {
            # Deep tier — top of the 15/60/120 source ladder (EASY/MEDIUM/HARD). The
            # payload budget scales with the pool so each source keeps the same content
            # density (the budget is a *total* split across sources — see
            # _apply_payload_budget — so raising max_sources without raising the budget
            # would just starve the extra sources).
            "max_sources": 120,
            "max_sources_per_domain": 6,
            "max_sources_per_task": 24,
            "payload_char_budget": 140000,
            "conflict_source_limit": 30,
            "evidence_source_limit": 30,
            "report_instruction": (
                "Write a very comprehensive deep-dive report. Expand the analysis substantially, "
                "cover major subtopics in detail, use more year-by-year or category-by-category breakdowns when relevant, "
                "and synthesize a wider portion of the available evidence into a long-form answer."
            ),
        },
    }
    
    SYSTEM_PROMPT = """
    You are an expert Research Analyst. Your job is to take raw, messy data collected by internet search bots and synthesize it into a comprehensive, well-structured, and easy-to-read report that directly answers the user's original query.

    INPUT:
    You will receive the original user prompt and a JSON list of data gathered by bots. Each item contains a 'source_id', 'url', 'domain', 'source_quality', 'title', 'content' (raw text from the page), 'source_type', 'confidence', and 'caution_flags'.

    SOURCE RELIABILITY HIERARCHY — use this to weight claims:
    - source_type="primary" + confidence="high": official documentation, .gov, .edu — anchor your key findings here
    - source_type="editorial" + confidence="high"/"medium": expert reviews, tested comparisons — reliable for product/technical claims
    - source_type="general" + confidence="medium": mainstream press, reputable blogs — good supporting evidence
    - source_type="community": Reddit, HN, Stack Overflow — use only for direct user experience or anecdotal context, not statistics
    - source_type="speculative" OR caution_flags contains "speculative": treat as forecast/rumor, never as established fact — always use phrases like "according to rumors", "analysts predict", "reportedly"
    - confidence="low" OR caution_flags contains "low_confidence": secondary evidence only, corroborate with stronger sources when possible

    EVIDENCE GROUPS (evidence_groups field):
    - Claims supported by multiple sources in the same evidence group have strong multi-source backing — prioritize these in key findings
    - Single-source groups labeled "weak" should be presented with appropriate hedging

    YOUR TASK — write a publication-quality analytical report, not a summary of pages:
    1. Read all content; ignore navigation text, cookie warnings, and ads.
    2. Synthesize, don't list. Build an argument that answers the question — compare and reconcile what sources say, explain the *why* / mechanism, and draw out implications and trade-offs. Never write "Source 1 says X, Source 2 says Y."
    3. Weight claims by the reliability hierarchy above; anchor key findings in primary/high-confidence sources.
    4. Write in the SAME LANGUAGE as the user's original prompt (Spanish→Spanish, Russian→Russian, etc.).

    REQUIRED STRUCTURE (Markdown):
    - Open with an answer-first **Executive summary**: the direct answer to the question in 1–2 sentences, then 3–6 key takeaways as bullets, each carrying its [Sn] citation(s).
    - Then the analytical body, organized into sections with **descriptive, informative headings** (never "Section 1" / "Main findings") — each section should answer a real facet of the question.
    - Near the end, a section that explicitly separates **what is well-established** vs **what is contested or uncertain** vs **open questions** the evidence does not resolve.
    - End with a short **Conclusion / bottom line**.
    - A "Sources" list is appended automatically — do not write one.

    WRITING QUALITY:
    - Be specific: prefer concrete numbers, dates, named entities, and examples over vague generalities. Cut filler and hedge-padding.
    - Use a Markdown **table** whenever you compare options or present quantitative data across categories — it reads far better than prose.
    - Cite every factual claim inline with [S1], [S2] using the exact source_id values.
    - When sources conflict, prefer primary/high-confidence ones and state the disagreement explicitly.
    - Produce a genuinely comprehensive report: expand major sections, cover meaningful subcategories or year-by-year breakdowns when the topic supports it.

    DO NOT:
    - Hallucinate or make up facts not present in the provided text.
    - Present speculative predictions or rumors (source_type="speculative") as established facts.
    - Give equal weight to low-confidence sources when stronger primary or expert sources exist.
    - State contested or weakly supported claims with absolute certainty when the sources only support softer wording.
    - Output any internal reasoning — just the final Markdown report.
    """

    SECTION_SYSTEM_PROMPT = """
    You are a Research Analyst writing one part of a larger report. Analyze the provided subset of sources and write deep analytical findings.

    Rules:
    - Write ONLY analytical body sections (## descriptive heading, paragraphs, bullets) — no Introduction, Conclusion, Summary, or Sources.
    - Synthesize, don't list: reconcile what the sources say, explain the *why* / mechanism, and note implications. Never write "Source 1 says…, Source 2 says…".
    - Be specific: prefer concrete numbers, dates, named entities, and examples over vague generalities.
    - Use a Markdown table when comparing options or presenting quantitative data across categories.
    - Cite every factual claim with inline [Sn] using the exact source_id values from gathered_data, e.g. [S13].
    - Do not invent information. If the sources are limited, say so briefly.
    """

    SYNTHESIS_SYSTEM_PROMPT = """
    You are a senior Research Analyst assembling several partial analyses (each covering different sources) into ONE publication-quality report.

    REQUIRED STRUCTURE:
    - Open with an answer-first **Executive summary**: the direct answer to the research question in 1–2 sentences, then 3–6 key takeaways as bullets, each carrying its [Sn] citation(s).
    - Then the analytical body, organized into sections with **descriptive, informative headings** that map to the facets of the question (not "Section 1").
    - Near the end, a section separating **what is well-established** vs **what is contested/uncertain** vs **open questions**.
    - End with a short **Conclusion / bottom line**. Do NOT include a Sources section (appended automatically).

    RULES:
    - Merge and DEDUPLICATE across the partial analyses into a single unified argument — do not repeat the same point in multiple sections, and remove meta-commentary like "Section 1 analyzed…".
    - Preserve all inline citations [Sn] EXACTLY as they appear in the partials — never renumber, drop, or invent them.
    - Keep concrete specifics (numbers, dates, names); convert comparative/quantitative prose into Markdown tables where it improves clarity.
    - Output only the final Markdown report, no internal reasoning.
    """

    EDITOR_SYSTEM_PROMPT = """
    You are a senior editor at a research publication. You receive a complete DRAFT research report and return a publication-quality FINAL version. You sharpen and tighten — you do NOT summarize substance away; the final report should be as comprehensive as the draft, just better.

    Make it better by:
    - Ensuring it OPENS with a tight answer-first **Executive summary**: the direct answer in 1–2 sentences, then 3–6 key takeaways as bullets (each keeping its [Sn] citations). Add it if missing; tighten it if present.
    - Giving every section a **descriptive, informative heading** (never "Section 1", "Main findings").
    - Making claims **specific** — keep the concrete numbers, dates, names, and examples; cut vague filler, throat-clearing, and hedge-padding.
    - Converting comparative or quantitative prose into a clean **Markdown table** where it improves clarity.
    - Ensuring there is a section separating **well-established** vs **contested/uncertain** vs **open questions**.
    - Removing repetition and redundancy so each point is made once, in the right place.

    HARD CONSTRAINTS — violating these breaks the system:
    - Do NOT invent facts or add claims not supported by the draft.
    - Do NOT add, drop, or renumber [Sn] citations — preserve each one on the sentence it supports.
    - Do NOT change the report's language.
    - Do NOT add a Sources section (it is appended separately).
    - Output ONLY the final Markdown report — no preamble, no notes about what you changed.
    """

    # Minimum number of sources to activate parallel section mode on HARD depth.
    _PARALLEL_SECTION_MIN_SOURCES = 18
    _PARALLEL_SECTION_CHUNK = 12
    # Cap HARD writer sections so the deeper pool stays ~6 same-size section calls
    # (72 sources / chunk 12 = 6); a guard if max_sources is raised further later.
    _PARALLEL_SECTION_HARD_MAX_SECTIONS = 6
    # MEDIUM also uses the multi-stage writer (outline → sections → stitch) once it
    # has enough evidence, but with tighter bounds than HARD to cap cost/latency.
    # At 60 sources the 3-section cap yields ~20-source sections (3 calls + synthesis).
    _PARALLEL_SECTION_MIN_SOURCES_MEDIUM = 12
    _PARALLEL_SECTION_CHUNK_MEDIUM = 8
    _PARALLEL_SECTION_MEDIUM_MAX_SECTIONS = 3

    def __init__(
        self,
        llm,
        source_critic: SourceCriticAgent | None = None,
        evidence_mapper: EvidenceMapperAgent | None = None,
        claim_verifier: ClaimVerifierAgent | None = None,
        report_critic: "ReportCriticAgent | None" = None,
    ):
        super().__init__(llm)
        self.source_critic = source_critic or SourceCriticAgent()
        self.evidence_mapper = evidence_mapper or EvidenceMapperAgent()
        self.claim_verifier = claim_verifier or ClaimVerifierAgent()
        self.report_critic = report_critic or ReportCriticAgent()

    def _normalize_text(self, value: str | None) -> str:
        return rust_accel.normalize_text(value)

    def _content_fingerprint(self, title: str, content: str) -> str:
        return rust_accel.content_fingerprint(title, content, 250)

    def _trusted_domain_score(self, url: str) -> int:
        domain = urlparse(url).netloc.lower().removeprefix("www.")
        if not domain:
            return 0
        if domain in self.TRUSTED_DOMAIN_EXACT_MATCHES:
            return 200
        if any(domain.endswith(suffix) for suffix in self.TRUSTED_DOMAIN_SUFFIXES):
            return 150
        if domain.endswith(".github.io"):
            return 40
        return 0

    def _low_value_domain_penalty(self, url: str) -> int:
        domain = urlparse(url).netloc.lower().removeprefix("www.")
        if not domain:
            return 0
        if domain in self.LOW_VALUE_DOMAIN_EXACT_MATCHES:
            return 120
        if any(token in domain for token in self.LOW_VALUE_DOMAIN_SUBSTRINGS):
            return 90
        return 0

    def _source_quality_score(self, source_quality: str | None) -> int:
        if source_quality == "high":
            return 180
        if source_quality == "medium":
            return 60
        return 0

    def _authority_hint_score(self, url: str, title: str, content: str) -> int:
        normalized_title = self._normalize_text(title).lower()
        normalized_content = self._normalize_text(content).lower()
        normalized_url = (url or "").lower()
        score = 0
        if any(token in normalized_url for token in ("/docs", "/documentation", "/reference", "/api")):
            score += 120
        if any(token in normalized_title for token in ("documentation", "docs", "reference", "api", "guide", "manual")):
            score += 80
        if any(token in normalized_content[:600] for token in ("official documentation", "api reference", "reference guide")):
            score += 60
        return score

    def _score_source(self, url: str, title: str, content: str, source_quality: str | None = None) -> int:
        normalized_title = self._normalize_text(title)
        normalized_content = self._normalize_text(content)
        score = len(normalized_content)
        if normalized_title:
            score += 100
        score += self._trusted_domain_score(url)
        score += self._source_quality_score(source_quality)
        score += self._authority_hint_score(url, title, content)
        score -= self._low_value_domain_penalty(url)
        if "failed to extract content" in normalized_content.lower():
            score -= 5000
        score -= self._speculative_penalty(url, title, content, source_quality)
        return score

    def _speculative_penalty(self, url: str, title: str, content: str, source_quality: str | None = None) -> int:
        normalized_title = self._normalize_text(title).lower()
        normalized_content = self._normalize_text(content).lower()
        normalized_url = (url or "").lower()
        score = 0
        if any(token in normalized_title for token in self.SPECULATIVE_TITLE_TOKENS):
            score += 130
        if any(token in normalized_url for token in ("prediction", "predictions", "gadgets", "coming", "future")):
            score += 35
        content_window = normalized_content[:700]
        speculative_hits = sum(1 for token in self.SPECULATIVE_CONTENT_TOKENS if token in content_window)
        score += speculative_hits * 15
        if source_quality == "low":
            score += 45
        return score

    def _detect_topics(self, prompt: str, tasks: List[SearchTask]) -> set[str]:
        haystack_parts = [self._normalize_text(prompt)]
        for task in tasks:
            haystack_parts.append(self._normalize_text(task.description))
            haystack_parts.extend(self._normalize_text(query) for query in task.queries or [])
        return combined_topics(*haystack_parts)

    def _topic_domain_adjustment(
        self,
        url: str,
        title: str,
        content: str,
        source_quality: str | None = None,
        topics: set[str] | None = None,
    ) -> int:
        normalized_domain = urlparse(url).netloc.lower().removeprefix("www.")
        normalized_title = self._normalize_text(title).lower()
        normalized_content = self._normalize_text(content).lower()
        normalized_url = (url or "").lower()
        topics = topics or set()
        score = 0

        for topic_name in topics:
            policy = TOPIC_POLICIES[topic_name]
            has_strong_editorial_signal = any(
                token in normalized_title or token in normalized_content[:500] or token in normalized_url
                for token in policy.strong_editorial_tokens
            )
            if normalized_domain in policy.premium_domains:
                score += 220
            if normalized_domain in policy.secondary_domains:
                score += 60
            if normalized_domain in policy.weak_domains:
                score -= 180
            if any(token in normalized_domain for token in policy.weak_domain_substrings):
                score -= 120
            if any(token in normalized_title for token in policy.generic_listicle_tokens) and not has_strong_editorial_signal:
                score -= 140
            if any(token in normalized_content[:450] for token in policy.generic_listicle_tokens) and not has_strong_editorial_signal:
                score -= 80
            if any(token in normalized_title for token in policy.weak_signal_tokens):
                score -= 95
            if any(token in normalized_content[:500] for token in policy.weak_signal_tokens):
                score -= 70
            if topic_name == "docs_programming":
                if any(token in normalized_url for token in ("/docs", "/documentation", "/reference", "/manual", "/api", "/extensions", "/async", "/tutorial/")):
                    score += 120
                if any(token in normalized_title for token in ("documentation", "reference", "api", "extensions", "async / await", "user guide")):
                    score += 90
                if any(token in normalized_title for token in ("comparison", "versus", "vs", "showdown", "which framework is best", "in-depth comparison")) and not has_strong_editorial_signal:
                    score -= 120
                if any(token in normalized_content[:500] for token in ("use cases", "pros and cons", "which one to choose", "key differences")) and not has_strong_editorial_signal:
                    score -= 75
        if any(token in normalized_url for token in ("rumor", "rumours", "rumors", "launch-date", "price-in", "upcoming")):
            score -= 75
        if source_quality == "low":
            score -= 40

        return score

    def _should_exclude_source(
        self,
        url: str,
        title: str,
        content: str,
        source_quality: str | None = None,
        topics: set[str] | None = None,
    ) -> bool:
        topics = topics or set()
        penalty = self._speculative_penalty(url, title, content, source_quality)
        trusted_score = self._trusted_domain_score(url)
        if source_quality == "low" and penalty >= 160 and trusted_score <= 0:
            return True
        normalized_content = self._normalize_text(content).lower()
        if source_quality == "low" and len(normalized_content) < 220 and penalty >= 80:
            return True
        if topics:
            topic_score = self._topic_domain_adjustment(url, title, content, source_quality, topics=topics)
            if topic_score <= -180 and trusted_score <= 0 and source_quality != "high":
                return True
            if topic_score <= -120 and source_quality == "low" and len(normalized_content) < 1200:
                return True
        return False

    def _compact_source_content(self, content: str) -> str:
        return rust_accel.compact_source_content(content, self.MAX_SOURCE_CONTENT_CHARS)

    def _content_budget_for_source(self, candidate: dict) -> int:
        quality = candidate.get("source_quality")
        trusted_score = self._trusted_domain_score(candidate.get("url") or "")
        if quality == "high" or trusted_score >= 150:
            return self.MAX_PREMIUM_SOURCE_CONTENT_CHARS
        if quality == "medium" or trusted_score > 0:
            return self.MAX_MEDIUM_SOURCE_CONTENT_CHARS
        return self.MAX_LOW_SOURCE_CONTENT_CHARS

    def _budget_compacted_content(self, content: str, char_budget: int) -> str:
        budget = max(220, min(self.MAX_SOURCE_CONTENT_CHARS, char_budget))
        return rust_accel.compact_source_content(content, budget)

    def _apply_payload_budget(self, candidates: list[dict], payload_char_budget: int) -> list[dict]:
        remaining_budget = max(payload_char_budget, 2000)
        budgeted_candidates: list[dict] = []
        for index, candidate in enumerate(candidates):
            reserved_tail = max(0, len(candidates) - index - 1) * 220
            available_budget = remaining_budget - reserved_tail
            target_budget = min(self._content_budget_for_source(candidate), max(220, available_budget))
            if target_budget < 220:
                break
            compacted = self._budget_compacted_content(candidate.get("content") or "", target_budget)
            if not compacted:
                continue
            budgeted_candidates.append({**candidate, "content": compacted})
            remaining_budget -= len(compacted)
            if remaining_budget <= 220:
                break
        return budgeted_candidates

    def _resolve_depth_profile(self, depth: SearchDepth | None) -> dict:
        if depth is None:
            depth = SearchDepth.MEDIUM
        return self.DEPTH_ANALYSIS_PROFILES[depth]

    def _prepare_aggregated_data(self, prompt: str, tasks: List[SearchTask], depth: SearchDepth | None = None) -> tuple[list[dict], object]:
        topics = self._detect_topics(prompt, tasks)
        profile = self._resolve_depth_profile(depth)
        aggregated_candidates = []
        for task in tasks:
            if task.status != "completed" or not task.result:
                continue

            for res in task.result:
                title = self._normalize_text(res.get("title"))
                content = self._normalize_text(res.get("content"))
                url = res.get("url")
                if not url or not content or "failed to extract content" in content.lower():
                    continue
                source_quality = res.get("source_quality") or "low"

                aggregated_candidates.append(
                    {
                        "task_description": task.description,
                        "url": url,
                        "domain": res.get("domain") or urlparse(url).netloc.lower() or None,
                        "source_quality": source_quality,
                        "title": title or None,
                        "content": self._compact_source_content(content),
                    }
                )

        selected_candidates = rust_accel.select_analyzer_sources(
            aggregated_candidates,
            topics=topics,
            max_sources=profile["max_sources"],
            max_sources_per_domain=profile["max_sources_per_domain"],
            max_sources_per_task=profile["max_sources_per_task"],
        )

        # Stable sort: high-quality sources first so they receive more content budget.
        _quality_order = {"high": 0, "medium": 1, "low": 2}
        selected_candidates.sort(key=lambda c: _quality_order.get(c.get("source_quality") or "low", 2))

        selected_candidates = self._apply_payload_budget(
            selected_candidates,
            payload_char_budget=profile["payload_char_budget"],
        )

        aggregated_data = [
            {
                "source_id": f"S{index}",
                **{key: value for key, value in candidate.items() if key != "_score"},
            }
            for index, candidate in enumerate(selected_candidates, start=1)
        ]
        return self.source_critic.assess_sources(aggregated_data)

    _EVIDENCE_GROUP_LIMITS = {
        SearchDepth.EASY: 5,
        SearchDepth.MEDIUM: 8,
        SearchDepth.HARD: 12,
    }

    def _extract_evidence_groups(self, aggregated_data: list[dict], depth: SearchDepth | None = None) -> tuple[list[dict], object]:
        max_groups = self._EVIDENCE_GROUP_LIMITS.get(depth or SearchDepth.MEDIUM, 8)
        return self.evidence_mapper.build_evidence_groups(
            aggregated_data=aggregated_data,
            stopwords=self.STOPWORDS,
            generic_tokens=self.CONFLICT_GENERIC_TOKENS,
            negation_tokens=self.NEGATION_TOKENS,
            max_groups=max_groups,
        )

    # Patterns the synthesis LLM sometimes prepends as meta-commentary.
    _LLM_PREAMBLE = re.compile(
        r"(?i)^(ниже\s+представлен|ниже\s+приведён|below\s+is\s+(the|a)\s+(synthesized|final|complete)|"
        r"the\s+following\s+is\s+(a|the)\s+(synthesized|final)|вот\s+синтезирован|"
        r"данный\s+отчёт\s+объединяет|объединяю\s+все|синтезирую\s+все|"
        r"i\s+have\s+(combined|merged|synthesized)|here\s+is\s+(the|a)\s+(synthesized|final))[^\n]*\n+"
    )

    # A short prose fragment the writer sometimes leaks onto the front of a table row,
    # breaking the Markdown table (e.g. "Based on the available sources, | cell | cell |").
    _POLLUTED_TABLE_ROW = re.compile(r"(?m)^[ \t]*([^|\n]{1,80}?[,:])\s*(\|.*\|)[ \t]*$")

    def _clean_table_rows(self, text: str) -> str:
        """Strip a leaked sentence fragment that prefixes a table row, so the table renders.

        Only fires when the fragment ends in a comma/colon (a dangling clause) and the rest
        of the line is a real table row (>=2 pipes) — legitimate prose with a pipe is untouched.
        """
        def _fix(m: "re.Match") -> str:
            return m.group(2) if m.group(2).count("|") >= 2 else m.group(0)
        return self._POLLUTED_TABLE_ROW.sub(_fix, text)

    def _post_process_report(self, report: str, language: str) -> str:
        normalized = report.replace("\r\n", "\n").strip()
        normalized = re.sub(r"\n{3,}", "\n\n", normalized)
        normalized = re.sub(r"(?m)^[ \t]+$", "", normalized)
        # Strip LLM meta-commentary preamble that sometimes appears before the first heading.
        normalized = self._LLM_PREAMBLE.sub("", normalized).strip()
        normalized = self._clean_table_rows(normalized)

        localized_sources_heading = self._sources_heading(language)
        if re.search(r"(?im)^sources:\s*$", normalized):
            normalized = re.sub(r"(?im)^sources:\s*$", localized_sources_heading, normalized)
        elif re.search(r"(?im)^источники:\s*$", normalized):
            normalized = re.sub(r"(?im)^источники:\s*$", localized_sources_heading, normalized)
        elif re.search(r"(?im)^#*\s*(sources|источники)\s*$", normalized) is None:
            normalized = f"{normalized}\n\n{localized_sources_heading}"

        normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()
        return normalized

    def _detect_language(self, text: str) -> str:
        normalized = self._normalize_text(text).lower()
        if not normalized:
            return "unknown"

        cyrillic_count = sum(1 for char in normalized if "а" <= char <= "я" or char == "ё")
        latin_count = sum(1 for char in normalized if "a" <= char <= "z")
        if cyrillic_count >= 6 and cyrillic_count >= latin_count / 3:
            return "ru"

        tokens = re.findall(r"[a-záéíóúñü]+", normalized)
        if not tokens:
            return "unknown"

        scores = {
            language: sum(1 for token in tokens if token in hints)
            for language, hints in self.LANGUAGE_HINTS.items()
        }
        best_language = max(scores, key=scores.get)
        if scores[best_language] <= 0:
            return "en" if latin_count else "unknown"
        return best_language

    def _language_instruction(self, language: str) -> str:
        if language == "ru":
            return "Write the full report in Russian."
        if language == "es":
            return "Write the full report in Spanish."
        if language == "en":
            return "Write the full report in English."
        return "Write the full report in the same language as the original prompt."

    def _sources_heading(self, language: str) -> str:
        return "## Источники" if language == "ru" else "## Sources"

    def _conflicts_heading(self, language: str) -> str:
        return "## Противоречия и неопределённости" if language == "ru" else "## Conflicts And Uncertainties"

    def _report_notes_heading(self, language: str) -> str:
        return "## Примечания к отчёту" if language == "ru" else "## Report Notes"

    def _used_sources_heading(self, language: str) -> str:
        return "### Использованные источники" if language == "ru" else "### Used Sources"

    def _additional_sources_heading(self, language: str) -> str:
        return "### Дополнительные релевантные источники" if language == "ru" else "### Additional Relevant Sources"

    def _quality_note_messages(self, language: str) -> dict[str, str]:
        if language == "ru":
            return {
                "missing_intro": "В отчёте отсутствует явный заголовок введения.",
                "missing_conclusion": "В отчёте отсутствует явный заголовок заключения.",
                "no_inline_citations": "В отчёте нет встроенных ссылок на источники.",
                "no_sources": "Для анализа не было доступно пригодных извлечённых источников.",
                "few_sources": "Отчёт опирается менее чем на два пригодных источника.",
                "small_subset": "В финальном отчёте используется лишь небольшая часть доступных источников.",
                "weak_support": "Некоторые цитируемые строки слабо подтверждаются указанными источниками.",
                "empty_sources": "Раздел источников присутствует, но в нём нет реально использованных ссылок.",
            }
        return {
            "missing_intro": "The report is missing a clear introduction heading.",
            "missing_conclusion": "The report is missing a clear conclusion heading.",
            "no_inline_citations": "The report does not cite any sources inline.",
            "no_sources": "No usable extracted sources were available for analysis.",
            "few_sources": "The report is based on fewer than two usable sources.",
            "small_subset": "Only a small subset of the available sources is cited in the final report.",
            "weak_support": "Some cited lines appear weakly supported by their attached sources.",
            "empty_sources": "The sources section is present but no cited sources were included under it.",
        }

    def _plan_outline_block(self, plan_questions: list[str] | None) -> str:
        """Render the plan's sub-questions as an outline directive so the report answers
        exactly what was asked, with sections mapped to the facets of the question."""
        questions = [q.strip() for q in (plan_questions or []) if q and q.strip()]
        if not questions:
            return ""
        numbered = "\n".join(f"{i}. {q}" for i, q in enumerate(dict.fromkeys(questions), start=1))
        return (
            "Organize the report so it directly answers these planned sub-questions, "
            "with a descriptive section (or clearly labelled part) for each facet — merge "
            "overlapping ones, and don't leave any unanswered if the evidence covers it:\n"
            f"{numbered}\n\n"
        )

    def _build_user_prompt(
        self,
        input_data: dict,
        language: str,
        retry: bool = False,
        depth: SearchDepth | None = None,
        plan_questions: list[str] | None = None,
    ) -> str:
        instruction = self._language_instruction(language)
        if retry:
            instruction = (
                f"{instruction} Your previous answer used the wrong language. "
                "Rewrite the report fully in the requested language and keep factual citations."
            )
        profile = self._resolve_depth_profile(depth)
        expanded_report_instruction = (
            f"{profile['report_instruction']} Use a broader portion of the available source pool where it materially improves coverage."
        )
        confidence_instruction = (
            "When evidence comes from editorial lists, critic roundups, box-office summaries, or mixed-quality sources, "
            "use cautious wording such as 'according to these sources', 'several sources highlight', "
            "'editorial selections suggest', or 'the available data indicates' instead of absolute claims. "
            "If support is weak or mixed, acknowledge that explicitly instead of overstating certainty."
        )
        return (
            "Please analyze this data and generate the final report. "
            f"{instruction} "
            f"{expanded_report_instruction} "
            "Lead with an answer-first Executive summary (direct answer + 3–6 key takeaways with [Sn]), "
            "use descriptive section headings, prefer concrete specifics over vague wording, and use Markdown "
            "tables for comparative or quantitative data. "
            f"{confidence_instruction} "
            "Prefer concrete reported developments over speculative future-looking claims. "
            "Use evidence_groups to identify where multiple sources reinforce the same point. "
            "If a source is mostly predictive, label it as a forecast rather than a confirmed development. "
            "Separate what is well-established from what is contested or unresolved.\n\n"
            f"{self._plan_outline_block(plan_questions)}"
            f"{json.dumps(input_data, ensure_ascii=False)}"
        )

    def _build_repair_prompt(
        self,
        input_data: dict,
        language: str,
        report_body: str,
        uncited_lines: list[str],
        unsupported_lines: list[str],
    ) -> str:
        sources = input_data.get("gathered_data") or []
        source_table_lines = []
        for item in sources:
            sid = item.get("source_id", "")
            title = (item.get("title") or "")[:80]
            snippet = (item.get("content") or "")[:120].replace("\n", " ")
            source_table_lines.append(f"{sid}: {title} — {snippet}")
        source_table = "\n".join(source_table_lines)
        feedback_lines = "\n".join(f"- {line}" for line in uncited_lines[:8]) or "- none"
        unsupported_feedback = "\n".join(f"- {line}" for line in unsupported_lines[:8]) or "- none"
        return (
            f"{self._language_instruction(language)} "
            "Rewrite only the report body so that every factual paragraph or bullet includes inline citations. "
            "Do not include a Sources section in your answer. "
            "Use only the available source IDs, do not invent citations, and preserve markdown headings. "
            "Every citation should support the sentence it is attached to; remove weak or mismatched citations.\n\n"
            "Available sources (ID: title — excerpt):\n"
            f"{source_table}\n\n"
            "Lines that need citations added:\n"
            f"{feedback_lines}\n\n"
            "Lines with weak or mismatched citations to fix:\n"
            f"{unsupported_feedback}\n\n"
            "Current report body:\n"
            f"{report_body}"
        )

    def _extract_used_source_ids(self, report_body: str) -> list[str]:
        return rust_accel.extract_used_source_ids(report_body)

    def _sanitize_citations(self, report: str, valid_source_ids: set[str]) -> str:
        return rust_accel.sanitize_citations(report, valid_source_ids)

    def _source_display_label(self, source: dict) -> str:
        """Return a short human-readable label for a source (title or domain)."""
        title = (source.get("title") or "").strip()
        url   = source.get("url") or ""
        domain = source.get("domain") or urlparse(url).netloc.removeprefix("www.")
        if title:
            return (title[:80] + "…") if len(title) > 80 else title
        return domain or url

    def _source_line(self, source_id: str, source: dict, bold: bool = True) -> str:
        """Render one source as a proper markdown list item with a clickable link."""
        url   = source.get("url") or ""
        label = self._source_display_label(source)
        # Escape brackets so [S2] in the label doesn't confuse the markdown parser.
        id_part = f"**\\[{source_id}\\]**" if bold else f"\\[{source_id}\\]"
        if url:
            return f"- {id_part} [{label}]({url})"
        return f"- {id_part} {label}"

    def _rebuild_sources_section(self, report: str, aggregated_data: list[dict], language: str) -> str:
        without_sources = self.SOURCE_HEADING_PATTERN.sub("", report).strip()
        valid_sources = {item["source_id"]: item for item in aggregated_data}
        sanitized = self._sanitize_citations(without_sources, set(valid_sources))
        used_source_ids = self._extract_used_source_ids(sanitized)
        # List every gathered-but-uncited source (capped) so the Sources section is
        # never empty when evidence exists; reliability is conveyed via inline citations
        # and the source-critic annotations rather than by dropping low-quality sources.
        used_set = set(used_source_ids)
        additional_source_ids = [
            item["source_id"]
            for item in aggregated_data
            if item["source_id"] not in used_set
        ][:8]  # cap to avoid bloated sources section

        lines = [self._sources_heading(language)]
        if used_source_ids:
            lines.append(self._used_sources_heading(language))
            for source_id in used_source_ids:
                source = valid_sources.get(source_id)
                if source is None:
                    continue
                lines.append(self._source_line(source_id, source, bold=True))

        if additional_source_ids:
            lines.append(self._additional_sources_heading(language))
            for source_id in additional_source_ids:
                source = valid_sources.get(source_id)
                if source is None:
                    continue
                lines.append(self._source_line(source_id, source, bold=False))

        return f"{sanitized.strip()}\n\n" + "\n".join(lines)

    def _body_without_sources(self, report: str) -> str:
        return self.SOURCE_HEADING_PATTERN.sub("", report).strip()

    def _line_requires_citation(self, line: str) -> bool:
        stripped = line.strip()
        if not stripped:
            return False
        if stripped.startswith("#"):
            return False
        if len(stripped) < 45:
            return False
        # Source list items (e.g. "- [S12] https://..." or "- **\[S12\]** [title](url)")
        if re.match(r"^-\s*(?:\*{0,2}\\?\[S\d+\\?\]\*{0,2})", stripped):
            return False
        lowered = stripped.lower()
        if (
            lowered.startswith("source")
            or lowered.startswith("sources")
            or lowered.startswith("report notes")
            or lowered.startswith("источник")
            or lowered.startswith("источники")
            or lowered.startswith("примечания к отчету")
            or lowered.startswith("примечания к отчёту")
        ):
            return False
        return bool(re.search(r"[A-Za-zА-Яа-я0-9]", stripped))

    def _uncited_claim_lines(self, report: str) -> list[str]:
        uncited_lines: list[str] = []
        for line in self._body_without_sources(report).splitlines():
            if not self._line_requires_citation(line):
                continue
            if self.CITATION_PATTERN.search(line):
                continue
            uncited_lines.append(self._normalize_text(line))
        return uncited_lines

    def _line_tokens_for_citation_audit(self, line: str) -> set[str]:
        lowered = self._normalize_text(line).lower()
        return {
            token
            for token in re.findall(r"[a-zа-я0-9]+", lowered)
            if len(token) >= 4 and token not in self.STOPWORDS and token not in self.CONFLICT_GENERIC_TOKENS
        }

    def _source_token_index(self, aggregated_data: list[dict]) -> dict[str, set[str]]:
        return {
            item["source_id"]: self._line_tokens_for_citation_audit(item.get("content") or "")
            for item in aggregated_data
        }

    def _unsupported_citation_lines(self, report: str, aggregated_data: list[dict]) -> list[str]:
        source_tokens = self._source_token_index(aggregated_data)
        unsupported_lines: list[str] = []
        valid_source_ids = set(source_tokens)
        for line in self._body_without_sources(report).splitlines():
            normalized_line = self._normalize_text(line)
            if not self._line_requires_citation(normalized_line):
                continue
            cited_source_ids = {
                source_id
                for source_id in self._extract_used_source_ids(normalized_line)
                if source_id in valid_source_ids
            }
            if not cited_source_ids:
                continue
            line_tokens = self._line_tokens_for_citation_audit(normalized_line)
            if len(line_tokens) < 2:
                continue
            supported = False
            for source_id in cited_source_ids:
                overlap = line_tokens & source_tokens.get(source_id, set())
                if len(overlap) >= 2:
                    supported = True
                    break
            if not supported:
                unsupported_lines.append(normalized_line)
        return unsupported_lines

    def _insufficient_evidence_lines(self, report: str, aggregated_data: list[dict]) -> list[str]:
        insufficient_lines: list[str] = []
        valid_source_ids = {item["source_id"] for item in aggregated_data}
        for line in self._body_without_sources(report).splitlines():
            normalized_line = self._normalize_text(line)
            if not self._line_requires_citation(normalized_line):
                continue
            cited_source_ids = [
                source_id
                for source_id in self._extract_used_source_ids(normalized_line)
                if source_id in valid_source_ids
            ]
            if not cited_source_ids:
                continue
            if len(set(cited_source_ids)) >= 2:
                continue
            lowered = normalized_line.lower()
            if any(
                marker in lowered
                for marker in (
                    "best ",
                    "worst ",
                    "clearly",
                    "definitively",
                    "always",
                    "never",
                    "лучш",
                    "худш",
                    "всегда",
                    "никогда",
                    "однозначно",
                    "явно",
                )
            ):
                insufficient_lines.append(normalized_line)
        return insufficient_lines

    def _looks_like_structured_report(self, report: str) -> bool:
        return "## " in report or len(report) >= 400 or report.count("\n") >= 4

    def _repair_report_citations(
        self,
        input_data: dict,
        language: str,
        report_body: str,
        uncited_lines: list[str],
        unsupported_lines: list[str],
        model: str | None = None,
    ) -> str:
        repair_prompt = self._build_repair_prompt(
            input_data,
            language,
            report_body,
            uncited_lines,
            unsupported_lines,
        )
        kwargs = {"temperature": 0.2}
        # Dedicated repair model wins; otherwise reuse the run's selected model.
        repair_model = settings.deepseek_repair_model or model
        if repair_model:
            kwargs["model"] = repair_model
        return self.llm.generate(
            system_prompt=self.SYSTEM_PROMPT,
            user_prompt=repair_prompt,
            **kwargs,
        )

    def _best_supporting_source_id(self, line: str, aggregated_data: list[dict]) -> str | None:
        line_tokens = self._line_tokens_for_citation_audit(line)
        if len(line_tokens) < 2:
            return None

        best_source_id = None
        best_overlap = 0
        for source in aggregated_data:
            source_id = source["source_id"]
            source_tokens = self._line_tokens_for_citation_audit(source.get("content") or "")
            overlap = len(line_tokens & source_tokens)
            if overlap > best_overlap:
                best_overlap = overlap
                best_source_id = source_id

        if best_overlap < 2:
            return None
        return best_source_id

    def _patch_line_with_source(self, line: str, source_id: str) -> str:
        if self.CITATION_PATTERN.search(line):
            return self.CITATION_PATTERN.sub(f"[{source_id}]", line)
        stripped = line.rstrip()
        if stripped.endswith((".", "!", "?")):
            return f"{stripped} [{source_id}]"
        return f"{stripped} [{source_id}]"

    def _deterministic_repair_report_body(
        self,
        report_body: str,
        aggregated_data: list[dict],
        uncited_lines: list[str],
        unsupported_lines: list[str],
    ) -> str | None:
        issue_lines = list(dict.fromkeys(uncited_lines + unsupported_lines))
        if not issue_lines or len(issue_lines) > settings.analyzer_local_repair_issue_threshold:
            return None

        patched = report_body
        replacements = 0
        for issue_line in issue_lines:
            source_id = self._best_supporting_source_id(issue_line, aggregated_data)
            if source_id is None:
                return None
            escaped = re.escape(issue_line)
            updated = re.sub(
                escaped,
                lambda match: self._patch_line_with_source(match.group(0), source_id),
                patched,
                count=1,
            )
            if updated == patched:
                return None
            patched = updated
            replacements += 1

        return patched if replacements == len(issue_lines) else None

    def _extract_candidate_claims(self, aggregated_data: list[dict]) -> list[dict]:
        claims: list[dict] = []
        for source in aggregated_data:
            content = source.get("content") or ""
            for sentence in self.SENTENCE_PATTERN.split(content):
                normalized_sentence = self._normalize_text(sentence)
                if len(normalized_sentence) < 50 or len(normalized_sentence) > 260:
                    continue

                lowered = normalized_sentence.lower()
                tokens = [
                    token for token in re.findall(r"[a-z0-9]+", lowered)
                    if len(token) >= 4 and token not in self.STOPWORDS
                ]
                unique_tokens: list[str] = []
                for token in tokens:
                    if token not in unique_tokens:
                        unique_tokens.append(token)

                if len(unique_tokens) < 2:
                    continue

                numbers = tuple(
                    number
                    for number in re.findall(r"\b\d+(?:\.\d+)?\b", lowered)
                    if not self._is_likely_year(number)
                )
                has_negation = any(token in lowered for token in self.NEGATION_TOKENS)
                claims.append(
                    {
                        "source_id": source["source_id"],
                        "sentence": normalized_sentence,
                        "tokens": unique_tokens[:6],
                        "numbers": numbers,
                        "has_negation": has_negation,
                    }
                )
        return claims

    def _is_likely_year(self, value: str) -> bool:
        if "." in value:
            return False
        try:
            number = int(value)
        except ValueError:
            return False
        return 1900 <= number <= 2100

    def _informative_shared_tokens(self, left: dict, right: dict) -> set[str]:
        return {
            token
            for token in set(left["tokens"]) & set(right["tokens"])
            if token not in self.CONFLICT_GENERIC_TOKENS
        }

    def _claims_overlap(self, left: dict, right: dict) -> bool:
        shared_tokens = self._informative_shared_tokens(left, right)
        return len(shared_tokens) >= 2

    def _claims_conflict(self, left: dict, right: dict) -> bool:
        if left["source_id"] == right["source_id"]:
            return False
        shared_tokens = self._informative_shared_tokens(left, right)
        if len(shared_tokens) < 2:
            return False
        if left["has_negation"] != right["has_negation"]:
            return True

        left_numbers = set(left["numbers"])
        right_numbers = set(right["numbers"])
        if left_numbers and right_numbers and left_numbers != right_numbers and len(shared_tokens) >= 3:
            return True
        return False

    def _detect_conflicts(self, aggregated_data: list[dict]) -> list[dict]:
        return rust_accel.detect_conflicts(
            aggregated_data=aggregated_data,
            stopwords=self.STOPWORDS,
            generic_tokens=self.CONFLICT_GENERIC_TOKENS,
            negation_tokens=self.NEGATION_TOKENS,
            max_conflicts=3,
        )

    def _is_substantive_conflict_sentence(self, sentence: str) -> bool:
        """Return True only for real claim sentences, not titles or questions."""
        stripped = sentence.strip()
        if stripped.endswith("?"):
            return False
        words = stripped.split()
        if len(words) < 6:
            return False
        lowered = stripped.lower()
        has_number = bool(re.search(r"\b\d+\b", lowered))
        has_negation = any(tok in lowered for tok in self.NEGATION_TOKENS)
        has_claim_verb = any(tok in lowered for tok in (
            # English
            "will", "would", "could", "replace", "eliminate", "reduce", "increase",
            "exceed", "exceeds", "surpass", "outpace", "claim", "claims", "argue",
            "argues", "versus", "compared",
            # Russian — future / present claim verbs
            "заменит", "сократит", "исчезнут", "появятся", "вырастет", "снизится",
            "превышает", "превысил", "уступает", "опережает", "составляет",
            "достигает", "достиг", "утверждает", "считает", "планирует",
            "против", "тогда как", "в отличие",
        ))
        return has_number or has_negation or has_claim_verb

    def _inject_conflicts_section(self, report: str, conflicts: list[dict], language: str) -> str:
        if not conflicts or self.CONFLICT_HEADING_PATTERN.search(report):
            return report

        substantive = [
            c for c in conflicts
            # at least one of the two sentences must be a concrete claim
            if any(self._is_substantive_conflict_sentence(s) for s in c.get("sentences", []))
        ]
        if not substantive:
            return report

        lines = [self._conflicts_heading(language)]
        for conflict in substantive:
            left_source, right_source = conflict["source_ids"]
            left_sentence, right_sentence = conflict["sentences"]
            if language == "ru":
                lines.append(
                    f"- Тема: {conflict['topic']}. Причина: {conflict.get('reason') or 'существенное расхождение'}. "
                    f'Данные: "{left_sentence}" [{left_source}] против "{right_sentence}" [{right_source}].'
                )
            else:
                lines.append(
                    f"- Topic: {conflict['topic']}. Reason: {conflict.get('reason') or 'material discrepancy'}. "
                    f'Evidence: "{left_sentence}" [{left_source}] versus "{right_sentence}" [{right_source}].'
                )

        insertion = "\n".join(lines)
        conclusion_match = re.search(r"(?im)^##\s+(Conclusion|Заключение)\s*$", report)
        if conclusion_match:
            return f"{report[:conclusion_match.start()].rstrip()}\n\n{insertion}\n\n{report[conclusion_match.start():].lstrip()}"
        return f"{report.strip()}\n\n{insertion}"

    def _report_quality_notes(self, report: str, aggregated_data: list[dict], language: str) -> list[str]:
        notes: list[str] = []
        messages = self._quality_note_messages(language)
        normalized = report.lower()
        used_source_ids = self._extract_used_source_ids(report)
        unsupported_lines = self._unsupported_citation_lines(report, aggregated_data)

        if not self.INTRODUCTION_HEADING_PATTERN.search(report):
            notes.append(messages["missing_intro"])
        if not self.CONCLUSION_HEADING_PATTERN.search(report):
            notes.append(messages["missing_conclusion"])
        if not used_source_ids:
            notes.append(messages["no_inline_citations"])
        if not aggregated_data:
            notes.append(messages["no_sources"])
        elif len(aggregated_data) < 2:
            notes.append(messages["few_sources"])
        elif len(used_source_ids) < min(2, len(aggregated_data)):
            notes.append(messages["small_subset"])
        if unsupported_lines:
            notes.append(messages["weak_support"])
        if ("## sources" in normalized or "## источники" in normalized) and report.strip().endswith(
            self._sources_heading(language)
        ):
            notes.append(messages["empty_sources"])

        return notes

    def _inject_report_notes(self, report: str, notes: list[str], language: str) -> str:
        if not notes or self.REPORT_NOTES_HEADING_PATTERN.search(report):
            return report

        section = self._report_notes_heading(language) + "\n" + "\n".join(f"- {note}" for note in notes)
        sources_match = re.search(r"(?im)^##\s+(Sources|Источники)\s*$", report)
        if sources_match:
            return f"{report[:sources_match.start()].rstrip()}\n\n{section}\n\n{report[sources_match.start():].lstrip()}"
        return f"{report.strip()}\n\n{section}"

    def _generate_report(
        self,
        input_data: dict,
        language: str,
        retry: bool = False,
        depth: SearchDepth | None = None,
        model: str | None = None,
        streaming_callback: Optional[Callable[[str], None]] = None,
        reasoning_callback: Optional[Callable[[str], None]] = None,
        plan_questions: list[str] | None = None,
    ) -> str:
        user_prompt = self._build_user_prompt(input_data, language, retry=retry, depth=depth, plan_questions=plan_questions)
        return self.llm.generate(
            system_prompt=self.SYSTEM_PROMPT,
            user_prompt=user_prompt,
            streaming_callback=streaming_callback,
            reasoning_callback=reasoning_callback,
            model=model,
            temperature=0.3,
        )

    def _build_section_user_prompt(self, chunk: list[dict], prompt: str, language: str, depth: SearchDepth | None) -> str:
        instruction = self._language_instruction(language)
        profile = self._resolve_depth_profile(depth)
        section_input = {
            "original_prompt": prompt,
            "gathered_data": chunk,
        }
        return (
            f"{instruction} "
            f"{profile['report_instruction']} "
            "Analyze ONLY these sources and write detailed analytical findings. "
            "Cite every factual claim with [Sn] using the exact source_id values from gathered_data. "
            "Do NOT write Introduction, Conclusion, or Sources sections — only the body content sections.\n\n"
            f"{json.dumps(section_input, ensure_ascii=False)}"
        )

    def _build_synthesis_user_prompt(
        self,
        prompt: str,
        section_drafts: list[str],
        language: str,
        depth: SearchDepth | None,
        conflicts: list[dict] | None = None,
        evidence_groups: list[dict] | None = None,
        source_summary=None,
        plan_questions: list[str] | None = None,
    ) -> str:
        instruction = self._language_instruction(language)
        profile = self._resolve_depth_profile(depth)
        drafts_text = "\n\n".join(
            f"=== SECTION {i + 1} ===\n{draft}"
            for i, draft in enumerate(section_drafts)
        )
        meta: dict = {}
        if source_summary is not None:
            try:
                meta["source_summary"] = source_summary.model_dump()
            except Exception:
                logger.warning("analyzer_source_summary_dump_failed", exc_info=True)
        if conflicts:
            meta["detected_conflicts"] = conflicts
        if evidence_groups:
            meta["evidence_groups"] = evidence_groups
        meta_block = (
            f"\n\nContext metadata:\n{json.dumps(meta, ensure_ascii=False)}"
            if meta else ""
        )
        return (
            f"{instruction} "
            f"{profile['report_instruction']} "
            "Below are partial research analyses from different source groups. "
            "Merge them into ONE coherent report that opens with an answer-first Executive summary "
            "(direct answer + 3–6 key takeaways with [Sn]), uses descriptive section headings, and ends "
            "with a section separating well-established from contested/open points plus a short bottom line. "
            "Deduplicate across the partials — make each point once. "
            "Preserve all inline citations [Sn] exactly. Do NOT include a Sources section. "
            "Use detected_conflicts to surface material disagreements. "
            "Use evidence_groups to emphasise findings supported by multiple sources.\n\n"
            f"{self._plan_outline_block(plan_questions)}"
            f"Research question: {prompt}"
            f"{meta_block}\n\n"
            f"{drafts_text}"
        )

    def _build_editor_prompt(self, report: str, prompt: str, language: str, plan_questions: list[str] | None) -> str:
        return (
            f"{self._language_instruction(language)} "
            "Edit the draft below into a publication-quality final report following your editing rules. "
            "Preserve every [Sn] citation exactly and keep the report comprehensive.\n\n"
            f"{self._plan_outline_block(plan_questions)}"
            f"Research question: {prompt}\n\n"
            f"Draft report:\n{report}"
        )

    def _editor_preserved_citations(self, before: str, after: str) -> bool:
        """Guard against an editor that drops/renumbers citations or truncates the report."""
        if not (after or "").strip():
            return False
        before_ids = set(self._extract_used_source_ids(before))
        after_ids = set(self._extract_used_source_ids(after))
        if before_ids and len(after_ids & before_ids) < 0.5 * len(before_ids):
            return False  # lost more than half the citations — editor went rogue
        if len(after.strip()) < 0.4 * len(before.strip()):
            return False  # summarized away substance
        return True

    def _maybe_edit_report(
        self,
        report: str,
        prompt: str,
        language: str,
        depth: SearchDepth | None,
        model: str | None,
        plan_questions: list[str] | None,
    ) -> str:
        """Final editorial pass: tighten prose, enforce answer-first structure, dedupe.

        One extra LLM call — gated to substantial reports (MEDIUM/HARD) and behind a flag.
        Defensive: falls back to the unedited report if the editor drops citations or fails.
        """
        if not settings.report_editor_enabled:
            return report
        if depth not in (SearchDepth.MEDIUM, SearchDepth.HARD):
            return report  # not worth the extra call on EASY
        if not (report or "").strip():
            return report
        try:
            edited = self.llm.generate(
                system_prompt=self.EDITOR_SYSTEM_PROMPT,
                user_prompt=self._build_editor_prompt(report, prompt, language, plan_questions),
                model=model,
                temperature=0.3,
            )
            edited = (edited or "").strip()
            if self._editor_preserved_citations(report, edited):
                logger.info("report_editor_applied before=%d after=%d", len(report), len(edited))
                return edited
            logger.warning("report_editor_discarded reason=citation_or_length_guard")
        except Exception:  # pragma: no cover - defensive
            logger.warning("report_editor_failed", exc_info=True)
        return report

    def _run_parallel_section_analysis(
        self,
        aggregated_data: list[dict],
        prompt: str,
        language: str,
        depth: SearchDepth | None,
        section_done_callback: Optional[Callable[[int, str], None]] = None,
        conflicts: list[dict] | None = None,
        evidence_groups: list[dict] | None = None,
        source_summary=None,
        model: str | None = None,
        chunk_size: int | None = None,
        max_sections: int | None = None,
        synthesis_streaming_callback: Optional[Callable[[str], None]] = None,
        plan_questions: list[str] | None = None,
    ) -> str:
        chunk_size = chunk_size or self._PARALLEL_SECTION_CHUNK
        # When a section cap is given, grow the chunk so we never exceed it
        # (bounds the number of section LLM calls — used for MEDIUM).
        if max_sections is not None and aggregated_data:
            even_chunk = (len(aggregated_data) + max_sections - 1) // max_sections
            chunk_size = max(chunk_size, even_chunk)
        chunks = [
            aggregated_data[i: i + chunk_size]
            for i in range(0, len(aggregated_data), chunk_size)
        ]
        n = len(chunks)
        logger.info("analyzer_parallel_sections chunks=%d total_sources=%d", n, len(aggregated_data))

        section_drafts: list[Optional[str]] = [None] * n

        def _run_section(idx: int, chunk: list[dict]) -> None:
            user_prompt = self._build_section_user_prompt(chunk, prompt, language, depth)
            draft = self.llm.generate(
                system_prompt=self.SECTION_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                model=model,
                temperature=0.3,
            )
            section_drafts[idx] = draft
            logger.info("analyzer_section_done idx=%d chars=%d", idx, len(draft))
            if section_done_callback:
                section_done_callback(idx, draft)

        with ThreadPoolExecutor(max_workers=min(n, max(1, settings.analyzer_section_concurrency))) as executor:
            futures = [executor.submit(_run_section, i, chunk) for i, chunk in enumerate(chunks)]
            for future in as_completed(futures):
                future.result()  # surface any exceptions

        completed_drafts = [d for d in section_drafts if d]
        synthesis_prompt = self._build_synthesis_user_prompt(
            prompt, completed_drafts, language, depth,
            conflicts=conflicts,
            evidence_groups=evidence_groups,
            source_summary=source_summary,
            plan_questions=plan_questions,
        )
        logger.info("analyzer_synthesis_start section_count=%d", len(completed_drafts))
        # Stream the synthesis token-by-token so the merged report appears live instead
        # of all-at-once after a multi-minute call (perceived-speed win, model-agnostic).
        return self.llm.generate(
            system_prompt=self.SYNTHESIS_SYSTEM_PROMPT,
            user_prompt=synthesis_prompt,
            model=model,
            temperature=0.3,
            streaming_callback=synthesis_streaming_callback,
        )

    @maybe_traceable(name="analyzer_run_analysis", run_type="llm")
    def run_analysis(
        self,
        prompt: str,
        tasks: List[SearchTask],
        depth: SearchDepth | None = None,
        model: str | None = None,
        streaming_callback: Optional[Callable[[str], None]] = None,
        reasoning_callback: Optional[Callable[[str], None]] = None,
    ) -> str:
        started_at = time.perf_counter()
        prepare_started_at = time.perf_counter()
        aggregated_data, source_summary = self._prepare_aggregated_data(prompt, tasks, depth=depth)
        prepare_ms = (time.perf_counter() - prepare_started_at) * 1000

        profile = self._resolve_depth_profile(depth)
        conflict_pool = aggregated_data[: profile["conflict_source_limit"]]
        evidence_pool = aggregated_data[: profile["evidence_source_limit"]]

        conflict_started_at = time.perf_counter()
        conflicts = self._detect_conflicts(conflict_pool)
        conflict_ms = (time.perf_counter() - conflict_started_at) * 1000

        evidence_started_at = time.perf_counter()
        evidence_groups, evidence_summary = self._extract_evidence_groups(evidence_pool, depth=depth)
        evidence_ms = (time.perf_counter() - evidence_started_at) * 1000
        prompt_language = self._detect_language(prompt)
        # Plan sub-questions drive the report outline so it answers exactly what was asked.
        plan_questions = list(dict.fromkeys(
            (t.description or "").strip() for t in tasks if getattr(t, "description", "").strip()
        ))[:12]

        is_hard = depth == SearchDepth.HARD
        is_medium = depth == SearchDepth.MEDIUM
        use_parallel = (
            (is_hard and len(aggregated_data) >= self._PARALLEL_SECTION_MIN_SOURCES)
            or (is_medium and len(aggregated_data) >= self._PARALLEL_SECTION_MIN_SOURCES_MEDIUM)
        )

        llm_started_at = time.perf_counter()
        if use_parallel:
            # Multi-stage writer: concurrent section drafts on source chunks, then one
            # synthesis call. Avoids a single oversized blocking call and yields a more
            # structured report. MEDIUM uses tighter bounds than HARD.
            partial_sections: list[str] = []
            _section_lock = __import__("threading").Lock()

            def _section_done(idx: int, draft: str) -> None:
                with _section_lock:
                    partial_sections.append(draft)
                if streaming_callback:
                    combined = "\n\n---\n\n".join(partial_sections)
                    streaming_callback(combined)

            section_chunk = self._PARALLEL_SECTION_CHUNK_MEDIUM if is_medium else self._PARALLEL_SECTION_CHUNK
            section_cap = (
                self._PARALLEL_SECTION_MEDIUM_MAX_SECTIONS if is_medium
                else self._PARALLEL_SECTION_HARD_MAX_SECTIONS
            )
            result = self._run_parallel_section_analysis(
                aggregated_data, prompt, prompt_language, depth,
                section_done_callback=_section_done,
                conflicts=conflicts,
                evidence_groups=evidence_groups,
                source_summary=source_summary,
                model=model,
                chunk_size=section_chunk,
                max_sections=section_cap,
                synthesis_streaming_callback=streaming_callback,
                plan_questions=plan_questions,
            )
        else:
            input_data = {
                "original_prompt": prompt,
                "gathered_data": aggregated_data,
                "source_summary": source_summary.model_dump(),
                "detected_conflicts": conflicts,
                "evidence_groups": evidence_groups,
                "evidence_summary": evidence_summary.model_dump(),
            }
            logger.info("AnalyzerAgent starting generation. Aggregated %d sources.", len(aggregated_data))
            result = self._generate_report(
                input_data, prompt_language, depth=depth, model=model,
                streaming_callback=streaming_callback,
                reasoning_callback=reasoning_callback,
                plan_questions=plan_questions,
            )
            # Skip language retry for HARD (avoids an extra multi-minute LLM call).
            if not is_hard and prompt_language != "unknown":
                report_language = self._detect_language(result)
                if report_language not in {prompt_language, "unknown"}:
                    logger.warning(
                        "AnalyzerAgent detected language mismatch. prompt=%s report=%s. Retrying once.",
                        prompt_language,
                        report_language,
                    )
                    result = self._generate_report(
                        input_data, prompt_language, retry=True, depth=depth, model=model,
                        plan_questions=plan_questions,
                    )
        # Final editorial pass (MEDIUM/HARD): tighten prose, enforce answer-first structure, dedupe.
        result = self._maybe_edit_report(result, prompt, prompt_language, depth, model, plan_questions)
        # Stream the final (possibly edited) report once so the UI lands on the polished version.
        if streaming_callback:
            streaming_callback(result)
        llm_ms = (time.perf_counter() - llm_started_at) * 1000

        normalized = self._post_process_report(result, prompt_language)
        with_conflicts = self._inject_conflicts_section(normalized, conflicts, prompt_language)
        rebuilt = self._rebuild_sources_section(with_conflicts, aggregated_data, prompt_language)
        uncited_lines = self._uncited_claim_lines(rebuilt)
        unsupported_lines = self._unsupported_citation_lines(rebuilt, aggregated_data)
        repair_ms = 0.0
        # Deterministic (regex-based) repair runs for all depths — it's instant.
        # LLM repair is skipped for HARD to avoid an extra 2-3 min call.
        allow_llm_repair = not is_hard
        if aggregated_data and (uncited_lines or unsupported_lines) and self._looks_like_structured_report(rebuilt):
            report_body = self._body_without_sources(rebuilt)
            repaired_body = self._deterministic_repair_report_body(
                report_body,
                aggregated_data,
                uncited_lines,
                unsupported_lines,
            )
            if repaired_body is not None:
                rebuilt = self._rebuild_sources_section(repaired_body, aggregated_data, prompt_language)
            elif allow_llm_repair:
                logger.warning(
                    "AnalyzerAgent detected citation issues. Repairing report citations once. uncited_count=%s unsupported_count=%s",
                    len(uncited_lines),
                    len(unsupported_lines),
                )
                repair_started_at = time.perf_counter()
                input_data_for_repair = {
                    "original_prompt": prompt,
                    "gathered_data": aggregated_data,
                    "source_summary": source_summary.model_dump(),
                    "detected_conflicts": conflicts,
                    "evidence_groups": evidence_groups,
                    "evidence_summary": evidence_summary.model_dump(),
                }
                repaired = self._repair_report_citations(
                    input_data_for_repair,
                    prompt_language,
                    report_body,
                    uncited_lines,
                    unsupported_lines,
                    model=model,
                )
                repair_ms = (time.perf_counter() - repair_started_at) * 1000
                normalized = self._post_process_report(repaired, prompt_language)
                with_conflicts = self._inject_conflicts_section(normalized, conflicts, prompt_language)
                rebuilt = self._rebuild_sources_section(with_conflicts, aggregated_data, prompt_language)
        notes = self._report_quality_notes(rebuilt, aggregated_data, prompt_language)
        remaining_uncited_lines = self._uncited_claim_lines(rebuilt)
        remaining_unsupported_lines = self._unsupported_citation_lines(rebuilt, aggregated_data)
        insufficient_evidence_lines = self._insufficient_evidence_lines(rebuilt, aggregated_data)
        verified_report, verification_summary = self.claim_verifier.verify_and_downgrade(
            rebuilt,
            prompt_language,
            remaining_uncited_lines,
            remaining_unsupported_lines,
            insufficient_evidence_lines,
        )
        # P3 verifier: surface per-claim confidence + plan-vs-report coverage gaps
        # as inline sections. Deterministic — no extra LLM call.
        verification_report = self.report_critic.build(
            "",
            tasks,
            evidence_groups,
            verified_report,
            claim_summary=verification_summary,
        )
        verified_report = self.report_critic.inject(
            verified_report, verification_report, prompt_language
        )
        final_notes = list(notes)
        final_notes.extend(
            note
            for note in verification_summary.verification_notes
            if note not in final_notes
        )
        # Inject a transparency "Report Notes" section when (and only when) there are
        # genuine quality issues — well-formed reports get no notes. Supports the
        # verifiable-research goal and is asserted by the analyzer test contract.
        if final_notes:
            logger.info("analyzer_quality_notes count=%d notes=%s", len(final_notes), final_notes)
        final_report = self._inject_report_notes(verified_report, final_notes, prompt_language)
        total_ms = (time.perf_counter() - started_at) * 1000
        logger.info(
            "analyzer_finalize_completed source_count=%s chars_sent=%s conflict_count=%s evidence_group_count=%s "
            "high_confidence_sources=%s downgraded_lines=%s prepare_ms=%.2f conflict_ms=%.2f evidence_ms=%.2f "
            "llm_ms=%.2f repair_ms=%.2f total_ms=%.2f parallel=%s",
            len(aggregated_data),
            sum(len(item.get("content") or "") for item in aggregated_data),
            len(conflicts),
            len(evidence_groups),
            source_summary.high_confidence_sources,
            verification_summary.downgraded_lines,
            prepare_ms,
            conflict_ms,
            evidence_ms,
            llm_ms,
            repair_ms,
            total_ms,
            use_parallel,
        )
        return final_report

    def run(self, input_data: str) -> str:
        """Satisfy BaseAgent abstract interface; delegates to run_analysis with no tasks."""
        return self.run_analysis(prompt=input_data, tasks=[], depth=None)

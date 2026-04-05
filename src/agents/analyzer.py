import logging
import json
import re
import time
from typing import List
from urllib.parse import urlparse
from src.core.agent import BaseAgent
from src.core import rust_accel
from src.api.schemas import SearchTask
from src.config import settings
from src.observability import maybe_traceable
from src.source_quality_policy import TOPIC_POLICIES, combined_topics

logger = logging.getLogger(__name__)

class AnalyzerAgent(BaseAgent):
    MAX_ANALYZER_SOURCES = 12
    MAX_SOURCES_PER_DOMAIN = 2
    MAX_SOURCES_PER_TASK = 4
    MAX_SOURCE_CONTENT_CHARS = 1600
    MAX_PREMIUM_SOURCE_CONTENT_CHARS = 1600
    MAX_MEDIUM_SOURCE_CONTENT_CHARS = 1000
    MAX_LOW_SOURCE_CONTENT_CHARS = 700
    CITATION_PATTERN = re.compile(r"\[S(\d+)\]")
    SENTENCE_PATTERN = re.compile(r"(?<=[.!?])\s+")
    SOURCE_HEADING_PATTERN = re.compile(r"(?ims)\n##\s+(Sources|Источники)\s*$.*\Z")
    CONFLICT_HEADING_PATTERN = re.compile(r"(?im)^##\s+(Conflicts And Uncertainties|Противоречия и неопределенности|Противоречия и неопределённости)\s*$")
    REPORT_NOTES_HEADING_PATTERN = re.compile(r"(?im)^##\s+(Report Notes|Примечания к отчету|Примечания к отчёту)\s*$")
    INTRODUCTION_HEADING_PATTERN = re.compile(r"(?im)^##\s+(Introduction|Введение)\s*$")
    CONCLUSION_HEADING_PATTERN = re.compile(r"(?im)^##\s+(Conclusion|Заключение)\s*$")
    LANGUAGE_HINTS = {
        "ru": {"и", "в", "не", "что", "для", "как", "это", "на", "по"},
        "es": {"el", "la", "los", "las", "para", "como", "una", "con", "del"},
        "en": {"the", "and", "for", "with", "that", "from", "this", "into", "small"},
    }
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
    CONSUMER_TECH_QUERY_TOKENS = {
        "smartphone",
        "smartphones",
        "phone",
        "phones",
        "iphone",
        "android",
        "flagship",
        "flagships",
        "camera phone",
        "galaxy",
        "pixel",
        "oneplus",
        "xiaomi",
        "oppo",
        "honor",
        "foldable",
        "chipset",
        "benchmark",
        "смартфон",
        "смартфоны",
        "смартфонов",
        "телефон",
        "телефоны",
        "телефонов",
        "айфон",
        "флагман",
        "флагманы",
    }
    CONSUMER_TECH_PREMIUM_DOMAIN_EXACT_MATCHES = {
        "gsmarena.com",
        "dxomark.com",
        "pcmag.com",
        "cnet.com",
        "techradar.com",
        "techadvisor.com",
        "notebookcheck.net",
        "androidauthority.com",
        "tomsguide.com",
        "theverge.com",
        "apple.com",
        "samsung.com",
        "news.samsung.com",
        "blog.google",
        "store.google.com",
        "google.com",
        "oneplus.com",
        "mi.com",
        "xiaomi.com",
        "oppo.com",
        "honor.com",
    }
    CONSUMER_TECH_SECONDARY_DOMAIN_EXACT_MATCHES = {
        "gizmochina.com",
        "gadgets360.com",
        "stuff.tv",
        "independent.co.uk",
    }
    CONSUMER_TECH_WEAK_DOMAIN_EXACT_MATCHES = {
        "gizbot.com",
        "timesnownews.com",
        "vertu.com",
        "axis-intelligence.com",
        "gadgetph.com",
        "asumetech.com",
        "techspecs.info",
        "techindeep.com",
        "technicalforum.org",
        "macprices.net",
        "nyongesasande.com",
        "brandvm.com",
        "mobileradar.com",
        "techtimes.com",
        "dialoguepakistan.com",
        "techarc.net",
        "wirefly.com",
        "news.wirefly.com",
        "techrankup.com",
        "asymco.com",
        "futureinsights.com",
        "rank1one.com",
        "gistoftheday.com",
        "cashkr.com",
        "theconsumers.guide",
        "techoble.com",
        "rave-tech.com",
        "couponscurry.com",
    }
    CONSUMER_TECH_WEAK_DOMAIN_SUBSTRINGS = (
        "buyersguide",
        "buyers-guide",
        "rankings-guide",
        "best-phones",
        "best-smartphones",
        "top-smartphones",
        "top-phones",
        "smartphone-rankings",
        "consumers.guide",
        "futureinsights",
        "rank1one",
        "gistoftheday",
    )
    CONSUMER_TECH_STRONG_EDITORIAL_TOKENS = (
        "review",
        "reviews",
        "tested",
        "benchmark",
        "benchmarks",
        "camera test",
        "hands-on",
        "comparison",
        "battery life",
        "performance test",
        "editor's choice",
        "lab test",
        "official",
        "launch",
    )
    CONSUMER_TECH_GENERIC_LISTICLE_TOKENS = (
        "best phones",
        "best smartphones",
        "top phones",
        "top smartphones",
        "buyers guide",
        "buying guide",
        "most anticipated",
        "best camera phone",
        "best camera phones",
        "best gaming phones",
        "phone buying guide",
        "smartphone buying guide",
        "smartphone rankings",
        "performance ranking",
        "top flagship phones",
    )
    CONSUMER_TECH_WEAK_SIGNAL_TOKENS = (
        "rumor",
        "rumors",
        "rumour",
        "rumoured",
        "expected to launch",
        "launch date",
        "price in",
        "upcoming",
        "what to expect",
        "predictions for",
    )
    
    SYSTEM_PROMPT = """
    You are an expert Research Analyst. Your job is to take raw, messy data collected by internet search bots and synthesize it into a comprehensive, well-structured, and easy-to-read report that directly answers the user's original query.

    INPUT:
    You will receive the original user prompt and a JSON list of data gathered by bots. Each item contains a 'source_id', 'url', 'domain', 'source_quality', 'title', and 'content' (raw text from the page).

    YOUR TASK:
    1. Read all the provided content. 
    2. Ignore generic website navigation text, cookie warnings, or irrelevant ads.
    3. Synthesize the core information.
    4. Write a detailed, structured final report in Markdown.
    5. The report MUST be written in the SAME LANGUAGE as the user's original prompt (if the prompt is in Spanish, write in Spanish; if Russian, in Russian, etc.).
    6. Include an "Introduction", "Key Findings / Main Sections", and a "Conclusion".
    7. Use inline source references like [S1], [S2] when you make factual claims.
    8. Include a "Sources" list at the end with the source IDs and URLs you actually used.
    9. Prefer higher-quality and more authoritative sources when sources conflict, but do not ignore useful unique evidence from medium-quality sources.

    DO NOT:
    - Hallucinate or make up facts not present in the provided text.
    - Present speculative predictions or gadget rumors as established facts.
    - Give equal weight to weak hype posts when stronger primary or expert sources exist.
    - Output any internal reasoning, just the final markdown report.
    """

    def run(self, input_data: str) -> str:
        # Dummy implementation to satisfy abstract base class
        return ""

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

    def _apply_payload_budget(self, candidates: list[dict]) -> list[dict]:
        remaining_budget = max(settings.analyzer_payload_char_budget, 2000)
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

    def _prepare_aggregated_data(self, prompt: str, tasks: List[SearchTask]) -> list[dict]:
        topics = self._detect_topics(prompt, tasks)
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
            max_sources=self.MAX_ANALYZER_SOURCES,
            max_sources_per_domain=self.MAX_SOURCES_PER_DOMAIN,
            max_sources_per_task=self.MAX_SOURCES_PER_TASK,
        )

        selected_candidates = self._apply_payload_budget(selected_candidates)

        return [
            {
                "source_id": f"S{index}",
                **{key: value for key, value in candidate.items() if key != "_score"},
            }
            for index, candidate in enumerate(selected_candidates, start=1)
        ]

    def _extract_evidence_groups(self, aggregated_data: list[dict]) -> list[dict]:
        return rust_accel.extract_evidence_groups(
            aggregated_data=aggregated_data,
            stopwords=self.STOPWORDS,
            generic_tokens=self.CONFLICT_GENERIC_TOKENS,
            negation_tokens=self.NEGATION_TOKENS,
            max_groups=5,
        )

    def _post_process_report(self, report: str, language: str) -> str:
        normalized = report.replace("\r\n", "\n").strip()
        normalized = re.sub(r"\n{3,}", "\n\n", normalized)
        normalized = re.sub(r"(?m)^[ \t]+$", "", normalized)

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

    def _build_user_prompt(self, input_data: dict, language: str, retry: bool = False) -> str:
        instruction = self._language_instruction(language)
        if retry:
            instruction = (
                f"{instruction} Your previous answer used the wrong language. "
                "Rewrite the report fully in the requested language and keep factual citations."
            )
        return (
            "Please analyze this data and generate the final report. "
            f"{instruction} "
            "Prefer concrete reported developments over speculative future-looking claims. "
            "Use evidence_groups to identify where multiple sources reinforce the same point. "
            "If a source is mostly predictive, label it as a forecast rather than a confirmed development. "
            "If sources disagree, add a section titled 'Conflicts And Uncertainties' and cite the competing evidence.\n\n"
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
        valid_source_ids = ", ".join(item["source_id"] for item in input_data.get("gathered_data", []))
        feedback_lines = "\n".join(f"- {line}" for line in uncited_lines[:6]) or "- none"
        unsupported_feedback = "\n".join(f"- {line}" for line in unsupported_lines[:6]) or "- none"
        return (
            f"{self._language_instruction(language)} "
            "Rewrite only the report body so that every factual paragraph or bullet includes inline citations. "
            "Do not include a Sources section in your answer. "
            "Use only the available source IDs, do not invent citations, and preserve markdown headings. "
            "Every citation should support the sentence it is attached to; remove weak or mismatched citations.\n\n"
            f"Available source IDs: {valid_source_ids}\n\n"
            "Likely uncited lines:\n"
            f"{feedback_lines}\n\n"
            "Likely weakly-supported cited lines:\n"
            f"{unsupported_feedback}\n\n"
            "Current report body:\n"
            f"{report_body}\n\n"
            "Research payload:\n"
            f"{json.dumps(input_data, ensure_ascii=False)}"
        )

    def _extract_used_source_ids(self, report_body: str) -> list[str]:
        return rust_accel.extract_used_source_ids(report_body)

    def _sanitize_citations(self, report: str, valid_source_ids: set[str]) -> str:
        return rust_accel.sanitize_citations(report, valid_source_ids)

    def _rebuild_sources_section(self, report: str, aggregated_data: list[dict], language: str) -> str:
        without_sources = self.SOURCE_HEADING_PATTERN.sub("", report).strip()
        valid_sources = {item["source_id"]: item for item in aggregated_data}
        sanitized = self._sanitize_citations(without_sources, set(valid_sources))
        used_source_ids = self._extract_used_source_ids(sanitized)
        additional_source_ids = [
            item["source_id"]
            for item in aggregated_data
            if item["source_id"] not in set(used_source_ids)
        ]

        lines = [self._sources_heading(language)]
        if used_source_ids:
            lines.append(self._used_sources_heading(language))
            for source_id in used_source_ids:
                source = valid_sources.get(source_id)
                if source is None:
                    continue
                lines.append(f"- [{source_id}] {source['url']}")

        if additional_source_ids:
            lines.append(self._additional_sources_heading(language))
            for source_id in additional_source_ids:
                source = valid_sources.get(source_id)
                if source is None:
                    continue
                lines.append(f"- [{source_id}] {source['url']}")

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

    def _looks_like_structured_report(self, report: str) -> bool:
        return "## " in report or len(report) >= 400 or report.count("\n") >= 4

    def _repair_report_citations(
        self,
        input_data: dict,
        language: str,
        report_body: str,
        uncited_lines: list[str],
        unsupported_lines: list[str],
    ) -> str:
        repair_prompt = self._build_repair_prompt(
            input_data,
            language,
            report_body,
            uncited_lines,
            unsupported_lines,
        )
        kwargs = {"temperature": 0.2}
        if settings.deepseek_repair_model:
            kwargs["model"] = settings.deepseek_repair_model
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

    def _inject_conflicts_section(self, report: str, conflicts: list[dict], language: str) -> str:
        if not conflicts or self.CONFLICT_HEADING_PATTERN.search(report):
            return report

        lines = [self._conflicts_heading(language)]
        for conflict in conflicts:
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
        conclusion_match = re.search(r"(?im)^##\s+Conclusion\s*$", report)
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

    def _generate_report(self, input_data: dict, language: str, retry: bool = False) -> str:
        user_prompt = self._build_user_prompt(input_data, language, retry=retry)
        return self.llm.generate(
            system_prompt=self.SYSTEM_PROMPT,
            user_prompt=user_prompt,
            temperature=0.3,
        )

    @maybe_traceable(name="analyzer_run_analysis", run_type="llm")
    def run_analysis(self, prompt: str, tasks: List[SearchTask]) -> str:
        started_at = time.perf_counter()
        prepare_started_at = time.perf_counter()
        aggregated_data = self._prepare_aggregated_data(prompt, tasks)
        prepare_ms = (time.perf_counter() - prepare_started_at) * 1000

        conflict_pool = aggregated_data[: settings.analyzer_conflict_source_limit]
        evidence_pool = aggregated_data[: settings.analyzer_evidence_source_limit]

        conflict_started_at = time.perf_counter()
        conflicts = self._detect_conflicts(conflict_pool)
        conflict_ms = (time.perf_counter() - conflict_started_at) * 1000

        evidence_started_at = time.perf_counter()
        evidence_groups = self._extract_evidence_groups(evidence_pool)
        evidence_ms = (time.perf_counter() - evidence_started_at) * 1000
        prompt_language = self._detect_language(prompt)

        input_data = {
            "original_prompt": prompt,
            "gathered_data": aggregated_data,
            "detected_conflicts": conflicts,
            "evidence_groups": evidence_groups,
        }

        logger.info(f"AnalyzerAgent starting generation. Aggregated {len(aggregated_data)} sources.")
        llm_started_at = time.perf_counter()
        result = self._generate_report(input_data, prompt_language)
        if prompt_language != "unknown":
            report_language = self._detect_language(result)
            if report_language not in {prompt_language, "unknown"}:
                logger.warning(
                    "AnalyzerAgent detected language mismatch. prompt=%s report=%s. Retrying once.",
                    prompt_language,
                    report_language,
                )
                result = self._generate_report(input_data, prompt_language, retry=True)
        llm_ms = (time.perf_counter() - llm_started_at) * 1000

        normalized = self._post_process_report(result, prompt_language)
        with_conflicts = self._inject_conflicts_section(normalized, conflicts, prompt_language)
        rebuilt = self._rebuild_sources_section(with_conflicts, aggregated_data, prompt_language)
        uncited_lines = self._uncited_claim_lines(rebuilt)
        unsupported_lines = self._unsupported_citation_lines(rebuilt, aggregated_data)
        repair_ms = 0.0
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
            else:
                logger.warning(
                    "AnalyzerAgent detected citation issues. Repairing report citations once. uncited_count=%s unsupported_count=%s",
                    len(uncited_lines),
                    len(unsupported_lines),
                )
                repair_started_at = time.perf_counter()
                repaired = self._repair_report_citations(
                    input_data,
                    prompt_language,
                    report_body,
                    uncited_lines,
                    unsupported_lines,
                )
                repair_ms = (time.perf_counter() - repair_started_at) * 1000
                normalized = self._post_process_report(repaired, prompt_language)
                with_conflicts = self._inject_conflicts_section(normalized, conflicts, prompt_language)
                rebuilt = self._rebuild_sources_section(with_conflicts, aggregated_data, prompt_language)
        notes = self._report_quality_notes(rebuilt, aggregated_data, prompt_language)
        final_report = self._inject_report_notes(rebuilt, notes, prompt_language)
        total_ms = (time.perf_counter() - started_at) * 1000
        logger.info(
            "analyzer_finalize_completed source_count=%s chars_sent=%s conflict_count=%s evidence_group_count=%s prepare_ms=%.2f conflict_ms=%.2f evidence_ms=%.2f llm_ms=%.2f repair_ms=%.2f total_ms=%.2f",
            len(aggregated_data),
            sum(len(item.get("content") or "") for item in aggregated_data),
            len(conflicts),
            len(evidence_groups),
            prepare_ms,
            conflict_ms,
            evidence_ms,
            llm_ms,
            repair_ms,
            total_ms,
        )
        return final_report

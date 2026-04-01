import logging
import json
import re
from typing import List
from urllib.parse import urlparse
from src.core.agent import BaseAgent
from src.api.schemas import SearchTask
from src.observability import maybe_traceable
from src.source_quality_policy import TOPIC_POLICIES, combined_topics

logger = logging.getLogger(__name__)

class AnalyzerAgent(BaseAgent):
    MAX_ANALYZER_SOURCES = 12
    MAX_SOURCES_PER_DOMAIN = 2
    MAX_SOURCES_PER_TASK = 4
    MAX_SOURCE_CONTENT_CHARS = 1600
    CITATION_PATTERN = re.compile(r"\[S(\d+)\]")
    SENTENCE_PATTERN = re.compile(r"(?<=[.!?])\s+")
    SOURCE_HEADING_PATTERN = re.compile(r"(?ims)\n##\s+Sources\s*$.*\Z")
    CONFLICT_HEADING_PATTERN = re.compile(r"(?im)^##\s+Conflicts And Uncertainties\s*$")
    REPORT_NOTES_HEADING_PATTERN = re.compile(r"(?im)^##\s+Report Notes\s*$")
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
        if not value:
            return ""
        return re.sub(r"\s+", " ", value).strip()

    def _content_fingerprint(self, title: str, content: str) -> str:
        normalized_title = self._normalize_text(title).lower()
        normalized_content = self._normalize_text(content).lower()
        return f"{normalized_title}|{normalized_content[:250]}"

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
        normalized = self._normalize_text(content)
        if len(normalized) <= self.MAX_SOURCE_CONTENT_CHARS:
            return normalized

        sentences = self.SENTENCE_PATTERN.split(normalized)
        compact_parts: list[str] = []
        current_length = 0
        for sentence in sentences:
            cleaned = self._normalize_text(sentence)
            if not cleaned:
                continue
            next_length = current_length + len(cleaned) + (1 if compact_parts else 0)
            if next_length > self.MAX_SOURCE_CONTENT_CHARS:
                break
            compact_parts.append(cleaned)
            current_length = next_length

        if compact_parts:
            compact = " ".join(compact_parts)
            if len(compact) < len(normalized):
                return compact.rstrip() + " ..."
            return compact

        return normalized[: self.MAX_SOURCE_CONTENT_CHARS].rstrip() + " ..."

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
                if self._should_exclude_source(
                    url,
                    title,
                    content,
                    source_quality,
                    topics=topics,
                ):
                    continue

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

        best_by_url: dict[str, dict] = {}
        for candidate in aggregated_candidates:
            existing = best_by_url.get(candidate["url"])
            candidate_score = self._score_source(
                candidate.get("url") or "",
                candidate.get("title") or "",
                candidate.get("content") or "",
                candidate.get("source_quality"),
            )
            if topics:
                candidate_score += self._topic_domain_adjustment(
                    candidate.get("url") or "",
                    candidate.get("title") or "",
                    candidate.get("content") or "",
                    candidate.get("source_quality"),
                    topics=topics,
                )

            existing_score = None
            if existing is not None:
                existing_score = self._score_source(
                    existing.get("url") or "",
                    existing.get("title") or "",
                    existing.get("content") or "",
                    existing.get("source_quality"),
                )
                if topics:
                    existing_score += self._topic_domain_adjustment(
                        existing.get("url") or "",
                        existing.get("title") or "",
                        existing.get("content") or "",
                        existing.get("source_quality"),
                        topics=topics,
                    )

            if existing is None or candidate_score > (existing_score or 0):
                best_by_url[candidate["url"]] = candidate

        best_by_fingerprint: dict[str, tuple[int, dict]] = {}
        for candidate in best_by_url.values():
            fingerprint = self._content_fingerprint(
                candidate.get("title") or "",
                candidate.get("content") or "",
            )
            score = self._score_source(
                candidate.get("url") or "",
                candidate.get("title") or "",
                candidate.get("content") or "",
                candidate.get("source_quality"),
            )
            if topics:
                score += self._topic_domain_adjustment(
                    candidate.get("url") or "",
                    candidate.get("title") or "",
                    candidate.get("content") or "",
                    candidate.get("source_quality"),
                    topics=topics,
                )
            existing = best_by_fingerprint.get(fingerprint)
            if existing is None or score > existing[0]:
                best_by_fingerprint[fingerprint] = (score, candidate)

        ranked_candidates: list[dict] = []
        for _, item in best_by_fingerprint.values():
            score = self._score_source(
                item.get("url") or "",
                item.get("title") or "",
                item.get("content") or "",
                item.get("source_quality"),
            )
            if topics:
                score += self._topic_domain_adjustment(
                    item.get("url") or "",
                    item.get("title") or "",
                    item.get("content") or "",
                    item.get("source_quality"),
                    topics=topics,
                )
            ranked_candidates.append({**item, "_score": score})

        ranked_candidates.sort(key=lambda item: item["_score"], reverse=True)

        selected_candidates: list[dict] = []
        domain_counts: dict[str, int] = {}
        task_counts: dict[str, int] = {}
        for candidate in ranked_candidates:
            domain = (candidate.get("domain") or "").lower()
            task_description = candidate.get("task_description") or ""
            is_strong_source = candidate.get("source_quality") == "high"

            if domain and domain_counts.get(domain, 0) >= self.MAX_SOURCES_PER_DOMAIN and not is_strong_source:
                continue
            if task_description and task_counts.get(task_description, 0) >= self.MAX_SOURCES_PER_TASK and not is_strong_source:
                continue

            selected_candidates.append(candidate)
            if domain:
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
            if task_description:
                task_counts[task_description] = task_counts.get(task_description, 0) + 1

            if len(selected_candidates) >= self.MAX_ANALYZER_SOURCES:
                break

        unique_domains = {
            (candidate.get("domain") or "").lower()
            for candidate in ranked_candidates
            if candidate.get("domain")
        }
        if len(unique_domains) <= 1 and len(selected_candidates) < self.MAX_ANALYZER_SOURCES:
            selected_urls = {candidate["url"] for candidate in selected_candidates}
            for candidate in ranked_candidates:
                if candidate["url"] in selected_urls:
                    continue
                selected_candidates.append(candidate)
                if len(selected_candidates) >= self.MAX_ANALYZER_SOURCES:
                    break

        return [
            {
                "source_id": f"S{index}",
                **{key: value for key, value in candidate.items() if key != "_score"},
            }
            for index, candidate in enumerate(selected_candidates, start=1)
        ]

    def _post_process_report(self, report: str) -> str:
        normalized = report.replace("\r\n", "\n").strip()
        normalized = re.sub(r"\n{3,}", "\n\n", normalized)
        normalized = re.sub(r"(?m)^[ \t]+$", "", normalized)

        if re.search(r"(?im)^sources:\s*$", normalized):
            normalized = re.sub(r"(?im)^sources:\s*$", "## Sources", normalized)
        elif re.search(r"(?im)^#*\s*sources\s*$", normalized) is None:
            normalized = f"{normalized}\n\n## Sources"

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
            "If a source is mostly predictive, label it as a forecast rather than a confirmed development. "
            "If sources disagree, add a section titled 'Conflicts And Uncertainties' and cite the competing evidence.\n\n"
            f"{json.dumps(input_data, ensure_ascii=False)}"
        )

    def _extract_used_source_ids(self, report_body: str) -> list[str]:
        ordered_ids: list[str] = []
        for match in self.CITATION_PATTERN.finditer(report_body):
            source_id = f"S{match.group(1)}"
            if source_id not in ordered_ids:
                ordered_ids.append(source_id)
        return ordered_ids

    def _sanitize_citations(self, report: str, valid_source_ids: set[str]) -> str:
        def replace(match: re.Match[str]) -> str:
            source_id = f"S{match.group(1)}"
            return match.group(0) if source_id in valid_source_ids else ""

        sanitized = self.CITATION_PATTERN.sub(replace, report)
        sanitized = re.sub(r"\[(?:,\s*)+\]", "", sanitized)
        sanitized = re.sub(r"[ \t]{2,}", " ", sanitized)
        sanitized = re.sub(r"\s+([,.;:])", r"\1", sanitized)
        return sanitized

    def _rebuild_sources_section(self, report: str, aggregated_data: list[dict]) -> str:
        without_sources = self.SOURCE_HEADING_PATTERN.sub("", report).strip()
        valid_sources = {item["source_id"]: item for item in aggregated_data}
        sanitized = self._sanitize_citations(without_sources, set(valid_sources))
        used_source_ids = self._extract_used_source_ids(sanitized)

        lines = ["## Sources"]
        for source_id in used_source_ids:
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
        if stripped.lower().startswith("source") or stripped.lower().startswith("report notes"):
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

    def _looks_like_structured_report(self, report: str) -> bool:
        return "## " in report or len(report) >= 400 or report.count("\n") >= 4

    def _repair_report_citations(
        self,
        input_data: dict,
        language: str,
        report: str,
        uncited_lines: list[str],
    ) -> str:
        valid_source_ids = ", ".join(item["source_id"] for item in input_data.get("gathered_data", []))
        feedback_lines = "\n".join(f"- {line}" for line in uncited_lines[:6])
        repair_prompt = (
            f"{self._language_instruction(language)} "
            "Rewrite the report so that every factual paragraph or bullet includes inline citations. "
            "Use only the available source IDs, do not invent citations, and preserve markdown headings.\n\n"
            f"Available source IDs: {valid_source_ids}\n\n"
            "Likely uncited lines:\n"
            f"{feedback_lines}\n\n"
            "Current report:\n"
            f"{report}\n\n"
            "Research payload:\n"
            f"{json.dumps(input_data, ensure_ascii=False)}"
        )
        return self.llm.generate(
            system_prompt=self.SYSTEM_PROMPT,
            user_prompt=repair_prompt,
            temperature=0.2,
        )

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
        claims = self._extract_candidate_claims(aggregated_data)
        conflicts: list[dict] = []
        seen_pairs: set[tuple[str, str]] = set()

        for index, left in enumerate(claims):
            for right in claims[index + 1:]:
                if not self._claims_conflict(left, right):
                    continue

                pair_key = tuple(sorted((left["source_id"], right["source_id"])))
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                shared_tokens = sorted(self._informative_shared_tokens(left, right))
                reason = "material discrepancy"
                if left["has_negation"] != right["has_negation"]:
                    reason = "one source affirms the claim while the other negates it"
                elif set(left["numbers"]) and set(right["numbers"]) and set(left["numbers"]) != set(right["numbers"]):
                    reason = "the sources report different concrete figures"
                conflicts.append(
                    {
                        "topic": ", ".join(shared_tokens[:3]) or "source disagreement",
                        "source_ids": [left["source_id"], right["source_id"]],
                        "sentences": [left["sentence"], right["sentence"]],
                        "reason": reason,
                    }
                )
                if len(conflicts) >= 3:
                    return conflicts
        return conflicts

    def _inject_conflicts_section(self, report: str, conflicts: list[dict], language: str) -> str:
        if not conflicts or self.CONFLICT_HEADING_PATTERN.search(report):
            return report

        lines = ["## Conflicts And Uncertainties"]
        for conflict in conflicts:
            left_source, right_source = conflict["source_ids"]
            left_sentence, right_sentence = conflict["sentences"]
            lines.append(
                f"- Topic: {conflict['topic']}. Reason: {conflict.get('reason') or 'material discrepancy'}. "
                f'Evidence: "{left_sentence}" [{left_source}] versus "{right_sentence}" [{right_source}].'
            )

        insertion = "\n".join(lines)
        conclusion_match = re.search(r"(?im)^##\s+Conclusion\s*$", report)
        if conclusion_match:
            return f"{report[:conclusion_match.start()].rstrip()}\n\n{insertion}\n\n{report[conclusion_match.start():].lstrip()}"
        return f"{report.strip()}\n\n{insertion}"

    def _report_quality_notes(self, report: str, aggregated_data: list[dict]) -> list[str]:
        notes: list[str] = []
        normalized = report.lower()
        used_source_ids = self._extract_used_source_ids(report)

        if not self.INTRODUCTION_HEADING_PATTERN.search(report):
            notes.append("The report is missing a clear introduction heading.")
        if not self.CONCLUSION_HEADING_PATTERN.search(report):
            notes.append("The report is missing a clear conclusion heading.")
        if not used_source_ids:
            notes.append("The report does not cite any sources inline.")
        if not aggregated_data:
            notes.append("No usable extracted sources were available for analysis.")
        elif len(aggregated_data) < 2:
            notes.append("The report is based on fewer than two usable sources.")
        elif len(used_source_ids) < min(2, len(aggregated_data)):
            notes.append("Only a small subset of the available sources is cited in the final report.")
        if "## sources" in normalized and report.strip().endswith("## Sources"):
            notes.append("The sources section is present but no cited sources were included under it.")

        return notes

    def _inject_report_notes(self, report: str, notes: list[str]) -> str:
        if not notes or self.REPORT_NOTES_HEADING_PATTERN.search(report):
            return report

        section = "## Report Notes\n" + "\n".join(f"- {note}" for note in notes)
        sources_match = re.search(r"(?im)^##\s+Sources\s*$", report)
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
        aggregated_data = self._prepare_aggregated_data(prompt, tasks)
        conflicts = self._detect_conflicts(aggregated_data)
        prompt_language = self._detect_language(prompt)

        input_data = {
            "original_prompt": prompt,
            "gathered_data": aggregated_data,
            "detected_conflicts": conflicts,
        }

        logger.info(f"AnalyzerAgent starting generation. Aggregated {len(aggregated_data)} sources.")
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

        normalized = self._post_process_report(result)
        with_conflicts = self._inject_conflicts_section(normalized, conflicts, prompt_language)
        rebuilt = self._rebuild_sources_section(with_conflicts, aggregated_data)
        uncited_lines = self._uncited_claim_lines(rebuilt)
        if aggregated_data and uncited_lines and self._looks_like_structured_report(rebuilt):
            logger.warning(
                "AnalyzerAgent detected uncited claim lines. Repairing report citations once. uncited_count=%s",
                len(uncited_lines),
            )
            repaired = self._repair_report_citations(input_data, prompt_language, rebuilt, uncited_lines)
            normalized = self._post_process_report(repaired)
            with_conflicts = self._inject_conflicts_section(normalized, conflicts, prompt_language)
            rebuilt = self._rebuild_sources_section(with_conflicts, aggregated_data)
        notes = self._report_quality_notes(rebuilt, aggregated_data)
        return self._inject_report_notes(rebuilt, notes)

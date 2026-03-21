import logging
import json
import re
from typing import List
from urllib.parse import urlparse
from src.core.agent import BaseAgent
from src.api.schemas import SearchTask

logger = logging.getLogger(__name__)

class AnalyzerAgent(BaseAgent):
    CITATION_PATTERN = re.compile(r"\[S(\d+)\]")
    SOURCE_HEADING_PATTERN = re.compile(r"(?ims)\n##\s+Sources\s*$.*\Z")
    LANGUAGE_HINTS = {
        "ru": {"и", "в", "не", "что", "для", "как", "это", "на", "по"},
        "es": {"el", "la", "los", "las", "para", "como", "una", "con", "del"},
        "en": {"the", "and", "for", "with", "that", "from", "this", "into", "small"},
    }
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

    def _score_source(self, url: str, title: str, content: str) -> int:
        normalized_title = self._normalize_text(title)
        normalized_content = self._normalize_text(content)
        score = len(normalized_content)
        if normalized_title:
            score += 100
        score += self._trusted_domain_score(url)
        if "failed to extract content" in normalized_content.lower():
            score -= 5000
        return score

    def _prepare_aggregated_data(self, tasks: List[SearchTask]) -> list[dict]:
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

                truncated_text = content[:3000] + "..." if len(content) > 3000 else content
                aggregated_candidates.append(
                    {
                        "task_description": task.description,
                        "url": url,
                        "domain": res.get("domain") or urlparse(url).netloc.lower() or None,
                        "source_quality": res.get("source_quality") or "low",
                        "title": title or None,
                        "content": truncated_text,
                    }
                )

        best_by_url: dict[str, dict] = {}
        for candidate in aggregated_candidates:
            existing = best_by_url.get(candidate["url"])
            if existing is None or self._score_source(
                candidate.get("url") or "",
                candidate.get("title") or "",
                candidate.get("content") or "",
            ) > self._score_source(
                existing.get("url") or "",
                existing.get("title") or "",
                existing.get("content") or "",
            ):
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
            )
            existing = best_by_fingerprint.get(fingerprint)
            if existing is None or score > existing[0]:
                best_by_fingerprint[fingerprint] = (score, candidate)

        ranked_candidates = sorted(
            (item for _, item in best_by_fingerprint.values()),
            key=lambda item: self._score_source(
                item.get("url") or "",
                item.get("title") or "",
                item.get("content") or "",
            ),
            reverse=True,
        )
        selected_candidates = ranked_candidates[:20]
        return [
            {
                "source_id": f"S{index}",
                **candidate,
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
            f"Please analyze this data and generate the final report. {instruction}\n\n"
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
        sanitized = re.sub(r"\s{2,}", " ", sanitized)
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

    def _generate_report(self, input_data: dict, language: str, retry: bool = False) -> str:
        user_prompt = self._build_user_prompt(input_data, language, retry=retry)
        return self.llm.generate(
            system_prompt=self.SYSTEM_PROMPT,
            user_prompt=user_prompt,
            temperature=0.3,
        )

    def run_analysis(self, prompt: str, tasks: List[SearchTask]) -> str:
        aggregated_data = self._prepare_aggregated_data(tasks)
        prompt_language = self._detect_language(prompt)

        input_data = {
            "original_prompt": prompt,
            "gathered_data": aggregated_data
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
        return self._rebuild_sources_section(normalized, aggregated_data)

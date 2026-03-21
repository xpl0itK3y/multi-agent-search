import logging
import json
import re
from typing import List
from urllib.parse import urlparse
from src.core.agent import BaseAgent
from src.api.schemas import SearchTask
from src.observability import maybe_traceable

logger = logging.getLogger(__name__)

class AnalyzerAgent(BaseAgent):
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
            "Please analyze this data and generate the final report. "
            f"{instruction} "
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

                numbers = tuple(re.findall(r"\b\d+(?:\.\d+)?\b", lowered))
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

    def _claims_overlap(self, left: dict, right: dict) -> bool:
        shared_tokens = set(left["tokens"]) & set(right["tokens"])
        return len(shared_tokens) >= 2

    def _claims_conflict(self, left: dict, right: dict) -> bool:
        if left["source_id"] == right["source_id"]:
            return False
        if not self._claims_overlap(left, right):
            return False
        if left["has_negation"] != right["has_negation"]:
            return True

        left_numbers = set(left["numbers"])
        right_numbers = set(right["numbers"])
        if left_numbers and right_numbers and left_numbers != right_numbers:
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

                shared_tokens = sorted(set(left["tokens"]) & set(right["tokens"]))
                conflicts.append(
                    {
                        "topic": ", ".join(shared_tokens[:3]) or "source disagreement",
                        "source_ids": [left["source_id"], right["source_id"]],
                        "sentences": [left["sentence"], right["sentence"]],
                    }
                )
                if len(conflicts) >= 3:
                    return conflicts
        return conflicts

    def _inject_conflicts_section(self, report: str, conflicts: list[dict]) -> str:
        if not conflicts or self.CONFLICT_HEADING_PATTERN.search(report):
            return report

        lines = ["## Conflicts And Uncertainties"]
        for conflict in conflicts:
            left_source, right_source = conflict["source_ids"]
            left_sentence, right_sentence = conflict["sentences"]
            lines.append(
                f"- On {conflict['topic']}, {left_source} and {right_source} provide conflicting evidence: "
                f'"{left_sentence}" [{left_source}] versus "{right_sentence}" [{right_source}].'
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
        aggregated_data = self._prepare_aggregated_data(tasks)
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
        with_conflicts = self._inject_conflicts_section(normalized, conflicts)
        rebuilt = self._rebuild_sources_section(with_conflicts, aggregated_data)
        notes = self._report_quality_notes(rebuilt, aggregated_data)
        return self._inject_report_notes(rebuilt, notes)

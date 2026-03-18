import logging
import json
import re
from typing import List
from src.core.agent import BaseAgent
from src.api.schemas import SearchTask

logger = logging.getLogger(__name__)

class AnalyzerAgent(BaseAgent):
    
    SYSTEM_PROMPT = """
    You are an expert Research Analyst. Your job is to take raw, messy data collected by internet search bots and synthesize it into a comprehensive, well-structured, and easy-to-read report that directly answers the user's original query.

    INPUT:
    You will receive the original user prompt and a JSON list of data gathered by bots. Each item contains a 'source_id', 'url', 'title', and 'content' (raw text from the page).

    YOUR TASK:
    1. Read all the provided content. 
    2. Ignore generic website navigation text, cookie warnings, or irrelevant ads.
    3. Synthesize the core information.
    4. Write a detailed, structured final report in Markdown.
    5. The report MUST be written in the SAME LANGUAGE as the user's original prompt (if the prompt is in Spanish, write in Spanish; if Russian, in Russian, etc.).
    6. Include an "Introduction", "Key Findings / Main Sections", and a "Conclusion".
    7. Use inline source references like [S1], [S2] when you make factual claims.
    8. Include a "Sources" list at the end with the source IDs and URLs you actually used.

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

    def _score_source(self, title: str, content: str) -> int:
        normalized_title = self._normalize_text(title)
        normalized_content = self._normalize_text(content)
        score = len(normalized_content)
        if normalized_title:
            score += 100
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
                        "title": title or None,
                        "content": truncated_text,
                    }
                )

        best_by_url: dict[str, dict] = {}
        for candidate in aggregated_candidates:
            existing = best_by_url.get(candidate["url"])
            if existing is None or self._score_source(
                candidate.get("title") or "",
                candidate.get("content") or "",
            ) > self._score_source(existing.get("title") or "", existing.get("content") or ""):
                best_by_url[candidate["url"]] = candidate

        best_by_fingerprint: dict[str, tuple[int, dict]] = {}
        for candidate in best_by_url.values():
            fingerprint = self._content_fingerprint(
                candidate.get("title") or "",
                candidate.get("content") or "",
            )
            score = self._score_source(candidate.get("title") or "", candidate.get("content") or "")
            existing = best_by_fingerprint.get(fingerprint)
            if existing is None or score > existing[0]:
                best_by_fingerprint[fingerprint] = (score, candidate)

        ranked_candidates = sorted(
            (item for _, item in best_by_fingerprint.values()),
            key=lambda item: self._score_source(item.get("title") or "", item.get("content") or ""),
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

    def run_analysis(self, prompt: str, tasks: List[SearchTask]) -> str:
        aggregated_data = self._prepare_aggregated_data(tasks)

        input_data = {
            "original_prompt": prompt,
            "gathered_data": aggregated_data
        }

        user_prompt = f"Please analyze this data and generate the final report:\n\n{json.dumps(input_data, ensure_ascii=False)}"

        logger.info(f"AnalyzerAgent starting generation. Aggregated {len(aggregated_data)} sources.")
        result = self.llm.generate(
            system_prompt=self.SYSTEM_PROMPT,
            user_prompt=user_prompt,
            temperature=0.3,
        )
        return self._post_process_report(result)

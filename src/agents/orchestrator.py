import json
import re
import uuid
from src.core.agent import BaseAgent
from src.api.schemas import SearchDepth, TaskStatus
from src.observability import maybe_traceable
from src.search_depth_profiles import get_depth_profile
from src.source_quality_policy import combined_topics

class OrchestratorAgent(BaseAgent):
    LANGUAGE_HINTS = {
        "ru": {"и", "в", "не", "что", "для", "как", "это", "на", "по"},
        "es": {"el", "la", "los", "las", "para", "como", "una", "con", "del"},
        "en": {"the", "and", "for", "with", "that", "from", "this", "into", "small"},
    }
    
    SYSTEM_PROMPT = """
                        You are a Search Orchestrator. Your job is to decompose a complex user query into independent search tasks for automated bots.

                        OUTPUT FORMAT
                        Return ONLY valid JSON — no preamble, no commentary, no markdown fences.
                        The JSON must be an array of objects with this exact structure:

                        [
                        {
                            "description": "Short description of what this task searches for (in user's language)",
                            "queries": ["query 1", "query 2", "query 3"]
                        }
                        ]

                        LANGUAGE RULES
                        - "description" → always in the user's language
                        - "queries" → in the language most effective for the topic:
                        - Technical / scientific / global topics → English
                        - Local / regional / cultural topics → user's language
                        - When uncertain → use both (one query per language)

                        DECOMPOSITION RULES
                        - Split the query by independent subtopics, data types, or time periods
                        - Each task must be fully independent (no task should depend on results of another)
                        - Tasks must not duplicate each other — each covers a unique angle
                        - Queries within one task are variations of the same search intent (different phrasings, synonyms)
                        - Each task must have 2–3 queries

                        TASK COUNT
                        - If the user specifies a number → generate EXACTLY that many tasks
                        - If not specified → generate 2–5 tasks based on query complexity:
                        - Simple query (one clear intent) → 2 tasks
                        - Medium query → 3 tasks
                        - Complex / multi-faceted query → 4–5 tasks

                        EDGE CASES
                        - If the query is already simple and atomic → return 1 task, do not over-decompose
                        - If the query is ambiguous → decompose by the most likely interpretations
                        - If the query is in mixed languages → detect the dominant language and use it for descriptions
                    """

    def _normalize_text(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", value).strip()

    def _detect_language(self, text: str) -> str:
        normalized = self._normalize_text(text).lower()
        if not normalized:
            return "unknown"

        cyrillic_count = sum(1 for char in normalized if "а" <= char <= "я" or char == "ё")
        latin_count = sum(1 for char in normalized if "a" <= char <= "z")
        if cyrillic_count >= 4 and cyrillic_count >= latin_count / 3:
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

    def _fallback_description(self, prompt: str, index: int, language: str) -> str:
        prompt_text = self._normalize_text(prompt)
        if language == "ru":
            return f"Направление поиска {index}: {prompt_text}"
        if language == "es":
            return f"Linea de busqueda {index}: {prompt_text}"
        return f"Search angle {index}: {prompt_text}"

    def _normalize_description_language(self, description: str, prompt: str, index: int) -> str:
        target_language = self._detect_language(prompt)
        description_text = self._normalize_text(description)
        if not description_text:
            return self._fallback_description(prompt, index, target_language)

        description_language = self._detect_language(description_text)
        if target_language in {"unknown", description_language} or description_language == "unknown":
            return description_text
        return self._fallback_description(prompt, index, target_language)

    def _dedupe_queries(self, queries: list[str]) -> list[str]:
        seen: set[str] = set()
        deduped: list[str] = []
        for query in queries:
            normalized = self._normalize_text(query)
            if not normalized:
                continue
            key = normalized.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(normalized)
        return deduped

    def _shape_docs_queries(self, prompt: str, description: str, queries: list[str]) -> list[str]:
        description_text = self._normalize_text(description).lower()
        prompt_text = self._normalize_text(prompt).lower()
        normalized_queries = self._dedupe_queries(queries)

        mentions_fastapi = "fastapi" in description_text or "fastapi" in prompt_text
        mentions_flask = "flask" in description_text or "flask" in prompt_text

        doc_queries: list[str] = []
        if mentions_fastapi:
            if any(token in description_text for token in ("производ", "performance", "async", "асинх", "feature", "возможност", "function", "функц")):
                doc_queries.append("FastAPI official documentation async reference")
            else:
                doc_queries.append("FastAPI official documentation REST API reference")
        if mentions_flask:
            if any(token in description_text for token in ("extension", "extensions", "расширен", "feature", "возможност", "function", "функц")):
                doc_queries.append("Flask official documentation extensions reference")
            else:
                doc_queries.append("Flask official documentation REST API patterns")

        if mentions_fastapi and mentions_flask:
            if any(token in description_text for token in ("сравнен", "compare", "comparison", "performance", "выбор", "choose")):
                doc_queries.append("FastAPI vs Flask official documentation comparison")

        comparison_queries = [
            query for query in normalized_queries
            if any(token in query.lower() for token in ("vs", "comparison", "compare", "benchmark", "performance"))
        ]
        neutral_queries = [
            query for query in normalized_queries
            if query not in comparison_queries
        ]

        shaped = self._dedupe_queries(doc_queries + comparison_queries[:1] + neutral_queries)
        return shaped[:3] if shaped else normalized_queries[:3]

    def _normalize_queries(self, prompt: str, description: str, queries: list[str]) -> list[str]:
        topics = combined_topics(prompt, description, " ".join(queries or []))
        normalized_queries = self._dedupe_queries(queries)
        if "docs_programming" in topics:
            return self._shape_docs_queries(prompt, description, normalized_queries)
        return normalized_queries[:3]

    @maybe_traceable(name="orchestrator_decompose", run_type="llm")
    def run_decompose(self, prompt: str, depth: SearchDepth) -> list:
        task_count = get_depth_profile(depth)["task_count"]
        
        custom_system_prompt = self.SYSTEM_PROMPT + f"\n    Generate EXACTLY {task_count} search tasks."
        
        response_text = self.llm.generate(
            system_prompt=custom_system_prompt,
            user_prompt=prompt
        )
        
        clean_text = response_text.strip()
        if clean_text.startswith("```json"):
            clean_text = clean_text[7:]
        if clean_text.endswith("```"):
            clean_text = clean_text[:-3]
        clean_text = clean_text.strip()
        
        try:
            tasks_raw = json.loads(clean_text)
            
            enriched_tasks = []
            for index, item in enumerate(tasks_raw, start=1):
                enriched_tasks.append({
                    "id": str(uuid.uuid4()),
                    "description": self._normalize_description_language(
                        item.get("description", ""),
                        prompt,
                        index,
                    ),
                    "queries": self._normalize_queries(
                        prompt,
                        item.get("description", ""),
                        item.get("queries", []),
                    ),
                    "status": TaskStatus.PENDING
                })
            return enriched_tasks
        except json.JSONDecodeError:
            return [{
                "id": str(uuid.uuid4()),
                "description": "Error parsing LLM response", 
                "queries": [prompt],
                "status": TaskStatus.FAILED
            }]

    def run(self, input_data: str) -> str:
        tasks = self.run_decompose(input_data, SearchDepth.EASY)
        return json.dumps(tasks, ensure_ascii=False)

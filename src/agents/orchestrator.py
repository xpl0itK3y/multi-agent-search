import json
import re
import uuid
from src.core.agent import BaseAgent
from src.domain import SearchDepth, TaskStatus
from src.agents.language_utils import LANGUAGE_HINTS
from src.observability import maybe_traceable
from src.search_depth_profiles import get_depth_profile
from src.source_quality_policy import combined_topics

class OrchestratorAgent(BaseAgent):
    DOCS_FRAMEWORKS: dict[str, dict] = {
        "fastapi": {
            "display": "FastAPI",
            "context_tokens": ("производ", "performance", "async", "асинх", "feature", "возможност", "function", "функц"),
            "context_query": "FastAPI official documentation async reference",
            "default_query": "FastAPI official documentation REST API reference",
        },
        "flask": {
            "display": "Flask",
            "context_tokens": ("extension", "extensions", "расширен", "feature", "возможност", "function", "функц"),
            "context_query": "Flask official documentation extensions reference",
            "default_query": "Flask official documentation REST API patterns",
        },
        "django": {
            "display": "Django",
            "context_tokens": ("orm", "model", "migration", "модел", "миграц"),
            "context_query": "Django official documentation ORM models reference",
            "default_query": "Django official documentation views and routing guide",
        },
        "express": {
            "display": "Express",
            "context_tokens": ("middleware", "router", "async", "route"),
            "context_query": "Express.js official documentation middleware reference",
            "default_query": "Express.js official documentation REST API guide",
        },
        "spring": {
            "display": "Spring",
            "context_tokens": ("boot", "security", "jpa", "bean"),
            "context_query": "Spring Boot official documentation reference guide",
            "default_query": "Spring Framework official documentation REST API",
        },
        "rails": {
            "display": "Rails",
            "context_tokens": ("active record", "migration", "model", "scaffold"),
            "context_query": "Ruby on Rails official documentation Active Record reference",
            "default_query": "Ruby on Rails official documentation routing REST",
        },
        "laravel": {
            "display": "Laravel",
            "context_tokens": ("eloquent", "migration", "blade", "route"),
            "context_query": "Laravel official documentation Eloquent ORM reference",
            "default_query": "Laravel official documentation routing REST API",
        },
        "nextjs": {
            "display": "Next.js",
            "context_tokens": ("app router", "server component", "ssr", "api route"),
            "context_query": "Next.js official documentation App Router reference",
            "default_query": "Next.js official documentation API routes guide",
        },
        "react": {
            "display": "React",
            "context_tokens": ("hook", "hooks", "component", "state", "effect", "хук"),
            "context_query": "React official documentation hooks reference",
            "default_query": "React official documentation component API",
        },
        "vue": {
            "display": "Vue",
            "context_tokens": ("composition", "component", "reactive", "directive"),
            "context_query": "Vue.js official documentation Composition API reference",
            "default_query": "Vue.js official documentation component guide",
        },
        "angular": {
            "display": "Angular",
            "context_tokens": ("service", "module", "directive", "rxjs"),
            "context_query": "Angular official documentation services dependency injection",
            "default_query": "Angular official documentation HTTP client REST",
        },
        "fastify": {
            "display": "Fastify",
            "context_tokens": ("plugin", "schema", "hook", "validation"),
            "context_query": "Fastify official documentation plugins reference",
            "default_query": "Fastify official documentation REST API guide",
        },
        "gin": {
            "display": "Gin",
            "context_tokens": ("middleware", "router", "handler", "context"),
            "context_query": "Gin framework official documentation middleware reference",
            "default_query": "Gin framework official documentation REST API guide",
        },
        "actix": {
            "display": "Actix",
            "context_tokens": ("actor", "handler", "middleware", "extractor"),
            "context_query": "Actix-web official documentation extractors reference",
            "default_query": "Actix-web official documentation REST API guide",
        },
    }
    COMPARISON_TOKENS = (
        "сравнен", "compare", "comparison", "performance", "выбор", "choose", "vs", "versus",
    )

    # Single source of truth lives in src/agents/language_utils.py (A-6).
    LANGUAGE_HINTS = LANGUAGE_HINTS

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

        detected = [
            (keyword, config)
            for keyword, config in self.DOCS_FRAMEWORKS.items()
            if keyword in description_text or keyword in prompt_text
        ]

        doc_queries: list[str] = []
        for _keyword, config in detected[:2]:
            has_context = any(token in description_text for token in config["context_tokens"])
            doc_queries.append(config["context_query"] if has_context else config["default_query"])

        if len(detected) >= 2:
            is_comparison = any(token in description_text for token in self.COMPARISON_TOKENS)
            if is_comparison:
                left = detected[0][1]["display"]
                right = detected[1][1]["display"]
                doc_queries.append(f"{left} vs {right} official documentation comparison")

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
        tasks = self.run_decompose(input_data, SearchDepth.MEDIUM)
        return json.dumps(tasks, ensure_ascii=False)

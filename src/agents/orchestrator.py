import json
import uuid
from src.core.agent import BaseAgent
from src.api.schemas import SearchDepth, TaskStatus

class OrchestratorAgent(BaseAgent):
    
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

    def run_decompose(self, prompt: str, depth: SearchDepth) -> list:
        depth_map = {
            SearchDepth.EASY: 2,
            SearchDepth.MEDIUM: 4,
            SearchDepth.HARD: 6
        }
        task_count = depth_map.get(depth, 2)
        
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
            for item in tasks_raw:
                enriched_tasks.append({
                    "id": str(uuid.uuid4()),
                    "description": item.get("description", "No description"),
                    "queries": item.get("queries", []),
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

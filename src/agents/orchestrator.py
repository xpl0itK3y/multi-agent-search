import json
import uuid
from src.core.agent import BaseAgent
from src.api.schemas import SearchDepth, TaskStatus

class OrchestratorAgent(BaseAgent):
    
    SYSTEM_PROMPT = """
    You are a Search Orchestrator. Your goal is to break down a complex user query into multiple independent search tasks for automated bots.
    
    ## CRITICAL RULES
    1. Output MUST be in VALID JSON format.
    2. The JSON must be a list of objects, each with "description" and "queries" (a list of 2-3 search queries).
    3. Detect the user's language and respond in the SAME language for descriptions.
    4. Search queries should be in the language most relevant to the topic (usually the user's language or English for technical topics).
    5. Do NOT include any preamble or meta-commentary. ONLY the JSON.
    
    ## TASK COUNT
    You must generate EXACTLY the number of tasks requested.
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
            
            # Enrich tasks with IDs and Statuses
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

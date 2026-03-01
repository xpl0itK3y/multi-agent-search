import logging
import json
from typing import List, Dict, Any
from src.core.agent import BaseAgent
from src.api.schemas import SearchTask

logger = logging.getLogger(__name__)

class AnalyzerAgent(BaseAgent):
    
    SYSTEM_PROMPT = """
    You are an expert Research Analyst. Your job is to take raw, messy data collected by internet search bots and synthesize it into a comprehensive, well-structured, and easy-to-read report that directly answers the user's original query.

    INPUT:
    You will receive the original user prompt and a JSON list of data gathered by bots. Each item contains a 'url', 'title', and 'content' (raw text from the page).

    YOUR TASK:
    1. Read all the provided content. 
    2. Ignore generic website navigation text, cookie warnings, or irrelevant ads.
    3. Synthesize the core information.
    4. Write a detailed, structured final report in Markdown.
    5. The report MUST be written in the SAME LANGUAGE as the user's original prompt (if the prompt is in Spanish, write in Spanish; if Russian, in Russian, etc.).
    6. Include an "Introduction", "Key Findings / Main Sections", and a "Conclusion".
    7. Include a "Sources" list at the end with the URLs you actually used.

    DO NOT:
    - Hallucinate or make up facts not present in the provided text.
    - Output any internal reasoning, just the final markdown report.
    """

    def run(self, input_data: str) -> str:
        # Dummy implementation to satisfy abstract base class
        return ""

    def run_analysis(self, prompt: str, tasks: List[SearchTask]) -> str:
        
        # Aggregate text from all completed tasks
        aggregated_data = []
        for task in tasks:
            if task.status == 'completed' and task.result:
                # task.result is List[Dict[str, Any]] with url, title, content
                for res in task.result:
                    # Truncate content slightly to ensure we don't blow up context too fast
                    # E.g., limit each page to 3000 chars during aggregation
                    text = res.get("content", "")
                    truncated_text = text[:3000] + "..." if len(text) > 3000 else text
                    
                    aggregated_data.append({
                        "task_description": task.description,
                        "url": res.get("url"),
                        "title": res.get("title"),
                        "content": truncated_text
                    })

        input_data = {
            "original_prompt": prompt,
            "gathered_data": aggregated_data
        }

        user_prompt = f"Please analyze this data and generate the final report:\n\n{json.dumps(input_data, ensure_ascii=False)}"

        messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ]

        logger.info(f"AnalyzerAgent starting generation. Aggregated {len(aggregated_data)} sources.")
        result = self.llm.generate(messages, temperature=0.3)
        return result

import json
import logging

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You are a research intake assistant. Given a research request, decide whether you need clarification to produce a focused plan.

Return ONLY a JSON array of 0 to 3 short clarifying questions (strings).
- If the request is already specific and clear, return [].
- Ask only high-value questions (scope, time period, geography, audience, desired depth) that would MATERIALLY change the research direction.
- Keep questions short and concrete. Write them in the user's language.
- No preamble, no markdown, no extra keys — just the JSON array."""


class ClarifierAgent:
    """Generates 0-3 high-value clarifying questions before planning (skippable)."""

    def __init__(self, llm):
        self.llm = llm

    def generate_questions(self, prompt: str) -> list[str]:
        if self.llm is None:
            return []
        try:
            raw = self.llm.generate(system_prompt=_SYSTEM_PROMPT, user_prompt=prompt)
            clean = raw.strip()
            if clean.startswith("```"):
                clean = clean.split("```")[1]
                if clean.startswith("json"):
                    clean = clean[4:]
                clean = clean.strip()
            questions = json.loads(clean)
            if isinstance(questions, list):
                result = [str(q).strip() for q in questions if str(q).strip()]
                logger.info("clarifier_questions count=%d", len(result))
                return result[:3]
        except Exception as exc:
            logger.warning("clarifier_failed error=%s", exc)
        return []

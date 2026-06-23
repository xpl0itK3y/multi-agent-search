"""Source stance / viewpoint-balance agent.

A report can be thoroughly cited yet systematically one-sided — every source on one side of
a debate. This agent (one LLM call on the fast model) derives the question's central
proposition, labels each cited source's stance toward it (supports / opposes / neutral), and
the service aggregates the balance. It runs only on contestable, opinion-shaped questions;
for factual "what is X" questions stance is not applicable.

This is *viewpoint* diversity — the complement to source independence (origin diversity).
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from src.domain import StanceBalance, StanceSource
from src.core.llm import LLMProvider

logger = logging.getLogger(__name__)

_VALID = {"supports", "opposes", "neutral"}

_SYSTEM = """You analyze the STANCE of research sources toward the central claim of a question.
First, state the single central proposition the question is really asking to evaluate — phrased as a claim that can be supported or opposed (not a neutral topic).
Then, for EACH numbered source, label its stance toward THAT proposition:
- "supports": argues for, or gives evidence favouring, the proposition
- "opposes": argues against, or gives evidence undercutting it
- "neutral": background, mixed, or takes no side
Return ONLY JSON: {"proposition": "...", "stances": [{"source_id": "S1", "stance": "supports"}, ...]}.
No markdown, no commentary."""


class StanceAgent:
    def __init__(self, llm: Optional[LLMProvider] = None) -> None:
        self.llm = llm

    def assess(
        self, prompt: str, sources_by_id: dict[str, dict], language: str = "ru", model: Optional[str] = None
    ) -> StanceBalance:
        if self.llm is None or not sources_by_id:
            return StanceBalance()
        listing = "\n".join(
            f"{sid}: {((src.get('content') or src.get('title') or '')[:400]).strip()}"
            for sid, src in sources_by_id.items()
        )
        user = (
            f"Question: {prompt}\n\n"
            f"Sources:\n{listing}\n\n"
            f"Write the proposition in {'Russian' if language == 'ru' else 'the question language'}."
        )
        data = self._call_json(_SYSTEM, user, model)
        if not isinstance(data, dict):
            return StanceBalance()

        stances: list[StanceSource] = []
        for raw in data.get("stances") or []:
            if not isinstance(raw, dict):
                continue
            sid = str(raw.get("source_id") or "").strip()
            stance = str(raw.get("stance") or "").strip().lower()
            if sid in sources_by_id and stance in _VALID:
                stances.append(StanceSource(source_id=sid, stance=stance))

        supports = sum(1 for s in stances if s.stance == "supports")
        opposes = sum(1 for s in stances if s.stance == "opposes")
        neutral = sum(1 for s in stances if s.stance == "neutral")
        decisive = supports + opposes
        # Need at least two stances and some non-neutral signal to call it a balance.
        if len(stances) < 2 or decisive == 0:
            return StanceBalance()

        dominant = "supports" if supports > opposes else "opposes" if opposes > supports else "balanced"
        skew = max(supports, opposes) / decisive
        return StanceBalance(
            applicable=True,
            proposition=str(data.get("proposition") or "").strip()[:300],
            supports=supports,
            opposes=opposes,
            neutral=neutral,
            dominant_side=dominant,
            skew=round(skew, 4),
            sources=stances,
        )

    def _call_json(self, system_prompt: str, user_prompt: str, model: Optional[str]):
        kwargs = {"model": model} if model else {}
        try:
            raw = self.llm.generate(system_prompt=system_prompt, user_prompt=user_prompt, **kwargs)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("stance_llm_failed error=%s", exc)
            return None
        clean = (raw or "").strip()
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
            clean = clean.strip()
        start, end = clean.find("{"), clean.rfind("}")
        if start != -1 and end != -1 and end > start:
            clean = clean[start : end + 1]
        try:
            return json.loads(clean)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("stance_json_parse_failed error=%s", exc)
            return None

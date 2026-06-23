"""Adversarial red-team pass.

After the report is synthesized, this agent identifies the load-bearing claims the
conclusion depends on, actively searches for evidence *against* them, and judges
whether each claim survives. The result is a "weaknesses & counter-arguments"
section — the differentiator: the report stress-tests itself before it ships.

Two batched LLM calls (extract+counter-queries, then judge) keep it cheap enough to
run inside the finalize budget on the fast model.
"""
from __future__ import annotations

import json
import logging
from typing import Callable, Optional

from src.domain import RedTeamFinding, RedTeamReport
from src.core.llm import LLMProvider

logger = logging.getLogger(__name__)

_VERDICTS = {"refuted", "contested", "qualified", "holds"}

_EXTRACT_SYSTEM = """You are an adversarial red-team analyst.
Given a research report, find the load-bearing factual claims the conclusion most depends on
(the ones that, if wrong, change the answer). Ignore hedged or already-uncertain statements.
For each claim, write 2 web search queries designed to find evidence AGAINST it — refutations,
criticism, limitations, failures, or contradicting studies.
Return ONLY a JSON array, no markdown, no commentary:
[{"claim": "<concise claim>", "counter_queries": ["<query1>", "<query2>"]}]"""

_JUDGE_SYSTEM = """You are an adversarial fact-checker judging whether each claim survives counter-evidence.
For each claim and its counter-evidence snippets, choose exactly one verdict:
- "refuted": credible counter-evidence directly contradicts the claim
- "contested": credible sources genuinely disagree
- "qualified": the claim is true but misses an important caveat/limitation
- "holds": no credible counter-evidence was found (the claim survived the attack)
Write a 1-2 sentence "challenge": what the report under-weights, or why the claim holds.
Be skeptical but fair — do not invent contradictions that the snippets do not support.
Return ONLY a JSON array, no markdown, no commentary:
[{"claim": "<claim>", "verdict": "<verdict>", "challenge": "<1-2 sentences>"}]"""


class RedTeamAgent:
    def __init__(self, llm: Optional[LLMProvider] = None) -> None:
        self.llm = llm

    # ── public API ────────────────────────────────────────────────────────────

    def challenge(
        self,
        prompt: str,
        report: str,
        search_fn: Callable[[str], list[dict]],
        language: str = "ru",
        model: Optional[str] = None,
        max_claims: int = 5,
    ) -> RedTeamReport:
        """Stress-test the report's key claims against counter-evidence."""
        if self.llm is None or not (report or "").strip():
            return RedTeamReport()

        claims = self._extract_claims(prompt, report, language, model, max_claims)
        if not claims:
            return RedTeamReport()

        # Counter-search each claim's adversarial queries → evidence snippets.
        evidence: dict[int, list[dict]] = {}
        for index, claim in enumerate(claims):
            snippets: list[dict] = []
            for query in (claim.get("counter_queries") or [])[:2]:
                for result in (self._safe_search(search_fn, query) or [])[:4]:
                    url = result.get("url")
                    if not url:
                        continue
                    snippets.append(
                        {
                            "url": url,
                            "title": result.get("title") or "",
                            "content": (result.get("content") or result.get("snippet") or "")[:600],
                        }
                    )
            evidence[index] = snippets

        findings = self._judge(claims, evidence, language, model)
        challenged = sum(1 for f in findings if f.verdict in ("refuted", "contested", "qualified"))
        held = sum(1 for f in findings if f.verdict == "holds")
        return RedTeamReport(findings=findings, challenged=challenged, held=held)

    # ── LLM steps ───────────────────────────────────────────────────────────────

    def _extract_claims(
        self, prompt: str, report: str, language: str, model: Optional[str], max_claims: int
    ) -> list[dict]:
        user = (
            f"Research topic: {prompt}\n\n"
            f"Report (analyze this):\n{report[:8000]}\n\n"
            f"Identify at most {max_claims} load-bearing claims and 2 counter-queries each."
        )
        data = self._call_json(_EXTRACT_SYSTEM, user, model)
        claims: list[dict] = []
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                claim = str(item.get("claim") or "").strip()
                queries = [str(q).strip() for q in (item.get("counter_queries") or []) if str(q).strip()]
                if claim and queries:
                    claims.append({"claim": claim, "counter_queries": queries[:2]})
        return claims[:max_claims]

    def _judge(
        self, claims: list[dict], evidence: dict[int, list[dict]], language: str, model: Optional[str]
    ) -> list[RedTeamFinding]:
        blocks: list[str] = []
        for index, claim in enumerate(claims):
            snippets = evidence.get(index) or []
            joined = "\n".join(f"  - {s['title']}: {s['content']}" for s in snippets) or "  - (no counter-evidence found)"
            blocks.append(f"Claim {index + 1}: {claim['claim']}\nCounter-evidence:\n{joined}")
        user = (
            "Judge each claim against its counter-evidence. "
            f"Respond in {'Russian' if language == 'ru' else 'the report language'}.\n\n"
            + "\n\n".join(blocks)
        )
        data = self._call_json(_JUDGE_SYSTEM, user, model)
        findings: list[RedTeamFinding] = []
        if isinstance(data, list):
            for index, item in enumerate(data):
                if not isinstance(item, dict):
                    continue
                verdict = str(item.get("verdict") or "holds").strip().lower()
                if verdict not in _VERDICTS:
                    verdict = "holds"
                claim_text = str(item.get("claim") or (claims[index]["claim"] if index < len(claims) else "")).strip()
                urls = list(dict.fromkeys(s["url"] for s in (evidence.get(index) or [])))[:4]
                findings.append(
                    RedTeamFinding(
                        claim=claim_text,
                        verdict=verdict,
                        challenge=str(item.get("challenge") or "").strip(),
                        source_urls=urls,
                    )
                )
        return findings

    # ── helpers ───────────────────────────────────────────────────────────────

    def _call_json(self, system_prompt: str, user_prompt: str, model: Optional[str]):
        kwargs = {"model": model} if model else {}
        try:
            raw = self.llm.generate(system_prompt=system_prompt, user_prompt=user_prompt, **kwargs)
        except Exception as exc:
            logger.warning("red_team_llm_failed error=%s", exc)
            return None
        return self._parse_json(raw)

    @staticmethod
    def _parse_json(raw: str):
        clean = (raw or "").strip()
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
            clean = clean.strip()
        # Tolerate prose around the array — slice to the outermost brackets.
        start, end = clean.find("["), clean.rfind("]")
        if start != -1 and end != -1 and end > start:
            clean = clean[start : end + 1]
        try:
            return json.loads(clean)
        except Exception as exc:
            logger.warning("red_team_json_parse_failed error=%s", exc)
            return None

    @staticmethod
    def _safe_search(search_fn: Callable[[str], list[dict]], query: str) -> list[dict]:
        try:
            return search_fn(query) or []
        except Exception as exc:
            logger.warning("red_team_search_failed query=%r error=%s", query, exc)
            return []

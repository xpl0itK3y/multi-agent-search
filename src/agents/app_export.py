"""AI-generated custom export ("build me an app/page").

The user writes a free-text design brief; this agent asks the LLM to produce a
complete, self-contained, single-file HTML document that presents the research data
the way the user described. One LLM call; the output is downloaded as a .html file.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from src.core.llm import LLMProvider

logger = logging.getLogger(__name__)

_SYSTEM = """You are an expert front-end developer. Produce a COMPLETE, self-contained, single-file HTML5 document that presents the given research the way the user asks.
Hard rules:
- One file. Inline ALL CSS in a <style> tag; vanilla JS only if needed, inline in <script>.
- NO external resources whatsoever (no CDNs, web fonts, images by URL, trackers) — it must work fully offline.
- Responsive, clean, modern. Use the provided report text and data; keep [Sn] citations if you show claims.
- Do not invent facts beyond the provided data.
Return ONLY the HTML document, starting with <!DOCTYPE html>. No markdown fences, no commentary."""


class AppExportAgent:
    def __init__(self, llm: Optional[LLMProvider] = None) -> None:
        self.llm = llm

    def generate(
        self,
        instruction: str,
        prompt: str,
        report: str,
        scorecard: Optional[dict] = None,
        findings: Optional[list] = None,
        sources: Optional[list] = None,
        model: Optional[str] = None,
    ) -> str:
        if self.llm is None or not (report or "").strip():
            return ""
        data = {
            "trust": scorecard or {},
            "key_findings": (findings or [])[:12],
            "sources": (sources or [])[:30],
        }
        user = (
            f"Research topic: {prompt}\n\n"
            f"User's design request: {instruction.strip() or 'A clean, professional one-page summary.'}\n\n"
            f"Report (markdown):\n{report[:6000]}\n\n"
            f"Structured data (JSON):\n{json.dumps(data, ensure_ascii=False)[:6000]}\n\n"
            "Build the single-file HTML page now."
        )
        try:
            raw = self.llm.generate(system_prompt=_SYSTEM, user_prompt=user, model=model, temperature=0.4)
        except Exception as exc:
            logger.warning("app_export_llm_failed error=%s", exc)
            return ""
        return self._clean_html(raw)

    @staticmethod
    def _clean_html(raw: str) -> str:
        text = (raw or "").strip()
        if text.startswith("```"):
            # strip a leading ```html fence and the trailing ```
            text = text.split("```", 2)
            text = text[1] if len(text) > 1 else (raw or "")
            if text.lower().startswith("html"):
                text = text[4:]
            text = text.rsplit("```", 1)[0].strip()
        low = text.lower()
        start = low.find("<!doctype")
        if start == -1:
            start = low.find("<html")
        if start > 0:
            text = text[start:]
        if "<html" not in text.lower():
            return ""  # not usable HTML
        return text.strip()

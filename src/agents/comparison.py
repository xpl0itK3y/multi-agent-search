"""Structured comparison table.

When the research is a side-by-side comparison ("compare X vs Y"), extract a scored
table from the already-synthesized report: options as columns, criteria as rows, each
cell carrying the [Sn] source ids that back it — so every cell stays verifiable.

One LLM call on the fast model; returns an empty table when the report isn't a comparison.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Optional

from src.api.schemas import ComparisonCell, ComparisonRow, ComparisonTable
from src.core.llm import LLMProvider

logger = logging.getLogger(__name__)

_SYSTEM = """You turn a research report into a structured comparison table.
Only if the report compares 2+ named options (products, tools, options, approaches):
- "options": the things compared (column headers, 2-5 of them)
- "rows": criteria/dimensions; each row has "criterion" and "cells"
- each cell: {"option": <one of options>, "value": <concise, <=12 words>, "source_ids": ["S1", ...]}
  Use only [Sn] ids that appear in the report; omit ids you cannot find.
- "recommendation": one sentence on which option wins and when (optional)
If the report is NOT a comparison of named options, return {"options": [], "rows": []}.
Return ONLY JSON, no markdown, no commentary."""


class ComparisonAgent:
    def __init__(self, llm: Optional[LLMProvider] = None) -> None:
        self.llm = llm

    def build(self, prompt: str, report: str, language: str = "ru", model: Optional[str] = None) -> ComparisonTable:
        if self.llm is None or not (report or "").strip():
            return ComparisonTable()
        user = (
            f"Research topic: {prompt}\n\n"
            f"Report:\n{report[:9000]}\n\n"
            f"Build the comparison table. Write values/recommendation in {'Russian' if language == 'ru' else 'the report language'}."
        )
        data = self._call_json(_SYSTEM, user, model)
        if not isinstance(data, dict):
            return ComparisonTable()
        options = [str(o).strip() for o in (data.get("options") or []) if str(o).strip()][:5]
        if len(options) < 2:
            return ComparisonTable()
        rows: list[ComparisonRow] = []
        for raw_row in data.get("rows") or []:
            if not isinstance(raw_row, dict):
                continue
            criterion = str(raw_row.get("criterion") or "").strip()
            if not criterion:
                continue
            cells: list[ComparisonCell] = []
            for raw_cell in raw_row.get("cells") or []:
                if not isinstance(raw_cell, dict):
                    continue
                value = str(raw_cell.get("value") or "").strip()
                if not value:
                    continue
                cells.append(
                    ComparisonCell(
                        option=str(raw_cell.get("option") or "").strip(),
                        value=value,
                        source_ids=[str(s).strip() for s in (raw_cell.get("source_ids") or []) if str(s).strip()][:4],
                    )
                )
            if cells:
                rows.append(ComparisonRow(criterion=criterion, cells=cells))
        if not rows:
            return ComparisonTable()
        return ComparisonTable(options=options, rows=rows[:12], recommendation=str(data.get("recommendation") or "").strip())

    def _call_json(self, system_prompt: str, user_prompt: str, model: Optional[str]):
        kwargs = {"model": model} if model else {}
        try:
            raw = self.llm.generate(system_prompt=system_prompt, user_prompt=user_prompt, **kwargs)
        except Exception as exc:
            logger.warning("comparison_llm_failed error=%s", exc)
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
        except Exception as exc:
            logger.warning("comparison_json_parse_failed error=%s", exc)
            return None

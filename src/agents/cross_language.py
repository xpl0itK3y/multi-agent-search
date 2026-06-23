"""Cross-language research: search the question in other relevant languages and surface what
non-query-language sources uniquely contribute.

Most research tools search only in the query's language, so the user sees one linguistic
bubble. This agent (1) picks the other languages most likely to add authoritative or
under-reported coverage and translates the question into a search query for each, and (2)
after the sources are gathered, extracts findings unique to (or disagreeing with) the
non-query-language sources. Language *diversity* — the complement to source independence
(origin diversity) and stance (viewpoint diversity).

Language detection is a dependency-free Unicode-script + stopword heuristic.
"""
from __future__ import annotations

import json
import logging
import re
from collections import Counter
from typing import Optional

from src.agents.language_utils import LANGUAGE_HINTS
from src.domain import CrossLanguageFinding
from src.core.llm import LLMProvider

logger = logging.getLogger(__name__)

# Latin-script function-word fingerprints (extends the shared ru/en/es hints).
_LATIN_HINTS: dict[str, set[str]] = {
    "en": LANGUAGE_HINTS["en"],
    "es": LANGUAGE_HINTS["es"],
    "fr": {"le", "la", "les", "des", "une", "et", "pour", "dans", "avec", "que", "est", "sur"},
    "de": {"der", "die", "und", "das", "ist", "ein", "den", "von", "mit", "für", "auch", "sich"},
    "it": {"il", "la", "che", "di", "per", "una", "con", "del", "sono", "gli", "come", "nel"},
    "pt": {"de", "que", "para", "uma", "com", "não", "dos", "como", "mais", "por", "uma"},
}
_SCRIPT_LANG = {
    "cyrillic": "ru", "hangul": "ko", "arabic": "ar", "hebrew": "he", "devanagari": "hi", "greek": "el",
}


def _script(ch: str) -> str:
    o = ord(ch)
    if 0x0400 <= o <= 0x04FF:
        return "cyrillic"
    if 0x3040 <= o <= 0x30FF:
        return "kana"
    if 0x4E00 <= o <= 0x9FFF:
        return "han"
    if 0xAC00 <= o <= 0xD7AF:
        return "hangul"
    if 0x0600 <= o <= 0x06FF:
        return "arabic"
    if 0x0590 <= o <= 0x05FF:
        return "hebrew"
    if 0x0900 <= o <= 0x097F:
        return "devanagari"
    if 0x0370 <= o <= 0x03FF:
        return "greek"
    if (0x41 <= o <= 0x5A) or (0x61 <= o <= 0x7A) or (0x00C0 <= o <= 0x024F):
        return "latin"
    return "other"


def detect_language(text: str) -> str:
    """Dominant-script + stopword language guess. Returns an ISO 639-1 code or 'unknown'."""
    sample = (text or "")[:2000]
    counts = Counter(_script(c) for c in sample if c.isalpha())
    counts.pop("other", None)
    if not counts:
        return "unknown"
    dominant = counts.most_common(1)[0][0]
    if dominant == "kana":
        return "ja"
    if dominant == "han":
        return "ja" if counts.get("kana") else "zh"
    if dominant in _SCRIPT_LANG:
        return _SCRIPT_LANG[dominant]
    if dominant == "latin":
        words = set(re.findall(r"[a-zà-ÿ]+", sample.lower()))
        best, best_score = "en", 0
        for lang, hints in _LATIN_HINTS.items():
            score = len(words & hints)
            if score > best_score:
                best, best_score = lang, score
        return best
    return "unknown"


class CrossLanguageAgent:
    def __init__(self, llm: Optional[LLMProvider] = None) -> None:
        self.llm = llm

    def plan(self, prompt: str, query_language: str, max_targets: int = 2) -> tuple[list[str], list[str]]:
        """Pick up to ``max_targets`` other languages + a translated search query for each."""
        if self.llm is None or not (prompt or "").strip():
            return [], []
        system = (
            "You expand a research question for cross-language coverage. Pick up to "
            f"{max_targets} languages OTHER than '{query_language}' most likely to have "
            "authoritative, first-hand, or under-reported coverage of THIS specific topic "
            "(local regulators, regional press, domestic research). For each, write ONE effective "
            "web-search query in that language's native script. Return ONLY JSON: "
            '{"languages": ["zh","de"], "queries": ["<query>", "<query>"]} — ISO 639-1 codes, '
            "excluding the question's own language, languages[i] paired with queries[i]. "
            'If no other language helps, return {"languages": [], "queries": []}.'
        )
        data = self._call_json(system, f"Question ({query_language}): {prompt}")
        if not isinstance(data, dict):
            return [], []
        langs = [str(x).strip().lower()[:5] for x in (data.get("languages") or []) if str(x).strip()]
        queries = [str(q).strip() for q in (data.get("queries") or []) if str(q).strip()]
        n = min(len(langs), len(queries), max_targets)
        return langs[:n], queries[:n]

    def surface(self, prompt: str, base_language: str, foreign_by_lang: dict[str, list[str]]) -> list[CrossLanguageFinding]:
        """Findings unique to (or disagreeing with) the non-base-language sources."""
        if self.llm is None or not foreign_by_lang:
            return []
        blocks = []
        for lang, snippets in foreign_by_lang.items():
            joined = "\n".join(f"- {s[:300]}" for s in snippets[:5])
            blocks.append(f"[{lang}]\n{joined}")
        system = (
            f"You compare what non-{base_language} sources add to a research question. From the "
            f"foreign-language snippets, extract findings UNIQUE to them or that DISAGREE with the "
            f"typical {base_language}-language narrative — facts, data, regulations, or perspectives a "
            f"{base_language}-only search would miss. Be specific; skip generic overlap. Write each "
            f"finding in {base_language}. Return ONLY JSON: "
            '{"findings": [{"lang": "zh", "finding": "<one specific point>"}]} (max 6). '
            'If they add nothing distinctive, return {"findings": []}.'
        )
        user = f"Question: {prompt}\n\nForeign-language sources:\n" + "\n\n".join(blocks)
        data = self._call_json(system, user)
        if not isinstance(data, dict):
            return []
        out: list[CrossLanguageFinding] = []
        for raw in (data.get("findings") or []):
            if not isinstance(raw, dict):
                continue
            finding = str(raw.get("finding") or "").strip()
            if finding:
                out.append(CrossLanguageFinding(lang=str(raw.get("lang") or "").strip().lower()[:5], finding=finding[:300]))
        return out[:6]

    def _call_json(self, system_prompt: str, user_prompt: str):
        try:
            raw = self.llm.generate(system_prompt=system_prompt, user_prompt=user_prompt)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("cross_language_llm_failed error=%s", exc)
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
        except Exception:  # pragma: no cover - defensive
            return None

"""Deterministic numeric & contradiction check (no LLM).

Numbers are where LLMs fail most confidently and mislead readers most: a report says
"grew 40%" when the source says 14%, quotes "$3.2B" where the source has "$2.3B", or
invents a figure outright. Perplexity and Gemini never verify a quoted statistic against
its source. This agent:

  1. extracts every *significant* figure (percent / money / scaled / large count / year)
     that sits in a cited sentence, and checks whether the cited [Sn] source actually
     contains a matching number (within tolerance);
  2. flags internal contradictions — the same quantity stated with conflicting values in
     different parts of the report.

Purely lexical/numeric — no truth judgement. It catches figures whose own source does not
contain them, and figures the report disagrees with itself about.
"""
from __future__ import annotations

import re

from src.api.schemas import NumericCheck, NumericClaim, NumericContradiction

_CITATION = re.compile(r"\[S(\d+)\]")
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+")
_WORD = re.compile(r"[^\W\d_]{3,}", re.UNICODE)

# A figure: optional currency, a numeric core (with thousands/decimal separators), optional
# scale word or percent sign. Case-insensitive; covers EN + RU scale words.
_SCALE = {
    "%": ("percent", 1.0), "percent": ("percent", 1.0), "percents": ("percent", 1.0),
    "процент": ("percent", 1.0), "процента": ("percent", 1.0), "процентов": ("percent", 1.0),
    "thousand": ("scaled", 1e3), "thousands": ("scaled", 1e3), "тыс": ("scaled", 1e3),
    "million": ("scaled", 1e6), "millions": ("scaled", 1e6), "mn": ("scaled", 1e6), "млн": ("scaled", 1e6),
    "billion": ("scaled", 1e9), "billions": ("scaled", 1e9), "bn": ("scaled", 1e9), "млрд": ("scaled", 1e9),
    "trillion": ("scaled", 1e12), "trillions": ("scaled", 1e12), "трлн": ("scaled", 1e12),
}
_NUM = re.compile(
    r"(?P<cur>[$€£₽])?\s?"
    r"(?P<num>\d{1,3}(?:[ , ]\d{3})+(?:[.,]\d+)?|\d+(?:[.,]\d+)?)"
    r"\s?(?P<scale>%|percents?|процент\w*|thousands?|millions?|billions?|trillions?|bn|mn|млрд|млн|тыс\.?|трлн\.?)?",
    re.IGNORECASE,
)

_STOPWORDS = {
    "the", "and", "for", "are", "with", "that", "this", "from", "into", "than", "then",
    "have", "has", "был", "была", "было", "были", "это", "как", "что", "для", "при",
    "также", "более", "может", "быть", "если", "или", "так", "его", "about", "over", "около",
    "примерно", "than", "которые", "около", "почти", "their", "these", "those", "which",
}
_REL_TOL = 0.02  # relative tolerance — absorbs rounding ("$2.30B" vs "$2.34B")


class NumericCheckAgent:
    def check(self, report: str, sources_by_id: dict[str, dict]) -> NumericCheck:
        if not (report or "").strip():
            return NumericCheck()

        checked = matched = 0
        unsupported: list[NumericClaim] = []
        # for contradiction detection: subject-token-set -> [(value_str, canon, kind, sentence)]
        by_subject: dict[frozenset, list[tuple[str, float, str, str]]] = {}

        for sentence in _SENTENCE_SPLIT.split(report):
            ids = _CITATION.findall(sentence)
            clean = _CITATION.sub("", sentence).strip(" -*#•\t")
            figures = self._figures(clean)
            if not figures:
                continue
            tokens = self._tokens(clean)
            for raw, canon, kind, ctx_tokens in figures:
                key = frozenset(ctx_tokens)
                by_subject.setdefault(key, []).append((raw, canon, kind, clean))
                if not ids:
                    continue  # uncited figure — can't verify against a source
                # check against every cited source in the sentence; supported if any contains it
                ok = False
                have_source_text = False
                for n in dict.fromkeys(ids):
                    source = sources_by_id.get(f"S{n}")
                    content = (source or {}).get("content") or ""
                    if not content.strip():
                        continue
                    have_source_text = True
                    if any(self._close(canon, sv, kind) for sv in self._source_values(content)):
                        ok = True
                        break
                if not have_source_text:
                    continue  # source unavailable — don't penalize
                checked += 1
                if ok:
                    matched += 1
                else:
                    unsupported.append(
                        NumericClaim(
                            value=raw,
                            subject=self._subject(clean, tokens),
                            source_id=f"S{ids[0]}",
                            sentence=clean[:240],
                        )
                    )

        return NumericCheck(
            total=checked,
            supported=matched,
            integrity=(matched / checked) if checked else 0.0,
            unsupported=unsupported[:20],
            contradictions=self._contradictions(by_subject),
        )

    # ── contradiction detection ─────────────────────────────────────────────────

    def _contradictions(self, by_subject: dict) -> list[NumericContradiction]:
        out: list[NumericContradiction] = []
        for tokens, entries in by_subject.items():
            if len(entries) < 2 or len(tokens) < 2:
                continue
            # group only same-kind figures; flag if any two disagree beyond tolerance
            by_kind: dict[str, list] = {}
            for raw, canon, kind, sentence in entries:
                by_kind.setdefault(kind, []).append((raw, canon, sentence))
            for kind, group in by_kind.items():
                distinct: list[tuple[str, float, str]] = []
                for raw, canon, sentence in group:
                    if not any(self._close(canon, c, kind) for _, c, _ in distinct):
                        distinct.append((raw, canon, sentence))
                if len(distinct) >= 2:
                    out.append(
                        NumericContradiction(
                            subject=" ".join(sorted(tokens))[:80],
                            values=[d[0] for d in distinct][:4],
                            sentences=[d[2][:200] for d in distinct][:4],
                        )
                    )
        return out[:10]

    # ── figure extraction / matching ────────────────────────────────────────────

    def _figures(self, text: str) -> list[tuple[str, float, str, tuple[str, ...]]]:
        """Significant figures in a sentence: (raw, canonical value, kind, context tokens)."""
        figures: list[tuple[str, float, str, tuple[str, ...]]] = []
        for m in _NUM.finditer(text):
            canon = self._canon(m.group("num"))
            if canon is None:
                continue
            scale = (m.group("scale") or "").lower().rstrip(".")
            kind, mult = self._scale_kind(scale)
            cur = m.group("cur")
            if cur and kind == "plain":
                kind = "money"
            value = canon * mult
            if kind == "plain" and 1900 <= canon <= 2099 and canon == int(canon):
                kind = "year"
            if not self._significant(kind, value):
                continue
            raw = m.group(0).strip()
            ctx = self._context_tokens(text, m.start(), m.end())
            figures.append((raw, value, kind, ctx))
        return figures

    def _source_values(self, content: str) -> list[float]:
        values: list[float] = []
        for m in _NUM.finditer(content):
            canon = self._canon(m.group("num"))
            if canon is None:
                continue
            _, mult = self._scale_kind((m.group("scale") or "").lower().rstrip("."))
            values.append(canon * mult)
        return values

    @staticmethod
    def _scale_kind(scale: str) -> tuple[str, float]:
        if not scale:
            return "plain", 1.0
        for token, (kind, mult) in _SCALE.items():
            if scale.startswith(token):
                return kind, mult
        return "plain", 1.0

    @staticmethod
    def _significant(kind: str, value: float) -> bool:
        if kind in ("percent", "money", "scaled"):
            return True
        if kind == "year":
            return True
        return abs(value) >= 100  # skip small bare counts ("3 factors") to keep precision high

    @staticmethod
    def _canon(num: str) -> float | None:
        """Locale-tolerant parse: strip spaces; resolve , vs . as thousands/decimal."""
        s = num.replace(" ", "").replace(" ", "")
        if "," in s and "." in s:
            # the rightmost separator is the decimal point
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            s = s.replace(",", "") if re.fullmatch(r"\d{1,3}(,\d{3})+", s) else s.replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None

    @staticmethod
    def _close(a: float, b: float, kind: str) -> bool:
        if kind == "year":
            return round(a) == round(b)
        denom = max(abs(a), abs(b), 1.0)
        if abs(a - b) / denom <= _REL_TOL:
            return True
        if kind == "percent" and abs(a - b) <= 1.0:  # tolerate ±1 point rounding
            return True
        return False

    def _context_tokens(self, text: str, start: int, end: int) -> tuple[str, ...]:
        """Salient words immediately around the number — identifies what it measures."""
        before = self._tokens(text[:start])[-3:]
        after = self._tokens(text[end:])[:3]
        return tuple(before + after)

    def _subject(self, sentence: str, tokens: list[str]) -> str:
        return " ".join(tokens[:6])

    @staticmethod
    def _tokens(text: str) -> list[str]:
        return [t for t in _WORD.findall((text or "").lower()) if t not in _STOPWORDS]

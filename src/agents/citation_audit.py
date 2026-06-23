"""Deterministic citation audit (no LLM).

For every inline ``[Sn]`` citation, check whether the cited source's text actually backs
the sentence — by lexical overlap. Reports are frequently written in one language while
citing sources in another, so matching is LANGUAGE-AWARE:

  * when the source shares the claim's primary language, the full token overlap is scored
    leniently (the normal same-language case),
  * when it doesn't, only language-agnostic ANCHORS (numbers + Latin names/acronyms like
    "OpenAI", "GPT-5.6", "IPO") can match — these survive across languages, and
  * if a foreign-language source can't be judged from anchors, the claim is "unverified"
    rather than flagged — a foreign-language citation is never falsely called fabricated.

Integrity is scored over VERIFIABLE claims only (supported / (supported + unsupported)).
This is a lexical match, not a truth judgement: it catches citations whose source never
mentions the claim's terms — the common "fabricated citation" failure.
"""
from __future__ import annotations

import re

from src.domain import CitationAudit, CitationGround

_CITATION = re.compile(r"\[S(\d+)\]")
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+")

# Script-specific (language) tokens vs language-agnostic anchors.
_CYR = re.compile(r"[а-яё]{3,}")
_CJK = re.compile(r"[぀-ヿ一-鿿]{2,}")
_LATIN = re.compile(r"[a-z][a-z0-9.+#-]{2,}")   # acronyms / product names: openai, gpt-5.6, ipo
_NUM = re.compile(r"\d[\d.,]*")

# Strip source boilerplate so the overlap is against real prose, not nav/logos/ads.
_IMG = re.compile(r"!\[[^\]]*\]\([^)]*\)")
_MDLINK = re.compile(r"\[([^\]]+)\]\([^)]*\)")
_URL = re.compile(r"https?://\S+")

_NATIVE_T = 0.20    # share of same-language claim terms that must appear in the source
_ANCHOR_T = 0.50    # share of language-agnostic anchors that must appear (cross-language)
_MIN_ANCHORS = 3    # below this, a foreign-language claim is "unverified", not "unsupported"

_STOPWORDS = {
    "the", "and", "for", "are", "with", "that", "this", "from", "into", "than", "then",
    "have", "has", "был", "была", "было", "были", "это", "как", "что", "для", "при",
    "также", "более", "может", "быть", "если", "или", "так", "его", "their", "these",
    "those", "which", "while", "about", "would", "could", "should", "там",
}


class CitationAuditAgent:
    def audit(self, report: str, sources_by_id: dict[str, dict]) -> CitationAudit:
        """``sources_by_id``: {"S1": {"content","url","title"}, ...} as the report numbered them."""
        if not report or not sources_by_id:
            return CitationAudit()

        report_has_cyr = bool(_CYR.search(report.lower()))
        checks: list[tuple[str, str]] = []          # (claim, status: ok | no | unverified)
        best: dict[str, tuple[float, str, bool]] = {}  # source_id -> (score, quote, single_ok)
        foreign: dict[str, bool] = {}               # source_id -> different language than the report

        for sentence in _SENTENCE_SPLIT.split(report):
            ids = _CITATION.findall(sentence)
            if not ids:
                continue
            claim = _CITATION.sub("", sentence).strip(" -*#•\t")
            c_cyr, c_cjk, c_lat, c_num = self._tokens(claim)
            if len(c_cyr) + len(c_cjk) + len(c_lat) + len(c_num) < 4:
                continue  # too short to judge meaningfully
            claim_terms = c_cyr | c_cjk | c_lat | c_num

            s_cyr: set[str] = set()
            s_cjk: set[str] = set()
            s_lat: set[str] = set()
            s_num: set[str] = set()
            saw_source = False
            for n in dict.fromkeys(ids):  # dedupe, preserve order
                source_id = f"S{n}"
                source = sources_by_id.get(source_id)
                if not source:
                    continue
                saw_source = True
                content = self._clean(source.get("content") or "")
                t_cyr, t_cjk, t_lat, t_num = self._tokens(content)
                s_cyr |= t_cyr
                s_cjk |= t_cjk
                s_lat |= t_lat
                s_num |= t_num
                if source_id not in foreign:
                    foreign[source_id] = report_has_cyr and not _CYR.search(content.lower())
                quote, score = self._best_passage(claim_terms, content)
                if source_id not in best or score > best[source_id][0]:
                    best[source_id] = (score, quote, score >= _NATIVE_T)
            if not saw_source:
                continue

            src_terms = s_cyr | s_cjk | s_lat | s_num
            full_cov = len(claim_terms & src_terms) / len(claim_terms) if claim_terms else 0.0
            anchors = c_lat | c_num
            anchor_cov = len(anchors & (s_lat | s_num)) / len(anchors) if anchors else 0.0
            # The source "shares the language" when it has tokens in the claim's primary script.
            primary = self._primary(c_cyr, c_cjk, c_lat)
            shares = (
                (primary == "cyr" and s_cyr)
                or (primary == "cjk" and s_cjk)
                or (primary == "lat" and s_lat)
            )

            if shares:
                checks.append((claim, "ok" if full_cov >= _NATIVE_T else "no"))
            elif anchor_cov >= _ANCHOR_T and len(anchors) >= 2:
                checks.append((claim, "ok"))
            elif len(anchors) >= _MIN_ANCHORS:
                checks.append((claim, "no"))     # enough anchors to judge, and they're absent
            else:
                checks.append((claim, "unverified"))  # foreign source, too few anchors to judge

        supported = sum(1 for _, s in checks if s == "ok")
        unsupported_claims = [c for c, s in checks if s == "no"]
        unverified = sum(1 for _, s in checks if s == "unverified")
        verifiable = supported + len(unsupported_claims)

        grounding = [
            CitationGround(
                source_id=source_id,
                url=(sources_by_id.get(source_id) or {}).get("url", "") or "",
                title=(sources_by_id.get(source_id) or {}).get("title", "") or "",
                quote=quote,
                # Never red-flag a citation we couldn't read (foreign-language source).
                supported=single_ok or foreign.get(source_id, False),
            )
            for source_id, (_score, quote, single_ok) in best.items()
        ]
        return CitationAudit(
            total=verifiable,
            supported=supported,
            integrity=(supported / verifiable) if verifiable else (1.0 if checks else 0.0),
            unverified=unverified,
            unsupported_claims=unsupported_claims[:20],
            grounding=grounding,
        )

    # ── matching ────────────────────────────────────────────────────────────────

    @staticmethod
    def _primary(cyr: set, cjk: set, lat: set) -> str:
        counts = {"cyr": len(cyr), "cjk": len(cjk), "lat": len(lat)}
        return max(counts, key=counts.get)

    @staticmethod
    def _clean(content: str) -> str:
        """Drop markdown image/link/URL boilerplate so overlap is against real prose."""
        content = _IMG.sub(" ", content or "")
        content = _MDLINK.sub(r"\1", content)
        content = _URL.sub(" ", content)
        return content

    @staticmethod
    def _tokens(text: str) -> tuple[set[str], set[str], set[str], set[str]]:
        """Return (cyrillic, cjk, latin, numbers)."""
        low = (text or "").lower()
        cyr = {t for t in _CYR.findall(low) if t not in _STOPWORDS}
        cjk = set(_CJK.findall(low))
        lat = {t for t in _LATIN.findall(low) if len(t) >= 3 and t not in _STOPWORDS}
        num = set()
        for raw in _NUM.findall(low):
            digits = re.sub(r"\D", "", raw)
            if len(digits) >= 2:  # ignore single digits (noise)
                num.add(digits)
        return cyr, cjk, lat, num

    def _best_passage(self, claim_terms: set[str], content: str) -> tuple[str, float]:
        """Return the source passage best overlapping the claim, and the overlap share."""
        if not claim_terms:
            return "", 0.0
        best_text, best_score = "", 0.0
        for passage in _SENTENCE_SPLIT.split(content):
            passage = passage.strip()
            if not passage:
                continue
            cyr, cjk, lat, num = self._tokens(passage)
            overlap = len(claim_terms & (cyr | cjk | lat | num))
            score = overlap / len(claim_terms)
            if score > best_score:
                best_text, best_score = passage, score
        return best_text[:240].strip(), best_score

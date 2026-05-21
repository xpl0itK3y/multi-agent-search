"""
report_utils.py — shared report post-processing utilities.

Used by both streamlit_app.py (UI rendering) and report_export.py (PDF/DOCX).
"""
from __future__ import annotations

import re
from urllib.parse import urlparse

# ── Regex constants (compiled once at import time) ────────────────────────────

_PREAMBLE = re.compile(
    r"(?i)^("
    r"ниже\s+представлен|ниже\s+приведён|"
    r"below\s+is\s+(the|a)\s+(synthesized|final|complete)|"
    r"the\s+following\s+is\s+(a|the)\s+(synthesized|final)|"
    r"вот\s+синтезирован|данный\s+отчёт\s+объединяет|синтезирую\s+все|"
    r"i\s+have\s+(combined|merged|synthesized)|"
    r"here\s+is\s+(the|a)\s+(synthesized|final)"
    r")[^\n]*\n+"
)

_NOTES_SECTION = re.compile(
    r"\n+##\s+(Примечания\s+к\s+отчёту|Примечания\s+к\s+отчету"
    r"|Report\s+Notes|Notas\s+del\s+informe)\s*\n.*?(?=\n## |\Z)",
    re.DOTALL | re.IGNORECASE,
)

_OLD_SOURCE = re.compile(
    r"^- \[S(\d+)\] (https?://\S+)$",
    re.MULTILINE,
)


def _source_label(url: str) -> str:
    """Return domain for bare-URL labels so they don't overflow."""
    try:
        return urlparse(url).netloc.removeprefix("www.") or url
    except Exception:
        return url


def clean_report(report: str) -> str:
    """Strip LLM preamble, quality-notes section, and upgrade legacy source lines.

    Safe to call on any report string — old DB-stored or freshly generated.
    Idempotent: calling it twice produces the same result.
    """
    # 1. Remove LLM meta-commentary before the first heading
    report = _PREAMBLE.sub("", report).strip()

    # 2. Remove "Примечания к отчёту / Report Notes" section
    report = _NOTES_SECTION.sub("", report)

    # 3. Upgrade old bare-URL source lines to clickable markdown links
    def _replace(m: re.Match) -> str:
        sid = f"S{m.group(1)}"
        url = m.group(2)
        return f"- **\\[{sid}\\]** [{_source_label(url)}]({url})"

    return _OLD_SOURCE.sub(_replace, report)

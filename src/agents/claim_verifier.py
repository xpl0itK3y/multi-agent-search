import re

from src.domain import ClaimVerificationSummary


class ClaimVerifierAgent:
    SOFTENER_BY_LANGUAGE = {
        "ru": "По имеющимся данным, ",
        "en": "Based on the available sources, ",
        "es": "Según las fuentes disponibles, ",
    }
    # Only replace genuinely absolute / unqualified certainty words.
    # We intentionally exclude "clearly→apparently" and "always→typically" because
    # those replacements break factual meaning and produce awkward sentences.
    # "полностью" is also excluded — it means "fully/completely" which is often correct.
    ABSOLUTE_PATTERNS = (
        (re.compile(r"(?i)\b(однозначно|безусловно)\b"), "по всей видимости"),
        (re.compile(r"(?i)\b(definitely|certainly)\b"), "generally"),
        (re.compile(r"(?i)\b(siempre)\b"), "generalmente"),
        (re.compile(r"(?i)\b(sin duda)\b"), "aparentemente"),
    )
    NOTE_MESSAGES = {
        "ru": {
            "weak_support": "Некоторые формулировки были смягчены, потому что подтверждение источниками оставалось ограниченным.",
            "uncited": "В отчёте остались строки, которым по структуре всё ещё нужны более явные ссылки на источники.",
            "insufficient_evidence": "Некоторые выводы были дополнительно смягчены, потому что им не хватало достаточного количества независимых подтверждений.",
        },
        "en": {
            "weak_support": "Some wording was softened because source support remained limited.",
            "uncited": "Some report lines still appear to need clearer source attribution.",
            "insufficient_evidence": "Some conclusions were softened further because they lacked enough independent supporting evidence.",
        },
        "es": {
            "weak_support": "Algunas formulaciones se suavizaron porque el respaldo de las fuentes seguía siendo limitado.",
            "uncited": "Algunas líneas del informe todavía parecen necesitar una atribución de fuentes más clara.",
            "insufficient_evidence": "Algunas conclusiones se suavizaron aún más porque no tenían suficiente evidencia independiente de apoyo.",
        },
    }
    STRONG_CLAIM_PATTERNS = (
        re.compile(r"(?i)\b(best|worst|always|never|clearly|definitively|proves|guarantees)\b"),
        re.compile(r"(?i)\b(лучш|худш|всегда|никогда|однозначно|явно|доказывает|гарантирует)\b"),
    )
    EXISTING_SOFTENERS = (
        "по имеющимся данным",
        "на основании доступных источников",
        "по доступным источникам",
        "based on the available sources",
        "according to the available sources",
        "the available data indicates",
        "según las fuentes disponibles",
    )

    def _softener(self, language: str) -> str:
        return self.SOFTENER_BY_LANGUAGE.get(language, self.SOFTENER_BY_LANGUAGE["en"])

    def _soften_absolute_terms(self, line: str) -> str:
        """Replace absolute-language tokens in uncited or weakly-cited lines.

        Lines already backed by ≥ 2 inline source references are considered
        sufficiently supported — we leave their wording intact to avoid
        degrading clear, well-cited analytical language.
        """
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            return line
        # Skip well-cited lines — two or more [Sn] references is adequate support.
        if len(re.findall(r"\[S\d+\]", stripped)) >= 2:
            return line
        softened = stripped
        for pattern, replacement in self.ABSOLUTE_PATTERNS:
            softened = pattern.sub(replacement, softened)
        return line.replace(stripped, softened, 1) if softened != stripped else line

    def _soften_line(self, line: str, language: str) -> str:
        """Prepend the hedging prefix to a flagged line (after absolute-term pass)."""
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            return line

        lowered = stripped.lower()
        if any(lowered.startswith(prefix) for prefix in self.EXISTING_SOFTENERS):
            return line

        softened = self._softener(language) + stripped[0].lower() + stripped[1:]
        return line.replace(stripped, softened, 1)

    def _looks_strong_or_overbroad(self, line: str) -> bool:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            return False
        citation_count = len(re.findall(r"\[S\d+\]", stripped))
        if citation_count >= 2:
            return False
        lowered = stripped.lower()
        if any(lowered.startswith(prefix) for prefix in self.EXISTING_SOFTENERS):
            return False
        return any(pattern.search(stripped) for pattern in self.STRONG_CLAIM_PATTERNS)

    def verify_and_downgrade(
        self,
        report: str,
        language: str,
        uncited_lines: list[str],
        unsupported_lines: list[str],
        insufficient_evidence_lines: list[str] | None = None,
        max_softened: int = 4,
    ) -> tuple[str, ClaimVerificationSummary]:
        insufficient_evidence_lines = insufficient_evidence_lines or []
        # Only soften lines that are explicitly flagged as unsupported/insufficient.
        # Avoid blanket softening via _looks_strong_or_overbroad — it fires on normal
        # analytical language ("best practices", "лучший вариант") and floods the
        # report with "По имеющимся данным". Cap total softened lines to prevent spam.
        issue_lines = set(dict.fromkeys(unsupported_lines + insufficient_evidence_lines))
        downgraded_lines = 0
        softened_count = 0
        updated_lines: list[str] = []

        for line in report.splitlines():
            normalized = line.strip()
            # Apply absolute-term replacement on all lines (subtle, no prefix added).
            term_softened = self._soften_absolute_terms(line)
            if normalized in issue_lines and softened_count < max_softened:
                softened = self._soften_line(term_softened, language)
                if softened != line:
                    downgraded_lines += 1
                    softened_count += 1
                updated_lines.append(softened)
            else:
                if term_softened != line:
                    downgraded_lines += 1
                updated_lines.append(term_softened)

        notes: list[str] = []
        messages = self.NOTE_MESSAGES.get(language, self.NOTE_MESSAGES["en"])
        if unsupported_lines:
            notes.append(messages["weak_support"])
        if uncited_lines:
            notes.append(messages["uncited"])
        if insufficient_evidence_lines:
            notes.append(messages["insufficient_evidence"])

        summary = ClaimVerificationSummary(
            uncited_lines=len(uncited_lines),
            unsupported_lines=len(unsupported_lines),
            insufficient_evidence_lines=len(insufficient_evidence_lines),
            downgraded_lines=downgraded_lines,
            verification_notes=notes,
        )
        return "\n".join(updated_lines), summary

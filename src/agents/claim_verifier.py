import re

from src.api.schemas import ClaimVerificationSummary


class ClaimVerifierAgent:
    SOFTENER_BY_LANGUAGE = {
        "ru": "По имеющимся данным, ",
        "en": "Based on the available sources, ",
        "es": "Según las fuentes disponibles, ",
    }
    ABSOLUTE_PATTERNS = (
        (re.compile(r"(?i)\b(однозначно|всегда|полностью|безусловно)\b"), "по имеющимся данным"),
        (re.compile(r"(?i)\b(always|definitely|clearly|certainly)\b"), "based on the available sources"),
        (re.compile(r"(?i)\b(siempre|claramente|sin duda)\b"), "según las fuentes disponibles"),
    )
    NOTE_MESSAGES = {
        "ru": {
            "weak_support": "Некоторые формулировки были смягчены, потому что подтверждение источниками оставалось ограниченным.",
            "uncited": "В отчёте остались строки, которым по структуре всё ещё нужны более явные ссылки на источники.",
        },
        "en": {
            "weak_support": "Some wording was softened because source support remained limited.",
            "uncited": "Some report lines still appear to need clearer source attribution.",
        },
        "es": {
            "weak_support": "Algunas formulaciones se suavizaron porque el respaldo de las fuentes seguía siendo limitado.",
            "uncited": "Algunas líneas del informe todavía parecen necesitar una atribución de fuentes más clara.",
        },
    }

    def _softener(self, language: str) -> str:
        return self.SOFTENER_BY_LANGUAGE.get(language, self.SOFTENER_BY_LANGUAGE["en"])

    def _soften_line(self, line: str, language: str) -> str:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            return line

        lowered = stripped.lower()
        if lowered.startswith(("по имеющимся данным", "based on the available sources", "según las fuentes disponibles")):
            return line

        softened = stripped
        for pattern, replacement in self.ABSOLUTE_PATTERNS:
            softened = pattern.sub(replacement, softened)
        if softened == stripped:
            softened = self._softener(language) + stripped[0].lower() + stripped[1:]
        return line.replace(stripped, softened, 1)

    def verify_and_downgrade(
        self,
        report: str,
        language: str,
        uncited_lines: list[str],
        unsupported_lines: list[str],
    ) -> tuple[str, ClaimVerificationSummary]:
        issue_lines = list(dict.fromkeys(unsupported_lines))
        downgraded_lines = 0
        updated_lines: list[str] = []

        for line in report.splitlines():
            normalized = line.strip()
            if normalized in issue_lines:
                softened = self._soften_line(line, language)
                if softened != line:
                    downgraded_lines += 1
                updated_lines.append(softened)
            else:
                updated_lines.append(line)

        notes: list[str] = []
        messages = self.NOTE_MESSAGES.get(language, self.NOTE_MESSAGES["en"])
        if unsupported_lines:
            notes.append(messages["weak_support"])
        if uncited_lines:
            notes.append(messages["uncited"])

        summary = ClaimVerificationSummary(
            uncited_lines=len(uncited_lines),
            unsupported_lines=len(unsupported_lines),
            downgraded_lines=downgraded_lines,
            verification_notes=notes,
        )
        return "\n".join(updated_lines), summary

"""Shared language detection and analysis lexicons."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Iterable


# Common function-word fingerprints for the languages supported by the report writer.
LANGUAGE_HINTS: dict[str, set[str]] = {
    "ru": {"и", "в", "не", "что", "для", "как", "это", "на", "по", "который", "также"},
    "es": {
        "el", "la", "los", "las", "para", "como", "una", "con", "del", "que", "por",
        "desde", "entre", "también", "tambien",
    },
    "en": {"the", "and", "for", "with", "that", "from", "this", "into", "small"},
}


@dataclass(frozen=True)
class AnalysisLexicon:
    stopwords: frozenset[str]
    generic_tokens: frozenset[str]
    negation_tokens: frozenset[str]


ANALYSIS_LEXICONS: dict[str, AnalysisLexicon] = {
    "en": AnalysisLexicon(
        stopwords=frozenset({
            "the", "and", "for", "with", "that", "this", "from", "into", "their", "there",
            "about", "have", "has", "had", "were", "was", "will", "would", "could", "should",
            "than", "then", "over", "under", "using", "used", "uses", "also", "only", "more",
            "most", "less", "very", "some", "many", "much", "when", "where", "while", "which",
            "what", "your", "they", "them", "being", "been", "because", "through", "each", "same",
            "such", "make", "made", "like", "just", "small",
        }),
        generic_tokens=frozenset({
            "api", "apis", "backend", "comparison", "compare", "django", "fastapi", "flask",
            "framework", "frameworks", "platform", "platforms", "production", "support", "supports",
            "system", "systems", "company", "companies", "service", "services", "python",
        }),
        negation_tokens=frozenset({
            "no", "not", "never", "without", "lack", "lacks", "cannot", "can't", "doesn't", "don't",
        }),
    ),
    "ru": AnalysisLexicon(
        stopwords=frozenset({
            "и", "в", "во", "не", "что", "для", "как", "это", "на", "по", "из", "или", "при",
            "его", "ее", "её", "их", "который", "которая", "которое", "которые", "которых", "также",
            "этого", "этой", "этом", "этих", "были", "было", "была", "будет", "могут", "может",
            "более", "менее", "после", "перед", "между", "через", "около", "согласно", "поскольку",
            "однако", "только", "своих", "такой", "такая", "такие", "того", "чтобы", "каждый",
            "много", "часто", "очень",
        }),
        generic_tokens=frozenset({
            "апи", "бэкенд", "система", "системы", "платформа", "платформы", "компания", "компании",
            "сервис", "сервисы", "фреймворк", "фреймворки", "поддержка", "поддерживает", "сравнение",
            "сравнить", "продакшн", "производство",
        }),
        negation_tokens=frozenset({
            "не", "нет", "без", "никогда", "отсутствует", "отсутствуют", "невозможно",
        }),
    ),
    "es": AnalysisLexicon(
        stopwords=frozenset({
            "el", "la", "los", "las", "un", "una", "unos", "unas", "de", "del", "y", "o", "que",
            "para", "como", "con", "por", "esta", "este", "estos", "estas", "desde", "entre", "sobre",
            "según", "segun", "también", "tambien", "solo", "sólo", "cuando", "donde", "mientras",
            "porque", "cada", "mismo", "misma", "mismos", "mismas", "tiene", "tienen", "tuvo", "fueron",
            "era", "será", "seran", "puede", "pueden", "podría", "podria", "deberían", "deberian", "más",
            "mas", "menos", "mucho", "muchos", "algunas",
        }),
        generic_tokens=frozenset({
            "api", "backend", "comparación", "comparacion", "comparar", "empresa", "empresas", "marco",
            "marcos", "plataforma", "plataformas", "producción", "produccion", "servicio", "servicios",
            "sistema", "sistemas", "soporte", "soporta",
        }),
        negation_tokens=frozenset({
            "no", "sin", "nunca", "carece", "carecen", "imposible",
        }),
    ),
}


def merge_analysis_lexicons(languages: Iterable[str]) -> AnalysisLexicon:
    selected = [
        ANALYSIS_LEXICONS[language]
        for language in set(languages)
        if language in ANALYSIS_LEXICONS
    ]
    if not selected:
        selected = [ANALYSIS_LEXICONS["en"]]
    return AnalysisLexicon(
        stopwords=frozenset().union(*(lexicon.stopwords for lexicon in selected)),
        generic_tokens=frozenset().union(*(lexicon.generic_tokens for lexicon in selected)),
        negation_tokens=frozenset().union(*(lexicon.negation_tokens for lexicon in selected)),
    )


def analysis_lexicon_for_sources(
    aggregated_data: list[dict],
    language_detector: Callable[[str], str] | None = None,
) -> AnalysisLexicon:
    """Select source lexicons using the canonical language detector.

    The lazy import avoids a module cycle: ``cross_language`` owns detection while importing
    the shared hint tables from this module.
    """
    if language_detector is None:
        from src.agents.cross_language import detect_language

        language_detector = detect_language
    languages = {"en"}
    for source in aggregated_data:
        text = f"{source.get('title') or ''} {source.get('content') or ''}"
        languages.add(language_detector(text))
    return merge_analysis_lexicons(languages)


COMBINED_ANALYSIS_LEXICON = merge_analysis_lexicons(ANALYSIS_LEXICONS)


def contains_negation(text: str, negation_tokens: Iterable[str]) -> bool:
    """Match negation terms as words, avoiding substrings such as ``not`` in ``notable``."""
    lowered = (text or "").lower()
    return any(
        re.search(rf"(?<!\w){re.escape(token)}(?!\w)", lowered, flags=re.UNICODE)
        for token in negation_tokens
    )

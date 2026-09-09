import pytest

from src.agents.analyzer import AnalyzerAgent
from src.agents.evidence_mapper import EvidenceMapperAgent
from src.agents.language_utils import (
    ANALYSIS_LEXICONS,
    analysis_lexicon_for_sources,
    contains_negation,
)


class _NoopLLM:
    def generate(self, *args, **kwargs):
        return "{}"


def test_language_lexicons_include_required_russian_and_spanish_terms():
    assert {"не", "нет", "без", "отсутствует"} <= ANALYSIS_LEXICONS["ru"].negation_tokens
    assert {"no", "sin", "nunca"} <= ANALYSIS_LEXICONS["es"].negation_tokens
    assert {"который", "также"} <= ANALYSIS_LEXICONS["ru"].stopwords
    assert {"компания", "компании"} <= ANALYSIS_LEXICONS["ru"].generic_tokens


def test_source_language_selects_only_relevant_optional_lexicons():
    russian = analysis_lexicon_for_sources(
        [{"content": "Компания сообщает, что платформа не работает без подключения."}]
    )
    spanish = analysis_lexicon_for_sources(
        [{"content": "La compañía informa que la producción nunca funciona sin conexión."}]
    )

    assert "отсутствует" in russian.negation_tokens
    assert "nunca" not in russian.negation_tokens
    assert "nunca" in spanish.negation_tokens
    assert "отсутствует" not in spanish.negation_tokens
    assert "without" in russian.negation_tokens
    assert "without" in spanish.negation_tokens


def test_negation_matching_uses_word_boundaries():
    assert contains_negation("This mode is not supported.", {"not"})
    assert not contains_negation("This is a notable improvement.", {"not"})
    assert contains_negation("Режим не поддерживается.", {"не"})
    assert not contains_negation("Нужно внешнее подключение.", {"не"})


@pytest.mark.parametrize(
    "left,right",
    [
        (
            "Платформа Альфа поддерживает автономный режим работы для полевых команд и не требует подключения к центральной сети.",
            "Платформа Альфа поддерживает автономный режим работы для полевых команд и требует подключения к центральной сети.",
        ),
        (
            "La plataforma Alfa ofrece procesamiento local para equipos móviles sin conexión permanente a la red central.",
            "La plataforma Alfa ofrece procesamiento local para equipos móviles con conexión permanente a la red central.",
        ),
    ],
)
def test_conflict_candidates_recognize_localized_negation(left, right):
    analyzer = AnalyzerAgent(_NoopLLM())

    candidates = analyzer._detect_conflict_candidates(
        [
            {"source_id": "S1", "content": left},
            {"source_id": "S2", "content": right},
        ]
    )

    assert len(candidates) == 1
    assert candidates[0]["source_ids"] == ["S1", "S2"]


def test_evidence_mapper_filters_russian_stopwords_and_generic_tokens():
    groups, summary = EvidenceMapperAgent().build_evidence_groups(
        [
            {
                "source_id": "S1",
                "content": "Компания также сообщает, что квантовый процессор снижает энергопотребление на 30 процентов во время длительных вычислений.",
            },
            {
                "source_id": "S2",
                "content": "Компания также сообщает, что квантовый процессор снижает энергопотребление на 28 процентов во время независимых испытаний.",
            },
        ]
    )

    assert len(groups) == 1
    assert groups[0]["source_ids"] == ["S1", "S2"]
    assert "компания" not in groups[0]["topic"]
    assert "также" not in groups[0]["topic"]
    assert summary.multi_source_group_count == 1

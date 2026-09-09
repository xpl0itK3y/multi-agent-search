"""Labeled language-detection eval (language-fix).

Guards the single detector used throughout planning, analysis, and report delivery."""
import pytest

from src.agents.cross_language import detect_language

# (text, expected) for the languages the report writer supports.
LABELED = [
    ("Сравни состояние рынка электромобилей в России и Европе на 2026 год", "ru"),
    ("Каковы ключевые барьеры коммерциализации твердотельных аккумуляторов в 2026", "ru"),
    ("What is the current state of solid-state battery commercialization in 2026?", "en"),
    ("Analyze the trade-offs between Rust and Go for high-throughput backend services", "en"),
    ("Compara el estado de las baterías de estado sólido para vehículos eléctricos", "es"),
    ("¿Cuál es el consenso científico actual sobre el ayuno intermitente?", "es"),
]


@pytest.mark.parametrize("text,expected", LABELED)
def test_language_detector_matches_label(text, expected):
    assert detect_language(text) == expected


@pytest.mark.parametrize("text,expected", [
    ("人工智能在医疗诊断中的应用现状与未来发展趋势研究报告", "zh"),
    ("これは日本語で書かれた研究レポートのテストです、よろしく", "ja"),
])
def test_cross_language_handles_cjk(text, expected):
    assert detect_language(text) == expected

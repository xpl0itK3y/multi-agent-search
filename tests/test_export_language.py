"""EXPORT-LANGUAGE: PDF/DOCX/HTML exports take their labels and <html lang> from the
research's stored language, not from a second heuristic run over the prompt."""

import io

import pytest

from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth
from src.repositories import InMemoryTaskStore
from src.services import ResearchService
from src.ui import report_export
from src.ui.report_export import generate_docx, generate_html, generate_pdf

# Spanish without ñ/¿/¡ — the retired prompt heuristic called this English.
_SPANISH_PROMPT = "Impacto de la energia solar en los hogares de Chile"


@pytest.fixture(autouse=True)
def _builtin_pdf_fonts(monkeypatch):
    # These tests read labels back out of the PDF; keep the font the same on every host
    # (DejaVu is only picked up where /usr/share/fonts has it).
    monkeypatch.setattr(
        report_export,
        "_register_pdf_fonts",
        lambda: {"r": "Helvetica", "b": "Helvetica-Bold", "i": "Helvetica-Oblique", "bi": "Helvetica-BoldOblique"},
    )


def _pdf_text(data: bytes) -> str:
    from pypdf import PdfReader

    return "\n".join(page.extract_text() or "" for page in PdfReader(io.BytesIO(data)).pages)


def _docx_text(data: bytes) -> str:
    from docx import Document

    return "\n".join(paragraph.text for paragraph in Document(io.BytesIO(data)).paragraphs)


def _completed_research(store: InMemoryTaskStore, prompt: str, language: str):
    record = store.add_research(
        ResearchRequest(prompt=prompt, depth=SearchDepth.EASY), task_ids=[], language=language
    )
    store.update_research_status(record.id, ResearchStatus.COMPLETED, "## Resumen\nTexto del informe [S1].")
    return record


def test_prompt_language_heuristic_is_gone():
    assert not hasattr(report_export, "_detect_lang")


def test_pdf_labels_follow_the_given_language():
    text = _pdf_text(generate_pdf("## Body\nText.", _SPANISH_PROMPT, "easy", language="es"))
    assert "Informe de investigación" in text
    assert "Profundidad" in text


def test_docx_labels_follow_the_given_language():
    text = _docx_text(generate_docx("## Body\nText.", "An English prompt", "easy", language="ru"))
    assert "Исследовательский отчёт" in text
    assert "Глубина" in text


def test_unknown_language_gets_english_labels_and_lang_attribute():
    assert "Research Report" in _docx_text(generate_docx("Text.", "Q", language="zh"))
    assert generate_html("Text.", "Q", language="unknown").decode("utf-8").startswith(
        '<!DOCTYPE html><html lang="en">'
    )


def test_html_lang_attribute_is_the_stored_language():
    html = generate_html("Text.", "Q", language="es").decode("utf-8")
    assert html.startswith('<!DOCTYPE html><html lang="es">')
    assert 'lang="ru"' not in html


@pytest.mark.parametrize(
    "fmt, extract, expected",
    [
        ("pdf", _pdf_text, "Informe de investigación"),
        ("docx", _docx_text, "Informe de investigación"),
        ("html", lambda data: data.decode("utf-8"), '<html lang="es">'),
    ],
)
def test_service_exports_use_the_stored_research_language(fmt, extract, expected):
    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)
    record = _completed_research(store, _SPANISH_PROMPT, "es")

    data, _, _ = service.export_research_report(record.id, fmt)

    assert expected in extract(data)


def test_html_export_labels_are_spanish_for_a_spanish_research():
    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)
    record = _completed_research(store, _SPANISH_PROMPT, "es")

    html = service.export_research_report(record.id, "html")[0].decode("utf-8")

    assert "Informe de investigación" in html
    assert "min de lectura" in html

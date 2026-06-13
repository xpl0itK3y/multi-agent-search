from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.services.research_service import ResearchService
from src.ui.report_export import _markdown_to_html, generate_html


def test_markdown_to_html_basics():
    md = "## Heading\n\nSome **bold** text [S1].\n\n- one\n- two\n\nSee [link](https://a.com)."
    html = _markdown_to_html(md)
    assert "<h2>Heading</h2>" in html
    assert "<strong>bold</strong>" in html
    assert 'sup class="cite">[S1]</sup>' in html
    assert "<ul>" in html and "<li>one</li>" in html
    assert '<a href="https://a.com"' in html


def test_markdown_escapes_raw_html():
    html = _markdown_to_html("A <script>alert(1)</script> & co")
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_generate_html_includes_report_and_scorecard():
    out = generate_html(
        "## Report\nBody [S1].",
        "Compare X and Y",
        "hard",
        "2026-06-13T10:00:00",
        scorecard={
            "coverage_pct": 80, "integrity_pct": 90, "supported": 9, "total": 10,
            "sources": 12, "high_sources": 3, "has_redteam": True, "challenged": 2, "held": 4,
        },
    ).decode("utf-8")
    assert out.startswith("<!DOCTYPE html>")
    assert "Compare X and Y" in out
    assert "80%" in out and "90%" in out and ">12<" in out
    assert "<article>" in out and "Body" in out


def test_generate_html_without_scorecard_has_no_cards():
    out = generate_html("Body.", "Q").decode("utf-8")
    assert '<div class="cards">' not in out
    assert "Body." in out


def test_export_html_via_service():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    record = store.add_research(ResearchRequest(prompt="Compare X and Y", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(record.id, ResearchStatus.COMPLETED, "## Report\nBody [S1].")
    data, media, name = svc.export_research_report(record.id, "html")
    assert media.startswith("text/html")
    assert name.endswith(".html")
    assert b"<!DOCTYPE html>" in data and b"Compare X and Y" in data

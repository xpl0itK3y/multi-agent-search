from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.services.research_service import ResearchService
from src.ui.report_export import _markdown_to_html, generate_html


def test_markdown_to_html_basics():
    md = "## Heading\n\nSome **bold** text [S1].\n\n- one\n- two\n\nSee [link](https://a.com)."
    html, toc = _markdown_to_html(md)
    assert '<h2 id="heading">Heading</h2>' in html
    assert "<strong>bold</strong>" in html
    assert 'sup class="cite">[S1]</sup>' in html
    assert "<ul>" in html and "<li>one</li>" in html
    assert '<a href="https://a.com"' in html
    assert toc == [(2, "heading", "Heading")]  # outline drives the table of contents


def test_markdown_renders_pipe_tables():
    md = "Intro.\n\n| A | B |\n|---|---|\n| 1 | 2 |\n| 3 | 4 |"
    html, _ = _markdown_to_html(md)
    assert '<div class="table-wrap"><table>' in html
    assert "<th>A</th>" in html and "<td>1</td>" in html
    assert "<p>|" not in html  # not leaked as a literal-pipe paragraph


def test_markdown_escapes_raw_html():
    html, _ = _markdown_to_html("A <script>alert(1)</script> & co")
    assert "<script>alert" not in html
    assert "&lt;script&gt;" in html


def test_escaped_citations_normalized_and_no_katex_collision():
    # \[Sn\] (escaped citation the LLM sometimes emits) must render as a citation, not leak as
    # literal text or collide with KaTeX's \[…\] delimiter; inline \(…\) math is left for KaTeX.
    html, _ = _markdown_to_html("Утверждение \\[S15\\], формула \\(T_2\\) и обычная [S20].")
    assert html.count('class="cite"') == 2  # both \[S15\] and [S20] become citations
    assert "\\[S15\\]" not in html  # no raw escaped citation survives
    assert "\\(T_2\\)" in html  # inline math preserved (rendered by KaTeX at view time)


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


def test_html_theme_dark_uses_dark_vars():
    out = generate_html("Body.", "Q", theme="dark").decode("utf-8")
    assert "#0e0f17" in out  # web-app dark background
    assert "prefers-color-scheme" not in out  # explicit theme → no auto-switch


def test_html_auto_theme_has_dark_media():
    out = generate_html("Body.", "Q").decode("utf-8")  # default = auto
    assert "prefers-color-scheme:dark" in out


def test_html_custom_accent_applied_and_sanitized():
    ok = generate_html("Body.", "Q", theme="custom", accent="#ff0066", base="light").decode("utf-8")
    assert "--accent:#ff0066" in ok
    # a CSS-injection attempt via the accent param is rejected (not a hex color)
    bad = generate_html("Body.", "Q", theme="custom", accent="red;}body{display:none").decode("utf-8")
    assert "}body{display:none" not in bad  # the injection payload is not reflected into the CSS


def test_html_uses_web_app_fonts_and_palette():
    out = generate_html("Body.", "Q", theme="light").decode("utf-8")
    assert "#5b54e8" in out  # brand indigo accent — matches the web app, not the old coral
    assert "Lora" in out and "Inter" in out  # web-app serif + sans
    assert "fonts.googleapis.com" in out  # fonts loaded for fidelity


def test_export_html_via_service():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    record = store.add_research(ResearchRequest(prompt="Compare X and Y", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(record.id, ResearchStatus.COMPLETED, "## Report\nBody [S1].")
    data, media, name = svc.export_research_report(record.id, "html")
    assert media.startswith("text/html")
    assert name.endswith(".html")
    assert b"<!DOCTYPE html>" in data and b"Compare X and Y" in data


def test_export_md_via_service():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rec = store.add_research(ResearchRequest(prompt="Topic here", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(rec.id, ResearchStatus.COMPLETED, "## Report\nBody.")
    data, media, name = svc.export_research_report(rec.id, "md")
    assert media.startswith("text/markdown") and name.endswith(".md")
    assert data == b"## Report\nBody."


def test_export_json_via_service():
    import json as _json
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rec = store.add_research(ResearchRequest(prompt="Topic here", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(rec.id, ResearchStatus.COMPLETED, "## Report\nBody.")
    data, media, name = svc.export_research_report(rec.id, "json")
    assert media.startswith("application/json") and name.endswith(".json")
    obj = _json.loads(data)
    assert obj["prompt"] == "Topic here" and obj["report"] == "## Report\nBody."
    assert "verification" in obj and "sources" in obj

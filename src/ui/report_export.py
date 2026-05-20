"""
report_export.py — PDF and DOCX export for research reports.

Usage:
    pdf_bytes  = generate_pdf(report_markdown, prompt, depth, created_at)
    docx_bytes = generate_docx(report_markdown, prompt, depth, created_at)
"""
from __future__ import annotations

import io
import os
import re
from datetime import datetime
from typing import Optional

# ──────────────────────────────────────────────────────────────────────────────
# Language detection
# ──────────────────────────────────────────────────────────────────────────────

def _detect_lang(text: str) -> str:
    """Return 'ru' if text contains significant Cyrillic content, else 'en'."""
    if not text:
        return "en"
    cyrillic = sum(1 for c in text if "Ѐ" <= c <= "ӿ")
    return "ru" if cyrillic / len(text) > 0.15 else "en"


_EXPORT_PREAMBLE = re.compile(
    r"(?i)^(ниже\s+представлен|ниже\s+приведён|below\s+is\s+(the|a)\s+(synthesized|final|complete)|"
    r"the\s+following\s+is\s+(a|the)\s+(synthesized|final)|вот\s+синтезирован|"
    r"данный\s+отчёт\s+объединяет|синтезирую\s+все|"
    r"i\s+have\s+(combined|merged|synthesized)|here\s+is\s+(the|a)\s+(synthesized|final))[^\n]*\n+"
)
_EXPORT_NOTES_SECTION = re.compile(
    r"\n+##\s+(Примечания\s+к\s+отчёту|Report\s+Notes|Notas\s+del\s+informe)\s*\n.*?(?=\n## |\Z)",
    re.DOTALL | re.IGNORECASE,
)


def _clean_report_for_export(report: str) -> str:
    """Strip LLM meta-commentary preamble and quality notes section before export."""
    report = _EXPORT_PREAMBLE.sub("", report).strip()
    report = _EXPORT_NOTES_SECTION.sub("", report)
    return report


_LABELS: dict[str, dict[str, str]] = {
    "en": {
        "title":     "Research Report",
        "depth":     "Depth",
        "generated": "Generated",
        "page":      "Page",
    },
    "ru": {
        "title":     "Исследовательский отчёт",
        "depth":     "Глубина",
        "generated": "Создан",
        "page":      "Стр.",
    },
}

# ──────────────────────────────────────────────────────────────────────────────
# Font resolution (DejaVu → Unicode/Cyrillic; Helvetica → ASCII fallback)
# ──────────────────────────────────────────────────────────────────────────────

_DEJAVU_DIR = "/usr/share/fonts/truetype/dejavu"
_DEJAVU_REGULAR = "DejaVuSans.ttf"
_DEJAVU_BOLD    = "DejaVuSans-Bold.ttf"

_fonts_registered = False


def _register_pdf_fonts() -> dict[str, str]:
    """Register DejaVu TTF fonts if available; fall back to Helvetica."""
    global _fonts_registered
    try:
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.pdfmetrics import registerFontFamily
        from reportlab.pdfbase.ttfonts import TTFont

        r_path = os.path.join(_DEJAVU_DIR, _DEJAVU_REGULAR)
        b_path = os.path.join(_DEJAVU_DIR, _DEJAVU_BOLD)

        if os.path.isfile(r_path) and os.path.isfile(b_path):
            if not _fonts_registered:
                pdfmetrics.registerFont(TTFont("DV",   r_path))
                pdfmetrics.registerFont(TTFont("DV-B", b_path))
                # use regular as fallback for italic variants
                registerFontFamily("DV", normal="DV", bold="DV-B",
                                   italic="DV", boldItalic="DV-B")
                _fonts_registered = True
            return {"r": "DV", "b": "DV-B", "i": "DV", "bi": "DV-B"}
    except Exception:
        pass
    return {"r": "Helvetica", "b": "Helvetica-Bold",
            "i": "Helvetica-Oblique", "bi": "Helvetica-BoldOblique"}


# ──────────────────────────────────────────────────────────────────────────────
# Markdown parser
# ──────────────────────────────────────────────────────────────────────────────

_H2   = re.compile(r"^## (.+)$")
_H3   = re.compile(r"^### (.+)$")
_H4   = re.compile(r"^#### (.+)$")
_BULL = re.compile(r"^[-*] (.+)$")
_HR   = re.compile(r"^---+$")


def _parse_blocks(markdown: str) -> list[dict]:
    blocks: list[dict] = []
    lines = markdown.split("\n")
    i = 0
    while i < len(lines):
        raw = lines[i]
        stripped = raw.strip()

        if m := _H2.match(stripped):
            blocks.append({"type": "h2", "text": m.group(1)})
        elif m := _H3.match(stripped):
            blocks.append({"type": "h3", "text": m.group(1)})
        elif m := _H4.match(stripped):
            blocks.append({"type": "h4", "text": m.group(1)})
        elif _HR.match(stripped):
            blocks.append({"type": "hr"})
        elif m := _BULL.match(stripped):
            items: list[str] = []
            while i < len(lines):
                bm = _BULL.match(lines[i].strip())
                if not bm:
                    break
                items.append(bm.group(1))
                i += 1
            blocks.append({"type": "bullets", "items": items})
            continue
        elif stripped:
            parts: list[str] = []
            while i < len(lines):
                ln = lines[i].strip()
                if (not ln or _H2.match(ln) or _H3.match(ln)
                        or _H4.match(ln) or _HR.match(ln) or _BULL.match(ln)):
                    break
                parts.append(lines[i])
                i += 1
            text = " ".join(p.strip() for p in parts if p.strip())
            if text:
                blocks.append({"type": "para", "text": text})
            continue
        i += 1
    return blocks


# ──────────────────────────────────────────────────────────────────────────────
# PDF  (reportlab)
# ──────────────────────────────────────────────────────────────────────────────

def generate_pdf(
    report: str,
    prompt: str,
    depth: str = "",
    created_at: Optional[str] = None,
) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import (
        HRFlowable,
        ListFlowable,
        ListItem,
        Paragraph,
        SimpleDocTemplate,
        Spacer,
        Table,
        TableStyle,
    )

    report = _clean_report_for_export(report)
    lang  = _detect_lang(prompt)
    lbl   = _LABELS[lang]
    fonts = _register_pdf_fonts()
    F     = fonts["r"]
    FB    = fonts["b"]
    FI    = fonts["i"]
    FBI   = fonts["bi"]

    # ── palette ───────────────────────────────────────────────────────────────
    C_DARK    = colors.HexColor("#1e293b")
    C_BLUE    = colors.HexColor("#1d4ed8")
    C_BLUE_LT = colors.HexColor("#3b82f6")
    C_SLATE   = colors.HexColor("#334155")
    C_GRAY    = colors.HexColor("#64748b")
    C_LIGHT   = colors.HexColor("#f1f5f9")
    C_WHITE   = colors.white

    def S(name, **kw) -> ParagraphStyle:
        return ParagraphStyle(name, **kw)

    STYLES = {
        "h2":     S("h2",     fontName=FB,  fontSize=15,
                    textColor=C_BLUE,  leading=20, spaceAfter=4,  spaceBefore=14),
        "h3":     S("h3",     fontName=FB,  fontSize=12,
                    textColor=C_SLATE, leading=16, spaceAfter=3,  spaceBefore=10),
        "h4":     S("h4",     fontName=FBI, fontSize=10.5,
                    textColor=C_GRAY,  leading=14, spaceAfter=2,  spaceBefore=8),
        "para":   S("para",   fontName=F,   fontSize=10, leading=15,
                    textColor=C_DARK,  alignment=TA_JUSTIFY, spaceAfter=6),
        "bullet": S("bullet", fontName=F,   fontSize=10, leading=14,
                    textColor=C_DARK,  leftIndent=8, spaceAfter=2),
        "meta":   S("meta",   fontName=FI,  fontSize=9,
                    textColor=C_GRAY,  alignment=TA_LEFT),
        "title":  S("title",  fontName=FB,  fontSize=20,
                    textColor=C_WHITE, leading=26, alignment=TA_LEFT),
        "sub":    S("sub",    fontName=F,   fontSize=11,
                    textColor=C_WHITE, leading=15, alignment=TA_LEFT),
    }

    def _md_to_rl(text: str) -> str:
        text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        # unescape markdown bracket escapes written by _source_line()
        text = text.replace("\\[", "[").replace("\\]", "]")
        # markdown links [label](url) → clickable PDF link
        text = re.sub(
            r"\[([^\]]+)\]\((https?://[^\s)]+)\)",
            r'<a href="\2" color="#1d4ed8">\1</a>',
            text,
        )
        text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
        text = re.sub(
            r"\[S(\d+)\]",
            r'<font color="#1d4ed8" size="8"><super>[S\1]</super></font>',
            text,
        )
        return text

    # ── date string ───────────────────────────────────────────────────────────
    date_str = ""
    if created_at:
        try:
            dt = datetime.fromisoformat(created_at)
            date_str = dt.strftime("%d %b %Y")
        except Exception:
            date_str = created_at[:10]

    # ── header / footer ───────────────────────────────────────────────────────
    def _on_page(canvas, doc):
        canvas.saveState()
        W, H = A4
        canvas.setFillColor(C_BLUE)
        canvas.rect(0, H - 1.8*cm, W, 1.8*cm, fill=1, stroke=0)
        canvas.setFillColor(C_WHITE)
        canvas.setFont(FB, 9)
        canvas.drawString(2.5*cm, H - 1.15*cm, lbl["title"].upper())
        canvas.setFont(F, 8)
        prompt_short = (prompt[:80] + "…") if len(prompt) > 80 else prompt
        canvas.drawString(2.5*cm, H - 1.55*cm, prompt_short)
        if date_str:
            canvas.drawRightString(W - 2.5*cm, H - 1.35*cm, date_str)
        canvas.setFillColor(C_LIGHT)
        canvas.rect(0, 0, W, 1.1*cm, fill=1, stroke=0)
        canvas.setFillColor(C_GRAY)
        canvas.setFont(F, 8)
        canvas.drawCentredString(W / 2, 0.4*cm, f"{lbl['page']} {doc.page}")
        if depth:
            canvas.drawString(2.5*cm, 0.4*cm, f"{lbl['depth']}: {depth.upper()}")
        canvas.restoreState()

    # ── build story ───────────────────────────────────────────────────────────
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        topMargin=2.4*cm,
        bottomMargin=1.8*cm,
        leftMargin=2.5*cm,
        rightMargin=2.5*cm,
        onFirstPage=_on_page,
        onLaterPages=_on_page,
    )

    story = []

    # ── title block ───────────────────────────────────────────────────────────
    sub_text = prompt if len(prompt) <= 120 else prompt[:117] + "…"
    title_tbl = Table(
        [
            [Paragraph(lbl["title"], STYLES["title"])],
            [Paragraph(sub_text,     STYLES["sub"])],
        ],
        colWidths=[doc.width],
    )
    title_tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0, 0), (-1, -1), C_BLUE),
        ("LEFTPADDING",  (0, 0), (-1, -1), 14),
        ("RIGHTPADDING", (0, 0), (-1, -1), 14),
        ("TOPPADDING",   (0, 0), (0, 0),   14),
        ("BOTTOMPADDING",(0,-1), (-1,-1),  14),
        ("ROWBACKGROUNDS",(0,0), (-1,-1),  [C_BLUE]),
    ]))
    story.append(title_tbl)

    # meta row
    meta_parts = []
    if depth:
        meta_parts.append(f"{lbl['depth']}: <b>{depth.upper()}</b>")
    if date_str:
        meta_parts.append(f"{lbl['generated']}: <b>{date_str}</b>")
    if meta_parts:
        story.append(Spacer(1, 6))
        story.append(Paragraph(" &nbsp;·&nbsp; ".join(meta_parts), STYLES["meta"]))

    story.append(Spacer(1, 10))
    story.append(HRFlowable(width="100%", thickness=1, color=C_LIGHT))
    story.append(Spacer(1, 8))

    # ── content ───────────────────────────────────────────────────────────────
    for block in _parse_blocks(report):
        btype = block["type"]

        if btype == "h2":
            if block["text"].lower() in ("sources", "источники"):
                story.append(Spacer(1, 6))
                story.append(HRFlowable(width="100%", thickness=0.5, color=C_LIGHT))
            story.append(Paragraph(_md_to_rl(block["text"]), STYLES["h2"]))

        elif btype == "h3":
            story.append(Paragraph(_md_to_rl(block["text"]), STYLES["h3"]))

        elif btype == "h4":
            story.append(Paragraph(_md_to_rl(block["text"]), STYLES["h4"]))

        elif btype == "hr":
            story.append(Spacer(1, 4))
            story.append(HRFlowable(width="100%", thickness=0.5, color=C_LIGHT))
            story.append(Spacer(1, 4))

        elif btype == "para":
            story.append(Paragraph(_md_to_rl(block["text"]), STYLES["para"]))

        elif btype == "bullets":
            items = [
                ListItem(
                    Paragraph(_md_to_rl(item), STYLES["bullet"]),
                    leftIndent=16,
                    bulletColor=C_BLUE_LT,
                )
                for item in block["items"]
            ]
            story.append(ListFlowable(items, bulletType="bullet",
                                      leftIndent=12, spaceBefore=2, spaceAfter=4))

    doc.build(story)
    return buf.getvalue()


# ──────────────────────────────────────────────────────────────────────────────
# DOCX  (python-docx)
# ──────────────────────────────────────────────────────────────────────────────

def generate_docx(
    report: str,
    prompt: str,
    depth: str = "",
    created_at: Optional[str] = None,
) -> bytes:
    from docx import Document
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    from docx.shared import Cm, Pt, RGBColor

    report = _clean_report_for_export(report)
    lang = _detect_lang(prompt)
    lbl  = _LABELS[lang]

    def hex_rgb(hex_str: str) -> RGBColor:
        h = hex_str.lstrip("#")
        return RGBColor(int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))

    C_BLUE  = hex_rgb("1d4ed8")
    C_DARK  = hex_rgb("1e293b")
    C_SLATE = hex_rgb("334155")
    C_GRAY  = hex_rgb("64748b")

    doc = Document()

    for section in doc.sections:
        section.top_margin    = Cm(2.5)
        section.bottom_margin = Cm(2.5)
        section.left_margin   = Cm(3.0)
        section.right_margin  = Cm(2.5)

    # ── title block ───────────────────────────────────────────────────────────
    title_para = doc.add_paragraph()
    title_para.alignment = WD_ALIGN_PARAGRAPH.LEFT
    run = title_para.add_run(lbl["title"])
    run.font.size = Pt(26)
    run.font.bold = True
    run.font.color.rgb = C_BLUE
    title_para.paragraph_format.space_after = Pt(4)

    sub = doc.add_paragraph()
    sub.alignment = WD_ALIGN_PARAGRAPH.LEFT
    sub_run = sub.add_run(prompt if len(prompt) <= 140 else prompt[:137] + "…")
    sub_run.font.size = Pt(11)
    sub_run.font.color.rgb = C_SLATE
    sub_run.font.italic = True
    sub.paragraph_format.space_after = Pt(4)

    # meta line
    date_str = ""
    if created_at:
        try:
            dt = datetime.fromisoformat(created_at)
            date_str = dt.strftime("%d %b %Y")
        except Exception:
            date_str = created_at[:10]

    meta_parts = []
    if depth:
        meta_parts.append(f"{lbl['depth']}: {depth.upper()}")
    if date_str:
        meta_parts.append(f"{lbl['generated']}: {date_str}")
    if meta_parts:
        meta_p = doc.add_paragraph(" · ".join(meta_parts))
        meta_p.runs[0].font.size = Pt(9)
        meta_p.runs[0].font.color.rgb = C_GRAY
        meta_p.paragraph_format.space_after = Pt(10)

    def _add_divider():
        p = doc.add_paragraph()
        pPr = p._p.get_or_add_pPr()
        pBdr = OxmlElement("w:pBdr")
        bottom = OxmlElement("w:bottom")
        bottom.set(qn("w:val"), "single")
        bottom.set(qn("w:sz"), "4")
        bottom.set(qn("w:space"), "1")
        bottom.set(qn("w:color"), "CBD5E1")
        pBdr.append(bottom)
        pPr.append(pBdr)
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after = Pt(6)

    _add_divider()

    # ── inline markdown helper ────────────────────────────────────────────────
    def _add_hyperlink(paragraph, url: str, label: str):
        """Add a clickable hyperlink run to paragraph."""
        r_id = paragraph.part.relate_to(
            url,
            "http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink",
            is_external=True,
        )
        hl = OxmlElement("w:hyperlink")
        hl.set(qn("r:id"), r_id)
        new_run = OxmlElement("w:r")
        rPr = OxmlElement("w:rPr")
        rStyle = OxmlElement("w:rStyle")
        rStyle.set(qn("w:val"), "Hyperlink")
        rPr.append(rStyle)
        new_run.append(rPr)
        t = OxmlElement("w:t")
        t.text = label
        new_run.append(t)
        hl.append(new_run)
        paragraph._p.append(hl)

    def _add_inline(paragraph, text: str):
        # unescape markdown bracket escapes from _source_line()
        text = text.replace("\\[", "[").replace("\\]", "]")
        # split on bold, citations, and markdown links
        _INLINE_RE = re.compile(r"(\*\*[^*]+\*\*|\[S\d+\]|\[[^\]]+\]\(https?://[^\s)]+\))")
        for part in _INLINE_RE.split(text):
            if part.startswith("**") and part.endswith("**"):
                run = paragraph.add_run(part[2:-2])
                run.bold = True
            elif re.match(r"\[S\d+\]", part):
                run = paragraph.add_run(part)
                run.font.color.rgb = C_BLUE
                run.font.size = Pt(8)
                rPr = run._r.get_or_add_rPr()
                vertAlign = OxmlElement("w:vertAlign")
                vertAlign.set(qn("w:val"), "superscript")
                rPr.append(vertAlign)
            elif m := re.match(r"\[([^\]]+)\]\((https?://[^\s)]+)\)", part):
                _add_hyperlink(paragraph, m.group(2), m.group(1))
            elif part:
                run = paragraph.add_run(part)
                run.font.color.rgb = C_DARK

    # ── content ───────────────────────────────────────────────────────────────
    for block in _parse_blocks(report):
        btype = block["type"]

        if btype == "h2":
            p = doc.add_heading("", level=1)
            p.clear()
            run = p.add_run(block["text"])
            run.font.bold = True
            run.font.size = Pt(15)
            run.font.color.rgb = C_BLUE
            p.paragraph_format.space_before = Pt(14)
            p.paragraph_format.space_after = Pt(4)

        elif btype == "h3":
            p = doc.add_heading("", level=2)
            p.clear()
            run = p.add_run(block["text"])
            run.font.bold = True
            run.font.size = Pt(12)
            run.font.color.rgb = C_SLATE
            p.paragraph_format.space_before = Pt(10)
            p.paragraph_format.space_after = Pt(3)

        elif btype == "h4":
            p = doc.add_heading("", level=3)
            p.clear()
            run = p.add_run(block["text"])
            run.font.bold = True
            run.font.italic = True
            run.font.size = Pt(11)
            run.font.color.rgb = C_GRAY
            p.paragraph_format.space_before = Pt(8)

        elif btype == "hr":
            _add_divider()

        elif btype == "para":
            p = doc.add_paragraph()
            p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
            p.paragraph_format.space_after = Pt(6)
            _add_inline(p, block["text"])

        elif btype == "bullets":
            for item in block["items"]:
                p = doc.add_paragraph(style="List Bullet")
                p.paragraph_format.left_indent = Cm(0.5)
                p.paragraph_format.space_after = Pt(2)
                _add_inline(p, item)

    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()

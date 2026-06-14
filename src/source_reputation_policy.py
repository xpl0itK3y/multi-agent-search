"""Curated, transparent domain-reputation list for the source-credibility flag.

This is a *starter* set of widely-documented examples, grouped by WHY a domain is flagged —
not a verdict on any individual article, and deliberately not exhaustive. It is meant to be
extended from a published media-reliability dataset. Categories:

  satire       — satirical publication; content is not intended as fact
  fabricated   — repeatedly published fabricated / hoax / fake-news content
  conspiracy   — conspiracy theories, pseudoscience, or health misinformation
  state_media  — state-controlled outlet; weigh for bias rather than treat as false

Keys are *registered* domains (sub-domains collapse onto these). Surface the category and
reason to the user, not a bare "bad" — and let primary/expert sources speak through the
existing source-critic hierarchy.
"""

REPUTATION_FLAGS: dict[str, tuple[str, str]] = {
    # ── satire (not factual) ────────────────────────────────────────────────────
    "theonion.com": ("satire", "Satirical publication — content is not factual"),
    "babylonbee.com": ("satire", "Satirical publication — content is not factual"),
    "clickhole.com": ("satire", "Satirical publication — content is not factual"),
    "thedailymash.co.uk": ("satire", "Satirical publication — content is not factual"),
    "waterfordwhispersnews.com": ("satire", "Satirical publication — content is not factual"),
    "thebeaverton.com": ("satire", "Satirical publication — content is not factual"),

    # ── fabricated / fake-news / hoax ───────────────────────────────────────────
    "infowars.com": ("conspiracy", "Repeatedly published fabricated and conspiracy claims"),
    "beforeitsnews.com": ("fabricated", "User-generated site known for fabricated stories"),
    "worldnewsdailyreport.com": ("fabricated", "Fabricated hoax-news site"),
    "newspunch.com": ("fabricated", "Documented for repeatedly publishing false stories"),
    "yournewswire.com": ("fabricated", "Documented for repeatedly publishing false stories"),
    "nationalreport.net": ("fabricated", "Fabricated hoax-news site"),
    "empirenews.net": ("fabricated", "Fabricated hoax-news site"),
    "now8news.com": ("fabricated", "Fabricated hoax-news site"),
    "react365.com": ("fabricated", "Prank/fake-story generator — not a real outlet"),

    # ── conspiracy / pseudoscience / health misinformation ──────────────────────
    "naturalnews.com": ("conspiracy", "Pseudoscience and health misinformation"),
    "mercola.com": ("conspiracy", "Documented health misinformation"),
    "collective-evolution.com": ("conspiracy", "Pseudoscience and conspiracy content"),

    # ── state-controlled media (weigh for bias) ─────────────────────────────────
    "rt.com": ("state_media", "State-controlled outlet — weigh for bias"),
    "sputniknews.com": ("state_media", "State-controlled outlet — weigh for bias"),
    "sputnikglobe.com": ("state_media", "State-controlled outlet — weigh for bias"),
    "tass.com": ("state_media", "State news agency — weigh for bias"),
    "tass.ru": ("state_media", "State news agency — weigh for bias"),
    "presstv.ir": ("state_media", "State-controlled outlet — weigh for bias"),
    "globaltimes.cn": ("state_media", "State-controlled outlet — weigh for bias"),
    "cgtn.com": ("state_media", "State-controlled outlet — weigh for bias"),
}

CATEGORY_LABELS: dict[str, str] = {
    "satire": "Satire",
    "fabricated": "Fabricated / fake-news",
    "conspiracy": "Conspiracy / pseudoscience",
    "state_media": "State-controlled media",
}

"""Single source of truth for the shared domain-reputation lists (AUD-032, AUD-006).

The trusted lists were byte-identical across the search agent, the analyzer agent, and the Rust
acceleration bridge. The low-value lists had drifted four ways — notably the Rust *search* config
was missing 13 entries (youtube/passport/www-variants), so search scoring never down-weighted
them despite search.py declaring it should. They are unified here.

The union was validated against the eval harness deterministically (AUD-006): of the 32 distinct
domains the gold-quality fixture reports actually cite, ZERO are newly down-weighted by the union
— every added domain is social/video/shopping (youtube, tiktok, vk, medium, behance, …) that no
quality report relied on. So the change is conservative (it makes the existing low-value policy
consistent across stages), not a blind merge.
"""

TRUSTED_DOMAIN_EXACT_MATCHES = frozenset(
    {
        "developer.mozilla.org",
        "docs.python.org",
        "openai.com",
        "platform.openai.com",
        "wikipedia.org",
    }
)

TRUSTED_DOMAIN_SUFFIXES = (
    ".gov",
    ".edu",
    ".readthedocs.io",
)

# Low-value / low-signal domains: down-weighted (not excluded) during source scoring. Union of
# what the search + analysis stages each blocked, after the AUD-006 eval-harness check above.
# www-variants are kept so the list matches whether or not a given call site strips "www.".
LOW_VALUE_DOMAIN_EXACT_MATCHES = frozenset(
    {
        "linkedin.com", "www.linkedin.com",
        "pinterest.com", "www.pinterest.com",
        "facebook.com", "www.facebook.com",
        "x.com", "twitter.com",
        "tiktok.com", "www.tiktok.com",
        "vk.com", "www.vk.com",
        "medium.com", "www.medium.com",
        "behance.net", "www.behance.net",
        "youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be",
        "passport.yandex.ru",
        "eventify.io",
    }
)

LOW_VALUE_DOMAIN_SUBSTRINGS = (
    "bookmark",
    "newsnviews",
    "techandgadgetreviews",
    "techspymagazine",
    "trendhunter",
    "grokipedia",
    "outmaxshop",
)

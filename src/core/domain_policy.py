"""Single source of truth for the shared domain-reputation lists (AUD-032).

Only the *trusted* lists live here — they were byte-identical across the search agent, the
analyzer agent, and the Rust acceleration bridge, so duplicating them risked silent drift.

The low-value / low-signal lists have intentionally diverged between the search stage and the
analysis stage and are deliberately left in their respective agents: unifying them changes
which sources are filtered and should be validated against the eval harness, not merged blind.
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

"""AUD-006: the low-value domain lists are unified across all four scoring sites (analyzer,
search, and both rust_accel configs), and the union is conservative — it down-weights no domain
that a gold-quality fixture report actually cited. This encodes the deterministic eval-harness
check as a permanent regression guard."""
import glob
import json

from src.agents.analyzer import AnalyzerAgent
from src.agents.search import SearchAgent
from src.core import domain_policy, rust_accel


def test_low_value_lists_unified_across_all_stages():
    exact = set(domain_policy.LOW_VALUE_DOMAIN_EXACT_MATCHES)
    subs = set(domain_policy.LOW_VALUE_DOMAIN_SUBSTRINGS)
    assert set(AnalyzerAgent.LOW_VALUE_DOMAIN_EXACT_MATCHES) == exact
    assert set(SearchAgent.LOW_VALUE_DOMAIN_EXACT_MATCHES) == exact
    assert set(rust_accel._search_config()["low_value_domain_exact_matches"]) == exact
    assert set(rust_accel._analyzer_config()["low_value_domain_exact_matches"]) == exact
    assert set(AnalyzerAgent.LOW_VALUE_DOMAIN_SUBSTRINGS) == subs
    assert set(SearchAgent.LOW_VALUE_DOMAIN_SUBSTRINGS) == subs
    assert set(rust_accel._search_config()["low_value_domain_substrings"]) == subs
    assert set(rust_accel._analyzer_config()["low_value_domain_substrings"]) == subs


def test_unification_down_weights_no_gold_cited_domain():
    exact = set(domain_policy.LOW_VALUE_DOMAIN_EXACT_MATCHES)
    subs = domain_policy.LOW_VALUE_DOMAIN_SUBSTRINGS
    gold = set()
    for path in glob.glob("eval/fixtures/*.json"):
        for src in json.load(open(path)).get("sources", []):
            dom = (src.get("domain") or "").lower().removeprefix("www.")
            if dom:
                gold.add(dom)
    assert gold, "no gold fixtures found — guard would be vacuous"

    def blocked(dom: str) -> bool:
        return dom in exact or f"www.{dom}" in exact or any(t in dom for t in subs)

    offenders = sorted(d for d in gold if blocked(d))
    assert offenders == [], f"unification would down-weight gold-cited domains: {offenders}"


def test_drift_fix_youtube_now_blocked_in_search_config():
    # youtube/passport/www-variants were missing from the rust *search* config before unification
    assert "youtube.com" in domain_policy.LOW_VALUE_DOMAIN_EXACT_MATCHES
    assert "youtube.com" in rust_accel._search_config()["low_value_domain_exact_matches"]
    assert "passport.yandex.ru" in rust_accel._search_config()["low_value_domain_exact_matches"]


def test_candidate_token_lists_are_one_copy_across_search_and_rust_config():
    # Second AUD-006 pass: the rust search config had lost six shopping URL tokens.
    config = rust_accel._search_config()
    for name in ("LOW_SIGNAL_TITLE_TOKENS", "LOW_SIGNAL_URL_TOKENS", "LOW_SIGNAL_RESULT_TOKENS", "STRONG_RESULT_TOKENS"):
        shared = getattr(domain_policy, name)
        assert getattr(SearchAgent, name) is shared, name
        assert config[name.lower()] == list(shared), name
    assert "aliexpress." in config["low_signal_url_tokens"]


def test_candidate_url_tokens_block_no_gold_cited_url():
    urls = set()
    for path in glob.glob("eval/fixtures/*.json"):
        for src in json.load(open(path)).get("sources", []):
            if src.get("url"):
                urls.add(src["url"].lower())
    assert urls, "no gold fixture URLs found — guard would be vacuous"

    offenders = sorted(u for u in urls if any(t in u for t in domain_policy.LOW_SIGNAL_URL_TOKENS) or "/wall-" in u)
    assert offenders == [], f"URL tokens would drop gold-cited sources: {offenders}"

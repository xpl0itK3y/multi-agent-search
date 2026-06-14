from src.agents.source_reputation import SourceReputationAgent


def test_flags_known_low_credibility_and_state_media():
    sources = {
        "S1": {"url": "https://www.reuters.com/world/x", "domain": "reuters.com"},
        "S2": {"url": "https://www.theonion.com/story", "domain": "theonion.com"},
        "S3": {"url": "https://edition.rt.com/news", "domain": "edition.rt.com"},  # subdomain → rt.com
        "S4": {"url": "https://naturalnews.com/health", "domain": "naturalnews.com"},
    }
    r = SourceReputationAgent().assess(sources)
    assert r.total_sources == 4
    assert r.flagged_count == 3
    by_id = {f.source_id: f for f in r.flagged}
    assert "S1" not in by_id  # reputable outlet not flagged
    assert by_id["S2"].category == "satire"
    assert by_id["S3"].category == "state_media" and by_id["S3"].domain == "edition.rt.com"
    assert by_id["S4"].category == "conspiracy"
    assert set(r.categories) == {"satire", "state_media", "conspiracy"}


def test_domain_derived_from_url_when_missing():
    r = SourceReputationAgent().assess({"S1": {"url": "https://worldnewsdailyreport.com/x"}})
    assert r.flagged_count == 1 and r.flagged[0].category == "fabricated"


def test_clean_source_set_has_no_flags():
    sources = {
        "S1": {"domain": "nature.com"},
        "S2": {"domain": "arxiv.org"},
        "S3": {"domain": "github.com"},
    }
    r = SourceReputationAgent().assess(sources)
    assert r.flagged == [] and r.flagged_count == 0 and r.categories == []


def test_empty_input_is_safe():
    r = SourceReputationAgent().assess({})
    assert r.total_sources == 0 and r.flagged_count == 0

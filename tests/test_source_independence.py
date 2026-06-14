from src.agents.source_independence import SourceIndependenceAgent


def _wire_story(outlet: str) -> dict:
    # Same reprinted text body across different domains (a syndication / wire story).
    return {
        "url": f"https://{outlet}/article",
        "title": "Central bank holds interest rates steady amid inflation concerns",
        "content": (
            "The central bank held its benchmark interest rate steady on Wednesday, citing "
            "persistent inflation concerns and uncertain global growth, according to officials."
        ),
    }


def test_syndicated_reprints_collapse_to_one_origin():
    sources = {
        "S1": _wire_story("nytimes.com"),
        "S2": _wire_story("reuters-reprint.com"),
        "S3": _wire_story("apnews-reprint.com"),
        "S4": {
            "url": "https://independent-analysis.org/x",
            "title": "Why the rate decision matters for mortgages",
            "content": "Homeowners with variable mortgages will see little change after the unrelated housing report.",
        },
    }
    result = SourceIndependenceAgent().analyze(sources)
    assert result.total_sources == 4
    # 3 reprints fold into one origin → 2 independent origins of 4 sources.
    assert result.independent_origins == 2
    assert result.independence_score == 0.5
    assert result.dominant_origin_share == 0.75
    assert any(c.kind == "syndicated" and c.size == 3 for c in result.clusters)
    assert result.echo_warnings  # surfaces the echo chamber


def test_same_outlet_multiple_articles_not_independent():
    sources = {
        "S1": {"url": "https://www.cnn.com/a", "title": "Story A about elections", "content": "Coverage of the election results in detail one."},
        "S2": {"url": "https://edition.cnn.com/b", "title": "Story B about markets", "content": "Totally separate market analysis numbers two."},
        "S3": {"url": "https://bbc.co.uk/c", "title": "Different outlet", "content": "An unrelated independent report from another newsroom three."},
    }
    result = SourceIndependenceAgent().analyze(sources)
    # cnn.com and edition.cnn.com collapse to one outlet → 2 origins.
    assert result.independent_origins == 2
    cluster = next(c for c in result.clusters if c.kind == "single-domain")
    assert cluster.size == 2 and "cnn.com" in cluster.label


def test_all_independent_scores_one():
    sources = {
        "S1": {"url": "https://a.com/1", "title": "Alpha topic", "content": "Distinct content about alpha subjects entirely."},
        "S2": {"url": "https://b.org/2", "title": "Beta topic", "content": "Different writing covering beta matters separately."},
        "S3": {"url": "https://c.net/3", "title": "Gamma topic", "content": "Yet another unique discussion of gamma things."},
    }
    result = SourceIndependenceAgent().analyze(sources)
    assert result.independent_origins == 3
    assert result.independence_score == 1.0
    assert result.clusters == []
    assert result.echo_warnings == []


def test_edge_cases():
    assert SourceIndependenceAgent().analyze({}).total_sources == 0
    one = SourceIndependenceAgent().analyze({"S1": {"url": "https://a.com", "title": "x", "content": "y"}})
    assert one.independent_origins == 1 and one.independence_score == 1.0

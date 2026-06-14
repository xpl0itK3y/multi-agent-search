from src.agents.numeric_check import NumericCheckAgent


def _agent():
    return NumericCheckAgent()


def test_figure_matched_in_source():
    report = "Solar capacity grew 40% according to the report [S1]."
    sources = {"S1": {"content": "Officials said solar capacity grew about 40 percent last period.", "url": "a"}}
    r = _agent().check(report, sources)
    assert r.total == 1 and r.supported == 1
    assert r.integrity == 1.0 and r.unsupported == []


def test_mangled_statistic_flagged():
    report = "Solar capacity grew 40% sharply this period [S1]."
    sources = {"S1": {"content": "Officials said solar capacity grew about 14 percent last period."}}
    r = _agent().check(report, sources)
    assert r.total == 1 and r.supported == 0
    assert r.unsupported and r.unsupported[0].value.replace(" ", "") == "40%"


def test_money_scale_normalization_matches():
    report = "Annual revenue reached $2.3 billion overall [S1]."
    sources = {"S1": {"content": "The company reported revenue of 2,300 million dollars overall."}}
    r = _agent().check(report, sources)
    assert r.total == 1 and r.supported == 1  # $2.3B == 2,300 million


def test_year_must_match_exactly():
    report = "The company was founded in 1998 historically [S1]."
    sources = {"S1": {"content": "The firm was founded in 1989 in the region."}}
    r = _agent().check(report, sources)
    assert r.total == 1 and r.supported == 0  # 1998 != 1989


def test_percent_rounding_tolerated():
    report = "Adoption reached 40% overall [S1]."
    sources = {"S1": {"content": "Adoption reached 40.3 percent overall last year."}}
    r = _agent().check(report, sources)
    assert r.supported == 1  # within ±1 point


def test_small_bare_integers_skipped():
    report = "There are 3 key drivers and 5 main risks here [S1]."
    sources = {"S1": {"content": "irrelevant text"}}
    r = _agent().check(report, sources)
    assert r.total == 0  # 3 and 5 are below the significance floor


def test_internal_contradiction_detected():
    report = (
        "Total revenue was $2.3 billion overall [S1]. "
        "Total revenue was $3.2 billion overall [S2]."
    )
    sources = {
        "S1": {"content": "revenue of $2.3 billion"},
        "S2": {"content": "revenue of $3.2 billion"},
    }
    r = _agent().check(report, sources)
    assert r.contradictions
    vals = {v.replace(" ", "") for v in r.contradictions[0].values}
    assert "$2.3billion" in vals and "$3.2billion" in vals


def test_uncited_figure_not_counted():
    r = _agent().check("Growth was 40% this year.", {})
    assert r.total == 0 and r.unsupported == []


def test_source_unavailable_not_penalized():
    report = "Growth was 40% sharply this year [S1]."
    r = _agent().check(report, {"S1": {"content": ""}})
    assert r.total == 0  # can't verify against an empty source → not flagged


def test_empty_inputs_safe():
    assert _agent().check("", {}).total == 0
    assert _agent().check("No numbers worth checking here at all [S1].", {"S1": {"content": "x"}}).total == 0

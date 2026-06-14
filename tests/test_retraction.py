from src.agents.retraction import RetractionAgent

# Mirrors the real Crossref `message` shape for the retracted Wakefield 1998 paper.
_RETRACTED_MSG = {
    "title": ["RETRACTED: Ileal-lymphoid-nodular hyperplasia, non-specific colitis…"],
    "updated-by": [
        {"DOI": "10.1016/s0140-6736(04)15715-2", "type": "correction", "label": "Correction", "source": "retraction-watch"},
        {"DOI": "10.1016/s0140-6736(10)60175-4", "type": "retraction", "label": "Retraction", "source": "retraction-watch"},
    ],
}
_CONCERN_MSG = {"title": ["Some paper"], "updated-by": [{"type": "expression_of_concern", "label": "Expression of concern"}]}
_CLEAN_MSG = {"title": ["A perfectly fine paper"], "updated-by": []}


def _fetch_factory(by_doi: dict):
    return lambda doi: by_doi.get(doi)


def test_flags_retracted_doi_from_url():
    sources = {
        "S1": {"url": "https://www.thelancet.com/journals/lancet/article/PIIS0140-6736(97)11096-0/fulltext "
                      "doi:10.1016/S0140-6736(97)11096-0", "content": "vaccines and autism"},
        "S2": {"url": "https://example.org/ok", "content": "see doi 10.1234/clean.2020 for details"},
    }
    fetch = _fetch_factory({
        "10.1016/s0140-6736(97)11096-0": _RETRACTED_MSG,
        "10.1234/clean.2020": _CLEAN_MSG,
    })
    r = RetractionAgent().check(sources, fetch)
    assert r.checked_dois == 2 and r.retracted_count == 1
    assert len(r.flagged) == 1
    f = r.flagged[0]
    assert f.source_id == "S1" and f.kind == "retraction" and f.doi == "10.1016/s0140-6736(97)11096-0"


def test_expression_of_concern_is_softer_flag():
    sources = {"S1": {"content": "10.1111/concern.1"}}
    r = RetractionAgent().check(sources, _fetch_factory({"10.1111/concern.1": _CONCERN_MSG}))
    assert r.retracted_count == 0
    assert r.flagged and r.flagged[0].kind == "concern"


def test_clean_dois_produce_no_flags_but_are_counted():
    sources = {"S1": {"content": "10.1234/clean.2020"}}
    r = RetractionAgent().check(sources, _fetch_factory({"10.1234/clean.2020": _CLEAN_MSG}))
    assert r.checked_dois == 1 and r.flagged == []


def test_network_failure_degrades_gracefully():
    sources = {"S1": {"content": "backed by 10.1016/S0140-6736(97)11096-0"}}
    r = RetractionAgent().check(sources, lambda doi: None)  # fetch always fails/returns None
    assert r.checked_dois == 1 and r.flagged == []  # never raises, no false flags


def test_no_dois_means_no_lookups():
    fetch_calls = []
    r = RetractionAgent().check({"S1": {"url": "https://news.example/x", "content": "no identifiers here"}},
                                lambda doi: fetch_calls.append(doi))
    assert r.checked_dois == 0 and not fetch_calls and r.flagged == []


def test_doi_extraction_trims_trailing_punctuation():
    dois = RetractionAgent()._extract_dois("see (10.1016/j.cell.2020.01.001). also 10.1000/xyz, end")
    assert "10.1016/j.cell.2020.01.001" in dois
    assert "10.1000/xyz" in dois

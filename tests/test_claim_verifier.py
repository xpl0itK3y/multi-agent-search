"""AUD-037: coverage for ClaimVerifierAgent.verify_and_downgrade (trust pipeline)."""
from src.agents.claim_verifier import ClaimVerifierAgent


def _agent():
    return ClaimVerifierAgent()


def test_softens_flagged_unsupported_line_en():
    report = "The market will double next year."
    out, summary = _agent().verify_and_downgrade(
        report, "en", uncited_lines=[], unsupported_lines=[report]
    )
    assert out.startswith("Based on the available sources, ")
    assert summary.unsupported_lines == 1
    assert summary.downgraded_lines == 1


def test_softens_with_russian_prefix():
    line = "Рынок вырастет вдвое."
    out, _ = _agent().verify_and_downgrade(line, "ru", uncited_lines=[], unsupported_lines=[line])
    assert out.startswith("По имеющимся данным, ")


def test_respects_max_softened_cap():
    lines = [f"Claim number {i} is huge." for i in range(6)]
    out, _ = _agent().verify_and_downgrade(
        "\n".join(lines), "en", uncited_lines=[], unsupported_lines=lines, max_softened=4
    )
    softened = sum(1 for ln in out.splitlines() if ln.startswith("Based on the available sources, "))
    assert softened == 4


def test_replaces_absolute_terms_ru():
    line = "Это однозначно лучший выбор."
    out, _ = _agent().verify_and_downgrade(line, "ru", uncited_lines=[], unsupported_lines=[line])
    assert "однозначно" not in out
    assert "по всей видимости" in out


def test_absolute_terms_not_changed_in_well_cited_line():
    # A line with >=2 inline citations is considered adequately supported — wording is left intact.
    line = "Это однозначно так [S1] [S2]."
    assert _agent()._soften_absolute_terms(line) == line


def test_already_softened_line_not_double_prefixed():
    line = "Based on the available sources, the trend is up."
    out, _ = _agent().verify_and_downgrade(line, "en", uncited_lines=[], unsupported_lines=[line])
    assert out.count("Based on the available sources,") == 1


def test_summary_counts_and_notes():
    out, summary = _agent().verify_and_downgrade(
        "Some claim.", "en", uncited_lines=["a", "b"], unsupported_lines=["Some claim."]
    )
    assert summary.uncited_lines == 2
    assert summary.unsupported_lines == 1
    assert any("source attribution" in n for n in summary.verification_notes)
    assert any("source support" in n for n in summary.verification_notes)

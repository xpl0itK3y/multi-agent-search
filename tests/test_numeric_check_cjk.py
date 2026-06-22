"""AUD-037 / trust gap: Chinese scale words (万/亿) in numeric checks, incl. cross-language."""
from src.agents.numeric_check import NumericCheckAgent


def _agent():
    return NumericCheckAgent()


def test_chinese_yi_matched_against_english_source():
    # 2.3亿 = 2.3 * 1e8 = 230,000,000 ; "230 million" = 2.3e8.
    report = "收入达到2.3亿 [S1]."
    sources = {"S1": {"content": "Revenue reached 230 million.", "url": "a"}}
    r = _agent().check(report, sources)
    assert r.total == 1 and r.supported == 1


def test_chinese_wan_matched_against_english_source():
    # 150万 = 150 * 1e4 = 1,500,000 ; "1.5 million" = 1.5e6.
    report = "销量为150万台 [S1]."
    sources = {"S1": {"content": "Sales were 1.5 million units."}}
    r = _agent().check(report, sources)
    assert r.total == 1 and r.supported == 1


def test_chinese_figure_mismatch_is_flagged():
    # 2.3亿 (2.3e8) vs source "1.4亿" (1.4e8) — not within tolerance.
    report = "收入达到2.3亿 [S1]."
    sources = {"S1": {"content": "Revenue was 1.4亿 last year."}}
    r = _agent().check(report, sources)
    assert r.total == 1 and r.supported == 0
    assert r.unsupported

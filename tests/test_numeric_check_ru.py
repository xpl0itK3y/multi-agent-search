"""AUD-037: Russian-language numeric checks (₽, млн/млрд/трлн, проценты), incl. cross-language."""
from src.agents.numeric_check import NumericCheckAgent


def _agent():
    return NumericCheckAgent()


def test_ruble_billions_matched_cross_language():
    # RU report figure (₽2,3 млрд) verified against an EN source ("2.3 billion rubles").
    report = "Выручка достигла ₽2,3 млрд за год [S1]."
    sources = {"S1": {"content": "Annual revenue reached 2.3 billion rubles.", "url": "a"}}
    r = _agent().check(report, sources)
    assert r.total == 1 and r.supported == 1


def test_russian_millions_matched():
    report = "Продажи составили 540 млн единиц [S1]."
    sources = {"S1": {"content": "Sales were about 540 million units last year."}}
    r = _agent().check(report, sources)
    assert r.total == 1 and r.supported == 1


def test_russian_trillion_scale_normalization():
    report = "Объём рынка достиг 1,2 трлн рублей [S1]."
    sources = {"S1": {"content": "The market reached 1.2 trillion."}}
    r = _agent().check(report, sources)
    assert r.total == 1 and r.supported == 1


def test_russian_percent_mangled_is_flagged():
    report = "Доля рынка выросла на 40 процентов [S1]."
    sources = {"S1": {"content": "Market share grew about 14 percent."}}
    r = _agent().check(report, sources)
    assert r.total == 1 and r.supported == 0
    assert r.unsupported

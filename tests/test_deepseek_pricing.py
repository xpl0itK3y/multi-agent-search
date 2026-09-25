from datetime import datetime, timezone
import pytest

from src.providers.deepseek import calculate_deepseek_cost, is_deepseek_peak_hours, DeepSeekProvider


def test_deepseek_peak_hours():
    # Wednesday 02:00 UTC -> Peak (01:00-04:00)
    wed_peak = datetime(2026, 9, 23, 2, 0, tzinfo=timezone.utc)
    assert is_deepseek_peak_hours(wed_peak) is True

    # Wednesday 07:00 UTC -> Peak (06:00-10:00)
    wed_peak_2 = datetime(2026, 9, 23, 7, 0, tzinfo=timezone.utc)
    assert is_deepseek_peak_hours(wed_peak_2) is True

    # Wednesday 14:00 UTC -> Off-Peak
    wed_offpeak = datetime(2026, 9, 23, 14, 0, tzinfo=timezone.utc)
    assert is_deepseek_peak_hours(wed_offpeak) is False

    # Saturday 02:00 UTC -> Weekend is always Off-Peak
    sat_weekend = datetime(2026, 9, 26, 2, 0, tzinfo=timezone.utc)
    assert is_deepseek_peak_hours(sat_weekend) is False


def test_deepseek_flash_pricing():
    offpeak = datetime(2026, 9, 23, 14, 0, tzinfo=timezone.utc)
    # 1M prompt (cache miss) + 1M completion on Flash off-peak:
    # 0.15 + 0.60 = 0.75
    cost = calculate_deepseek_cost("deepseek-flash", 1_000_000, 1_000_000, cache_hit_tokens=0, at_time=offpeak)
    assert cost == pytest.approx(0.75)

    # With 1M cache hit + 1M completion:
    # 0.003 + 0.60 = 0.603
    cost_cached = calculate_deepseek_cost("deepseek-flash", 1_000_000, 1_000_000, cache_hit_tokens=1_000_000, at_time=offpeak)
    assert cost_cached == pytest.approx(0.603)


def test_deepseek_v4_pro_pricing():
    offpeak = datetime(2026, 9, 23, 14, 0, tzinfo=timezone.utc)
    # 1M prompt (cache miss) + 1M completion on V4 Pro off-peak:
    # 0.66 + 1.98 = 2.64
    cost = calculate_deepseek_cost("deepseek-v4-pro", 1_000_000, 1_000_000, cache_hit_tokens=0, at_time=offpeak)
    assert cost == pytest.approx(2.64)

    # Peak hours:
    # 1.32 + 3.96 = 5.28
    peak = datetime(2026, 9, 23, 2, 0, tzinfo=timezone.utc)
    cost_peak = calculate_deepseek_cost("deepseek-v4-pro", 1_000_000, 1_000_000, cache_hit_tokens=0, at_time=peak)
    assert cost_peak == pytest.approx(5.28)


def test_extract_usage():
    class DummyUsage:
        prompt_tokens = 1200
        completion_tokens = 800
        prompt_cache_hit_tokens = 900

    pt, ct, cht = DeepSeekProvider._extract_usage(DummyUsage())
    assert pt == 1200
    assert ct == 800
    assert cht == 900


OFF_PEAK = datetime(2026, 9, 23, 14, 0, tzinfo=timezone.utc)
FLASH_1M_IN_1M_OUT = 0.15 + 0.60
PRO_1M_IN_1M_OUT = 0.66 + 1.98


@pytest.mark.parametrize("base_model", ["deepseek-v4-pro", "deepseek-flash"])
@pytest.mark.parametrize("model", ["deepseek-reasoner", "deepseek-chat", "DeepSeek-Reasoner"])
def test_legacy_ids_are_billed_at_the_flash_tier_whatever_the_base_model(monkeypatch, model, base_model):
    # DeepSeek serves deepseek-reasoner / deepseek-chat as deepseek-v4-flash in thinking /
    # non-thinking mode, at the flash rates; thinking mode costs no extra.
    from src.config import settings

    monkeypatch.setattr(settings, "deepseek_model", base_model)
    cost = calculate_deepseek_cost(model, 1_000_000, 1_000_000, at_time=OFF_PEAK)
    assert cost == pytest.approx(FLASH_1M_IN_1M_OUT)  # not pro's 2.64


def test_every_catalog_model_and_alias_is_billed_at_its_catalog_tier(monkeypatch):
    from src.config import settings
    from src.model_catalog import MODEL_CATALOG, _ALIASES, get_model

    expected = {"pro": PRO_1M_IN_1M_OUT, "flash": FLASH_1M_IN_1M_OUT}
    for base_model in ("deepseek-v4-pro", "deepseek-flash"):
        monkeypatch.setattr(settings, "deepseek_model", base_model)
        for model_id in [option.id for option in MODEL_CATALOG] + list(_ALIASES):
            cost = calculate_deepseek_cost(model_id, 1_000_000, 1_000_000, at_time=OFF_PEAK)
            assert cost == pytest.approx(expected[get_model(model_id).tier]), model_id


@pytest.mark.parametrize(
    ("model", "base_model", "expected"),
    [
        # An unknown id is still guessed from its name ...
        ("deepseek-v5-pro", "deepseek-flash", PRO_1M_IN_1M_OUT),
        ("deepseek-v5-flash", "deepseek-v4-pro", FLASH_1M_IN_1M_OUT),
        # ... and one naming no tier takes the base model's tier, from the map first.
        ("deepseek-next", "deepseek-v4-pro", PRO_1M_IN_1M_OUT),
        ("deepseek-next", "deepseek-flash", FLASH_1M_IN_1M_OUT),
        ("deepseek-next", "deepseek-reasoner", FLASH_1M_IN_1M_OUT),
    ],
)
def test_unknown_ids_fall_back_to_name_heuristics(monkeypatch, model, base_model, expected):
    from src.config import settings

    monkeypatch.setattr(settings, "deepseek_model", base_model)
    assert calculate_deepseek_cost(model, 1_000_000, 1_000_000, at_time=OFF_PEAK) == pytest.approx(expected)


def test_test_mock_ids_keep_the_legacy_fallback_rates():
    cost = calculate_deepseek_cost("deepseek-test", 1_000_000, 1_000_000, at_time=OFF_PEAK)
    assert cost == pytest.approx(0.14 + 1.10)

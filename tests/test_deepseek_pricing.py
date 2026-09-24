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


def test_reasoner_is_billed_at_the_pro_tier_whatever_the_base_model(monkeypatch):
    from src.config import settings

    monkeypatch.setattr(settings, "deepseek_model", "deepseek-flash")
    offpeak = datetime(2026, 9, 23, 14, 0, tzinfo=timezone.utc)
    cost = calculate_deepseek_cost("deepseek-reasoner", 1_000_000, 1_000_000, at_time=offpeak)
    assert cost == pytest.approx(2.64)  # pro off-peak: 0.66 + 1.98, not flash's 0.75

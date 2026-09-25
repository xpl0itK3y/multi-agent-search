import threading

import pytest

from src.providers.deepseek import DeepSeekProvider


def test_deepseek_usage_increments_cost_metric(mocker):
    provider = DeepSeekProvider.__new__(DeepSeekProvider)
    provider._lock = threading.Lock()
    provider._prompt_tokens = 0
    provider._completion_tokens = 0
    observe_cost = mocker.patch("src.providers.deepseek.observe_llm_cost")

    provider._record_usage(100, 50, "deepseek-test")

    assert provider._prompt_tokens == 100
    assert provider._completion_tokens == 50
    observe_cost.assert_called_once_with(pytest.approx(0.000069), "deepseek-test")

import threading

import pytest

from src.observability.context import bind_observability_context
from src.observability import metrics
from src.providers.deepseek import DeepSeekProvider


def test_llm_cost_metric_uses_user_research_and_model_labels(mocker):
    counter = mocker.patch.object(metrics, "LLM_COST_USD_TOTAL")
    with bind_observability_context(user_id="user-7", research_id="research-9"):
        metrics.observe_llm_cost(0.125, "deepseek-test")

    counter.labels.assert_called_once_with(
        user_id="user-7",
        research_id="research-9",
        model="deepseek-test",
    )
    counter.labels.return_value.inc.assert_called_once_with(0.125)


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

"""SEC-METRICS: /metrics is labelled by route template and model only. A share token,
research id or user id never reaches it, and its series count stays flat as traffic
spreads over more researches."""
import uuid

import pytest

from src.api.schemas import ResearchRequest, SearchDepth
from src.observability import metrics
from src.observability.context import bind_observability_context
from src.observability.metrics import render_metrics


def _mas_series() -> set[str]:
    payload, _ = render_metrics()
    return {
        line.rsplit(" ", 1)[0]
        for line in payload.decode("utf-8").splitlines()
        if line.startswith("mas_")
    }


def test_llm_cost_metric_is_labelled_by_model_only(mocker):
    counter = mocker.patch.object(metrics, "LLM_COST_USD_TOTAL")
    with bind_observability_context(user_id="user-7", research_id="research-9"):
        metrics.observe_llm_cost(0.125, "deepseek-test")

    counter.labels.assert_called_once_with(model="deepseek-test")
    counter.labels.return_value.inc.assert_called_once_with(0.125)


@pytest.mark.anyio
async def test_public_share_request_is_recorded_by_route_template_not_token(client):
    token = f"sharetoken{uuid.uuid4().hex}"

    response = await client.get(f"/v1/public/research/{token}")

    assert response.status_code == 404
    text = render_metrics()[0].decode("utf-8")
    assert 'path="/v1/public/research/{token}"' in text
    assert token not in text


@pytest.mark.anyio
async def test_series_count_does_not_grow_over_50_researches(client):
    store = client._transport.app.state.research_service.task_store

    async def touch_research(index: int) -> str:
        research = store.add_research(
            ResearchRequest(prompt=f"cardinality topic {index}", depth=SearchDepth.EASY),
            task_ids=[],
        )
        assert (await client.get(f"/v1/research/{research.id}")).status_code == 200
        assert (await client.get(f"/v1/public/research/{uuid.uuid4().hex}")).status_code == 404
        with bind_observability_context(research_id=research.id, user_id=f"user-{index}"):
            metrics.observe_llm_cost(0.01, "deepseek-v4-pro")
        return research.id

    await touch_research(0)  # creates every label set the loop below uses
    before = _mas_series()
    research_ids = [await touch_research(index) for index in range(1, 51)]
    after = _mas_series()

    assert after == before
    assert not any(research_id in series for series in after for research_id in research_ids)
    assert any(series.startswith('mas_llm_cost_usd_total{model="deepseek-v4-pro"}') for series in after)

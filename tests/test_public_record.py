"""AUD-012: ResearchRecord returned to clients must not leak internal graph_state keys."""
import uuid

import pytest

from src.api.app import _public_record
from src.api.schemas import ResearchRecord, ResearchRequest, ResearchStatus, SearchDepth, TaskStatus


def _record(graph_state):
    return ResearchRecord(
        id="r1",
        prompt="hello world",
        depth=SearchDepth.EASY,
        status=ResearchStatus.COMPLETED,
        graph_state=graph_state,
    )


def test_public_record_strips_sensitive_keys_keeps_rest():
    rec = _record(
        {
            "share_token": "secret-token",
            "webhook_url": "http://internal/hook",
            "decompose_payload": {"a": 1},
            "step": "done",
            "model": "deepseek-chat",
        }
    )
    pub = _public_record(rec)
    assert "share_token" not in pub.graph_state
    assert "webhook_url" not in pub.graph_state
    assert "decompose_payload" not in pub.graph_state
    assert pub.graph_state["step"] == "done"
    assert pub.graph_state["model"] == "deepseek-chat"
    # The original (internal) record is untouched — only the returned copy is sanitized.
    assert rec.graph_state["share_token"] == "secret-token"


def test_public_record_handles_none_and_empty():
    assert _public_record(None) is None


SENSITIVE = {"share_token": "live-share-token", "webhook_url": "https://hooks.example.com/x", "decompose_payload": {"a": 1}}


def _research_with_sensitive_state(service):
    """A research whose searches are done, with every sensitive key in its graph_state."""
    store = service.task_store
    research = store.add_research(ResearchRequest(prompt="public record research", depth=SearchDepth.EASY), [])
    task = store.add_task(
        {
            "id": str(uuid.uuid4()),
            "research_id": research.id,
            "description": "t",
            "queries": ["q"],
            "status": TaskStatus.COMPLETED,
            "result": [{"content": "data", "url": "https://a.example", "title": "A"}],
        }
    )
    store.set_research_task_ids(research.id, [task.id])
    store.merge_research_graph_state(research.id, SENSITIVE)
    return research.id


def _assert_stripped(graph_state):
    assert not set(SENSITIVE) & set(graph_state)


@pytest.mark.anyio
async def test_finalize_response_strips_sensitive_keys(client, monkeypatch):
    service = client._transport.app.state.research_service
    monkeypatch.setattr(service, "analyzer", object())  # enqueueing needs one; nothing runs it here
    research_id = _research_with_sensitive_state(service)

    response = await client.post(f"/v1/research/{research_id}/finalize")

    assert response.status_code == 200, response.text
    _assert_stripped(response.json()["research"]["graph_state"])


@pytest.mark.anyio
async def test_graph_response_strips_sensitive_keys(client):
    service = client._transport.app.state.research_service
    research_id = _research_with_sensitive_state(service)

    response = await client.get(f"/v1/research/{research_id}/graph")

    assert response.status_code == 200, response.text
    _assert_stripped(response.json()["graph_state"])
    # Only the response is stripped: the stored state keeps its share link.
    assert service.task_store.get_research(research_id).graph_state["share_token"] == "live-share-token"

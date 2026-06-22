"""AUD-012: ResearchRecord returned to clients must not leak internal graph_state keys."""
from src.api.app import _public_record
from src.api.schemas import ResearchRecord, ResearchStatus, SearchDepth


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
    assert _public_record(_record({})).graph_state == {}

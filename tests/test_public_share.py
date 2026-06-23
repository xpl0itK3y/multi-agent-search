import pytest
from src.domain.errors import ServiceError

from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.services.research_service import ResearchService


def _completed(store, prompt="share me", report="# Report\nBody [S1]."):
    rec = store.add_research(ResearchRequest(prompt=prompt, depth=SearchDepth.EASY), task_ids=[], user_id="owner")
    store.update_research_status(rec.id, ResearchStatus.COMPLETED, report)
    return rec.id


def test_create_resolve_revoke():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rid = _completed(store)

    info = svc.create_share_link(rid)
    assert info.shared and len(info.token) >= 40  # 256-bit url-safe token
    assert svc.create_share_link(rid).token == info.token  # re-share is idempotent

    pub = svc.get_public_report(info.token)
    assert pub.prompt == "share me" and pub.final_report.startswith("# Report")

    svc.revoke_share_link(rid)
    with pytest.raises(ServiceError) as e:
        svc.get_public_report(info.token)
    assert e.value.status_code == 404  # revoked token stops resolving


def test_unshared_research_is_unreachable():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rid = _completed(store)  # never shared
    for tok in ["", "short", "z" * 50, rid]:  # random/empty/short tokens and even the id itself
        with pytest.raises(ServiceError) as e:
            svc.get_public_report(tok)
        assert e.value.status_code == 404


def test_public_report_does_not_leak_sensitive_fields():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rid = _completed(store)
    tok = svc.create_share_link(rid).token
    dumped = svc.get_public_report(tok).model_dump()
    assert "user_id" not in dumped
    assert "share_token" not in dumped
    assert "graph_state" not in dumped
    assert tok not in str(dumped)  # the token is never echoed back in the payload


def test_cannot_share_incomplete_research():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rec = store.add_research(ResearchRequest(prompt="work in progress", depth=SearchDepth.EASY), task_ids=[])
    with pytest.raises(ServiceError) as e:
        svc.create_share_link(rec.id)
    assert e.value.status_code == 409


def test_share_info_reflects_state():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rid = _completed(store)
    assert svc.get_share_info(rid).shared is False
    svc.create_share_link(rid)
    assert svc.get_share_info(rid).shared is True
    svc.revoke_share_link(rid)
    assert svc.get_share_info(rid).shared is False

"""AUD-011: when auth is on, a research is reachable only by its owner — unowned (NULL-owner)
rows are NOT cross-readable by an arbitrary authenticated user."""
import pytest
from fastapi import HTTPException

from src.api.schemas import ResearchRequest, SearchDepth
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


def _svc():
    return ResearchService(task_store=InMemoryTaskStore())


def _add(svc, user_id):
    return svc.task_store.add_research(
        ResearchRequest(prompt="hello world", depth=SearchDepth.EASY), task_ids=[], user_id=user_id
    )


def test_owner_can_access_their_research():
    svc = _svc()
    rec = _add(svc, "userA")
    assert svc._ensure_research_access(rec.id, "userA").id == rec.id


def test_other_user_cannot_access():
    svc = _svc()
    rec = _add(svc, "userA")
    with pytest.raises(HTTPException) as exc:
        svc._ensure_research_access(rec.id, "userB")
    assert exc.value.status_code == 404


def test_unowned_research_not_cross_readable_when_auth_on():
    svc = _svc()
    rec = _add(svc, None)  # NULL owner (created under AUTH_DISABLED)
    with pytest.raises(HTTPException) as exc:
        svc._ensure_research_access(rec.id, "anyUser")
    assert exc.value.status_code == 404


def test_no_scoping_when_auth_off():
    svc = _svc()
    rec = _add(svc, None)
    # auth disabled => scope_user_id passes None => no restriction
    assert svc._ensure_research_access(rec.id, None).id == rec.id

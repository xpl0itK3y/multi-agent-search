import pytest

from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.agents.catalog import AGENTS_CATALOG
from src.domain import MaintenanceActionRequest
from src.services import ResearchService


def test_agents_catalog_integrity():
    assert len(AGENTS_CATALOG) == 20
    valid_stages = {"planning", "search", "synthesis", "delivery"}
    agent_ids = set()
    for agent in AGENTS_CATALOG:
        assert agent.stage in valid_stages
        assert agent.name
        assert agent.role
        assert agent.description
        assert agent.source_file.startswith("src/agents/")
        assert agent.id not in agent_ids
        agent_ids.add(agent.id)


def test_in_memory_task_store_admin_methods():
    store = InMemoryTaskStore()

    # 1. LLM usage and analytics
    u_id = store.record_llm_usage(
        research_id="r1",
        user_id="u1",
        model="deepseek-chat",
        prompt_tokens=100,
        completion_tokens=50,
        total_tokens=150,
        estimated_cost_usd=0.0005,
    )
    assert u_id is not None

    analytics = store.get_admin_token_analytics()
    assert analytics.total_prompt_tokens == 100
    assert analytics.total_completion_tokens == 50
    assert analytics.total_tokens == 150
    assert len(analytics.by_model) == 1
    assert analytics.by_model[0].model == "deepseek-chat"

    # 2. Audit logs
    audit_id = store.record_admin_audit(
        actor_email="admin@test.com",
        action="cleanup_old_jobs",
        target_type="job",
        details={"deleted": 5},
        ip_address="127.0.0.1",
    )
    assert audit_id is not None
    logs = store.get_admin_audit_logs()
    assert len(logs) == 1
    assert logs[0].actor_email == "admin@test.com"
    assert logs[0].action == "cleanup_old_jobs"
    assert logs[0].details["deleted"] == 5

    # 3. Dry-run maintenance preview; executing goes through the service job paths
    # (ADMIN-MAINTENANCE), which then writes the audit row.
    preview = store.preview_maintenance_action("cleanup_old_jobs", {"days": 7})
    assert preview.dry_run is True
    assert preview.action == "cleanup_old_jobs"

    executed = ResearchService(task_store=store).execute_maintenance_action(
        MaintenanceActionRequest(action="cleanup_old_jobs", params={"days": 7}),
        actor_email="admin@test.com",
    )
    assert executed.dry_run is False

    # Check audit log was written for execute
    logs2 = store.get_admin_audit_logs()
    assert len(logs2) == 2

    # 4. Overview: health is the service's (it adds the LLM/broker probes)
    overview = ResearchService(task_store=store).get_admin_overview()
    assert "overall" in overview.system_health


@pytest.mark.postgres
def test_sqlalchemy_task_store_admin_methods(postgres_session_factory):
    # The throwaway test database, never the developer's DATABASE_URL.
    from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore

    store = SQLAlchemyTaskStore(postgres_session_factory)

    # 1. Audit log
    audit_id = store.record_admin_audit(
        actor_email="admin@test.com",
        action="preview_test",
        target_type="test",
        details={"foo": "bar"},
    )
    assert audit_id is not None
    logs = store.get_admin_audit_logs(limit=5)
    assert len(logs) >= 1
    assert any(entry.id == audit_id for entry in logs)

    # 2. Overview
    overview = ResearchService(task_store=store).get_admin_overview()
    assert overview.system_health.get("postgres") == "ok"

    # 3. Dry-run preview
    preview = store.preview_maintenance_action("cleanup_old_jobs", {"days": 30})
    assert preview.dry_run is True

    # 4. Token analytics
    analytics = store.get_admin_token_analytics()
    assert analytics.page == 1


def test_token_analytics_reuses_its_aggregates_across_page_changes(monkeypatch):
    """The totals and breakdowns scan all of llm_usage_logs; a page change within the
    cache window reads only the research page, and the aggregates refresh after it."""
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace

    from src.api.schemas import ResearchRequest, SearchDepth

    clock = [1000.0]
    monkeypatch.setattr("src.services.research_service.time", SimpleNamespace(monotonic=lambda: clock[0]))
    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)
    for n in range(3):
        research = store.add_research(ResearchRequest(prompt=f"topic {n}", depth=SearchDepth.EASY), task_ids=[])
        store.researches[research.id].created_at = datetime(2026, 9, 1, tzinfo=timezone.utc) + timedelta(minutes=n)
        store.record_llm_usage(research.id, None, "deepseek-chat", 10, 0, 10, 0.01)
    full_scans = []
    aggregate = store.get_admin_token_analytics
    monkeypatch.setattr(store, "get_admin_token_analytics", lambda **kw: full_scans.append(kw) or aggregate(**kw))

    first = service.get_admin_token_analytics(page=1, page_size=2)
    store.record_llm_usage(None, None, "deepseek-chat", 5, 0, 5, 0.01)
    clock[0] += ResearchService.TOKEN_ANALYTICS_CACHE_SECONDS - 1
    second = service.get_admin_token_analytics(page=2, page_size=2)

    assert len(full_scans) == 1
    assert (first.total_tokens, second.total_tokens, second.total_researches) == (30, 30, 3)
    assert (second.page, second.page_size) == (2, 2)
    assert [r.prompt for r in second.researches] == ["topic 0"]  # the page itself is fresh
    assert [r.prompt for r in first.researches] == ["topic 2", "topic 1"]

    clock[0] += 2
    third = service.get_admin_token_analytics(page=1, page_size=2)

    assert len(full_scans) == 2 and third.total_tokens == 35

"""ADMIN-QUERIES: the admin read methods behave the same on both TaskStore backends
(shape, filters, global ordering, pagination), and on Postgres they page in SQL with a
fixed number of statements instead of loading whole tables into Python."""
import re
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import event, update

from src.api.schemas import ResearchRequest, SearchDepth
from src.config import settings
from src.db.models import ResearchORM, UserEventORM, UserSessionORM
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore
from tests.postgres_helpers import truncate_runtime_tables

T0 = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
# A report or graph_state column selected whole (a JSONB key, graph_state[...] or ->, is fine).
_WHOLE_BLOB = r"researches\.(final_report|graph_state)\b(?!\s*(\[|->))"


@pytest.fixture(
    params=[
        "memory",
        pytest.param("postgres", marks=pytest.mark.postgres),
    ]
)
def store(request):
    if request.param == "memory":
        return InMemoryTaskStore()
    engine, session_factory = request.getfixturevalue("_postgres_test_db")
    truncate_runtime_tables(session_factory)
    return SQLAlchemyTaskStore(session_factory)


def _user(store, user_id, name=None, **identity):
    store.delete_user(user_id)
    user = store.create_user(user_id, f"{user_id}@example.com", None, **identity)
    if name:
        store.update_user_profile(user_id, name, None)
    return user


def _research(store, user_id, prompt, created_at, depth=SearchDepth.EASY):
    research = store.add_research(ResearchRequest(prompt=prompt, depth=depth), task_ids=[], user_id=user_id)
    if isinstance(store, InMemoryTaskStore):
        store.researches[research.id].created_at = created_at
    else:
        with store.session_scope() as session:
            session.execute(update(ResearchORM).where(ResearchORM.id == research.id).values(created_at=created_at))
    return research


def _chat_prompt(store, user_id, research_id, prompt, created_at):
    event_id = store.record_user_event(
        event_name="chat_prompt",
        event_category="prompt",
        user_id=user_id,
        details={"research_id": research_id, "prompt": prompt},
    )
    if isinstance(store, InMemoryTaskStore):
        next(e for e in store.user_events if e["id"] == event_id)["created_at"] = created_at
    else:
        with store.session_scope() as session:
            session.execute(update(UserEventORM).where(UserEventORM.id == event_id).values(created_at=created_at))
    return event_id


def _session(store, user_id, session_id, last_active_at, **fields):
    store.record_user_session(user_id=user_id, session_id=session_id, **fields)
    if isinstance(store, InMemoryTaskStore):
        next(s for s in store.user_sessions if s["session_id"] == session_id)["last_active_at"] = last_active_at
    else:
        with store.session_scope() as session:
            session.execute(
                update(UserSessionORM)
                .where(UserSessionORM.session_id == session_id)
                .values(last_active_at=last_active_at)
            )


def _usage(store, research_id, user_id, tokens, cost, model="deepseek-chat"):
    store.record_llm_usage(
        research_id=research_id,
        user_id=user_id,
        model=model,
        prompt_tokens=tokens,
        completion_tokens=0,
        total_tokens=tokens,
        estimated_cost_usd=cost,
    )


@contextmanager
def _statements(store):
    """SQL statements the block runs (Postgres leg); None on the memory leg."""
    if isinstance(store, InMemoryTaskStore):
        yield None
        return
    captured: list[str] = []
    bind = store.session_factory.kw["bind"]

    def record(conn, cursor, statement, parameters, context, executemany):
        captured.append(statement)

    event.listen(bind, "before_cursor_execute", record)
    try:
        yield captured
    finally:
        event.remove(bind, "before_cursor_execute", record)


# ── users list ────────────────────────────────────────────────────────────────


def _three_users(store):
    for user_id in ("adm-a", "adm-b", "adm-c"):
        _user(store, user_id)
    research_a = _research(store, "adm-a", "prompt a1", T0)
    _research(store, "adm-a", "prompt a2", T0 + timedelta(minutes=1))
    _research(store, "adm-a", "prompt a3", T0 + timedelta(minutes=2))
    research_c = _research(store, "adm-c", "prompt c1", T0 + timedelta(minutes=3))
    _usage(store, research_a.id, "adm-a", 10, 0.9)
    _usage(store, None, "adm-b", 300, 0.2)
    _usage(store, research_c.id, "adm-c", 50, 0.5)
    _usage(store, research_c.id, "adm-c", 5, 0.05)


@pytest.mark.parametrize(
    ("sort_by", "expected"),
    [
        ("tokens", ["adm-b", "adm-c", "adm-a"]),
        ("cost", ["adm-a", "adm-c", "adm-b"]),
        ("researches", ["adm-a", "adm-c", "adm-b"]),
    ],
)
def test_users_list_sorts_by_metric_across_pages(store, sort_by, expected):
    _three_users(store)

    pages = [store.get_admin_users_list(page=page, page_size=1, sort_by=sort_by) for page in (1, 2, 3)]

    assert [page.users[0].id for page in pages] == expected
    assert {page.total_users for page in pages} == {3}


def test_users_list_aggregates_and_latest_session(store):
    _three_users(store)
    _session(store, "adm-c", "s-new", T0 + timedelta(hours=1), ip_address="10.0.0.2", browser="Safari", os="macOS")
    _session(store, "adm-c", "s-old", T0, ip_address="10.0.0.1", browser="Firefox", os="Linux")

    users = {u.id: u for u in store.get_admin_users_list(page_size=10).users}

    assert (users["adm-a"].researches_count, users["adm-a"].total_tokens, users["adm-a"].total_cost_usd) == (3, 10, 0.9)
    assert (users["adm-c"].researches_count, users["adm-c"].total_tokens, users["adm-c"].total_cost_usd) == (1, 55, 0.55)
    assert (users["adm-b"].researches_count, users["adm-b"].total_tokens) == (0, 300)
    assert (users["adm-c"].last_browser, users["adm-c"].last_os, users["adm-c"].last_ip) == ("Safari", "macOS", "10.0.0.2")
    assert users["adm-b"].last_browser is None


def test_users_list_filters_search_role_and_takes_wildcards_literally(store, monkeypatch):
    _user(store, "ab_x")
    _user(store, "abzx", name="Zed", google_subject="g-abzx")
    _user(store, "abzp", admin_provisioned=True)  # scripts/create_admin.py
    # Listed too, but self-registered: no verified identity, so no admin rights.
    _user(store, "abzq")
    monkeypatch.setattr(settings, "admin_emails", "ABZX@example.com, abzp@example.com, abzq@example.com")

    assert [u.id for u in store.get_admin_users_list(search="b_x").users] == ["ab_x"]
    assert [u.id for u in store.get_admin_users_list(search=" zed ").users] == ["abzx"]
    assert store.get_admin_users_list(search="%").users == []
    admins = store.get_admin_users_list(role="admin")
    assert sorted((u.id, u.is_admin) for u in admins.users) == [("abzp", True), ("abzx", True)]
    assert admins.total_users == 2
    users = store.get_admin_users_list(role="user")
    assert sorted((u.id, u.is_admin) for u in users.users) == [("ab_x", False), ("abzq", False)]
    assert store.get_admin_user_detail("abzq").user.is_admin is False
    assert store.get_admin_user_detail("abzp").user.is_admin is True


def test_users_list_runs_a_fixed_number_of_statements(store):
    _three_users(store)
    with _statements(store) as statements:
        store.get_admin_users_list(page_size=10, sort_by="cost")
    if statements is not None:
        assert len(statements) == 3  # total, online, one page query (no per-user queries)
        assert "LATERAL" in statements[-1] and "LIMIT" in statements[-1]


@contextmanager
def _statements_with_parameters(store):
    """(SQL, bound parameters) of each statement the block runs; None on the memory leg."""
    if isinstance(store, InMemoryTaskStore):
        yield None
        return
    captured: list[tuple[str, object]] = []
    bind = store.session_factory.kw["bind"]

    def record(conn, cursor, statement, parameters, context, executemany):
        captured.append((statement, parameters))

    event.listen(bind, "before_cursor_execute", record)
    try:
        yield captured
    finally:
        event.remove(bind, "before_cursor_execute", record)


def _most_scans_of(store, statement, parameters, relation: str) -> int:
    """How many times EXPLAIN ANALYZE says the statement scanned ``relation`` at most in
    one plan node: a scan inside a LATERAL runs once per row it is joined to."""
    with store.session_factory.kw["bind"].connect() as conn:
        plan = conn.exec_driver_sql("EXPLAIN (ANALYZE, FORMAT JSON) " + statement, parameters).scalar_one()
    loops = []

    def walk(node):
        if node.get("Relation Name") == relation:
            loops.append(node["Actual Loops"])
        for child in node.get("Plans", []):
            walk(child)

    walk(plan[0]["Plan"])
    return max(loops)


@pytest.mark.parametrize("sort_by", ["last_seen", "registered", "tokens", "cost", "researches"])
def test_users_list_aggregates_only_the_page_it_returns(store, sort_by):
    """The laterals used to run for every account before the LIMIT, even for a sort on
    users columns alone: a full llm_usage_logs pass on each Users tab refresh."""
    for n in range(5):
        _user(store, f"page-{n}")
        research = _research(store, f"page-{n}", f"topic {n}", T0 + timedelta(minutes=n))
        _usage(store, research.id, f"page-{n}", 10 * (n + 1), 0.01 * (n + 1))
        _session(store, f"page-{n}", f"page-sess-{n}", T0 + timedelta(hours=n))

    with _statements_with_parameters(store) as statements:
        listing = store.get_admin_users_list(page=1, page_size=2, sort_by=sort_by)

    assert len(listing.users) == 2 and listing.total_users == 5
    if statements is not None:
        page_query, parameters = statements[-1]
        for relation in ("llm_usage_logs", "researches", "user_sessions"):
            assert _most_scans_of(store, page_query, parameters, relation) <= 2, relation


# ── user detail ───────────────────────────────────────────────────────────────


def test_user_detail_shape_and_order(store):
    _three_users(store)
    store.record_user_session(user_id="adm-a", session_id="s1", ip_address="10.0.0.9", browser="Edge", os="Windows")
    store.record_user_event(event_name="tab_focus", event_category="ui", user_id="adm-a", details={"n": 1})
    _usage(store, None, "adm-a", 7, 0.01, model="deepseek-v4-pro")

    detail = store.get_admin_user_detail("adm-a")

    assert detail.user.id == "adm-a" and detail.user.total_tokens == 17
    assert [r["prompt"] for r in detail.researches] == ["prompt a3", "prompt a2", "prompt a1"]
    assert set(detail.researches[0]) == {"id", "prompt", "depth", "status", "total_tokens", "cost_usd", "created_at"}
    assert (detail.researches[2]["total_tokens"], detail.researches[2]["cost_usd"]) == (10, 0.9)
    assert detail.researches[0]["depth"] == "easy" and detail.researches[0]["status"] == "processing"
    assert detail.recent_researches == detail.researches
    assert [e["event_name"] for e in detail.recent_events] == ["tab_focus"]
    assert detail.recent_events[0]["details"] == {"n": 1}
    assert [(s["session_id"], s["browser"]) for s in detail.sessions] == [("s1", "Edge")]
    assert set(detail.sessions[0]) == {
        "id", "session_id", "ip_address", "device_type", "browser", "os", "screen_res",
        "language", "timezone", "country", "city", "started_at", "last_active_at",
    }
    assert detail.token_breakdown == {
        "by_model": {
            "deepseek-chat": {"tokens": 10, "cost_usd": 0.9},
            "deepseek-v4-pro": {"tokens": 7, "cost_usd": 0.01},
        }
    }
    assert store.get_admin_user_detail("nobody") is None


def test_user_detail_never_loads_reports_or_graph_state(store):
    _three_users(store)
    with _statements(store) as statements:
        store.get_admin_user_detail("adm-a")
    if statements is not None:
        assert not [s for s in statements if re.search(_WHOLE_BLOB, s)]


# ── prompts ───────────────────────────────────────────────────────────────────


def _prompt_log(store):
    _user(store, "p-user", name="Pat")
    _user(store, "q-user")
    r1 = _research(store, "p-user", "first research", T0)
    r2 = _research(store, "q-user", "second 100% research", T0 + timedelta(minutes=2))
    _chat_prompt(store, "p-user", r1.id, "chat about apples", T0 + timedelta(minutes=1))
    _chat_prompt(store, "q-user", r2.id, "chat about pears", T0 + timedelta(minutes=3))
    r3 = _research(store, "p-user", "third research", T0 + timedelta(minutes=4))
    _usage(store, r1.id, "p-user", 40, 0.12345)
    _usage(store, r1.id, "p-user", 2, 0.00001)
    return r1, r2, r3


def test_prompts_merge_both_sources_newest_first_and_page(store):
    r1, r2, r3 = _prompt_log(store)

    first = store.get_admin_prompts(page=1, page_size=2)
    second = store.get_admin_prompts(page=2, page_size=2)
    third = store.get_admin_prompts(page=3, page_size=2)

    assert first.total_count == 5
    assert [p.prompt for p in first.prompts + second.prompts + third.prompts] == [
        "third research", "chat about pears", "second 100% research", "chat about apples", "first research",
    ]
    oldest = third.prompts[0]
    assert (oldest.id, oldest.prompt_type, oldest.research_id) == (f"res_{r1.id}", "research", r1.id)
    assert (oldest.total_tokens, oldest.cost_usd) == (42, 0.1235)
    assert (oldest.user_email, oldest.user_name, oldest.depth, oldest.status) == (
        "p-user@example.com", "Pat", "easy", "processing",
    )
    chat = first.prompts[1]
    assert chat.id.startswith("chat_") and chat.prompt_type == "chat"
    assert (chat.research_id, chat.user_id, chat.depth, chat.status, chat.total_tokens) == (r2.id, "q-user", None, None, 0)


def test_prompts_filters(store):
    r1, _r2, _r3 = _prompt_log(store)

    chats = store.get_admin_prompts(prompt_type="chat")
    assert [p.prompt for p in chats.prompts] == ["chat about pears", "chat about apples"]
    assert chats.total_count == 2
    assert [p.prompt for p in store.get_admin_prompts(prompt_type="research", user_id="p-user").prompts] == [
        "third research", "first research",
    ]
    assert [p.prompt for p in store.get_admin_prompts(search="APPLES").prompts] == ["chat about apples"]
    assert [p.prompt for p in store.get_admin_prompts(search="pat").prompts] == [
        "third research", "chat about apples", "first research",
    ]
    assert [p.prompt for p in store.get_admin_prompts(search="100%").prompts] == ["second 100% research"]
    assert store.get_admin_prompts(search="%").total_count == 1  # the one literal %
    assert store.get_admin_prompts(prompt_type="bogus").total_count == 0


def test_prompts_page_in_sql(store):
    _prompt_log(store)
    with _statements(store) as statements:
        store.get_admin_prompts(page=1, page_size=2)
    if statements is not None:
        assert len(statements) == 3  # count, page, usage for the page's researches
        page_query = statements[1]
        assert "UNION ALL" in page_query and "LIMIT" in page_query


# ── token analytics ───────────────────────────────────────────────────────────


def test_token_analytics_breakdowns_and_legacy_fallback(store):
    _user(store, "t-user")
    logged = _research(store, "t-user", "logged topic", T0, depth=SearchDepth.HARD)
    legacy = _research(store, "t-user", "legacy topic", T0 + timedelta(minutes=1))
    empty = _research(store, "t-user", "empty topic", T0 + timedelta(minutes=2))
    store.merge_research_graph_state(legacy.id, {"llm_token_usage": {"total_tokens": 77, "estimated_cost_usd": 0.5}})
    # A research with usage rows ignores its graph_state figure (no double counting).
    store.merge_research_graph_state(logged.id, {"llm_token_usage": {"total_tokens": 999, "estimated_cost_usd": 9.0}})
    _usage(store, logged.id, "t-user", 30, 0.3, model="deepseek-v4-pro")
    _usage(store, logged.id, "t-user", 20, 0.2, model="deepseek-chat")
    _usage(store, None, "t-user", 20, 0.1, model="deepseek-chat")

    analytics = store.get_admin_token_analytics(page=1, page_size=2)

    assert (analytics.total_tokens, analytics.total_cost_usd) == (70, 0.6)
    assert [(m.model, m.total_tokens, m.calls_count) for m in analytics.by_model] == [
        ("deepseek-chat", 40, 2),
        ("deepseek-v4-pro", 30, 1),
    ]
    assert [(d.depth, d.total_tokens, d.researches_count) for d in analytics.by_depth] == [("hard", 50, 1)]
    assert analytics.total_researches == 3
    assert [(r.prompt, r.total_tokens, r.estimated_cost_usd) for r in analytics.researches] == [
        ("empty topic", 0, 0.0),
        ("legacy topic", 77, 0.5),
    ]
    tail = store.get_admin_token_analytics(page=2, page_size=2).researches
    assert [(r.prompt, r.total_tokens, r.depth, r.status) for r in tail] == [("logged topic", 50, "hard", "processing")]
    # The export pages through the same rows without the totals and breakdowns.
    assert store.get_admin_token_research_usage(page=2, page_size=2) == tail
    assert store.get_admin_token_research_usage(page=1, page_size=2) == analytics.researches
    assert empty.id != legacy.id


def test_token_analytics_selects_only_the_legacy_usage_key(store):
    _user(store, "t-user")
    research = _research(store, "t-user", "blob topic", T0)
    store.merge_research_graph_state(research.id, {"llm_token_usage": {"total_tokens": 5}, "big": "x" * 1000})
    with _statements(store) as statements:
        store.get_admin_token_analytics()
    if statements is not None:
        assert not [s for s in statements if re.search(_WHOLE_BLOB, s)]


# ── per-user token stats (/v1/auth/token-stats) ───────────────────────────────


def test_user_token_analytics_orders_models_by_tokens(store):
    _user(store, "stats-user")
    research = _research(store, "stats-user", "stats topic", T0)
    _usage(store, research.id, "stats-user", 5, 0.01, model="deepseek-v4-pro")
    _usage(store, research.id, "stats-user", 30, 0.02, model="deepseek-chat")
    _usage(store, None, "stats-user", 30, 0.03, model="deepseek-flash")

    stats = store.get_user_token_analytics("stats-user")

    assert [(m["model"], m["total_tokens"], m["calls_count"]) for m in stats["by_model"]] == [
        ("deepseek-chat", 30, 1),
        ("deepseek-flash", 30, 1),
        ("deepseek-v4-pro", 5, 1),
    ]
    assert (stats["total_tokens"], stats["calls_count"], stats["researches_count"]) == (65, 3, 1)
    assert [(r["prompt"], r["total_tokens"], r["depth"], r["status"]) for r in stats["recent"]] == [
        ("stats topic", 35, "easy", "processing"),
    ]

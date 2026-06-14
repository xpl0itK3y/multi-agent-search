from datetime import datetime, timedelta, timezone

from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth
from src.config import settings
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.services.research_service import ResearchService


def _research(store, prompt="watch this question now", status=ResearchStatus.COMPLETED):
    rec = store.add_research(ResearchRequest(prompt=prompt, depth=SearchDepth.EASY), task_ids=[], user_id="u1")
    store.update_research_status(rec.id, status)
    return rec.id


def _iso(dt):
    return dt.isoformat()


def test_set_get_disable_watch():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rid = _research(store)

    w = svc.set_research_watch(rid, enabled=True, interval_seconds=60)  # below floor → clamped
    assert w.enabled is True
    assert w.interval_seconds == settings.watch_min_interval_seconds
    assert w.next_run_at and svc._parse_iso(w.next_run_at) > datetime.now(timezone.utc)

    assert svc.get_research_watch(rid).enabled is True

    off = svc.set_research_watch(rid, enabled=False, interval_seconds=None)
    assert off.enabled is False and off.next_run_at == ""


def test_unseen_change_then_acknowledge():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rid = _research(store)
    now = datetime.now(timezone.utc)
    store.update_research_graph_state(rid, {"watch": {
        "enabled": True, "interval_seconds": 3600,
        "last_change_at": _iso(now), "acknowledged_at": _iso(now - timedelta(hours=1)),
    }})

    assert svc.get_research_watch(rid).has_unseen_change is True
    acked = svc.acknowledge_research_watch(rid)
    assert acked.has_unseen_change is False
    assert acked.acknowledged_at == acked.last_change_at


def test_list_active_watch_research_ids():
    store = InMemoryTaskStore()
    watched = _research(store, "watched one here please")
    _research(store, "unwatched one here please")  # no watch
    store.update_research_graph_state(watched, {"watch": {"enabled": True, "interval_seconds": 3600}})
    assert store.list_active_watch_research_ids() == [watched]


def test_run_due_watches_fires_only_when_due_and_terminal():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    fired: list[str] = []
    svc._trigger_watch = lambda head, watch: (fired.append(head.id), 1)[1]  # stub the heavy re-run

    now = datetime.now(timezone.utc)
    past, future = _iso(now - timedelta(minutes=5)), _iso(now + timedelta(hours=2))

    due = _research(store, "due and completed here", ResearchStatus.COMPLETED)
    not_due = _research(store, "not due yet here ok", ResearchStatus.COMPLETED)
    still_running = _research(store, "due but still running", ResearchStatus.PROCESSING)
    for rid, nxt in ((due, past), (not_due, future), (still_running, past)):
        store.update_research_graph_state(rid, {"watch": {"enabled": True, "interval_seconds": 3600, "next_run_at": nxt}})

    assert svc.run_due_watches() == 1
    assert fired == [due]  # only the completed, past-due watch fires


def test_flag_watch_change_only_on_material_diff():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rid = _research(store)
    store.update_research_graph_state(rid, {
        "watch": {"enabled": True, "interval_seconds": 3600},
        "diff": {"new_claims": ["something genuinely new appeared"], "dropped_claims": [], "shifted_claims": [], "new_sources": 0},
    })
    svc._maybe_flag_watch_change(rid)
    assert svc.get_research_watch(rid).last_change_at  # change recorded

    # no material diff → no change flag
    rid2 = _research(store)
    store.update_research_graph_state(rid2, {
        "watch": {"enabled": True, "interval_seconds": 3600},
        "diff": {"new_claims": [], "dropped_claims": [], "shifted_claims": [], "new_sources": 0},
    })
    svc._maybe_flag_watch_change(rid2)
    assert svc.get_research_watch(rid2).last_change_at == ""

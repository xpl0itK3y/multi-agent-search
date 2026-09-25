"""TRAIL-APPEND: graph_trail appends are atomic, and the research SSE stream keeps
streaming once the trail reaches graph_trail_history_limit."""
import threading
import time

import pytest

import src.repositories.in_memory_task_store as in_memory_module
from src.api.app import format_trail_cursor, parse_trail_cursor, unsent_trail_entries
from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth
from src.config import settings
from src.core.graph_history import compact_graph_trail
from src.repositories import InMemoryTaskStore, SQLAlchemyTaskStore


def _append_concurrently(store, research_id, threads=6, per_thread=8):
    def worker(index):
        for n in range(per_thread):
            store.append_research_graph_event(research_id, {"step": "search", "detail": f"w{index}-{n}"})

    pool = [threading.Thread(target=worker, args=(index,)) for index in range(threads)]
    for thread in pool:
        thread.start()
    for thread in pool:
        thread.join()
    return threads * per_thread


def test_in_memory_concurrent_appends_keep_every_event(monkeypatch):
    # Widen the read-modify-write window so an unlocked append reliably loses events.
    def slow_compact(existing, incoming):
        time.sleep(0.002)
        return compact_graph_trail(existing, incoming)

    monkeypatch.setattr(in_memory_module, "compact_graph_trail", slow_compact)
    store = InMemoryTaskStore()
    record = store.add_research(ResearchRequest(prompt="trail topic", depth=SearchDepth.EASY), task_ids=[])

    expected = _append_concurrently(store, record.id)

    assert len(store.get_research(record.id).graph_trail) == expected


def test_append_returns_the_trail_not_the_record():
    store = InMemoryTaskStore()
    record = store.add_research(ResearchRequest(prompt="trail topic", depth=SearchDepth.EASY), task_ids=[])

    trail = store.append_research_graph_event(record.id, {"step": "analyze", "detail": "one"})

    assert [entry["detail"] for entry in trail] == ["one"]
    assert trail[0]["timestamp"]
    assert store.append_research_graph_event("missing", {"step": "x"}) is None


@pytest.mark.postgres
def test_sqlalchemy_concurrent_appends_keep_every_event(postgres_session_factory):
    store = SQLAlchemyTaskStore(postgres_session_factory)
    record = store.add_research(ResearchRequest(prompt="trail topic", depth=SearchDepth.EASY), task_ids=[])

    expected = _append_concurrently(store, record.id)

    trail = store.get_research(record.id).graph_trail
    assert len(trail) == expected
    assert [entry["timestamp"] for entry in trail] == sorted(entry["timestamp"] for entry in trail)


# ── SSE trail cursor ─────────────────────────────────────────────────────────


def _entry(second: int, detail: str) -> dict:
    return {"timestamp": f"2026-09-24T10:00:{second:02d}+00:00", "step": "search", "detail": detail}


def test_cursor_keeps_moving_once_the_trail_is_capped():
    first_page = [_entry(1, "a"), _entry(2, "b"), _entry(3, "c")]
    sent = unsent_trail_entries(first_page, None)
    assert [entry["detail"] for entry, _ in sent] == ["a", "b", "c"]
    cursor = sent[-1][1]

    # Same length as before (capped): the two oldest were trimmed, two new appended.
    capped = [_entry(3, "c"), _entry(4, "d"), _entry(5, "e")]
    assert [entry["detail"] for entry, _ in unsent_trail_entries(capped, cursor)] == ["d", "e"]


def test_cursor_handles_entries_sharing_a_timestamp():
    trail = [_entry(1, "a"), _entry(2, "b1"), _entry(2, "b2")]
    first = unsent_trail_entries(trail[:2], None)
    cursor = first[-1][1]
    assert cursor == (trail[1]["timestamp"], 1)

    assert [entry["detail"] for entry, _ in unsent_trail_entries(trail, cursor)] == ["b2"]
    assert unsent_trail_entries(trail, (trail[2]["timestamp"], 2)) == []


def test_cursor_round_trips_through_the_sse_id():
    cursor = ("2026-09-24T10:00:02.123456+00:00", 3)
    assert parse_trail_cursor(format_trail_cursor(cursor)) == cursor
    for junk in (None, "", "garbage", "ts|x", "|2"):
        assert parse_trail_cursor(junk) is None


def test_cursor_rejects_non_ascii_digits():
    # str.isdigit() accepts these, but int() rejects the superscripts; all are junk here.
    for junk in ("2026|\xb2", "2026|\xb9\xb2", "2026|١", "2026|3\xb3"):
        assert parse_trail_cursor(junk) is None


def _research_snapshot(store, research_id, status, trail):
    research = store.get_research(research_id).model_copy(deep=True)
    research.status = status
    research.graph_trail = trail
    return research


async def _stream(client, research_id, headers=None):
    events: list[tuple[str | None, str]] = []
    event_id = None
    async with client.stream("GET", f"/v1/research/{research_id}/events", headers=headers or {}) as response:
        assert response.status_code == 200
        async for line in response.aiter_lines():
            if line.startswith("id: "):
                event_id = line[len("id: "):]
            elif line.startswith("event: "):
                events.append((event_id, line[len("event: "):]))
                event_id = None
    return events


@pytest.mark.anyio
async def test_events_stream_delivers_steps_appended_after_the_cap(client, monkeypatch):
    monkeypatch.setattr(settings, "graph_trail_history_limit", 3)
    service = client._transport.app.state.research_service
    store = service.task_store
    record = store.add_research(ResearchRequest(prompt="stream past the cap", depth=SearchDepth.EASY), task_ids=[])
    snapshots = [
        _research_snapshot(store, record.id, ResearchStatus.PROCESSING, [_entry(1, "a"), _entry(2, "b"), _entry(3, "c")]),
        _research_snapshot(store, record.id, ResearchStatus.COMPLETED, [_entry(3, "c"), _entry(4, "d"), _entry(5, "e")]),
    ]
    monkeypatch.setattr(store, "get_research", lambda research_id: snapshots.pop(0) if len(snapshots) > 1 else snapshots[0])

    events = await _stream(client, record.id)

    trace_ids = [event_id for event_id, name in events if name == "trace_step"]
    assert len(trace_ids) == 5  # a, b, c and then d, e despite the unchanged length
    assert trace_ids[-1] == format_trail_cursor((_entry(5, "e")["timestamp"], 1))
    assert events[-1][1] == "done"


@pytest.mark.anyio
async def test_events_stream_resumes_after_last_event_id(client):
    service = client._transport.app.state.research_service
    store = service.task_store
    record = store.add_research(ResearchRequest(prompt="resume the stream", depth=SearchDepth.EASY), task_ids=[])
    for detail in ("a", "b", "c"):
        store.append_research_graph_event(record.id, {"step": "search", "detail": detail})
    store.update_research_status(record.id, ResearchStatus.COMPLETED, "final")
    trail = store.get_research(record.id).graph_trail
    after_first = unsent_trail_entries(trail[:1], None)[-1][1]

    events = await _stream(client, record.id, headers={"Last-Event-ID": format_trail_cursor(after_first)})

    assert len([name for _, name in events if name == "trace_step"]) == 2


@pytest.mark.anyio
async def test_events_stream_replays_everything_for_a_non_ascii_last_event_id(client):
    service = client._transport.app.state.research_service
    store = service.task_store
    record = store.add_research(ResearchRequest(prompt="junk resume id", depth=SearchDepth.EASY), task_ids=[])
    for detail in ("a", "b"):
        store.append_research_graph_event(record.id, {"step": "search", "detail": detail})
    store.update_research_status(record.id, ResearchStatus.COMPLETED, "final")
    timestamp = store.get_research(record.id).graph_trail[0]["timestamp"]

    # Raw header bytes: 0xB2 (superscript two) reaches the app as "\xb2" via latin-1.
    events = await _stream(client, record.id, headers={"Last-Event-ID": f"{timestamp}|".encode() + b"\xb2"})

    assert len([name for _, name in events if name == "trace_step"]) == 2

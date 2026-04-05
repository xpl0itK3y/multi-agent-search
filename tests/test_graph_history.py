from datetime import datetime, timedelta, timezone

from src.graph.history import compact_graph_step_events


def test_compact_graph_step_events_dedupes_and_keeps_latest_limit(mocker):
    now = datetime.now(timezone.utc)
    old_timestamp = (now - timedelta(minutes=10)).isoformat()
    recent_timestamp = (now - timedelta(minutes=1)).isoformat()
    newer_timestamp = (now - timedelta(seconds=30)).isoformat()

    existing = [
        {"timestamp": old_timestamp, "step": "analyze", "elapsed_ms": 100.0, "worker_name": "job-worker"},
        {"timestamp": recent_timestamp, "step": "verify", "elapsed_ms": 200.0, "worker_name": "job-worker"},
    ]
    incoming = [
        {"timestamp": recent_timestamp, "step": "verify", "elapsed_ms": 200.0, "worker_name": "job-worker"},
        {"timestamp": newer_timestamp, "step": "tie_break", "elapsed_ms": 300.0, "worker_name": "job-worker"},
    ]

    mocker.patch("src.graph.history.settings.graph_step_event_retention_seconds", 3600)
    mocker.patch("src.graph.history.settings.graph_step_event_history_limit", 2)

    compacted = compact_graph_step_events(existing, incoming)

    assert len(compacted) == 2
    assert compacted[0]["step"] == "verify"
    assert compacted[1]["step"] == "tie_break"


def test_compact_graph_step_events_drops_expired_entries(mocker):
    now = datetime.now(timezone.utc)
    expired_timestamp = (now - timedelta(days=2)).isoformat()
    recent_timestamp = (now - timedelta(minutes=5)).isoformat()

    mocker.patch("src.graph.history.settings.graph_step_event_retention_seconds", 3600)
    mocker.patch("src.graph.history.settings.graph_step_event_history_limit", 10)

    compacted = compact_graph_step_events(
        [{"timestamp": expired_timestamp, "step": "collect_context", "elapsed_ms": 100.0}],
        [{"timestamp": recent_timestamp, "step": "analyze", "elapsed_ms": 200.0}],
    )

    assert len(compacted) == 1
    assert compacted[0]["step"] == "analyze"

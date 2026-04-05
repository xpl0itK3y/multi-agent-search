from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from collections import deque
import threading

from src.observability.context import get_observability_context


@dataclass
class GraphStepMetricsSnapshot:
    run_count: int = 0
    failure_count: int = 0
    total_ms: float = 0.0
    avg_ms: float = 0.0


@dataclass
class GraphMetricsSnapshot:
    resume_count: int = 0
    replan_pass_count: int = 0
    tie_break_pass_count: int = 0
    analyze_pass_count: int = 0
    completed_run_count: int = 0
    steps: dict[str, GraphStepMetricsSnapshot] = None

    def __post_init__(self) -> None:
        if self.steps is None:
            self.steps = {
                "collect_context": GraphStepMetricsSnapshot(),
                "replan": GraphStepMetricsSnapshot(),
                "analyze": GraphStepMetricsSnapshot(),
                "verify": GraphStepMetricsSnapshot(),
                "tie_break": GraphStepMetricsSnapshot(),
            }


@dataclass
class GraphStepEventSnapshot:
    timestamp: str
    step: str
    elapsed_ms: float
    failed: bool = False
    research_id: str | None = None
    worker_name: str | None = None


class GraphMetricsRegistry:
    def __init__(self):
        self._lock = threading.Lock()
        self._metrics = GraphMetricsSnapshot()
        self._events = deque(maxlen=500)

    def record_resume(self) -> None:
        with self._lock:
            self._metrics.resume_count += 1

    def record_replan(self) -> None:
        with self._lock:
            self._metrics.replan_pass_count += 1

    def record_tie_break(self) -> None:
        with self._lock:
            self._metrics.tie_break_pass_count += 1

    def record_analyze(self) -> None:
        with self._lock:
            self._metrics.analyze_pass_count += 1

    def record_completed_run(self) -> None:
        with self._lock:
            self._metrics.completed_run_count += 1

    def record_step(self, step_name: str, elapsed_ms: float, *, failed: bool = False, research_id: str | None = None) -> None:
        with self._lock:
            step = self._metrics.steps.get(step_name)
            if step is None:
                return
            step.run_count += 1
            if failed:
                step.failure_count += 1
            normalized_elapsed_ms = round(max(elapsed_ms, 0.0), 2)
            step.total_ms = round(step.total_ms + normalized_elapsed_ms, 2)
            step.avg_ms = round(step.total_ms / step.run_count, 2) if step.run_count > 0 else 0.0
            observability_context = get_observability_context()
            self._events.append(
                GraphStepEventSnapshot(
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    step=step_name,
                    elapsed_ms=normalized_elapsed_ms,
                    failed=failed,
                    research_id=research_id,
                    worker_name=observability_context.get("worker_name"),
                )
            )

    def snapshot(self) -> dict:
        with self._lock:
            return asdict(self._metrics)

    def event_snapshot(self) -> list[dict]:
        with self._lock:
            return [asdict(item) for item in self._events]

    def reset(self) -> None:
        with self._lock:
            self._metrics = GraphMetricsSnapshot()
            self._events.clear()


_GRAPH_METRICS = GraphMetricsRegistry()


def get_graph_metrics_snapshot() -> dict:
    return _GRAPH_METRICS.snapshot()


def get_graph_step_events_snapshot() -> list[dict]:
    return _GRAPH_METRICS.event_snapshot()


def reset_graph_metrics() -> None:
    _GRAPH_METRICS.reset()


def record_graph_resume() -> None:
    _GRAPH_METRICS.record_resume()


def record_graph_replan() -> None:
    _GRAPH_METRICS.record_replan()


def record_graph_tie_break() -> None:
    _GRAPH_METRICS.record_tie_break()


def record_graph_analyze() -> None:
    _GRAPH_METRICS.record_analyze()


def record_graph_completed_run() -> None:
    _GRAPH_METRICS.record_completed_run()


def record_graph_step(step_name: str, elapsed_ms: float, research_id: str | None = None) -> None:
    _GRAPH_METRICS.record_step(step_name, elapsed_ms, failed=False, research_id=research_id)


def record_graph_step_failure(step_name: str, elapsed_ms: float, research_id: str | None = None) -> None:
    _GRAPH_METRICS.record_step(step_name, elapsed_ms, failed=True, research_id=research_id)

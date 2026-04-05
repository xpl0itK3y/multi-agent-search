from dataclasses import asdict, dataclass
import threading


@dataclass
class GraphMetricsSnapshot:
    resume_count: int = 0
    replan_pass_count: int = 0
    tie_break_pass_count: int = 0
    analyze_pass_count: int = 0
    completed_run_count: int = 0


class GraphMetricsRegistry:
    def __init__(self):
        self._lock = threading.Lock()
        self._metrics = GraphMetricsSnapshot()

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

    def snapshot(self) -> dict:
        with self._lock:
            return asdict(self._metrics)

    def reset(self) -> None:
        with self._lock:
            self._metrics = GraphMetricsSnapshot()


_GRAPH_METRICS = GraphMetricsRegistry()


def get_graph_metrics_snapshot() -> dict:
    return _GRAPH_METRICS.snapshot()


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

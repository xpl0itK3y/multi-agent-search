from dataclasses import asdict, dataclass
import threading


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

    def record_step(self, step_name: str, elapsed_ms: float, *, failed: bool = False) -> None:
        with self._lock:
            step = self._metrics.steps.get(step_name)
            if step is None:
                return
            step.run_count += 1
            if failed:
                step.failure_count += 1
            step.total_ms = round(step.total_ms + max(elapsed_ms, 0.0), 2)
            step.avg_ms = round(step.total_ms / step.run_count, 2) if step.run_count > 0 else 0.0

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


def record_graph_step(step_name: str, elapsed_ms: float) -> None:
    _GRAPH_METRICS.record_step(step_name, elapsed_ms, failed=False)


def record_graph_step_failure(step_name: str, elapsed_ms: float) -> None:
    _GRAPH_METRICS.record_step(step_name, elapsed_ms, failed=True)

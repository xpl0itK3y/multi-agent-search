"""AUD-016: the worker poll loop must survive a transient run_once failure, not crash."""
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_worker_module():
    spec = importlib.util.spec_from_file_location(
        "run_finalize_worker", ROOT / "scripts" / "run_finalize_worker.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_worker_loop_survives_transient_error(monkeypatch):
    mod = _load_worker_module()
    mod._shutdown.clear()
    monkeypatch.setattr(mod._shutdown, "wait", lambda *a, **k: False)  # no real sleeping

    calls = {"n": 0}

    worker = object()
    create_calls = {"n": 0}

    def fake_create_worker():
        create_calls["n"] += 1
        return worker

    def fake_run_once(active_worker):
        assert active_worker is worker
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient DB error")
        mod._shutdown.set()  # second pass: stop the loop cleanly
        return 0

    monkeypatch.setattr(mod, "_create_worker", fake_create_worker)
    monkeypatch.setattr(mod, "run_once", fake_run_once)
    monkeypatch.setattr(sys, "argv", ["run_finalize_worker.py", "--interval", "0"])

    assert mod.main() == 0  # did not propagate the exception / crash
    assert calls["n"] >= 2  # retried after the transient error instead of dying
    assert create_calls["n"] == 1  # service/engine live for the whole worker process

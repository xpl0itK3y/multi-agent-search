import threading
import time

from src.config import settings
from src.providers.rate_limit import LLMLimiter, get_llm_limiter


def test_llm_limiter_caps_concurrency(monkeypatch):
    monkeypatch.setattr(settings, "llm_max_concurrent", 2)
    monkeypatch.setattr(settings, "use_redis_broker", False)  # force in-process semaphore
    monkeypatch.setattr(settings, "llm_acquire_timeout_seconds", 5)
    limiter = LLMLimiter()

    current = 0
    peak = 0
    lock = threading.Lock()

    def worker():
        nonlocal current, peak
        with limiter.slot():
            with lock:
                current += 1
                peak = max(peak, current)
            time.sleep(0.05)
            with lock:
                current -= 1

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert peak <= 2  # never more than the cap concurrent


def test_llm_limiter_disabled_is_noop(monkeypatch):
    monkeypatch.setattr(settings, "llm_max_concurrent", 0)
    limiter = LLMLimiter()
    with limiter.slot():  # must not block or error
        pass


def test_get_llm_limiter_is_singleton():
    assert get_llm_limiter() is get_llm_limiter()

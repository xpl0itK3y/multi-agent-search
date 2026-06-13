"""Global LLM concurrency limiter.

Caps how many LLM calls run at once across ALL processes (API workers + job workers),
so a burst of researches can't hammer the provider into 429s. Backed by a leak-free
Redis sorted-set semaphore (expired holders are pruned on each acquire); falls back to
an in-process semaphore when Redis isn't configured. ``0`` disables limiting.
"""
from __future__ import annotations

import logging
import os
import random
import threading
import time
import uuid
from contextlib import contextmanager

from src.config import settings

logger = logging.getLogger(__name__)

_SEMAPHORE_KEY = "mas:llm:slots"
_TTL_MS = 300_000  # a holder older than this is assumed dead and pruned (calls cap at 120s)

# Atomically prune expired holders, then claim a slot if under the cap.
_ACQUIRE_LUA = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, tonumber(ARGV[1]) - tonumber(ARGV[2]))
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[3]) then
  redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
"""


class LLMLimiter:
    def __init__(self) -> None:
        self._cap = settings.llm_max_concurrent
        self._timeout = settings.llm_acquire_timeout_seconds
        self._redis = None
        self._script = None
        self._sema: threading.Semaphore | None = None
        if self._cap <= 0:
            return
        if settings.use_redis_broker and settings.redis_url:
            try:
                import redis

                self._redis = redis.from_url(settings.redis_url, decode_responses=True)
                self._redis.ping()
                self._script = self._redis.register_script(_ACQUIRE_LUA)
                logger.info("llm_limiter_redis cap=%d", self._cap)
            except Exception as exc:  # fall back to per-process limiting
                logger.warning("llm_limiter_redis_unavailable error=%s — using in-process semaphore", exc)
                self._redis = None
        if self._redis is None:
            self._sema = threading.Semaphore(self._cap)
            logger.info("llm_limiter_inprocess cap=%d", self._cap)

    @contextmanager
    def slot(self):
        """Hold a global slot for the duration of an LLM call (waits if at capacity)."""
        if self._cap <= 0:
            yield
            return
        if self._redis is not None:
            member = self._acquire_redis()
            try:
                yield
            finally:
                self._release_redis(member)
        else:
            acquired = self._sema.acquire(timeout=self._timeout) if self._sema else True
            if not acquired:
                logger.warning("llm_slot_wait_timeout — proceeding without a slot")
            try:
                yield
            finally:
                if acquired and self._sema:
                    self._sema.release()

    def _acquire_redis(self) -> str:
        member = f"{os.getpid()}:{uuid.uuid4().hex}"
        deadline = time.monotonic() + self._timeout
        while True:
            try:
                ok = self._script(keys=[_SEMAPHORE_KEY], args=[int(time.time() * 1000), _TTL_MS, self._cap, member])
            except Exception as exc:  # Redis hiccup — don't block the call, just proceed
                logger.warning("llm_slot_redis_error error=%s — proceeding", exc)
                return ""  # empty member → release is a no-op
            if ok == 1:
                return member
            if time.monotonic() >= deadline:
                logger.warning("llm_slot_wait_timeout — proceeding without a slot")
                return ""  # proceed anyway rather than deadlock
            time.sleep(0.2 + random.random() * 0.3)

    def _release_redis(self, member: str) -> None:
        if not member or self._redis is None:
            return
        try:
            self._redis.zrem(_SEMAPHORE_KEY, member)
        except Exception:  # best-effort — TTL prune is the safety net
            pass


_limiter: LLMLimiter | None = None
_init_lock = threading.Lock()


def get_llm_limiter() -> LLMLimiter:
    global _limiter
    if _limiter is None:
        with _init_lock:
            if _limiter is None:
                _limiter = LLMLimiter()
    return _limiter

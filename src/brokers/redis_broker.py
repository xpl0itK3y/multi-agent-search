import logging
from typing import Optional

import redis

logger = logging.getLogger(__name__)

SEARCH_JOBS_QUEUE = "mas:search_jobs"
FINALIZE_JOBS_QUEUE = "mas:finalize_jobs"

_LLM_RATE_LIMIT_LUA = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1]))
end
if count <= tonumber(ARGV[2]) then
  return 1
end
return 0
"""


class RedisBroker:
    """
    Redis-backed message broker for search and finalize job dispatch.

    Redis holds job IDs only; Postgres remains the source of truth for
    job state. Workers BLPOP a job_id from Redis, then atomically claim
    that specific job in Postgres before processing.

    If Redis is unreachable at push/pop time the call logs a warning and
    returns gracefully — workers fall back to Postgres polling.
    """

    def __init__(self, redis_url: str, pop_timeout_seconds: int = 2) -> None:
        # socket_timeout must exceed the BLPOP server-side timeout so a normal empty poll
        # doesn't trip it, while still bounding a silently-dropped connection.
        self._client = redis.from_url(
            redis_url,
            decode_responses=True,
            socket_timeout=pop_timeout_seconds + 5,
            socket_connect_timeout=3,
        )
        self._pop_timeout = pop_timeout_seconds

    # ------------------------------------------------------------------
    # Search jobs
    # ------------------------------------------------------------------

    def push_search_job(self, job_id: str) -> None:
        try:
            self._client.rpush(SEARCH_JOBS_QUEUE, job_id)
            logger.debug("redis_broker_push_search_job job_id=%s", job_id)
        except Exception as exc:
            logger.warning("redis_broker_push_search_job_failed job_id=%s error=%s", job_id, exc)

    def pop_search_job(self) -> Optional[str]:
        try:
            result = self._client.blpop(SEARCH_JOBS_QUEUE, timeout=self._pop_timeout)
            if result:
                _, job_id = result
                logger.debug("redis_broker_pop_search_job job_id=%s", job_id)
                return job_id
            return None
        except Exception as exc:
            logger.warning("redis_broker_pop_search_job_failed error=%s", exc)
            return None

    # ------------------------------------------------------------------
    # Finalize jobs
    # ------------------------------------------------------------------

    def push_finalize_job(self, job_id: str) -> None:
        try:
            self._client.rpush(FINALIZE_JOBS_QUEUE, job_id)
            logger.debug("redis_broker_push_finalize_job job_id=%s", job_id)
        except Exception as exc:
            logger.warning("redis_broker_push_finalize_job_failed job_id=%s error=%s", job_id, exc)

    def pop_finalize_job(self) -> Optional[str]:
        try:
            result = self._client.blpop(FINALIZE_JOBS_QUEUE, timeout=self._pop_timeout)
            if result:
                _, job_id = result
                logger.debug("redis_broker_pop_finalize_job job_id=%s", job_id)
                return job_id
            return None
        except Exception as exc:
            logger.warning("redis_broker_pop_finalize_job_failed error=%s", exc)
            return None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def ping(self) -> bool:
        try:
            return bool(self._client.ping())
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Live research events (pub/sub) — lets SSE wake on change instead of
    # polling Postgres every second (cuts DB load under many open streams).
    # ------------------------------------------------------------------

    @staticmethod
    def _research_channel(research_id: str) -> str:
        return f"mas:rt:{research_id}"

    def publish_research_event(self, research_id: str) -> None:
        try:
            self._client.publish(self._research_channel(research_id), "1")
        except Exception as exc:  # pub/sub is best-effort — SSE has a heartbeat fallback
            logger.warning("redis_publish_research_event_failed research_id=%s error=%s", research_id, exc)

    def research_listener(self, research_id: str):
        """A pubsub subscribed to one research's channel; caller polls get_message + closes it."""
        pubsub = self._client.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(self._research_channel(research_id))
        return pubsub

    def try_acquire_lock(self, name: str, ttl_seconds: int = 30) -> bool:
        """Best-effort single-flight lock (SET NX EX). True if acquired. On Redis error,
        returns True so a single-process deployment still runs the guarded work."""
        try:
            return bool(self._client.set(f"mas:lock:{name}", "1", nx=True, ex=ttl_seconds))
        except Exception as exc:
            logger.warning("redis_lock_failed name=%s error=%s", name, exc)
            return True

    def allow_llm_request(
        self,
        user_id: str,
        limit: int,
        window_seconds: int = 60,
    ) -> bool | None:
        """Shared fixed-window request limit. None asks the caller to use its fallback."""
        try:
            key = f"mas:llm:rate:{user_id}"
            return bool(
                self._client.eval(
                    _LLM_RATE_LIMIT_LUA,
                    1,
                    key,
                    max(1, window_seconds),
                    max(1, limit),
                )
            )
        except Exception as exc:
            logger.warning("redis_llm_rate_limit_failed user_id=%s error=%s", user_id, exc)
            return None

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

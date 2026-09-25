from contextlib import contextmanager
from datetime import datetime, timezone
import logging
import random
import threading
import time
from typing import Callable

from openai import APIConnectionError, APITimeoutError, OpenAI, RateLimitError

from src.config import settings
from src.core.llm import LLMProvider
from src.observability import get_observability_context, maybe_wrap_openai_client, observe_llm_cost
from src.providers.rate_limit import get_llm_limiter

logger = logging.getLogger(__name__)

_RETRYABLE = (RateLimitError, APIConnectionError, APITimeoutError)

# DeepSeek API pricing per 1M tokens (USD)
# Source: https://api-docs.deepseek.com/quick_start/pricing/
# Format: (input_cache_miss, input_cache_hit, output)
_DEEPSEEK_MODEL_PRICING: dict[str, dict[str, tuple[float, float, float]]] = {
    "pro": {
        "off_peak": (0.66, 0.022, 1.98),
        "peak":     (1.32, 0.044, 3.96),
    },
    "flash": {
        "off_peak": (0.15, 0.003, 0.60),
        "peak":     (0.30, 0.006, 1.20),
    },
}

# Per-call usage sink (USAGE-ACCOUNTING); bootstrap points it at the task store. It is
# called with keyword arguments: research_id and user_id as bound in the observability
# context, the model actually sent, the token counts and the unrounded cost.
LLMUsageSink = Callable[..., None]

# Legacy fallback rates for test mocks (e.g. "deepseek-test" in test_llm_cost_metrics.py)
_DEFAULT_PRICE_INPUT_PER_M = 0.14
_DEFAULT_PRICE_OUTPUT_PER_M = 1.10


def is_deepseek_peak_hours(dt: datetime | None = None) -> bool:
    """Check whether a given UTC time falls within DeepSeek's Peak hours.

    Peak hours: Monday to Friday, 01:00–04:00 and 06:00–10:00 UTC.
    All other times (including weekends) are Off-Peak (50% discount).
    """
    now = dt or datetime.now(timezone.utc)
    if now.weekday() >= 5:  # Saturday or Sunday
        return False
    return (1 <= now.hour < 4) or (6 <= now.hour < 10)


def calculate_deepseek_cost(
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
    cache_hit_tokens: int = 0,
    at_time: datetime | None = None,
) -> float:
    """Calculate the estimated USD cost of an LLM call according to DeepSeek's model-specific pricing and context caching."""
    m = (model or "").lower()
    # A reasoner (e.g. DEEPSEEK_REASONER_MODEL=deepseek-reasoner) is a reasoning model:
    # bill it at the pro tier rather than whichever tier the base model happens to use.
    if "pro" in m or "reasoner" in m:
        tier = "pro"
    elif "flash" in m or "chat" in m:
        tier = "flash"
    elif "test" in m:
        return (prompt_tokens * _DEFAULT_PRICE_INPUT_PER_M + completion_tokens * _DEFAULT_PRICE_OUTPUT_PER_M) / 1_000_000
    else:
        tier = "pro" if "pro" in settings.deepseek_model.lower() else "flash"

    period = "peak" if is_deepseek_peak_hours(at_time) else "off_peak"
    miss_rate, hit_rate, out_rate = _DEEPSEEK_MODEL_PRICING[tier][period]

    cache_hit = min(max(0, cache_hit_tokens), prompt_tokens)
    cache_miss = max(0, prompt_tokens - cache_hit)

    return (
        cache_miss * miss_rate
        + cache_hit * hit_rate
        + completion_tokens * out_rate
    ) / 1_000_000


class DeepSeekProvider(LLMProvider):

    def __init__(self, api_key: str = None, model: str = None):
        self.api_key = api_key or settings.deepseek_api_key
        self.model = model or settings.deepseek_model

        if not self.api_key:
            raise ValueError("DEEPSEEK_API_KEY is not set")

        self.client = maybe_wrap_openai_client(OpenAI(
            api_key=self.api_key,
            base_url="https://api.deepseek.com",
            timeout=120.0,
            max_retries=0,
        ))

        self._lock = threading.Lock()
        self._prompt_tokens: int = 0
        self._completion_tokens: int = 0
        self._cache_hit_tokens: int = 0
        self._cost_usd: float = 0.0
        self._usage_sink: LLMUsageSink | None = None

    # ── token tracking ────────────────────────────────────────────────────────

    @staticmethod
    def _extract_usage(usage) -> tuple[int, int, int]:
        pt = int(getattr(usage, "prompt_tokens", 0) or 0)
        ct = int(getattr(usage, "completion_tokens", 0) or 0)
        cht = getattr(usage, "prompt_cache_hit_tokens", None)
        if cht is None and hasattr(usage, "prompt_tokens_details") and usage.prompt_tokens_details:
            cht = getattr(usage.prompt_tokens_details, "cached_tokens", 0)
        return pt, ct, int(cht or 0)

    def _record_usage(
        self,
        prompt_tokens: int,
        completion_tokens: int,
        model: str,
        cache_hit_tokens: int = 0,
    ) -> None:
        cost = calculate_deepseek_cost(
            model=model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            cache_hit_tokens=cache_hit_tokens,
        )
        with self._lock:
            self._prompt_tokens     = getattr(self, "_prompt_tokens", 0) + prompt_tokens
            self._completion_tokens = getattr(self, "_completion_tokens", 0) + completion_tokens
            self._cache_hit_tokens  = getattr(self, "_cache_hit_tokens", 0) + cache_hit_tokens
            self._cost_usd          = getattr(self, "_cost_usd", 0.0) + cost
        observe_llm_cost(cost, model)
        self._emit_usage(model, prompt_tokens, completion_tokens, cache_hit_tokens, cost)

    def set_usage_sink(self, sink: LLMUsageSink | None) -> None:
        self._usage_sink = sink

    def _emit_usage(
        self,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        cache_hit_tokens: int,
        cost: float,
    ) -> None:
        """Hand one call's usage to the sink, attributed to the research/user the caller
        bound (the shared counters above cannot tell concurrent calls apart)."""
        sink = getattr(self, "_usage_sink", None)
        if sink is None:
            return
        context = get_observability_context()
        try:
            sink(
                research_id=context.get("research_id"),
                user_id=context.get("user_id"),
                model=model,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                cache_hit_tokens=cache_hit_tokens,
                estimated_cost_usd=cost,
            )
        except Exception:
            # Accounting must never fail the call it accounts for.
            logger.warning("llm_usage_record_failed model=%s", model, exc_info=True)

    @property
    def token_usage(self) -> dict:
        with self._lock:
            pt = getattr(self, "_prompt_tokens", 0)
            ct = getattr(self, "_completion_tokens", 0)
            cht = getattr(self, "_cache_hit_tokens", 0)
            cost = getattr(self, "_cost_usd", 0.0)
        return {
            "prompt_tokens":      pt,
            "completion_tokens":  ct,
            "cache_hit_tokens":   cht,
            "total_tokens":       pt + ct,
            "estimated_cost_usd": round(cost, 4),
        }

    def reset_usage(self) -> None:
        with self._lock:
            self._prompt_tokens     = 0
            self._completion_tokens = 0
            self._cache_hit_tokens  = 0
            self._cost_usd          = 0.0

    # ── generate ──────────────────────────────────────────────────────────────

    def _trusted_model_ids(self) -> set[str]:
        """Operator-configured models an internal call may name besides the user catalog
        (the reasoner, repair and red-team models). Read at call time, not import time."""
        configured = (
            self.model,
            settings.deepseek_model,
            settings.deepseek_reasoner_model,
            settings.deepseek_repair_model,
            settings.red_team_model,
        )
        return {model_id for model_id in configured if model_id}

    @contextmanager
    def _llm_slot(self, model: str):
        """The global LLM concurrency slot for one attempt. The usage the attempt reports
        (appended to the yielded list) is recorded once the slot is released: the usage sink
        writes to the database, and a slow pool or a stalled Postgres must not keep a slot
        from the next call. Reported tokens are billed, so they count even if the call
        then fails."""
        reported: list[tuple[int, int, int]] = []
        try:
            with get_llm_limiter().slot():
                yield reported
        finally:
            for prompt_tokens, completion_tokens, cache_hit_tokens in reported:
                self._record_usage(prompt_tokens, completion_tokens, model, cache_hit_tokens=cache_hit_tokens)

    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        streaming_callback=None,
        reasoning_callback=None,
        **kwargs,
    ) -> str:
        # Per-call model override (e.g. a reasoner for planning); pop so it doesn't
        # collide with the explicit model= below.
        from src.model_catalog import resolve_trusted_model_id

        requested = kwargs.pop("model", None) or self.model
        model = resolve_trusted_model_id(requested, self._trusted_model_ids())
        if model is None:
            # Neither selectable nor operator-configured (e.g. an id stored before the
            # request boundary validated it): never send it raw, but say so.
            logger.warning("deepseek_model_override_rejected requested=%r using=%s", requested, self.model)
            model = self.model
        # Streaming is also needed when we only want reasoning tokens.
        use_stream = streaming_callback is not None or reasoning_callback is not None
        if use_stream:
            # ask DeepSeek to include usage in the final streaming chunk
            kwargs.setdefault("stream_options", {"include_usage": True})

        last_exc: Exception | None = None
        max_attempts = max(1, settings.llm_retry_max_attempts)
        base_delay = settings.llm_retry_base_delay

        for attempt in range(max_attempts):
            try:
                # Hold a global concurrency slot for the whole call (incl. streaming) so a
                # burst of researches can't overrun the provider into 429s.
                with self._llm_slot(model) as reported_usage:
                    response = self.client.chat.completions.create(
                        model=model,
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user",   "content": user_prompt},
                        ],
                        stream=use_stream,
                        **kwargs,
                    )

                    if not use_stream:
                        if response.usage:
                            reported_usage.append(self._extract_usage(response.usage))
                        return response.choices[0].message.content

                    accumulated = ""
                    reasoning_accumulated = ""
                    for chunk in response:
                        # final usage chunk (stream_options include_usage)
                        if chunk.usage:
                            reported_usage.append(self._extract_usage(chunk.usage))
                        if not chunk.choices:
                            continue
                        delta = chunk.choices[0].delta
                        # Reasoning models expose a separate reasoning_content field on the delta.
                        reasoning_piece = getattr(delta, "reasoning_content", None) or ""
                        if reasoning_piece:
                            reasoning_accumulated += reasoning_piece
                            if reasoning_callback:
                                reasoning_callback(reasoning_accumulated)
                        content_piece = delta.content or ""
                        if content_piece:
                            accumulated += content_piece
                            if streaming_callback:
                                streaming_callback(accumulated)
                    return accumulated

            except _RETRYABLE as exc:
                last_exc = exc
                # Exponential backoff with jitter; longer for 429 so the provider recovers.
                wait = base_delay * (2 ** attempt) + random.uniform(0, base_delay)
                if isinstance(exc, RateLimitError):
                    wait *= 2
                logger.warning(
                    "deepseek_retryable_error attempt=%d/%d wait=%.1fs error=%s",
                    attempt + 1, max_attempts, wait, exc,
                )
                if attempt < max_attempts - 1:
                    time.sleep(wait)

        raise last_exc  # type: ignore[misc]

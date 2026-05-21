import logging
import threading
import time

from openai import APIConnectionError, APITimeoutError, OpenAI, RateLimitError

from src.config import settings
from src.core.llm import LLMProvider
from src.observability import maybe_wrap_openai_client

logger = logging.getLogger(__name__)

_RETRYABLE = (RateLimitError, APIConnectionError, APITimeoutError)
_MAX_ATTEMPTS = 3

# DeepSeek Chat pricing (USD per 1M tokens, cache-miss blended estimate)
_PRICE_INPUT_PER_M  = 0.14
_PRICE_OUTPUT_PER_M = 1.10


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

    # ── token tracking ────────────────────────────────────────────────────────

    def _record_usage(self, prompt_tokens: int, completion_tokens: int) -> None:
        with self._lock:
            self._prompt_tokens     += prompt_tokens
            self._completion_tokens += completion_tokens

    @property
    def token_usage(self) -> dict:
        with self._lock:
            pt = self._prompt_tokens
            ct = self._completion_tokens
        cost = (pt * _PRICE_INPUT_PER_M + ct * _PRICE_OUTPUT_PER_M) / 1_000_000
        return {
            "prompt_tokens":      pt,
            "completion_tokens":  ct,
            "total_tokens":       pt + ct,
            "estimated_cost_usd": round(cost, 4),
        }

    def reset_usage(self) -> None:
        with self._lock:
            self._prompt_tokens     = 0
            self._completion_tokens = 0

    # ── generate ──────────────────────────────────────────────────────────────

    def generate(self, system_prompt: str, user_prompt: str, streaming_callback=None, **kwargs) -> str:
        use_stream = streaming_callback is not None
        if use_stream:
            # ask DeepSeek to include usage in the final streaming chunk
            kwargs.setdefault("stream_options", {"include_usage": True})

        last_exc: Exception | None = None

        for attempt in range(_MAX_ATTEMPTS):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_prompt},
                    ],
                    stream=use_stream,
                    **kwargs,
                )

                if not use_stream:
                    if response.usage:
                        self._record_usage(
                            response.usage.prompt_tokens,
                            response.usage.completion_tokens,
                        )
                    return response.choices[0].message.content

                accumulated = ""
                for chunk in response:
                    # final usage chunk (stream_options include_usage)
                    if chunk.usage:
                        self._record_usage(
                            chunk.usage.prompt_tokens,
                            chunk.usage.completion_tokens,
                        )
                    delta = (chunk.choices[0].delta.content or "") if chunk.choices else ""
                    accumulated += delta
                    if delta:
                        streaming_callback(accumulated)
                return accumulated

            except _RETRYABLE as exc:
                last_exc = exc
                wait = 2 ** attempt
                logger.warning(
                    "deepseek_retryable_error attempt=%d/%d wait=%ds error=%s",
                    attempt + 1, _MAX_ATTEMPTS, wait, exc,
                )
                if attempt < _MAX_ATTEMPTS - 1:
                    time.sleep(wait)

        raise last_exc  # type: ignore[misc]

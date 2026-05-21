import logging
import time

from openai import APIConnectionError, APITimeoutError, OpenAI, RateLimitError

from src.config import settings
from src.core.llm import LLMProvider
from src.observability import maybe_wrap_openai_client

logger = logging.getLogger(__name__)

_RETRYABLE = (RateLimitError, APIConnectionError, APITimeoutError)
_MAX_ATTEMPTS = 3


class DeepSeekProvider(LLMProvider):

    def __init__(self, api_key: str = None, model: str = None):
        self.api_key = api_key or settings.deepseek_api_key
        self.model = model or settings.deepseek_model

        if not self.api_key:
            raise ValueError("DEEPSEEK_API_KEY is not set")

        self.client = maybe_wrap_openai_client(OpenAI(
            api_key=self.api_key,
            base_url="https://api.deepseek.com",
            timeout=120.0,          # hard cap: 2 min per call
            max_retries=0,          # we handle retries ourselves
        ))

    def generate(self, system_prompt: str, user_prompt: str, streaming_callback=None, **kwargs) -> str:
        use_stream = streaming_callback is not None
        last_exc: Exception | None = None

        for attempt in range(_MAX_ATTEMPTS):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    stream=use_stream,
                    **kwargs,
                )

                if not use_stream:
                    return response.choices[0].message.content

                accumulated = ""
                for chunk in response:
                    delta = (chunk.choices[0].delta.content or "") if chunk.choices else ""
                    accumulated += delta
                    if delta:
                        streaming_callback(accumulated)
                return accumulated

            except _RETRYABLE as exc:
                last_exc = exc
                wait = 2 ** attempt          # 1 s, 2 s, 4 s
                logger.warning(
                    "deepseek_retryable_error attempt=%d/%d wait=%ds error=%s",
                    attempt + 1, _MAX_ATTEMPTS, wait, exc,
                )
                if attempt < _MAX_ATTEMPTS - 1:
                    time.sleep(wait)

        raise last_exc  # type: ignore[misc]

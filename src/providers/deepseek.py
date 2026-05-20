import os
from openai import OpenAI
from src.core.llm import LLMProvider
from src.config import settings
from src.observability import maybe_wrap_openai_client

class DeepSeekProvider(LLMProvider):
    
    def __init__(self, api_key: str = None, model: str = None):
        self.api_key = api_key or settings.deepseek_api_key
        self.model = model or settings.deepseek_model
        
        if not self.api_key:
            raise ValueError("DEEPSEEK_API_KEY is not set")
        
        self.client = maybe_wrap_openai_client(OpenAI(
            api_key=self.api_key,
            base_url="https://api.deepseek.com"
        ))

    def generate(self, system_prompt: str, user_prompt: str, streaming_callback=None, **kwargs) -> str:
        use_stream = streaming_callback is not None
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

import os
from openai import OpenAI
from src.core.llm import LLMProvider
from src.config import settings

class DeepSeekProvider(LLMProvider):
    
    def __init__(self, api_key: str = None, model: str = None):
        self.api_key = api_key or settings.deepseek_api_key
        self.model = model or settings.deepseek_model
        
        if not self.api_key:
            raise ValueError("DEEPSEEK_API_KEY is not set")
        
        self.client = OpenAI(
            api_key=self.api_key,
            base_url="https://api.deepseek.com"
        )

    def generate(self, system_prompt: str, user_prompt: str, **kwargs) -> str:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            stream=False,
            **kwargs
        )
        return response.choices[0].message.content

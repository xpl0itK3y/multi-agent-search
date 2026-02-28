from abc import ABC, abstractmethod
from typing import Any, Dict, List

class LLMProvider(ABC):
    
    @abstractmethod
    def generate(self, system_prompt: str, user_prompt: str, **kwargs) -> str:
        pass

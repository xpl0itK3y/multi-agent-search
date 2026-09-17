from abc import ABC, abstractmethod
from typing import Callable, Optional

class LLMProvider(ABC):

    @abstractmethod
    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        streaming_callback: Optional[Callable[[str], None]] = None,
        **kwargs,
    ) -> str:
        pass

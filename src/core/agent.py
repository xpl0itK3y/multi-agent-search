from abc import ABC, abstractmethod
from .llm import LLMProvider

class BaseAgent(ABC):
    
    def __init__(self, llm: LLMProvider):
        self.llm = llm

    @abstractmethod
    def run(self, input_data: str) -> str:
        pass

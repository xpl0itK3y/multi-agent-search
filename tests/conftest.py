import pytest
from fastapi.testclient import TestClient
from src.api.app import app
from src.core.llm import LLMProvider

class MockLLMProvider(LLMProvider):
    def generate(self, system_prompt: str, user_prompt: str, **kwargs) -> str:
        return f"Optimized: {user_prompt}"

@pytest.fixture
def mock_llm():
    return MockLLMProvider()

@pytest.fixture
def client():
    return TestClient(app)

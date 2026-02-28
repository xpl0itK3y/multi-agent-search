import pytest
from src.agents.optimizer import PromptOptimizerAgent

def test_optimizer_agent_run(mock_llm):
    agent = PromptOptimizerAgent(mock_llm)
    user_input = "test prompt"
    result = agent.run(user_input)
    
    assert "Optimized: test prompt" in result
    assert agent.SYSTEM_PROMPT is not None

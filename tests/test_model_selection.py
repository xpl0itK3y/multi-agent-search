from src import model_catalog
from src.agents.analyzer import AnalyzerAgent


class _StubLLM:
    """Records the `model` kwarg each generate() call receives."""

    def __init__(self):
        self.models: list = []

    def generate(self, system_prompt, user_prompt, streaming_callback=None, reasoning_callback=None, **kwargs):
        self.models.append(kwargs.get("model"))
        return "## Introduction\nText [S1].\n## Conclusion\nDone [S1]."


def test_resolve_model_id_rejects_unknown_and_falls_back():
    assert model_catalog.resolve_model_id("evil-model", "deepseek-v4-pro") == "deepseek-v4-pro"
    assert model_catalog.resolve_model_id("deepseek-v4-flash", "deepseek-v4-pro") == "deepseek-v4-flash"
    assert model_catalog.resolve_model_id(None, "deepseek-v4-pro") == "deepseek-v4-pro"


def test_catalog_exposes_default_and_is_serializable():
    models = model_catalog.list_models()
    ids = {m["id"] for m in models}
    assert any(m["default"] for m in models)
    assert model_catalog.DEFAULT_MODEL_ID in ids
    assert "deepseek-v4-flash" in ids
    assert "deepseek-chat" in ids  # fast chat model is selectable
    assert model_catalog.resolve_model_id("deepseek-chat", "deepseek-v4-pro") == "deepseek-chat"


def test_analyzer_threads_selected_model_to_llm():
    llm = _StubLLM()
    analyzer = AnalyzerAgent(llm)
    analyzer._generate_report(
        {"original_prompt": "x", "gathered_data": [{"source_id": "S1", "content": "c", "url": "http://e.com"}]},
        "en",
        model="deepseek-v4-flash",
    )
    assert llm.models == ["deepseek-v4-flash"]


def test_analyzer_passes_none_model_when_unset():
    llm = _StubLLM()
    analyzer = AnalyzerAgent(llm)
    analyzer._generate_report({"original_prompt": "x", "gathered_data": []}, "en")
    assert llm.models == [None]

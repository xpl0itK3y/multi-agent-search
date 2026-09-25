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
    assert model_catalog.resolve_model_id("deepseek-v4.1-flash", "deepseek-v4-pro") == "deepseek-flash"
    assert model_catalog.resolve_model_id("deepseek-v4-flash", "deepseek-v4-pro") == "deepseek-flash"
    assert model_catalog.resolve_model_id("deepseek-flash", "deepseek-v4-pro") == "deepseek-flash"
    assert model_catalog.resolve_model_id(None, "deepseek-v4-pro") == "deepseek-v4-pro"


def test_catalog_exposes_default_and_is_serializable():
    models = model_catalog.list_models()
    ids = {m["id"] for m in models}
    assert any(m["default"] for m in models)
    assert model_catalog.DEFAULT_MODEL_ID in ids
    assert "deepseek-flash" in ids
    assert "deepseek-chat" in ids  # fast chat model is selectable
    assert model_catalog.resolve_model_id("deepseek-chat", "deepseek-v4-pro") == "deepseek-chat"


def test_analyzer_threads_selected_model_to_llm():
    llm = _StubLLM()
    analyzer = AnalyzerAgent(llm)
    analyzer._generate_report(
        {"original_prompt": "x", "gathered_data": [{"source_id": "S1", "content": "c", "url": "http://e.com"}]},
        "en",
        model="deepseek-flash",
    )
    assert llm.models == ["deepseek-flash"]


def test_analyzer_passes_none_model_when_unset():
    llm = _StubLLM()
    analyzer = AnalyzerAgent(llm)
    analyzer._generate_report({"original_prompt": "x", "gathered_data": []}, "en")
    assert llm.models == [None]


class _StubCompletions:
    """Stands in for client.chat.completions: records the model each call is sent."""

    def __init__(self):
        self.models: list[str] = []

    def create(self, model, messages, stream, **kwargs):
        from types import SimpleNamespace

        self.models.append(model)
        usage = SimpleNamespace(prompt_tokens=10, completion_tokens=5, prompt_cache_hit_tokens=0)
        message = SimpleNamespace(content="ok")
        return SimpleNamespace(usage=usage, choices=[SimpleNamespace(message=message)])


def _provider_with_stub_client(model="deepseek-v4-pro"):
    from types import SimpleNamespace

    from src.providers.deepseek import DeepSeekProvider

    provider = DeepSeekProvider(api_key="sk-test", model=model)
    completions = _StubCompletions()
    provider.client = SimpleNamespace(chat=SimpleNamespace(completions=completions))
    return provider, completions


def test_provider_sends_operator_configured_models_as_is(monkeypatch):
    # PROVIDER-MODEL: the reasoner/repair settings are not user-selectable, so the old
    # catalog-only check silently replaced them with the base model.
    from src.config import settings

    monkeypatch.setattr(settings, "deepseek_reasoner_model", "deepseek-reasoner")
    monkeypatch.setattr(settings, "deepseek_repair_model", "deepseek-fast-repair")
    provider, completions = _provider_with_stub_client()

    provider.generate("sys", "user", model="deepseek-reasoner")
    provider.generate("sys", "user", model="deepseek-fast-repair")
    provider.generate("sys", "user", model=settings.red_team_model)

    assert completions.models == ["deepseek-reasoner", "deepseek-fast-repair", "deepseek-chat"]


def test_provider_keeps_catalog_canonicalization_and_rejects_unknown_ids(monkeypatch, caplog):
    from src.config import settings

    monkeypatch.setattr(settings, "deepseek_reasoner_model", None)
    monkeypatch.setattr(settings, "deepseek_repair_model", None)
    provider, completions = _provider_with_stub_client()

    provider.generate("sys", "user", model="deepseek-v4.1-flash")  # stored alias
    with caplog.at_level("WARNING", logger="src.providers.deepseek"):
        provider.generate("sys", "user", model="evil-model")
    provider.generate("sys", "user")

    assert completions.models == ["deepseek-flash", "deepseek-v4-pro", "deepseek-v4-pro"]
    assert "deepseek_model_override_rejected" in caplog.text
    assert "evil-model" in caplog.text


def test_provider_trusts_settings_read_at_call_time(monkeypatch):
    from src.config import settings

    provider, completions = _provider_with_stub_client()
    monkeypatch.setattr(settings, "deepseek_repair_model", None)
    provider.generate("sys", "user", model="deepseek-late-repair")
    monkeypatch.setattr(settings, "deepseek_repair_model", "deepseek-late-repair")
    provider.generate("sys", "user", model="deepseek-late-repair")

    assert completions.models == ["deepseek-v4-pro", "deepseek-late-repair"]


def test_operator_models_do_not_become_user_selectable(monkeypatch):
    from src.config import settings

    monkeypatch.setattr(settings, "deepseek_reasoner_model", "deepseek-reasoner")
    assert "deepseek-reasoner" not in {m["id"] for m in model_catalog.list_models()}
    assert model_catalog.resolve_model_id("deepseek-reasoner", "deepseek-v4-pro") == "deepseek-v4-pro"

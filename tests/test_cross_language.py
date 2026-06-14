import json

from src.agents.cross_language import CrossLanguageAgent, detect_language
from src.core.llm import LLMProvider


class _StubLLM(LLMProvider):
    def __init__(self, payload):
        self.payload = payload

    def generate(self, system_prompt, user_prompt, **kwargs):
        return self.payload


def test_detect_language_by_script_and_words():
    assert detect_language("The quick brown fox jumps over the lazy dog and runs") == "en"
    assert detect_language("Это исследование показывает, что для здоровья это важно") == "ru"
    assert detect_language("El estudio muestra que para la salud esto es importante con datos") == "es"
    assert detect_language("Diese Studie zeigt, dass das für die Gesundheit wichtig ist und mehr") == "de"
    assert detect_language("这项研究表明对健康很重要") == "zh"
    assert detect_language("この研究は健康にとって重要であることを示しています") == "ja"
    assert detect_language("이 연구는 건강에 중요하다는 것을 보여줍니다") == "ko"
    assert detect_language("هذه الدراسة تظهر أنها مهمة للصحة") == "ar"
    assert detect_language("") == "unknown"


def test_plan_picks_languages_and_queries():
    payload = json.dumps({"languages": ["zh", "de", "en"], "queries": ["中文查询", "Deutsche Anfrage", "english"]})
    langs, queries = CrossLanguageAgent(_StubLLM(payload)).plan("Is X regulated?", "en", max_targets=2)
    assert langs == ["zh", "de"] and queries == ["中文查询", "Deutsche Anfrage"]  # capped to max_targets


def test_plan_empty_when_nothing_helps():
    langs, queries = CrossLanguageAgent(_StubLLM('{"languages": [], "queries": []}')).plan("q", "en")
    assert langs == [] and queries == []
    assert CrossLanguageAgent(None).plan("q", "en") == ([], [])


def test_surface_extracts_findings():
    payload = json.dumps({"findings": [
        {"lang": "zh", "finding": "Chinese regulator banned it in 2023"},
        {"lang": "de", "finding": "German study found no effect"},
        {"bad": "no finding key"},
    ]})
    out = CrossLanguageAgent(_StubLLM(payload)).surface("q", "en", {"zh": ["snippet"], "de": ["snippet"]})
    assert len(out) == 2 and out[0].lang == "zh" and "regulator" in out[0].finding


def test_surface_safe_on_garbage():
    assert CrossLanguageAgent(_StubLLM("not json")).surface("q", "en", {"zh": ["x"]}) == []
    assert CrossLanguageAgent(_StubLLM("{}")).surface("q", "en", {}) == []  # no foreign sources


def test_injected_task_is_a_valid_search_task():
    # The decompose loop calls SearchTask(**task_dict) / accesses task_dict['id'] — the injected
    # cross-language task must carry id + status + queries, or decompose fails (regression).
    from src.api.schemas import ResearchRequest, SearchDepth, SearchTask
    from src.repositories.in_memory_task_store import InMemoryTaskStore
    from src.services.research_service import ResearchService

    class _XL:
        def plan(self, prompt, lang, max_targets):
            return ["de"], ["Deutsche Anfrage zur Regulierung"]

    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store, cross_language_agent=_XL())
    rec = store.add_research(ResearchRequest(prompt="How does Germany regulate Sunday shopping?", depth=SearchDepth.EASY), task_ids=[])
    tasks_raw: list = []
    svc._maybe_add_cross_language_task(rec.id, "How does Germany regulate Sunday shopping?", tasks_raw)

    assert len(tasks_raw) == 1
    task = SearchTask(**{**tasks_raw[0], "research_id": rec.id})  # must not raise / KeyError 'id'
    assert task.id and task.queries == ["Deutsche Anfrage zur Regulierung"]
    assert store.get_research(rec.id).graph_state.get("cross_language_targets") == ["de"]

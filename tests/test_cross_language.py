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

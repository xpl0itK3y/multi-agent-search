import json

from src.agents.cross_language import CrossLanguageAgent, detect_language, dominant_script, language_script
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


def test_start_research_persists_query_language_once():
    from src.api.schemas import ResearchRequest, SearchDepth
    from src.repositories.in_memory_task_store import InMemoryTaskStore
    from src.services.research_service import ResearchService

    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)
    _response, research_id = service.start_research(
        ResearchRequest(
            prompt="这项研究表明对健康很重要",
            depth=SearchDepth.EASY,
        )
    )

    research = store.get_research(research_id)
    assert research is not None
    assert research.language == "zh"
    assert service._research_language(research) == "zh"
    graph_state = service.finalize_graph_runner._build_initial_state(
        research.id,
        research.prompt,
        [],
        research.depth,
    )
    assert graph_state["language"] == "zh"


def test_stored_query_language_is_not_inferred_from_report_content():
    from src.api.schemas import ResearchRequest, SearchDepth
    from src.repositories.in_memory_task_store import InMemoryTaskStore
    from src.services.research_service import ResearchService

    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="What changed?", depth=SearchDepth.EASY),
        task_ids=[],
        language="en",
    )
    research.final_report = "Русский заголовок источника"

    assert ResearchService(task_store=store)._research_language(research) == "en"


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
    rec = store.add_research(
        ResearchRequest(
            prompt="How does Germany regulate Sunday shopping?",
            depth=SearchDepth.EASY,
        ),
        task_ids=[],
        language="en",
    )
    tasks_raw: list = []
    svc._maybe_add_cross_language_task(rec.id, "How does Germany regulate Sunday shopping?", tasks_raw)

    assert len(tasks_raw) == 1
    task = SearchTask(**{**tasks_raw[0], "research_id": rec.id})  # must not raise / KeyError 'id'
    assert task.id and task.queries == ["Deutsche Anfrage zur Regulierung"]
    assert store.get_research(rec.id).graph_state.get("cross_language_targets") == ["de"]


def test_han_text_with_a_stray_kana_is_chinese():
    text = "北京 天气 预报：今天 下雨，气温 较低，风力 の 三级，空气 质量 良好，适合 出行"
    assert detect_language(text) == "zh"
    assert detect_language(text, strict=True) == "unknown"  # a few kana: zh or kanji-heavy ja
    assert detect_language("日本の電気自動車の販売台数は2024年に増加した", strict=True) == "ja"
    assert detect_language("这项研究表明对健康很重要", strict=True) == "zh"


def test_strict_detection_answers_only_when_confident():
    # No hint words: the default guess is 'en'.
    assert detect_language("Récord histórico en Argentina: 211 % anual según INDEC") == "en"
    assert detect_language("Récord histórico en Argentina: 211 % anual según INDEC", strict=True) == "unknown"
    # es/pt share que/para/como/por: a win on shared words is a tie-break.
    spanish_by_pt_hints = "Reportaje de que para por turismo de playa que para por verano de costa."
    assert detect_language(spanish_by_pt_hints) == "pt"
    assert detect_language(spanish_by_pt_hints, strict=True) == "unknown"
    assert detect_language("Per capita income con la data from the regional office", strict=True) == "unknown"
    assert detect_language("Short English text for the check", strict=True) == "unknown"  # too few words
    for text, expected in [
        ("The quick brown fox jumps over the lazy dog and runs into the forest with the others", "en"),
        ("El estudio muestra que para la salud esto es importante con datos del ministerio y los hospitales", "es"),
        ("Diese Studie zeigt, dass das für die Gesundheit wichtig ist und mehr Menschen sich damit befassen", "de"),
        ("O estudo mostra que a saúde é importante para uma vida com mais qualidade e não depende dos genes", "pt"),
        ("Le rapport montre que les prix des logements sont en hausse dans les villes et pour les familles", "fr"),
        ("Это исследование показывает, что для здоровья это важно", "ru"),
    ]:
        assert detect_language(text, strict=True) == expected == detect_language(text)


def test_strict_detection_needs_a_clearly_dominant_script():
    mixed = "Отчёт Report отчёт report данные data рынок market"
    assert detect_language(mixed, strict=True) == "unknown"
    assert dominant_script(mixed) is None
    assert dominant_script("Market transitioning through scaling phase.") == "latin"
    assert dominant_script("日本の電気自動車") == "cjk"
    assert language_script("ja") == language_script("zh") == "cjk"
    assert language_script("es") == "latin" and language_script("ru") == "cyrillic"
    assert language_script("unknown") is None

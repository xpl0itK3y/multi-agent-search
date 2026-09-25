from src.agents.citation_audit import CitationAuditAgent


def test_citation_audit_flags_matching_and_mismatched():
    report = (
        "Quantum computers use qubits and superposition to process information [S1]. "
        "The economy of Brazil grew rapidly last year [S2]."
    )
    sources = {
        "S1": {
            "content": "Qubits exploit superposition to represent states; quantum computers process information this way.",
            "url": "https://a.com",
            "title": "Qubits",
        },
        "S2": {"content": "A recipe for chocolate cake with flour and sugar.", "url": "https://b.com", "title": "Cake"},
    }
    audit = CitationAuditAgent().audit(report, sources)
    assert audit.total == 2
    assert audit.supported == 1
    assert 0.4 < audit.integrity < 0.6
    assert any("Brazil" in c or "economy" in c for c in audit.unsupported_claims)
    g1 = next(g for g in audit.grounding if g.source_id == "S1")
    assert g1.supported and "superposition" in g1.quote.lower()


def test_citation_audit_empty_inputs():
    assert CitationAuditAgent().audit("", {}).total == 0
    assert CitationAuditAgent().audit("No citations here at all.", {"S1": {"content": "x"}}).total == 0


def test_citation_audit_skips_too_short_claims():
    audit = CitationAuditAgent().audit("Yes [S1].", {"S1": {"content": "no match here whatsoever"}})
    assert audit.total == 0


def test_citation_audit_unknown_source_id_ignored():
    audit = CitationAuditAgent().audit(
        "This claim cites a source that does not exist in the pool [S9].",
        {"S1": {"content": "unrelated content about something"}},
    )
    assert audit.total == 0  # S9 not in pool -> nothing to check


def test_citation_audit_credits_pooled_synthesis():
    # No single source covers the whole synthesis bullet, but together they do.
    report = "Fasting improves insulin sensitivity, reduces arterial pressure, and preserves muscle mass [S1][S2][S3]."
    sources = {
        "S1": {"content": "Studies reported better insulin outcomes overall."},
        "S2": {"content": "Pressure readings fell during the period."},
        "S3": {"content": "Muscle was largely retained throughout."},
    }
    audit = CitationAuditAgent().audit(report, sources)
    assert audit.total == 1 and audit.supported == 1  # one claim, grounded by the pooled sources


def test_citation_audit_still_flags_fabrication_across_all_sources():
    # None of the cited sources mention the claim's terms -> still unsupported.
    report = "The Roman Empire collapsed because of dietary fasting trends [S1][S2]."
    sources = {"S1": {"content": "A guide to baking sourdough bread."}, "S2": {"content": "Tips for indoor gardening."}}
    audit = CitationAuditAgent().audit(report, sources)
    assert audit.total == 1 and audit.supported == 0


def test_citation_audit_cross_language_supported_via_anchors():
    # A Russian claim citing an English source is backed by shared anchors (names + numbers).
    report = "OpenAI готовит GPT-5.6 с контекстным окном 1500000 токенов к июню 2026 года [S1]."
    sources = {"S1": {"content": "OpenAI is preparing GPT-5.6 with a 1500000 token context window, expected June 2026."}}
    audit = CitationAuditAgent().audit(report, sources)
    assert audit.supported == 1 and audit.total == 1
    assert audit.unsupported_claims == []


def test_citation_audit_cross_language_unverifiable_is_not_flagged():
    # Russian prose citing an English source with no shared anchors -> 'unverified', never a red flag.
    report = "Системы находятся на переходном этапе развития и масштабирования всей отрасли [S1]."
    sources = {"S1": {"content": "The market is transitioning through a scaling phase across the industry."}}
    audit = CitationAuditAgent().audit(report, sources)
    assert audit.unverified == 1
    assert audit.total == 0  # nothing verifiable
    assert audit.unsupported_claims == []  # honest: not falsely flagged as fabricated


def test_english_report_listing_a_russian_title_still_flags_weak_citations():
    # One Cyrillic word anywhere (here a source title) used to mark the report Russian,
    # which made every English source "foreign" and forced its grounding to supported.
    report = (
        "The economy of Brazil grew rapidly last year across every sector [S1].\n\n"
        "## Sources\n- [S1] Рецепт шоколадного торта"
    )
    sources = {"S1": {"content": "A recipe for chocolate cake with flour and sugar.", "url": "https://b.com"}}
    audit = CitationAuditAgent().audit(report, sources, language="en")
    assert audit.supported == 0 and audit.total == 1
    assert next(g for g in audit.grounding if g.source_id == "S1").supported is False


def test_spanish_report_citing_an_english_source_is_unverified_not_flagged():
    report = "Los sistemas atraviesan una etapa de transición y escalamiento en toda la industria [S1]."
    # Clearly English (a same-script source is foreign only on a confident detection).
    sources = {
        "S1": {
            "content": (
                "The market is transitioning through a scaling phase across the industry, and that "
                "shift comes from demand for storage with this kind of capacity."
            )
        }
    }
    audit = CitationAuditAgent().audit(report, sources, language="es")
    assert audit.unverified == 1 and audit.total == 0
    assert audit.unsupported_claims == []
    assert audit.grounding[0].supported is True  # foreign source: never red-flagged


def test_spanish_report_citing_an_english_source_is_supported_via_name_anchors():
    report = "La empresa OpenAI prepara GPT-5.6 con una ventana de 1500000 tokens para 2026 [S1]."
    sources = {"S1": {"content": "OpenAI is preparing GPT-5.6 with a 1500000 token context window for 2026."}}
    audit = CitationAuditAgent().audit(report, sources, language="es")
    assert audit.supported == 1 and audit.total == 1


def test_same_language_citation_gets_no_foreign_exemption():
    report = "La economía de Brasil creció rápidamente el año pasado en todos los sectores [S1]."
    sources = {"S1": {"content": "Una receta de pastel de chocolate con harina y azúcar para la familia."}}
    audit = CitationAuditAgent().audit(report, sources, language="es")
    assert audit.supported == 0 and audit.total == 1
    assert audit.grounding[0].supported is False


def test_chinese_report_citing_an_english_source_is_not_red_flagged():
    report = "该系统正在经历过渡阶段，行业规模扩大，影响深远，前景广阔 [S1]。"
    sources = {"S1": {"content": "The market is transitioning through a scaling phase across the industry."}}
    audit = CitationAuditAgent().audit(report, sources, language="zh")
    assert audit.unverified == 1 and audit.unsupported_claims == []
    assert audit.grounding[0].supported is True  # was only exempted for Cyrillic reports


def test_citation_audit_uses_the_stored_research_language(mocker):
    from src.api.schemas import ResearchRequest, SearchDepth
    from src.repositories import InMemoryTaskStore
    from src.services import ResearchService

    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="Как растёт экономика Бразилии?", depth=SearchDepth.EASY),
        task_ids=[],
        language="es",
    )
    service = ResearchService(task_store=store)
    spy = mocker.spy(service.citation_auditor, "audit")

    service._audit_citations(
        "Texto del informe con una cita [S1].", research, [], aggregated=[{"source_id": "S1", "content": "x"}]
    )

    assert spy.call_args.kwargs["language"] == "es"


# ── foreign only on a confident detection (C7-2) ───────────────────────────────


def test_spanish_report_citing_an_unrelated_short_spanish_snippet_is_flagged():
    # No es hint words: the default detector calls it 'en', which made it foreign to the
    # Spanish report, so the fabricated citation came out 'unverified' and grounded.
    report = "La inflación mensual bajó de forma sostenida durante el último trimestre del año [S1]."
    sources = {"S1": {"content": "Récord histórico en Argentina: 211 % anual según INDEC."}}
    audit = CitationAuditAgent().audit(report, sources, language="es")
    assert audit.total == 1 and audit.supported == 0 and audit.unverified == 0
    assert audit.unsupported_claims
    assert audit.grounding[0].supported is False


def test_spanish_report_citing_a_spanish_snippet_with_portuguese_hint_words_is_flagged():
    # 'de/que/para/por' score higher for pt than for es; that is a tie-break, not a detection.
    report = "El gobierno aprobó un nuevo presupuesto para la educación pública del país [S1]."
    sources = {"S1": {"content": "Reportaje de que para por turismo de playa que para por verano de costa."}}
    audit = CitationAuditAgent().audit(report, sources, language="es")
    assert audit.total == 1 and audit.supported == 0
    assert audit.grounding[0].supported is False


def test_english_report_citing_an_english_snippet_with_italian_looking_words_is_flagged():
    report = "The central bank raised interest rates twice during the last quarter of the year [S1]."
    sources = {"S1": {"content": "Per capita income con la data from the regional statistics office."}}
    audit = CitationAuditAgent().audit(report, sources, language="en")
    assert audit.total == 1 and audit.supported == 0
    assert audit.grounding[0].supported is False


def test_chinese_report_citing_a_chinese_source_with_one_kana_is_flagged():
    # One stray の made the whole Chinese source 'ja', foreign to the zh report.
    report = "中国 电动汽车 销量 大幅 增长 [S1]"
    sources = {"S1": {"content": "北京 天气 预报：今天 下雨，气温 较低，风力 の 三级，空气 质量 良好，适合 出行"}}
    audit = CitationAuditAgent().audit(report, sources, language="zh")
    assert audit.total == 1 and audit.supported == 0 and audit.unverified == 0
    assert audit.grounding[0].supported is False


def test_russian_report_citing_a_short_english_snippet_is_still_exempt():
    # The language of a hint-less Latin snippet is unsure, but its script is not Cyrillic.
    report = "Системы находятся на переходном этапе развития и масштабирования всей отрасли [S1]."
    sources = {"S1": {"content": "Market transitioning through scaling phase."}}
    audit = CitationAuditAgent().audit(report, sources, language="ru")
    assert audit.unverified == 1 and audit.unsupported_claims == []
    assert audit.grounding[0].supported is True


# ── German nouns and the foreign grounding flag (C7-4) ─────────────────────────

_SIEMENS_EN = (
    "Siemens said that its revenue in China for 2024 reached 78 billion euros, and the growth "
    "came from strong demand for automation."
)


def test_german_report_citing_the_english_source_that_states_the_claim_is_supported():
    # German capitalises every noun: Umsatz, Milliarden and Nachfrage were taken for names,
    # never occur in the English source, and pushed the claim to 'no'.
    report = "Siemens erzielte 2024 in China einen Umsatz von 78 Milliarden Euro dank hoher Nachfrage [S1]."
    audit = CitationAuditAgent().audit(report, {"S1": {"content": _SIEMENS_EN}}, language="de")
    assert audit.total == 1 and audit.supported == 1
    assert audit.unsupported_claims == []
    assert audit.grounding[0].supported is True


def test_english_report_still_uses_capitalised_words_as_name_anchors():
    anchors = CitationAuditAgent._latin_name_anchors(
        "Revenue of Siemens in China reached 78 billion", {"revenue", "siemens", "china", "reached", "billion"}
    )
    assert anchors == {"siemens", "china"}
    german = CitationAuditAgent._latin_name_anchors(
        "Laut Bericht von OpenAI stieg der Umsatz mit GPT-5 stark",
        {"laut", "bericht", "von", "openai", "stieg", "der", "umsatz", "mit", "gpt-5", "stark"},
        capitalised_nouns=True,
    )
    assert german == {"openai", "gpt-5"}  # inner capitals and digits still count


def test_foreign_source_disproved_on_its_anchors_is_not_shown_as_grounded():
    # The claim was listed as unsupported while its citation hover said 'grounded'.
    report = "La empresa Nvidia vendió millones de chips GPU H100 durante 2023 en Europa [S1]."
    sources = {
        "S1": {
            "content": (
                "The weather in London was rainy for the whole week, and the forecast for this "
                "weekend is similar with more wind from the north."
            )
        }
    }
    audit = CitationAuditAgent().audit(report, sources, language="es")
    assert audit.total == 1 and audit.supported == 0 and audit.unsupported_claims
    assert audit.grounding[0].supported is False


def test_foreign_source_cited_by_one_disproved_and_one_unjudgeable_claim_stays_grounded():
    report = (
        "La empresa Nvidia vendió millones de chips GPU H100 durante 2023 en Europa [S1].\n"
        "Los sistemas atraviesan una etapa de transición y escalamiento en toda la industria [S1]."
    )
    sources = {
        "S1": {
            "content": (
                "The weather in London was rainy for the whole week, and the forecast for this "
                "weekend is similar with more wind from the north."
            )
        }
    }
    audit = CitationAuditAgent().audit(report, sources, language="es")
    assert audit.unverified == 1 and len(audit.unsupported_claims) == 1
    assert audit.grounding[0].supported is True  # not every citing claim was disproved

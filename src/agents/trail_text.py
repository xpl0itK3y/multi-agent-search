"""Localized ``detail`` text for progress-trail events (``graph_trail``).

A trail event carries stable machine keys (``step`` / ``agent`` / ``phase`` / ``action``)
that the frontend keys its labels and loop-back badges on, plus a human ``detail``
sentence. That sentence is shown in the live console and the audit trail, and the last
finalize-graph details are copied into the report itself, so it is written in the
research's stored language (``research.language``) instead of one hard-coded language.

Loop-back events (the finalize graph going back for more evidence) are identified by
their step/action codes, never by their text: ``replan`` / ``gap_analysis_loop``,
``tie_break`` / ``conflict_tie_break`` and ``verify_retry`` / ``critic_revision_loop``.
"""

from __future__ import annotations

from typing import Any

from src.agents.cross_language import detect_language

TRAIL_LANGUAGES = ("en", "ru", "es")

_YES_NO = {"en": ("yes", "no"), "ru": ("да", "нет"), "es": ("sí", "no")}

TRAIL_DETAILS: dict[str, dict[str, str]] = {
    # ── planning ────────────────────────────────────────────────────────────
    "plan_start": {
        "en": "Analyzing the research topic and framing the research tasks",
        "ru": "Анализ темы исследования и постановка исследовательских задач",
        "es": "Análisis del tema de investigación y planteamiento de las tareas",
    },
    "clarify": {
        "en": "Clarifying questions prepared to focus the research: {count}",
        "ru": "Сформировано уточняющих вопросов для фокуса исследования: {count}",
        "es": "Preguntas aclaratorias para enfocar la investigación: {count}",
    },
    "decompose": {
        "en": "Breaking the topic down into research directions (depth: {depth})",
        "ru": "Декомпозиция темы на направления (глубина: {depth})",
        "es": "Descomposición del tema en líneas de investigación (profundidad: {depth})",
    },
    "plan_review": {
        "en": "Plan drafted, items: {count}. Waiting for approval.",
        "ru": "Сформирован черновик плана, пунктов: {count}. Ожидание утверждения.",
        "es": "Borrador del plan listo, puntos: {count}. Esperando aprobación.",
    },
    "plan_ready": {
        "en": "Plan approved (search tasks: {count}), parallel data collection started",
        "ru": "План утвержден (поисковых задач: {count}), запущен параллельный сбор данных",
        "es": "Plan aprobado (tareas de búsqueda: {count}), comenzó la recopilación paralela de datos",
    },
    "cross_language": {
        "en": "Multilingual expansion: searching in {languages}",
        "ru": "Мультиязычное расширение: поиск на {languages}",
        "es": "Ampliación multilingüe: búsqueda en {languages}",
    },
    # ── search ──────────────────────────────────────────────────────────────
    "search_task_start": {
        "en": "Starting direction: {task}",
        "ru": "Запуск направления: {task}",
        "es": "Inicio de la línea de investigación: {task}",
    },
    "search_query": {
        "en": "Search: “{query}” — pages found: {count}",
        "ru": "Поиск: «{query}» — найдено страниц: {count}",
        "es": "Búsqueda: «{query}» — páginas encontradas: {count}",
    },
    "search_scrape": {
        "en": "Reading: {domain} ({chars} chars)",
        "ru": "Сканирование: {domain} ({chars} симв.)",
        "es": "Lectura: {domain} ({chars} caracteres)",
    },
    "search_task_complete": {
        "en": "Direction “{task}” done, sources selected: {count}",
        "ru": "Направление «{task}» обработано, отобрано источников: {count}",
        "es": "Línea «{task}» completada, fuentes seleccionadas: {count}",
    },
    # ── finalize graph: step starts ─────────────────────────────────────────
    "collect_context": {
        "en": "Assessing source credibility, structuring the evidence and finding gaps",
        "ru": "Оценка достоверности источников, структурирование доказательств и выявление белых пятен",
        "es": "Evaluación de la credibilidad de las fuentes, estructuración de la evidencia y detección de lagunas",
    },
    "replan": {
        "en": "↩ Gaps found in the data: going back to search for more sources",
        "ru": "↩ Обнаружены пробелы в данных: возврат на допоиск источников для полноты картины",
        "es": "↩ Se detectaron lagunas en los datos: vuelta a buscar más fuentes",
    },
    "analyze": {
        "en": "Synthesizing the report in depth: consolidating facts and marking citations",
        "ru": "Глубокий синтез аналитического отчёта, сведение фактов и разметка цитат",
        "es": "Síntesis profunda del informe: consolidación de hechos y marcado de citas",
    },
    "tie_break": {
        "en": "↩ Sources contradict each other: starting a tie-break search",
        "ru": "↩ Обнаружены противоречия между источниками: запуск арбитражного поиска (Tie-Break)",
        "es": "↩ Las fuentes se contradicen: inicio de una búsqueda de desempate",
    },
    "tie_break_conflicts": {
        "en": "↩ Contradictions found between sources ({count}): starting a tie-break search",
        "ru": "↩ Обнаружены противоречия в источниках ({count}): запуск арбитражного поиска (Tie-Break)",
        "es": "↩ Contradicciones entre fuentes ({count}): inicio de una búsqueda de desempate",
    },
    "verify": {
        "en": "Verifying the report's claims, checking citation accuracy and reviewing the draft",
        "ru": "Верификация утверждений отчёта, контроль точности цитирования и рецензирование",
        "es": "Verificación de las afirmaciones del informe, control de las citas y revisión del borrador",
    },
    "verify_retry": {
        "en": "↩ The reviewer sent the report back to AnalyzerAgent: fixing weak spots and strengthening the evidence",
        "ru": "↩ Рецензент вернул отчёт на доработку в AnalyzerAgent: устранение слабых мест и усиление доказательств",
        "es": "↩ El revisor devolvió el informe a AnalyzerAgent: corrección de puntos débiles y refuerzo de la evidencia",
    },
    # ── finalize graph: checkpoints (a step finished) ───────────────────────
    "collect_context_done": {
        "en": "Sources collected: {count}; follow-up search needed: {replan}",
        "ru": "Собрано источников: {count}; нужен допоиск: {replan}",
        "es": "Fuentes recopiladas: {count}; búsqueda adicional necesaria: {replan}",
    },
    "replan_done": {
        "en": "Follow-up tasks created: {tasks} (recommendations: {recommendations})",
        "ru": "Создано дополнительных задач: {tasks} (рекомендаций: {recommendations})",
        "es": "Tareas adicionales creadas: {tasks} (recomendaciones: {recommendations})",
    },
    "analyze_done": {
        "en": "Analysis pass {attempt} completed",
        "ru": "Проход анализа {attempt} завершён",
        "es": "Pasada de análisis {attempt} completada",
    },
    "tie_break_done": {
        "en": "Tie-break tasks created: {tasks} (recommendations: {recommendations})",
        "ru": "Создано задач арбитражного поиска: {tasks} (рекомендаций: {recommendations})",
        "es": "Tareas de desempate creadas: {tasks} (recomendaciones: {recommendations})",
    },
    "verify_done": {
        "en": "Review: weak support: {weak_support}; conflicts: {conflicts}; revision: {retry}; tie-break: {tie_break}",
        "ru": "Рецензия: слабая доказательная база: {weak_support}; противоречий: {conflicts}; доработка: {retry}; арбитражный поиск: {tie_break}",
        "es": "Revisión: respaldo débil: {weak_support}; contradicciones: {conflicts}; revisión adicional: {retry}; desempate: {tie_break}",
    },
    "complete": {
        "en": "Finalize graph completed, analysis passes: {count}",
        "ru": "Граф финализации завершён, проходов анализа: {count}",
        "es": "Grafo de finalización completado, pasadas de análisis: {count}",
    },
    "stale_recovered": {
        "en": "Finalize job {job_id} recovered after a timeout; resuming after step {step}",
        "ru": "Задача финализации {job_id} восстановлена после тайм-аута; продолжение после шага {step}",
        "es": "Tarea de finalización {job_id} recuperada tras un tiempo de espera; se reanuda tras el paso {step}",
    },
    # ── trust suite ─────────────────────────────────────────────────────────
    "redteam": {
        "en": "Analyzing counter-arguments and stress-testing the hypotheses",
        "ru": "Анализ контраргументов и стресс-тестирование гипотез",
        "es": "Análisis de contraargumentos y prueba de estrés de las hipótesis",
    },
    "audit": {
        "en": "Auditing citations, fact-checking and verifying sources",
        "ru": "Аудит цитат, фактчекинг и проверка источников",
        "es": "Auditoría de citas, verificación de hechos y comprobación de fuentes",
    },
    "independence": {
        "en": "Source independence check — independent clusters: {count}",
        "ru": "Проверка независимости источников — независимых кластеров: {count}",
        "es": "Comprobación de independencia de las fuentes — grupos independientes: {count}",
    },
    "reputation": {
        "en": "Assessing the academic and expert authority of the sources",
        "ru": "Оценка академического и экспертного авторитета источников",
        "es": "Evaluación de la autoridad académica y experta de las fuentes",
    },
    # {total}: figures checked against their cited source; {supported}: found there.
    "numeric_check": {
        "en": "Numeric fact cross-check — figures found in their cited source: {supported} of {total}",
        "ru": "Кросс-проверка численных фактов — показателей найдено в указанном источнике: {supported} из {total}",
        "es": "Verificación cruzada de datos numéricos — cifras halladas en su fuente citada: {supported} de {total}",
    },
    "viewpoints": {
        "en": "Weighing the balance of viewpoints and detecting bias",
        "ru": "Оценка баланса точек зрения и выявление предвзятости",
        "es": "Evaluación del equilibrio de puntos de vista y detección de sesgos",
    },
    "cross_language_analysis": {
        "en": "Comparing foreign and local sources (in other languages: {count})",
        "ru": "Сравнение зарубежных и локальных источников (иноязычных: {count})",
        "es": "Comparación de fuentes extranjeras y locales (en otros idiomas: {count})",
    },
    "completed": {
        "en": "Research complete, the final analytical report is ready",
        "ru": "Исследование завершено, итоговый аналитический отчёт готов",
        "es": "Investigación completada, el informe analítico final está listo",
    },
}


def trail_language(language: str | None) -> str:
    """The text-table language for ``language``: ru/en/es as stored, English otherwise."""
    return language if language in TRAIL_LANGUAGES else "en"


def trail_detail(key: str, language: str | None, **params: Any) -> str:
    """Render the ``key`` detail sentence in ``language``; booleans become a localized yes/no."""
    lang = trail_language(language)
    yes, no = _YES_NO[lang]
    values = {
        name: (yes if value else no) if isinstance(value, bool) else value
        for name, value in params.items()
    }
    return TRAIL_DETAILS[key][lang].format(**values)


def research_language(research) -> str:
    """The language stored on the research at creation, detected from the prompt for
    legacy rows stored as 'unknown'."""
    language = getattr(research, "language", None)
    if isinstance(language, str) and language and language != "unknown":
        return language
    return detect_language(getattr(research, "prompt", "") or "")

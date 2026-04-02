import os
from datetime import datetime, timezone
from html import escape
from typing import Any

import httpx
import streamlit as st
from src.api.schemas import SearchDepth
from src.search_depth_profiles import SEARCH_DEPTH_PROFILES, get_depth_profile


API_BASE_URL = os.getenv("STREAMLIT_API_BASE_URL", "http://localhost:8000").rstrip("/")
REQUEST_TIMEOUT_SECONDS = 30.0
WORKER_NAME = os.getenv("STREAMLIT_WORKER_NAME", "job-worker")
LANGUAGE_OPTIONS = {"en": "English", "ru": "Русский"}
TRANSLATIONS = {
    "en": {
        "research_console": "Research Console",
        "title": "Multi-Agent Search",
        "header_caption": "Run research flows, inspect queue state, and operate the worker/runtime from one screen.",
        "control": "Control",
        "language": "Interface Language",
        "api": "API",
        "research_id": "Research ID",
        "research_id_placeholder": "Paste or launch a research ID",
        "auto_refresh": "Auto refresh",
        "refresh_interval": "Refresh interval (sec)",
        "refresh_now": "Refresh now",
        "worker": "Worker",
        "no_heartbeat": "No heartbeat found for `{worker_name}`.",
        "processed_jobs": "Processed jobs: {count}",
        "extraction_attempts": "Extraction attempts: {count}",
        "extraction_success": "Extraction success: {count}",
        "extraction_failures": "Extraction failures: {count}",
        "extraction_success_rate": "Extraction success rate: {value}%",
        "extraction_avg_total_ms": "Avg extraction total: {value} ms",
        "last_seen": "Last seen: {timestamp}",
        "start_research": "Start Research",
        "research_prompt": "Research prompt",
        "research_placeholder": "Compare the latest battery storage trends, costs, and deployment constraints.",
        "search_level": "Search Level",
        "launch_research": "Launch Research",
        "research_prompt_required": "Research prompt is required.",
        "research_created": "Research created: {research_id}",
        "quick_scan": "Quick Scan",
        "balanced": "Balanced",
        "deep_dive": "Deep Dive",
        "profile_caption": "{description} About {task_count} search tasks and up to {source_limit} sources per task.",
        "queue_overview": "Queue Overview",
        "queue_backlog": "Queue Backlog",
        "queue_extraction_attempts": "Extraction Attempts",
        "queue_extraction_success": "Extraction Success",
        "queue_extraction_failures": "Extraction Failures",
        "queue_extraction_success_rate": "Extraction Success Rate",
        "queue_extraction_avg_total_ms": "Avg Extraction Total",
        "pending_search": "Pending Search",
        "running_search": "Running Search",
        "dead_search": "Dead Search",
        "pending_finalize": "Pending Finalize",
        "running_finalize": "Running Finalize",
        "dead_finalize": "Dead Finalize",
        "research_details": "Research Details",
        "no_research_selected": "Create a research job or paste an existing research ID in the sidebar.",
        "task_pipeline": "Task Pipeline",
        "task_filter": "Task Filter",
        "task_filter_placeholder": "Filter tasks by description, query, log, or ID",
        "task_status_filter": "Task Status Filter",
        "all_statuses": "All statuses",
        "no_tasks": "No tasks found for this research yet.",
        "final_report": "Final Report",
        "final_report_not_ready": "Final report is not ready yet.",
        "rendered": "Rendered",
        "raw_markdown": "Raw Markdown",
        "task_slots": "Task Slots",
        "completed_tasks": "Completed Tasks",
        "collected_sources_metric": "Collected Sources",
        "avg_sources_per_task": "Avg Sources / Task",
        "collected_sources": "Collected sources: {count}",
        "task_extraction_summary": "Extraction: {success}/{attempts} succeeded, {failures} failed, {selected} selected, avg chars {avg_chars}",
        "queries": "Queries",
        "recent_logs": "Recent Logs",
        "selected_sources": "Selected Sources",
        "no_sources_yet": "No sources collected for this task yet.",
        "open_source": "Open Source",
        "refresh_research": "Refresh Research",
        "enqueue_finalize": "Enqueue Finalize",
        "finalize_job_queued": "Finalize job queued: {job_id}",
        "latest_finalize_job": "Latest Finalize Job",
        "no_finalize_job": "No finalize job has been created for this research yet.",
        "finalize_running": "Finalize is running. The report will appear automatically after refresh.",
        "finalize_pending": "Finalize is queued and waiting for the worker.",
        "auto_refresh_enabled_finalize": "Auto refresh enabled while finalize is running.",
        "queue_actions": "Queue Actions",
        "recover_stale_search": "Recover Stale Search",
        "recover_stale_finalize": "Recover Stale Finalize",
        "cleanup_search_jobs": "Cleanup Search Jobs",
        "cleanup_finalize_jobs": "Cleanup Finalize Jobs",
        "run_full_maintenance": "Run Full Maintenance",
        "recovered_stale_search_jobs": "Recovered stale search jobs",
        "recovered_stale_finalize_jobs": "Recovered stale finalize jobs",
        "cleaned_search_jobs": "Cleaned search jobs",
        "cleaned_finalize_jobs": "Cleaned finalize jobs",
        "queue_maintenance": "Queue maintenance",
        "operational_view": "Operational View",
        "job_filter": "Job Filter",
        "job_filter_placeholder": "Filter by job, task, research, or error text",
        "running_search_jobs": "Running Search Jobs",
        "dead_letter_search_jobs": "Dead-Letter Search Jobs",
        "running_finalize_jobs": "Running Finalize Jobs",
        "dead_letter_finalize_jobs": "Dead-Letter Finalize Jobs",
        "no_jobs_bucket": "No jobs in this bucket.",
        "attempts": "Attempts: {attempt_count} / {max_attempts}",
        "updated": "Updated: {timestamp}",
        "task_id": "Task ID: {value}",
        "research_job_id": "Research ID: {value}",
        "error": "Error: {error}",
        "requeue_job": "Requeue {job_id}",
        "requeued_job": "Requeued {job_kind} job",
        "untitled_source": "Untitled source",
        "unknown_domain": "unknown-domain",
        "unknown_quality": "unknown-quality",
        "unknown_extraction": "unknown-extraction",
        "search_job_line": "Search job: {job_id} | attempts {attempt_count}/{max_attempts} | updated {updated_at}",
        "prompt": "Prompt",
        "created": "Created: {timestamp}",
        "research_status": "Research Status",
        "depth": "Depth: {value}",
        "profile": "Profile: {value}",
        "tasks": "Tasks: {count}",
        "target_breadth": "Target search breadth: about {task_count} tasks, up to {source_limit} sources per task",
        "already_queued": "already queued",
        "api_error": "API error {status_code}: {detail}",
        "connection_error": "Connection error: {error}",
        "worker_live": "Worker is active now.",
        "worker_stale": "Heartbeat looks stale. Long-running jobs may still be processing.",
        "worker_idle_ok": "Worker is idle.",
        "progress": "Progress",
        "progress_caption": "{completed}/{total} tasks completed",
        "all_tasks_completed": "All search tasks are complete. Finalize can be started.",
        "finalize_waiting": "Finalize is unavailable until all search tasks finish.",
        "task_status_breakdown": "Task status breakdown",
        "pending_tasks": "Pending",
        "running_tasks": "Running",
        "failed_tasks": "Failed",
        "completed_tasks_short": "Completed",
        "finalize_ready": "Finalize Ready",
        "yes": "Yes",
        "no": "No",
        "disable_finalize_reason": "Finalize is blocked until all tasks are in `completed` or `failed` state.",
        "showing_sources_preview": "Showing {shown} of {total} sources",
        "requeue_dead_search_job": "Requeue Dead Search Job",
    },
    "ru": {
        "research_console": "Консоль исследований",
        "title": "Мультиагентный поиск",
        "header_caption": "Запускайте research flow, смотрите состояние очередей и управляйте runtime с одного экрана.",
        "control": "Управление",
        "language": "Язык интерфейса",
        "api": "API",
        "research_id": "Research ID",
        "research_id_placeholder": "Вставьте существующий или новый research ID",
        "auto_refresh": "Автообновление",
        "refresh_interval": "Интервал обновления (сек)",
        "refresh_now": "Обновить сейчас",
        "worker": "Воркер",
        "no_heartbeat": "Heartbeat для `{worker_name}` не найден.",
        "processed_jobs": "Обработано задач: {count}",
        "extraction_attempts": "Попыток extraction: {count}",
        "extraction_success": "Успешных extraction: {count}",
        "extraction_failures": "Ошибок extraction: {count}",
        "extraction_success_rate": "Успешность extraction: {value}%",
        "extraction_avg_total_ms": "Средний total extraction: {value} мс",
        "last_seen": "Последняя активность: {timestamp}",
        "start_research": "Запуск исследования",
        "research_prompt": "Исследовательский запрос",
        "research_placeholder": "Сравни последние тренды в battery storage, стоимость и ограничения внедрения.",
        "search_level": "Уровень поиска",
        "launch_research": "Запустить исследование",
        "research_prompt_required": "Нужно заполнить исследовательский запрос.",
        "research_created": "Исследование создано: {research_id}",
        "quick_scan": "Быстрый проход",
        "balanced": "Сбалансированный",
        "deep_dive": "Глубокий поиск",
        "profile_caption": "{description} Примерно {task_count} search tasks и до {source_limit} источников на задачу.",
        "queue_overview": "Обзор очередей",
        "queue_backlog": "Общий backlog",
        "queue_extraction_attempts": "Попытки extraction",
        "queue_extraction_success": "Успешный extraction",
        "queue_extraction_failures": "Ошибки extraction",
        "queue_extraction_success_rate": "Успешность extraction",
        "queue_extraction_avg_total_ms": "Средний total extraction",
        "pending_search": "Search в ожидании",
        "running_search": "Search в работе",
        "dead_search": "Search в dead-letter",
        "pending_finalize": "Finalize в ожидании",
        "running_finalize": "Finalize в работе",
        "dead_finalize": "Finalize в dead-letter",
        "research_details": "Детали исследования",
        "no_research_selected": "Создай research job или вставь существующий research ID в sidebar.",
        "task_pipeline": "Пайплайн задач",
        "task_filter": "Фильтр задач",
        "task_filter_placeholder": "Фильтр по описанию, query, логам или ID",
        "task_status_filter": "Фильтр статуса задач",
        "all_statuses": "Все статусы",
        "no_tasks": "Для этого research задачи пока не найдены.",
        "final_report": "Финальный отчет",
        "final_report_not_ready": "Финальный отчет пока не готов.",
        "rendered": "Рендер",
        "raw_markdown": "Исходный Markdown",
        "task_slots": "Слоты задач",
        "completed_tasks": "Завершенные задачи",
        "collected_sources_metric": "Собранные источники",
        "avg_sources_per_task": "Среднее источников / задача",
        "collected_sources": "Собрано источников: {count}",
        "task_extraction_summary": "Extraction: успешно {success}/{attempts}, ошибок {failures}, выбрано {selected}, средний размер {avg_chars}",
        "queries": "Запросы",
        "recent_logs": "Последние логи",
        "selected_sources": "Выбранные источники",
        "no_sources_yet": "Для этой задачи источники пока не собраны.",
        "open_source": "Открыть источник",
        "refresh_research": "Обновить исследование",
        "enqueue_finalize": "Поставить finalize в очередь",
        "finalize_job_queued": "Finalize job поставлена: {job_id}",
        "latest_finalize_job": "Последняя finalize job",
        "no_finalize_job": "Для этого research еще не создавалась finalize job.",
        "finalize_running": "Finalize выполняется. Отчет появится автоматически после обновления.",
        "finalize_pending": "Finalize поставлен в очередь и ждет воркер.",
        "auto_refresh_enabled_finalize": "Автообновление включено на время finalize.",
        "queue_actions": "Действия с очередями",
        "recover_stale_search": "Восстановить stale search",
        "recover_stale_finalize": "Восстановить stale finalize",
        "cleanup_search_jobs": "Очистить search jobs",
        "cleanup_finalize_jobs": "Очистить finalize jobs",
        "run_full_maintenance": "Запустить полное обслуживание",
        "recovered_stale_search_jobs": "Восстановлены stale search jobs",
        "recovered_stale_finalize_jobs": "Восстановлены stale finalize jobs",
        "cleaned_search_jobs": "Очищены search jobs",
        "cleaned_finalize_jobs": "Очищены finalize jobs",
        "queue_maintenance": "Обслуживание очередей",
        "operational_view": "Операционный режим",
        "job_filter": "Фильтр jobs",
        "job_filter_placeholder": "Фильтр по job, task, research или тексту ошибки",
        "running_search_jobs": "Search jobs в работе",
        "dead_letter_search_jobs": "Search jobs в dead-letter",
        "running_finalize_jobs": "Finalize jobs в работе",
        "dead_letter_finalize_jobs": "Finalize jobs в dead-letter",
        "no_jobs_bucket": "В этой категории задач нет.",
        "attempts": "Попытки: {attempt_count} / {max_attempts}",
        "updated": "Обновлено: {timestamp}",
        "task_id": "Task ID: {value}",
        "research_job_id": "Research ID: {value}",
        "error": "Ошибка: {error}",
        "requeue_job": "Перепоставить {job_id}",
        "requeued_job": "Перепоставлена {job_kind} job",
        "untitled_source": "Источник без названия",
        "unknown_domain": "неизвестный-домен",
        "unknown_quality": "неизвестное-качество",
        "unknown_extraction": "неизвестный-статус-извлечения",
        "search_job_line": "Search job: {job_id} | попытки {attempt_count}/{max_attempts} | обновлено {updated_at}",
        "prompt": "Промпт",
        "created": "Создано: {timestamp}",
        "research_status": "Статус исследования",
        "depth": "Глубина: {value}",
        "profile": "Профиль: {value}",
        "tasks": "Задачи: {count}",
        "target_breadth": "Целевая ширина поиска: около {task_count} задач, до {source_limit} источников на задачу",
        "already_queued": "уже в очереди",
        "api_error": "Ошибка API {status_code}: {detail}",
        "connection_error": "Ошибка соединения: {error}",
        "worker_live": "Воркер сейчас активен.",
        "worker_stale": "Heartbeat выглядит устаревшим. Долгие jobs могут все еще выполняться.",
        "worker_idle_ok": "Воркер сейчас простаивает.",
        "progress": "Прогресс",
        "progress_caption": "Завершено задач: {completed}/{total}",
        "all_tasks_completed": "Все search tasks завершены. Можно запускать finalize.",
        "finalize_waiting": "Finalize недоступен, пока search tasks не завершатся.",
        "task_status_breakdown": "Срез по статусам задач",
        "pending_tasks": "В ожидании",
        "running_tasks": "В работе",
        "failed_tasks": "С ошибкой",
        "completed_tasks_short": "Завершено",
        "finalize_ready": "Finalize готов",
        "yes": "Да",
        "no": "Нет",
        "disable_finalize_reason": "Finalize заблокирован, пока все задачи не перейдут в `completed` или `failed`.",
        "showing_sources_preview": "Показано {shown} из {total} источников",
        "requeue_dead_search_job": "Перепоставить dead search job",
    },
}


def _t(key: str, **kwargs) -> str:
    language = st.session_state.get("ui_language", "en")
    template = TRANSLATIONS.get(language, TRANSLATIONS["en"]).get(key, key)
    return template.format(**kwargs)


def _get_client() -> httpx.Client:
    return httpx.Client(base_url=API_BASE_URL, timeout=REQUEST_TIMEOUT_SECONDS)


def _api_get(path: str) -> Any:
    with _get_client() as client:
        response = client.get(path)
        response.raise_for_status()
        return response.json()


def _api_post(path: str, payload: dict | None = None) -> Any:
    with _get_client() as client:
        response = client.post(path, json=payload)
        response.raise_for_status()
        return response.json()


def _render_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(20, 184, 166, 0.12), transparent 24%),
                radial-gradient(circle at top right, rgba(59, 130, 246, 0.10), transparent 22%),
                linear-gradient(180deg, #f8fafc 0%, #eef2f7 100%);
            color: #0f172a;
        }
        h1, h2, h3 {
            font-family: Georgia, "Times New Roman", serif;
            letter-spacing: -0.02em;
        }
        .stMarkdown, .stText, .stCaption, label, p, li, div[data-testid="stMetricValue"] {
            font-family: "Trebuchet MS", "Segoe UI", sans-serif;
        }
        [data-testid="stMain"] label,
        [data-testid="stMain"] p,
        [data-testid="stMain"] li,
        [data-testid="stMain"] span,
        [data-testid="stMain"] div {
            color: #0f172a;
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0f172a 0%, #111827 100%);
        }
        [data-testid="stSidebar"] * {
            color: #e5eefb;
        }
        .mas-panel {
            border: 1px solid rgba(148, 163, 184, 0.18);
            background: rgba(255, 255, 255, 0.92);
            border-radius: 18px;
            padding: 1rem 1.1rem;
            box-shadow: 0 16px 40px rgba(15, 23, 42, 0.08);
            margin-bottom: 0.8rem;
        }
        .mas-accent {
            color: #0f766e;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-size: 0.75rem;
        }
        .mas-status {
            display: inline-block;
            padding: 0.2rem 0.55rem;
            border-radius: 999px;
            font-size: 0.8rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }
        .mas-status.pending { background: #fef3c7; color: #92400e; }
        .mas-status.running, .mas-status.analyzing, .mas-status.busy, .mas-status.processing {
            background: #dbeafe; color: #1d4ed8;
        }
        .mas-status.completed, .mas-status.idle { background: #dcfce7; color: #166534; }
        .mas-status.failed, .mas-status.dead_letter, .mas-status.error {
            background: #fee2e2; color: #b91c1c;
        }
        .mas-kv {
            color: #475569;
            font-size: 0.92rem;
            margin-top: 0.25rem;
        }
        .mas-source {
            border-top: 1px solid rgba(148, 163, 184, 0.18);
            padding-top: 0.8rem;
            margin-top: 0.8rem;
        }
        div[data-testid="stButton"] > button,
        div[data-testid="stDownloadButton"] > button,
        div[data-testid="stLinkButton"] a {
            background: linear-gradient(135deg, #0f766e 0%, #0f4c81 100%) !important;
            color: #f8fafc !important;
            border: 1px solid rgba(15, 118, 110, 0.22) !important;
            border-radius: 12px !important;
            font-weight: 700 !important;
            box-shadow: 0 8px 20px rgba(15, 118, 110, 0.16);
        }
        div[data-testid="stButton"] > button:hover,
        div[data-testid="stDownloadButton"] > button:hover,
        div[data-testid="stLinkButton"] a:hover {
            background: linear-gradient(135deg, #115e59 0%, #1d4ed8 100%) !important;
            color: #ffffff !important;
            border-color: rgba(29, 78, 216, 0.30) !important;
        }
        div[data-testid="stButton"] > button p,
        div[data-testid="stDownloadButton"] > button p,
        div[data-testid="stLinkButton"] a p,
        div[data-testid="stButton"] > button span,
        div[data-testid="stDownloadButton"] > button span,
        div[data-testid="stLinkButton"] a span,
        div[data-testid="stButton"] > button div,
        div[data-testid="stDownloadButton"] > button div,
        div[data-testid="stLinkButton"] a div,
        div[data-testid="stButton"] > button *,
        div[data-testid="stDownloadButton"] > button *,
        div[data-testid="stLinkButton"] a * {
            color: #f8fafc !important;
            fill: #f8fafc !important;
            -webkit-text-fill-color: #f8fafc !important;
        }
        button[kind="primary"],
        button[kind="secondary"],
        button[data-testid="baseButton-primary"],
        button[data-testid="baseButton-secondary"] {
            color: #f8fafc !important;
        }
        button[kind="primary"] *,
        button[kind="secondary"] *,
        button[data-testid="baseButton-primary"] *,
        button[data-testid="baseButton-secondary"] * {
            color: #f8fafc !important;
            fill: #f8fafc !important;
            -webkit-text-fill-color: #f8fafc !important;
        }
        div[data-testid="stTextInput"] input,
        div[data-testid="stTextArea"] textarea,
        div[data-testid="stSelectbox"] div[data-baseweb="select"] > div,
        div[data-testid="stNumberInput"] input {
            background: rgba(255, 255, 255, 0.96) !important;
            color: #0f172a !important;
            border: 1px solid rgba(148, 163, 184, 0.35) !important;
        }
        div[data-testid="stMetric"] {
            background: rgba(255, 255, 255, 0.88);
            border: 1px solid rgba(148, 163, 184, 0.18);
            border-radius: 16px;
            padding: 0.8rem 0.9rem;
        }
        div[data-testid="stMetric"] label,
        div[data-testid="stMetric"] p,
        div[data-testid="stMetric"] span,
        div[data-testid="stMetric"] div {
            color: #0f172a !important;
        }
        div[data-testid="stMetricValue"] {
            color: #0f172a !important;
        }
        div[data-testid="stMetricLabel"] {
            color: #475569 !important;
        }
        div[data-testid="stAlert"] {
            background: rgba(219, 234, 254, 0.92) !important;
            border: 1px solid rgba(96, 165, 250, 0.22) !important;
        }
        div[data-testid="stAlert"] * {
            color: #0f172a !important;
        }
        div[data-testid="stExpander"] summary {
            background: #111827 !important;
            border-radius: 12px !important;
            padding: 0.4rem 0.75rem !important;
        }
        div[data-testid="stExpander"] summary *,
        div[data-testid="stExpander"] summary span,
        div[data-testid="stExpander"] summary p,
        div[data-testid="stExpander"] summary svg {
            color: #f8fafc !important;
            fill: #f8fafc !important;
        }
        div[data-testid="stTextArea"] label,
        div[data-testid="stSelectbox"] label,
        div[data-testid="stTextInput"] label,
        div[data-testid="stSlider"] label,
        div[data-testid="stCheckbox"] label {
            color: #0f172a !important;
            font-weight: 600;
        }
        div[data-testid="stSelectbox"] div[data-baseweb="select"] span,
        div[data-testid="stSelectbox"] div[data-baseweb="select"] div,
        div[data-testid="stTextArea"] textarea,
        div[data-testid="stTextInput"] input {
            color: #0f172a !important;
        }
        div[data-testid="stCodeBlock"] pre,
        div[data-testid="stCode"] pre {
            background: #1e293b !important;
            color: #e2e8f0 !important;
        }
        div[data-testid="stCodeBlock"] pre *,
        div[data-testid="stCode"] pre *,
        div[data-testid="stCodeBlock"] code,
        div[data-testid="stCode"] code,
        div[data-testid="stCodeBlock"] span,
        div[data-testid="stCode"] span {
            color: #e2e8f0 !important;
            fill: #e2e8f0 !important;
            -webkit-text-fill-color: #e2e8f0 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _status_badge(status: str | None) -> str:
    label = (status or "unknown").strip().lower()
    return f'<span class="mas-status {label}">{escape(label)}</span>'


def _format_timestamp(value: str | None) -> str:
    if not value:
        return "-"
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return value
    return parsed.strftime("%Y-%m-%d %H:%M:%S UTC")


def _is_recent_timestamp(value: str | None, threshold_seconds: int = 45) -> bool:
    if not value:
        return False
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - parsed).total_seconds() <= threshold_seconds


def _truncate(value: str | None, limit: int = 220) -> str:
    if not value:
        return ""
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit - 3]}..."


def _task_source_count(task: dict) -> int:
    if "result_count" in task:
        return int(task.get("result_count") or 0)
    return len(task.get("result") or [])


def _research_source_count(tasks: list[dict]) -> int:
    return sum(_task_source_count(task) for task in tasks)


def _safe_api_call(method, *args, ignore_status_codes: set[int] | None = None, **kwargs) -> Any | None:
    ignore_status_codes = ignore_status_codes or set()
    try:
        return method(*args, **kwargs)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in ignore_status_codes:
            return None
        detail = exc.response.text
        st.error(_t("api_error", status_code=exc.response.status_code, detail=detail))
    except httpx.HTTPError as exc:
        st.error(_t("connection_error", error=exc))
    return None


def _initialize_state() -> None:
    query_research_id = st.query_params.get("research_id", "")
    query_language = st.query_params.get("lang", "en")
    if isinstance(query_research_id, list):
        query_research_id = query_research_id[0] if query_research_id else ""
    if isinstance(query_language, list):
        query_language = query_language[0] if query_language else "en"

    st.session_state.setdefault("selected_research_id", query_research_id)
    st.session_state.setdefault("ui_language", query_language if query_language in LANGUAGE_OPTIONS else "en")
    st.session_state.setdefault("auto_refresh_enabled", False)
    st.session_state.setdefault("auto_refresh_seconds", 10)


def _sync_query_params() -> None:
    research_id = st.session_state.get("selected_research_id", "").strip()
    language = st.session_state.get("ui_language", "en")
    if research_id:
        st.query_params["research_id"] = research_id
    elif "research_id" in st.query_params:
        del st.query_params["research_id"]
    st.query_params["lang"] = language


def _render_auto_refresh() -> None:
    return


def _get_live_refresh_interval() -> int | None:
    selected_research_id = st.session_state.get("selected_research_id", "").strip()
    if selected_research_id:
        return 5
    if st.session_state.get("auto_refresh_enabled"):
        return max(int(st.session_state.get("auto_refresh_seconds", 10)), 3)
    return None


def _render_live_queue_fragment(run_every: int | None) -> None:
    @st.fragment(run_every=run_every)
    def _fragment() -> None:
        _render_queue_overview()

    _fragment()


def _render_live_research_fragment(run_every: int | None) -> None:
    @st.fragment(run_every=run_every)
    def _fragment() -> None:
        _render_research_details()

    _fragment()


def _render_header() -> None:
    st.markdown(f'<div class="mas-accent">{escape(_t("research_console"))}</div>', unsafe_allow_html=True)
    st.title(_t("title"))
    st.caption(_t("header_caption"))


def _render_sidebar() -> None:
    st.sidebar.header(_t("control"))
    st.session_state["ui_language"] = st.sidebar.selectbox(
        _t("language"),
        options=list(LANGUAGE_OPTIONS.keys()),
        index=list(LANGUAGE_OPTIONS.keys()).index(st.session_state.get("ui_language", "en")),
        format_func=lambda value: LANGUAGE_OPTIONS[value],
    )
    _sync_query_params()
    st.sidebar.caption(f"{_t('api')}: `{API_BASE_URL}`")

    selected_research_id = st.sidebar.text_input(
        _t("research_id"),
        value=st.session_state.get("selected_research_id", ""),
        placeholder=_t("research_id_placeholder"),
    ).strip()
    st.session_state["selected_research_id"] = selected_research_id
    _sync_query_params()

    st.sidebar.divider()
    st.session_state["auto_refresh_enabled"] = st.sidebar.checkbox(
        _t("auto_refresh"),
        value=st.session_state.get("auto_refresh_enabled", False),
    )
    st.session_state["auto_refresh_seconds"] = st.sidebar.slider(
        _t("refresh_interval"),
        min_value=3,
        max_value=30,
        value=int(st.session_state.get("auto_refresh_seconds", 10)),
        disabled=not st.session_state["auto_refresh_enabled"],
    )
    if st.sidebar.button(_t("refresh_now"), use_container_width=True):
        st.rerun()

    st.sidebar.divider()
    st.sidebar.subheader(_t("worker"))
    heartbeat = _safe_api_call(
        _api_get,
        f"/health/workers/{WORKER_NAME}",
        ignore_status_codes={404},
    )
    if heartbeat is None:
        st.sidebar.info(_t("no_heartbeat", worker_name=WORKER_NAME))
        return

    st.sidebar.markdown(_status_badge(heartbeat["status"]), unsafe_allow_html=True)
    st.sidebar.caption(_t("processed_jobs", count=heartbeat["processed_jobs"]))
    extraction_metrics = heartbeat.get("extraction_metrics") or {}
    st.sidebar.caption(_t("extraction_attempts", count=extraction_metrics.get("attempts", 0)))
    st.sidebar.caption(_t("extraction_success", count=extraction_metrics.get("success_count", 0)))
    st.sidebar.caption(_t("extraction_failures", count=extraction_metrics.get("failure_count", 0)))
    st.sidebar.caption(_t("extraction_success_rate", value=extraction_metrics.get("success_rate_percent", 0.0)))
    st.sidebar.caption(_t("extraction_avg_total_ms", value=extraction_metrics.get("avg_total_ms", 0.0)))
    st.sidebar.caption(_t("last_seen", timestamp=_format_timestamp(heartbeat["last_seen_at"])))
    heartbeat_is_recent = _is_recent_timestamp(heartbeat.get("last_seen_at"))
    heartbeat_status = (heartbeat.get("status") or "").strip().lower()
    if heartbeat_is_recent:
        st.sidebar.success(_t("worker_live"))
    elif heartbeat_status == "busy":
        st.sidebar.warning(_t("worker_stale"))
    else:
        st.sidebar.info(_t("worker_idle_ok"))
    if heartbeat.get("last_error"):
        st.sidebar.error(heartbeat["last_error"])


def _render_create_research() -> None:
    st.subheader(_t("start_research"))
    with st.form("start_research_form", clear_on_submit=False):
        prompt = st.text_area(
            _t("research_prompt"),
            height=140,
            placeholder=_t("research_placeholder"),
        )
        depth_options = [SearchDepth.EASY.value, SearchDepth.MEDIUM.value, SearchDepth.HARD.value]
        depth = st.selectbox(
            _t("search_level"),
            options=depth_options,
            index=1,
            format_func=lambda value: (
                f"{_t(SEARCH_DEPTH_PROFILES[SearchDepth(value)]['label'].lower().replace(' ', '_'))} ({value})"
            ),
        )
        profile = get_depth_profile(SearchDepth(depth))
        st.caption(
            _t(
                "profile_caption",
                description=profile["description"],
                task_count=profile["task_count"],
                source_limit=profile["source_limit"],
            )
        )
        submitted = st.form_submit_button(_t("launch_research"), use_container_width=True)

    if not submitted:
        return

    payload = {"prompt": prompt.strip(), "depth": depth}
    if not payload["prompt"]:
        st.warning(_t("research_prompt_required"))
        return

    result = _safe_api_call(_api_post, "/v1/research", payload)
    if result:
        st.session_state["selected_research_id"] = result["research_id"]
        _sync_query_params()
        st.success(_t("research_created", research_id=result["research_id"]))
        st.rerun()


def _run_queue_action(label: str, path: str) -> None:
    result = _safe_api_call(_api_post, path)
    if result is None:
        return
    st.success(f"{label}: {result}")
    st.rerun()


def _requeue_job(path: str, label: str) -> None:
    result = _safe_api_call(_api_post, path)
    if result is None:
        return
    st.success(f"{label}: {result['id']}")
    st.rerun()


def _render_job_card(job: dict, job_kind: str) -> None:
    status_html = _status_badge(job["status"])
    owner_id = job["task_id"] if job_kind == "search" else job["research_id"]
    st.markdown(
        f"""
        <div class="mas-panel">
            <div style="display:flex; justify-content:space-between; gap:1rem; align-items:center;">
                <strong>{escape(job['id'])}</strong>
                {status_html}
            </div>
            <div class="mas-kv">{escape(_t('attempts', attempt_count=job['attempt_count'], max_attempts=job['max_attempts']))}</div>
            <div class="mas-kv">{escape(_t('updated', timestamp=_format_timestamp(job.get('updated_at'))))}</div>
            <div class="mas-kv">{escape(_t('task_id' if job_kind == 'search' else 'research_job_id', value=owner_id))}</div>
            {f"<div class='mas-kv'>{escape(_t('error', error=job['error']))}</div>" if job.get('error') else ""}
        </div>
        """,
        unsafe_allow_html=True,
    )

    if job["status"] == "dead_letter":
        button_key = f"requeue-{job_kind}-{job['id']}"
        path = (
            f"/v1/search-jobs/{job['id']}/requeue"
            if job_kind == "search"
            else f"/v1/research/finalize-jobs/{job['id']}/requeue"
        )
        if st.button(_t("requeue_job", job_id=job["id"]), key=button_key, use_container_width=True):
            _requeue_job(path, _t("requeued_job", job_kind=job_kind))


def _render_job_section(title: str, jobs: list[dict], job_kind: str) -> None:
    st.markdown(f"**{title}**")
    if not jobs:
        st.caption(_t("no_jobs_bucket"))
        return
    for job in jobs:
        _render_job_card(job, job_kind)


def _filter_jobs(jobs: list[dict], query: str) -> list[dict]:
    normalized_query = query.strip().lower()
    if not normalized_query:
        return jobs

    filtered_jobs = []
    for job in jobs:
        haystack = " ".join(
            str(value)
            for value in [
                job.get("id"),
                job.get("task_id"),
                job.get("research_id"),
                job.get("status"),
                job.get("error"),
            ]
            if value
        ).lower()
        if normalized_query in haystack:
            filtered_jobs.append(job)
    return filtered_jobs


def _render_queue_overview() -> None:
    st.subheader(_t("queue_overview"))
    metrics = _safe_api_call(_api_get, "/health/queues")
    if not metrics:
        return

    queue_backlog = (
        metrics["pending_search_jobs"]
        + metrics["running_search_jobs"]
        + metrics["dead_letter_search_jobs"]
        + metrics["pending_finalize_jobs"]
        + metrics["running_finalize_jobs"]
        + metrics["dead_letter_finalize_jobs"]
    )
    st.metric(_t("queue_backlog"), queue_backlog)

    top = st.columns(3)
    top[0].metric(_t("pending_search"), metrics["pending_search_jobs"])
    top[1].metric(_t("running_search"), metrics["running_search_jobs"])
    top[2].metric(_t("dead_search"), metrics["dead_letter_search_jobs"])

    bottom = st.columns(3)
    bottom[0].metric(_t("pending_finalize"), metrics["pending_finalize_jobs"])
    bottom[1].metric(_t("running_finalize"), metrics["running_finalize_jobs"])
    bottom[2].metric(_t("dead_finalize"), metrics["dead_letter_finalize_jobs"])

    extraction = metrics.get("extraction_metrics") or {}
    extraction_row = st.columns(3)
    extraction_row[0].metric(_t("queue_extraction_attempts"), extraction.get("attempts", 0))
    extraction_row[1].metric(_t("queue_extraction_success"), extraction.get("success_count", 0))
    extraction_row[2].metric(_t("queue_extraction_failures"), extraction.get("failure_count", 0))
    derived_row = st.columns(2)
    derived_row[0].metric(_t("queue_extraction_success_rate"), f"{extraction.get('success_rate_percent', 0.0)}%")
    derived_row[1].metric(_t("queue_extraction_avg_total_ms"), f"{extraction.get('avg_total_ms', 0.0)} ms")

    st.markdown(f"**{_t('queue_actions')}**")
    action_rows = [st.columns(2), st.columns(2), st.columns(1)]
    if action_rows[0][0].button(_t("recover_stale_search"), use_container_width=True):
        _run_queue_action(_t("recovered_stale_search_jobs"), "/v1/search-jobs/recover-stale")
    if action_rows[0][1].button(_t("recover_stale_finalize"), use_container_width=True):
        _run_queue_action(_t("recovered_stale_finalize_jobs"), "/v1/research/finalize-jobs/recover-stale")
    if action_rows[1][0].button(_t("cleanup_search_jobs"), use_container_width=True):
        _run_queue_action(_t("cleaned_search_jobs"), "/v1/search-jobs/cleanup")
    if action_rows[1][1].button(_t("cleanup_finalize_jobs"), use_container_width=True):
        _run_queue_action(_t("cleaned_finalize_jobs"), "/v1/research/finalize-jobs/cleanup")
    if action_rows[2][0].button(_t("run_full_maintenance"), use_container_width=True):
        _run_queue_action(_t("queue_maintenance"), "/health/queues/maintenance")

    with st.expander(_t("operational_view"), expanded=False):
        job_filter = st.text_input(
            _t("job_filter"),
            value="",
            placeholder=_t("job_filter_placeholder"),
        )
        search_running = _safe_api_call(_api_get, "/v1/search-jobs?status=running") or []
        search_dead = _safe_api_call(_api_get, "/v1/search-jobs?status=dead_letter") or []
        finalize_running = _safe_api_call(_api_get, "/v1/research/finalize-jobs?status=running") or []
        finalize_dead = _safe_api_call(_api_get, "/v1/research/finalize-jobs?status=dead_letter") or []

        search_running = _filter_jobs(search_running, job_filter)
        search_dead = _filter_jobs(search_dead, job_filter)
        finalize_running = _filter_jobs(finalize_running, job_filter)
        finalize_dead = _filter_jobs(finalize_dead, job_filter)

        left, right = st.columns(2, gap="large")
        with left:
            _render_job_section(_t("running_search_jobs"), search_running, "search")
            _render_job_section(_t("dead_letter_search_jobs"), search_dead, "search")
        with right:
            _render_job_section(_t("running_finalize_jobs"), finalize_running, "finalize")
            _render_job_section(_t("dead_letter_finalize_jobs"), finalize_dead, "finalize")


def _render_source(result: dict, task_id: str, source_index: int) -> None:
    title = result.get("title") or result.get("url") or _t("untitled_source")
    url = result.get("url") or ""
    snippet = result.get("snippet") or result.get("content") or ""
    metadata = " | ".join(
        filter(
            None,
            [
                result.get("domain") or "unknown-domain",
                result.get("source_quality") or _t("unknown_quality"),
                result.get("extraction_status") or _t("unknown_extraction"),
            ],
        )
    )

    st.markdown(f"**{source_index}. {title}**")
    st.caption(metadata)
    if url:
        st.link_button(_t("open_source"), url, use_container_width=False)
    if snippet:
        st.write(_truncate(snippet, 320))


def _render_task(task: dict, index: int) -> None:
    search_job = task.get("latest_search_job")
    status_line = _status_badge(task["status"])
    if search_job:
        status_line = f"{status_line} {_status_badge(search_job['status'])}"

    with st.expander(f"{index}. {task['description']}", expanded=index == 1):
        st.markdown(status_line, unsafe_allow_html=True)
        st.caption(_t("task_id", value=task["id"]))
        st.caption(_t("collected_sources", count=_task_source_count(task)))
        task_metrics = task.get("search_metrics") or {}
        if task_metrics.get("extraction_attempts", 0) > 0:
            st.caption(
                _t(
                    "task_extraction_summary",
                    success=task_metrics.get("extraction_success_count", 0),
                    attempts=task_metrics.get("extraction_attempts", 0),
                    failures=task_metrics.get("extraction_failure_count", 0),
                    selected=task_metrics.get("selected_source_count", 0),
                    avg_chars=task_metrics.get("avg_content_chars", 0.0),
                )
            )

        if search_job:
            st.caption(
                _t(
                    "search_job_line",
                    job_id=search_job["id"],
                    attempt_count=search_job["attempt_count"],
                    max_attempts=search_job["max_attempts"],
                    updated_at=_format_timestamp(search_job.get("updated_at")),
                )
            )
            if search_job.get("error"):
                st.warning(search_job["error"])
            if search_job.get("status") == "dead_letter":
                if st.button(
                    _t("requeue_dead_search_job"),
                    key=f"requeue-dead-search-job-{search_job['id']}",
                    use_container_width=True,
                ):
                    _requeue_job(
                        f"/v1/search-jobs/{search_job['id']}/requeue",
                        _t("requeued_job", job_kind="search"),
                    )

        st.markdown(f"**{_t('queries')}**")
        st.code("\n".join(task["queries"]) or "-", language="text")

        logs = task.get("recent_logs") or task.get("logs") or []
        if logs:
            st.markdown(f"**{_t('recent_logs')}**")
            st.code("\n".join(logs[-10:]), language="text")

        results = task.get("source_preview") or task.get("result") or []
        if not results:
            st.info(_t("no_sources_yet"))
            return

        st.markdown(f"**{_t('selected_sources')}**")
        for source_index, result in enumerate(results, start=1):
            _render_source(result, task["id"], source_index)
        if task.get("result_count", len(results)) > len(results):
            st.caption(_t("showing_sources_preview", shown=len(results), total=task["result_count"]))


def _render_latest_finalize_job(latest_finalize_job: dict | None) -> None:
    if latest_finalize_job is None:
        st.info(_t("no_finalize_job"))
        return None

    st.markdown(f"**{_t('latest_finalize_job')}**")
    _render_job_card(latest_finalize_job, "finalize")
    return latest_finalize_job


def _render_research_details() -> None:
    research_id = st.session_state.get("selected_research_id", "").strip()
    st.subheader(_t("research_details"))

    if not research_id:
        st.info(_t("no_research_selected"))
        return

    research = _safe_api_call(_api_get, f"/v1/research/{research_id}/summary")
    if not research:
        return

    depth_profile = get_depth_profile(SearchDepth(research["depth"]))

    top_left, top_right = st.columns([2, 1], gap="large")
    with top_left:
        st.markdown(
            f"""
            <div class="mas-panel">
                <div class="mas-accent">{escape(_t('prompt'))}</div>
                <div>{escape(research['prompt'])}</div>
                <div class="mas-kv">{escape(_t('created', timestamp=_format_timestamp(research.get('created_at'))))}</div>
                <div class="mas-kv">{escape(_t('updated', timestamp=_format_timestamp(research.get('updated_at'))))}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with top_right:
        st.markdown(
            f"""
            <div class="mas-panel">
                <div class="mas-accent">{escape(_t('research_status'))}</div>
                <div>{_status_badge(research['status'])}</div>
                <div class="mas-kv">{escape(_t('depth', value=research['depth']))}</div>
                <div class="mas-kv">{escape(_t('profile', value=_t(depth_profile['label'].lower().replace(' ', '_'))))}</div>
                <div class="mas-kv">{escape(_t('tasks', count=len(research.get('task_ids', []))))}</div>
                <div class="mas-kv">{escape(_t('target_breadth', task_count=depth_profile['task_count'], source_limit=depth_profile['source_limit']))}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    tasks = research.get("tasks") or []
    completed_tasks = int(research.get("completed_tasks") or 0)
    pending_tasks = int(research.get("pending_tasks") or 0)
    running_tasks = int(research.get("running_tasks") or 0)
    failed_tasks = int(research.get("failed_tasks") or 0)
    finalize_ready = bool(research.get("finalize_ready"))
    total_sources = int(research.get("collected_sources") or 0)
    average_sources = research.get("avg_sources_per_task") or 0.0

    action_col1, action_col2 = st.columns([1, 1])
    if action_col1.button(_t("refresh_research"), use_container_width=True):
        st.rerun()
    if action_col2.button(
        _t("enqueue_finalize"),
        use_container_width=True,
        disabled=not finalize_ready,
        help=None if finalize_ready else _t("disable_finalize_reason"),
    ):
        result = _safe_api_call(_api_post, f"/v1/research/{research_id}/finalize")
        if result:
            st.session_state["auto_refresh_enabled"] = True
            st.session_state["auto_refresh_seconds"] = min(int(st.session_state.get("auto_refresh_seconds", 10)), 5)
            st.success(_t("finalize_job_queued", job_id=result.get("finalize_job_id") or _t("already_queued")))
            st.info(_t("auto_refresh_enabled_finalize"))
            st.rerun()

    latest_finalize_job = _render_latest_finalize_job(research.get("latest_finalize_job"))

    summary_cols = st.columns(5)
    summary_cols[0].metric(_t("task_slots"), len(research.get("task_ids", [])))
    summary_cols[1].metric(_t("completed_tasks"), completed_tasks)
    summary_cols[2].metric(_t("collected_sources_metric"), total_sources)
    summary_cols[3].metric(_t("avg_sources_per_task"), average_sources)
    summary_cols[4].metric(_t("finalize_ready"), _t("yes") if finalize_ready else _t("no"))

    progress_total = len(tasks)
    progress_value = (completed_tasks + failed_tasks) / progress_total if progress_total else 0.0
    st.markdown(f"**{_t('progress')}**")
    st.progress(
        progress_value,
        text=_t("progress_caption", completed=completed_tasks + failed_tasks, total=progress_total),
    )

    breakdown_cols = st.columns(4)
    breakdown_cols[0].metric(_t("pending_tasks"), pending_tasks)
    breakdown_cols[1].metric(_t("running_tasks"), running_tasks)
    breakdown_cols[2].metric(_t("failed_tasks"), failed_tasks)
    breakdown_cols[3].metric(_t("completed_tasks_short"), completed_tasks)

    if finalize_ready:
        st.success(_t("all_tasks_completed"))
    elif tasks:
        st.info(_t("finalize_waiting"))

    latest_finalize_status = (latest_finalize_job or {}).get("status")
    research_status = (research.get("status") or "").strip().lower()
    if latest_finalize_status == "running" or research_status == "analyzing":
        st.warning(_t("finalize_running"))
    elif latest_finalize_status == "pending":
        st.info(_t("finalize_pending"))

    st.subheader(_t("task_pipeline"))
    if not tasks:
        st.info(_t("no_tasks"))
    else:
        task_status_options = [_t("all_statuses")] + sorted(
            {task.get("status", "") for task in tasks if task.get("status")}
        )
        filter_left, filter_right = st.columns([1, 1.4])
        selected_status = filter_left.selectbox(
            _t("task_status_filter"),
            options=task_status_options,
        )
        task_filter = filter_right.text_input(
            _t("task_filter"),
            value="",
            placeholder=_t("task_filter_placeholder"),
        ).strip().lower()

        filtered_tasks = []
        for task in tasks:
            if selected_status != _t("all_statuses") and task.get("status") != selected_status:
                continue
            haystack = " ".join(
                [
                    task.get("id") or "",
                    task.get("description") or "",
                    " ".join(task.get("queries") or []),
                    " ".join(task.get("recent_logs") or task.get("logs") or []),
                ]
            ).lower()
            if task_filter and task_filter not in haystack:
                continue
            filtered_tasks.append(task)

        if not filtered_tasks:
            st.info(_t("no_tasks"))
            return

        for index, task in enumerate(filtered_tasks, start=1):
            _render_task(task, index)

    st.subheader(_t("final_report"))
    if not research.get("has_final_report"):
        st.info(_t("final_report_not_ready"))
        return

    report_payload = _safe_api_call(_api_get, f"/v1/research/{research_id}/report")
    if not report_payload:
        return
    final_report = report_payload.get("final_report")
    if not final_report:
        st.info(_t("final_report_not_ready"))
        return

    rendered_tab, raw_tab = st.tabs([_t("rendered"), _t("raw_markdown")])
    with rendered_tab:
        st.markdown(final_report)
    with raw_tab:
        st.code(final_report, language="markdown")


def main() -> None:
    st.set_page_config(
        page_title="Multi-Agent Search",
        page_icon="M",
        layout="wide",
    )
    _initialize_state()
    _render_styles()
    _render_auto_refresh()
    _render_header()
    _render_sidebar()
    live_refresh_interval = _get_live_refresh_interval()

    left, right = st.columns([1.05, 1.35], gap="large")
    with left:
        _render_create_research()
        st.divider()
        _render_live_queue_fragment(live_refresh_interval)
    with right:
        _render_live_research_fragment(live_refresh_interval)


if __name__ == "__main__":
    main()

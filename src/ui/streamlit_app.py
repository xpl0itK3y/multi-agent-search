import os
from datetime import datetime, timezone
from html import escape
from typing import Any

import httpx
import streamlit as st
from src.api.schemas import SearchDepth
from src.search_depth_profiles import SEARCH_DEPTH_PROFILES, get_depth_profile
from src.ui.report_export import generate_docx, generate_pdf


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
        "research_candidates": "Candidates",
        "research_extractions": "Extraction Attempts",
        "research_extraction_success": "Extraction Success",
        "research_selected_sources": "Selected Sources",
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
        "graph_loop": "Цикл графа",
        "graph_branching_active": "Ветвление активно",
        "graph_follow_up_tasks": "Follow-up задачи",
        "graph_replan_tasks": "Replan задачи",
        "graph_tie_break_tasks": "Tie-break задачи",
        "graph_follow_up_queries": "Follow-up Queries",
        "graph_no_follow_up_queries": "No follow-up graph queries were executed yet.",
        "graph_state": "Graph State",
        "graph_last_step": "Last Graph Step",
        "graph_resume_passes": "Graph Resumes",
        "graph_analyze_passes": "Analyze Passes",
        "graph_replan_passes": "Replan Passes",
        "graph_tie_break_passes": "Tie-Break Passes",
        "graph_trail": "Graph Trail",
        "graph_no_trail": "No graph trail has been recorded yet.",
        "graph_trail_step": "Step: {step}",
        "graph_trail_detail": "Detail: {detail}",
        "graph_resumed_recovery": "Resumed After Recovery",
        "graph_recovery_detail": "Recovery Detail",
        "graph_resume_count": "Graph resumes: {count}",
        "graph_replan_count": "Graph replans: {count}",
        "graph_tie_break_count": "Graph tie-breaks: {count}",
        "graph_analyze_count": "Graph analyze passes: {count}",
        "graph_step_metrics": "Graph Step Metrics",
        "graph_step_summary": "{step}: runs {runs}, failures {failures}, avg {avg_ms} ms",
        "graph_alerts": "Graph Alerts",
        "graph_alert_high_avg_ms": "{step}: average latency {current_value} ms exceeded {threshold} ms",
        "graph_alert_step_failures": "{step}: failure count {current_value} reached threshold {threshold}",
        "graph_alert_analyze_retries": "{step}: analyze retries {current_value} reached threshold {threshold}",
        "graph_alert_hint": "Hint: {hint}",
        "graph_alert_trend": "Graph Alert Trend",
        "graph_worsening_steps": "Worsening steps: {value}",
        "graph_improving_steps": "Improving steps: {value}",
        "graph_repeated_alerts": "Repeated alerts: {value}",
        "graph_top_researches": "Top research IDs: {value}",
        "graph_top_workers": "Top workers: {value}",
        "graph_recent_alerts": "Recent alert events",
        "graph_recent_alert_line": "{timestamp} | {step} | {code} | research={research_id} | worker={worker_name}",
        "maintenance_summary": "Maintenance Summary",
        "maintenance_last_run": "Last maintenance run: {timestamp}",
        "maintenance_compacted_count": "Compacted operational items: {count}",
        "maintenance_compacted_workers": "Compacted graph workers: {value}",
        "maintenance_compacted_researches": "Compacted graph researches: {value}",
        "maintenance_recent_runs": "Recent maintenance runs",
        "maintenance_recent_run_line": "{timestamp} | recovered={recovered} deleted={deleted} compacted={compacted} total={total}",
        "maintenance_recommendation_history": "Recommendation History",
        "maintenance_recommendation_summary": "Recommendation Summary",
        "maintenance_recommendation_counters": "Recommendation Snapshot",
        "maintenance_recommendation_counter_needs_action": "Needs action: {count}",
        "maintenance_recommendation_counter_acknowledged": "Acknowledged: {count}",
        "maintenance_recommendation_counter_resolved": "Resolved: {count}",
        "maintenance_recommendation_counter_reappeared": "Reappeared: {count}",
        "ops_snapshot": "Ops Snapshot",
        "ops_snapshot_health": "Health: {value}",
        "ops_snapshot_backlog": "Backlog: {value}",
        "ops_snapshot_graph_alerts": "Graph alerts: {value}",
        "ops_snapshot_maintenance_alerts": "Maintenance alerts: {value}",
        "ops_snapshot_needs_action": "Needs action: {value}",
        "maintenance_recommendation_analytics": "Runbook Analytics",
        "maintenance_recommendation_avg_ack_hours": "Average time to acknowledge: {value} h",
        "maintenance_recommendation_top_recurring": "Top recurring codes: {value}",
        "maintenance_recommendation_avg_resolution_hours": "Average time to resolve: {value} h",
        "maintenance_recommendation_oldest_unresolved_hours": "Oldest unresolved age: {value} h",
        "maintenance_recommendation_summary_line": "{code} | repeats={repeats} reappeared={reappeared} last_resolved={last_resolved}",
        "maintenance_recommendation_badge_needs_action": "Needs Action",
        "maintenance_recommendation_badge_acknowledged": "Acknowledged",
        "maintenance_recommendation_badge_monitor": "Monitor",
        "maintenance_recommendation_badge_resolved": "Resolved",
        "maintenance_recommendation_event_line": "{timestamp} | {code} | {event_type}",
        "maintenance_recommendation_event_note": "Note: {value}",
        "maintenance_trend": "Maintenance Trend",
        "maintenance_cleanup_direction": "Cleanup volume: {value}",
        "maintenance_avg_compacted": "Average compacted per run: {value}",
        "maintenance_total_sparkline": "Recent total counts: {value}",
        "maintenance_compacted_sparkline": "Recent compacted counts: {value}",
        "maintenance_alerts": "Maintenance Alerts",
        "maintenance_alert_cleanup_volume_growing": "Cleanup volume is growing: recent average {current_value} exceeded {threshold}",
        "maintenance_alert_high_compacted_average": "Average compacted count {current_value} exceeded {threshold}",
        "maintenance_alert_maintenance_stale": "Maintenance is stale: last run age {current_value}s exceeded {threshold}s",
        "maintenance_alert_hint": "Hint: {hint}",
        "operational_health": "Operational Health",
        "operational_health_score": "Operational score: {score}",
        "operational_health_reasons": "Reasons: {value}",
        "operational_health_trend": "Operational Health Trend",
        "operational_health_direction": "Score direction: {value}",
        "operational_health_average": "Average score: {value}",
        "operational_health_scores": "Recent scores: {value}",
        "operational_health_statuses": "Recent statuses: {value}",
        "operational_health_alerts": "Operational Health Alerts",
        "operational_health_alert_score_worsening": "Operational score is worsening: recent average {current_value} vs previous {threshold}",
        "operational_health_alert_repeated_critical_states": "Too many critical states in recent history: {current_value} reached {threshold}",
        "operational_health_alert_score_recovered": "Operational score recovered to {current_value} after degradation",
        "operational_health_alert_hint": "Hint: {hint}",
        "operational_recommendations": "Recommended Actions",
        "operational_recommendation_repeat": "Shown {count} times",
        "operational_recommendation_last_shown": "Last shown: {timestamp}",
        "operational_recommendation_acknowledged": "Acknowledged: {timestamp}",
        "operational_recommendation_resolved": "Resolved: {timestamp}",
        "operational_recommendation_note": "Operator note: {value}",
        "operational_recommendation_inactive": "No longer active",
        "operational_recommendation_ack": "Acknowledge",
        "operational_recommendation_resolve": "Mark Resolved",
        "operational_recommendation_note_input": "Resolution note",
        "health_healthy": "healthy",
        "health_warning": "warning",
        "health_critical": "critical",
        "history": "Recent Researches",
        "history_empty": "No researches yet.",
        "history_select": "Select",
        "history_ago_seconds": "{n}s ago",
        "history_ago_minutes": "{n}m ago",
        "history_ago_hours": "{n}h ago",
        "history_ago_days": "{n}d ago",
        "history_delete_confirm": "Delete this research?",
        "history_delete_yes": "Yes, delete",
        "history_delete_no": "Cancel",
        "download_pdf": "⬇ Download PDF",
        "download_docx": "⬇ Download Word",
        "download_preparing": "Preparing file…",
        "partial_report_banner": "Generating report… (streaming live preview)",
        "partial_report_sections_banner": "Analyzing sections in parallel… (live preview)",
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
        "queue_backlog": "Задач в очереди",
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
        "research_candidates": "Кандидаты",
        "research_extractions": "Попытки extraction",
        "research_extraction_success": "Успешный extraction",
        "research_selected_sources": "Выбранные источники",
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
        "graph_loop": "Graph Loop",
        "graph_branching_active": "Ветвление активно",
        "graph_follow_up_tasks": "Follow-up задачи",
        "graph_replan_tasks": "Replan задачи",
        "graph_tie_break_tasks": "Tie-break задачи",
        "graph_follow_up_queries": "Follow-up запросы",
        "graph_no_follow_up_queries": "Follow-up запросы графа пока не запускались.",
        "graph_state": "Состояние графа",
        "graph_last_step": "Последний шаг графа",
        "graph_resume_passes": "Resume графа",
        "graph_analyze_passes": "Analyze проходы",
        "graph_replan_passes": "Replan проходы",
        "graph_tie_break_passes": "Tie-break проходы",
        "graph_trail": "Трасса графа",
        "graph_no_trail": "Трасса графа пока не записана.",
        "graph_trail_step": "Шаг: {step}",
        "graph_trail_detail": "Детали: {detail}",
        "graph_resumed_recovery": "Resume после recovery",
        "graph_recovery_detail": "Детали recovery",
        "graph_resume_count": "Resume графа: {count}",
        "graph_replan_count": "Replan графа: {count}",
        "graph_tie_break_count": "Tie-break графа: {count}",
        "graph_analyze_count": "Analyze проходы графа: {count}",
        "graph_step_metrics": "Метрики шагов графа",
        "graph_step_summary": "{step}: запусков {runs}, ошибок {failures}, среднее {avg_ms} мс",
        "graph_alerts": "Проблемы графа",
        "graph_alert_high_avg_ms": "{step}: средняя задержка {current_value} мс превысила {threshold} мс",
        "graph_alert_step_failures": "{step}: число ошибок {current_value} достигло порога {threshold}",
        "graph_alert_analyze_retries": "{step}: число analyze retry {current_value} достигло порога {threshold}",
        "graph_alert_hint": "Подсказка: {hint}",
        "graph_alert_trend": "Тренд проблем графа",
        "graph_worsening_steps": "Шаги с ухудшением: {value}",
        "graph_improving_steps": "Шаги с улучшением: {value}",
        "graph_repeated_alerts": "Повторяющиеся alerts: {value}",
        "graph_top_researches": "Research ID с alert-ами чаще всего: {value}",
        "graph_top_workers": "Воркеры с alert-ами чаще всего: {value}",
        "graph_recent_alerts": "Последние alert-события",
        "graph_recent_alert_line": "{timestamp} | {step} | {code} | research={research_id} | worker={worker_name}",
        "maintenance_summary": "Сводка обслуживания",
        "maintenance_last_run": "Последний maintenance запуск: {timestamp}",
        "maintenance_compacted_count": "Сжатых operational элементов: {count}",
        "maintenance_compacted_workers": "Воркеры с compact graph data: {value}",
        "maintenance_compacted_researches": "Research с compact graph trail: {value}",
        "maintenance_recent_runs": "Последние maintenance запуски",
        "maintenance_recent_run_line": "{timestamp} | recovered={recovered} deleted={deleted} compacted={compacted} total={total}",
        "maintenance_recommendation_history": "История рекомендаций",
        "maintenance_recommendation_summary": "Сводка по рекомендациям",
        "maintenance_recommendation_counters": "Снимок по рекомендациям",
        "maintenance_recommendation_counter_needs_action": "Нужно действие: {count}",
        "maintenance_recommendation_counter_acknowledged": "Подтверждено: {count}",
        "maintenance_recommendation_counter_resolved": "Закрыто: {count}",
        "maintenance_recommendation_counter_reappeared": "Повторно всплыло: {count}",
        "ops_snapshot": "Снимок состояния",
        "ops_snapshot_health": "Состояние: {value}",
        "ops_snapshot_backlog": "Очередь: {value}",
        "ops_snapshot_graph_alerts": "Алертов графа: {value}",
        "ops_snapshot_maintenance_alerts": "Алертов обслуживания: {value}",
        "ops_snapshot_needs_action": "Нужно действие: {value}",
        "maintenance_recommendation_analytics": "Runbook аналитика",
        "maintenance_recommendation_avg_ack_hours": "Среднее время до подтверждения: {value} ч",
        "maintenance_recommendation_top_recurring": "Самые повторяющиеся коды: {value}",
        "maintenance_recommendation_avg_resolution_hours": "Среднее время до закрытия: {value} ч",
        "maintenance_recommendation_oldest_unresolved_hours": "Возраст самого старого незакрытого: {value} ч",
        "maintenance_recommendation_summary_line": "{code} | повторов={repeats} повторных всплытий={reappeared} последний resolved={last_resolved}",
        "maintenance_recommendation_badge_needs_action": "Нужно действие",
        "maintenance_recommendation_badge_acknowledged": "Подтверждено",
        "maintenance_recommendation_badge_monitor": "Наблюдать",
        "maintenance_recommendation_badge_resolved": "Закрыто",
        "maintenance_recommendation_event_line": "{timestamp} | {code} | {event_type}",
        "maintenance_recommendation_event_note": "Заметка: {value}",
        "maintenance_trend": "Тренд maintenance",
        "maintenance_cleanup_direction": "Объем cleanup: {value}",
        "maintenance_avg_compacted": "Средний compacted за run: {value}",
        "maintenance_total_sparkline": "Последние total counts: {value}",
        "maintenance_compacted_sparkline": "Последние compacted counts: {value}",
        "maintenance_alerts": "Проблемы maintenance",
        "maintenance_alert_cleanup_volume_growing": "Объем cleanup растет: недавнее среднее {current_value} превысило {threshold}",
        "maintenance_alert_high_compacted_average": "Средний compacted count {current_value} превысил {threshold}",
        "maintenance_alert_maintenance_stale": "Maintenance устарел: возраст последнего запуска {current_value}s превысил {threshold}s",
        "maintenance_alert_hint": "Подсказка: {hint}",
        "operational_health": "Состояние системы",
        "operational_health_score": "Оценка системы: {score}",
        "operational_health_reasons": "Причины: {value}",
        "operational_health_trend": "Тренд operational health",
        "operational_health_direction": "Динамика score: {value}",
        "operational_health_average": "Средний score: {value}",
        "operational_health_scores": "Последние score: {value}",
        "operational_health_statuses": "Последние статусы: {value}",
        "operational_health_alerts": "Проблемы operational health",
        "operational_health_alert_score_worsening": "Operational score ухудшается: недавнее среднее {current_value} против прошлого {threshold}",
        "operational_health_alert_repeated_critical_states": "Слишком много critical состояний в недавней истории: {current_value} достигло {threshold}",
        "operational_health_alert_score_recovered": "Operational score восстановился до {current_value} после деградации",
        "operational_health_alert_hint": "Подсказка: {hint}",
        "operational_recommendations": "Рекомендуемые действия",
        "operational_recommendation_repeat": "Показано {count} раз",
        "operational_recommendation_last_shown": "Последний показ: {timestamp}",
        "operational_recommendation_acknowledged": "Подтверждено: {timestamp}",
        "operational_recommendation_resolved": "Закрыто: {timestamp}",
        "operational_recommendation_note": "Заметка оператора: {value}",
        "operational_recommendation_inactive": "Больше не активно",
        "operational_recommendation_ack": "Подтвердить",
        "operational_recommendation_resolve": "Отметить как выполненное",
        "operational_recommendation_note_input": "Заметка о выполнении",
        "health_healthy": "в норме",
        "health_warning": "предупреждение",
        "health_critical": "критично",
        "history": "История запросов",
        "history_empty": "Нет исследований.",
        "history_select": "Открыть",
        "history_ago_seconds": "{n}с назад",
        "history_ago_minutes": "{n}м назад",
        "history_ago_hours": "{n}ч назад",
        "history_ago_days": "{n}д назад",
        "history_delete_confirm": "Удалить это исследование?",
        "history_delete_yes": "Да, удалить",
        "history_delete_no": "Отмена",
        "download_pdf": "⬇ Скачать PDF",
        "download_docx": "⬇ Скачать Word",
        "download_preparing": "Подготовка файла…",
        "partial_report_banner": "Формируем отчёт… (предпросмотр в реальном времени)",
        "partial_report_sections_banner": "Параллельный анализ секций… (предпросмотр)",
    },
}


def _t(key: str, **kwargs) -> str:
    language = st.session_state.get("ui_language", "en")
    template = TRANSLATIONS.get(language, TRANSLATIONS["en"]).get(key, key)
    return template.format(**kwargs)


def _relative_time(ts_str: str) -> str:
    try:
        from datetime import timezone as _tz
        ts = datetime.fromisoformat(ts_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=_tz.utc)
        diff = int((datetime.now(_tz.utc) - ts).total_seconds())
        if diff < 60:
            return _t("history_ago_seconds", n=diff)
        if diff < 3600:
            return _t("history_ago_minutes", n=diff // 60)
        if diff < 86400:
            return _t("history_ago_hours", n=diff // 3600)
        return _t("history_ago_days", n=diff // 86400)
    except Exception:
        return ""


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


def _api_delete(path: str) -> None:
    with _get_client() as client:
        response = client.delete(path)
        response.raise_for_status()


def _render_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: linear-gradient(160deg, #f0f4f8 0%, #e8edf5 100%);
            color: #0f172a;
        }
        h1, h2, h3 {
            font-family: "Segoe UI", system-ui, sans-serif;
            letter-spacing: -0.01em;
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
            background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
        }
        [data-testid="stSidebar"] * {
            color: #cbd5e1;
        }
        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 {
            color: #f1f5f9 !important;
        }
        .mas-panel {
            border: 1px solid rgba(148, 163, 184, 0.20);
            background: rgba(255, 255, 255, 0.95);
            border-radius: 12px;
            padding: 0.9rem 1rem;
            box-shadow: 0 4px 16px rgba(15, 23, 42, 0.06);
            margin-bottom: 0.7rem;
        }
        .mas-accent {
            color: #1d4ed8;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-size: 0.72rem;
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
        .mas-status.needs_action { background: #fee2e2; color: #b91c1c; }
        .mas-status.acknowledged { background: #dbeafe; color: #1d4ed8; }
        .mas-status.monitor { background: #fef3c7; color: #92400e; }
        .mas-status.resolved { background: #dcfce7; color: #166534; }
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
            background: linear-gradient(135deg, #1d4ed8 0%, #1e40af 100%) !important;
            color: #f8fafc !important;
            border: 1px solid rgba(29, 78, 216, 0.20) !important;
            border-radius: 8px !important;
            font-weight: 600 !important;
            box-shadow: 0 2px 8px rgba(29, 78, 216, 0.18);
        }
        div[data-testid="stButton"] > button:hover,
        div[data-testid="stDownloadButton"] > button:hover,
        div[data-testid="stLinkButton"] a:hover {
            background: linear-gradient(135deg, #1e40af 0%, #1d4ed8 100%) !important;
            color: #ffffff !important;
            border-color: rgba(29, 78, 216, 0.40) !important;
            box-shadow: 0 4px 12px rgba(29, 78, 216, 0.28) !important;
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
            background: rgba(255, 255, 255, 0.90);
            border: 1px solid rgba(148, 163, 184, 0.22);
            border-radius: 10px;
            padding: 0.7rem 0.85rem;
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
            background: #1e293b !important;
            border-radius: 8px !important;
            padding: 0.35rem 0.7rem !important;
        }
        div[data-testid="stExpander"] summary *,
        div[data-testid="stExpander"] summary span,
        div[data-testid="stExpander"] summary p,
        div[data-testid="stExpander"] summary svg {
            color: #e2e8f0 !important;
            fill: #e2e8f0 !important;
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
        [data-testid="stSidebar"] .stButton > button {
            text-align: left !important;
            font-size: 0.75rem !important;
            padding: 0.25rem 0.5rem !important;
            min-height: 0 !important;
            height: auto !important;
            line-height: 1.3 !important;
            white-space: pre-wrap !important;
            word-break: break-word !important;
            border-radius: 6px !important;
        }
        [data-testid="stSidebar"] [data-testid="column"]:last-child .stButton > button {
            padding: 0.25rem 0.2rem !important;
            font-size: 0.8rem !important;
            text-align: center !important;
            opacity: 0.55;
        }
        [data-testid="stSidebar"] [data-testid="column"]:last-child .stButton > button:hover {
            opacity: 1;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _status_badge(status: str | None) -> str:
    label = (status or "unknown").strip().lower()
    return f'<span class="mas-status {label}">{escape(label)}</span>'


def _runbook_badge(status_key: str, label: str) -> str:
    normalized_key = (status_key or "monitor").strip().lower()
    return f'<span class="mas-status {escape(normalized_key)}">{escape(label)}</span>'


def _format_timestamp(value: str | None) -> str:
    if not value:
        return "-"
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return value
    return parsed.strftime("%Y-%m-%d %H:%M:%S UTC")


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


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


_TERMINAL_STATUSES = {"completed", "failed"}
_ACTIVE_STATUSES = {"processing", "analyzing"}


def _get_live_refresh_interval() -> int | None:
    """Adaptive interval: fast during active work, stops on terminal state."""
    research_id = st.session_state.get("selected_research_id", "").strip()
    if not research_id:
        return None
    cached = st.session_state.get(f"_rs_{research_id}", "")
    if cached in _TERMINAL_STATUSES:
        return None  # stop polling once done
    if cached in _ACTIVE_STATUSES:
        return 2     # fast during active processing
    return 4         # moderate for pending/unknown


def _get_queue_refresh_interval() -> int:
    """Queue polls faster when jobs are active."""
    return 3 if st.session_state.get("_queue_has_active", False) else 20


def _render_live_queue_fragment() -> None:
    interval = _get_queue_refresh_interval()

    @st.fragment(run_every=interval)
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


_STEP_LABEL = {
    "collect_context": "🔍 Collect context",
    "analyze": "🧠 Analyze",
    "verify": "✅ Verify",
    "replan": "🔄 Replan",
    "tie_break": "⚖️ Tie-break",
    "complete": "🏁 Complete",
}


def _render_graph_trail(research_id: str) -> None:
    graph = _safe_api_call(_api_get, f"/v1/research/{research_id}/graph", ignore_status_codes={404})
    if not graph:
        return
    trail = graph.get("graph_trail") or []
    if not trail:
        st.caption(_t("graph_no_trail"))
        return
    # Show last 10 entries
    for entry in trail[-10:]:
        step = entry.get("step") or ""
        detail = entry.get("detail") or ""
        label = _STEP_LABEL.get(step, f"◦ {step}")
        detail_html = (
            f' — <span style="color:#64748b">{escape(detail)}</span>'
            if detail else ""
        )
        st.markdown(
            f'<div style="font-size:0.78rem;line-height:1.4;margin-bottom:0.2rem;">'
            f'<span style="font-weight:600;">{escape(label)}</span>'
            f'{detail_html}'
            f"</div>",
            unsafe_allow_html=True,
        )


_STATUS_ICON = {
    "completed": "✅",
    "processing": "⏳",
    "analyzing": "🔍",
    "failed": "❌",
    "pending": "🕐",
}


def _render_sidebar_history() -> None:
    @st.fragment(run_every=15)
    def _history_inner() -> None:
        # Fetch fresh data; on error keep showing cached version so history
        # never appears empty during the brief API-call window.
        fresh = _safe_api_call(_api_get, "/v1/research?limit=15", ignore_status_codes={404})
        if fresh is not None:          # None → 404/error → keep cache
            st.session_state["_history_cache"] = fresh
        history = st.session_state.get("_history_cache") or []

        st.markdown(f"**{_t('history')}**")
        if not history:
            st.caption(_t("history_empty"))
            return
        current_id = st.session_state.get("selected_research_id", "").strip()
        confirm_id = st.session_state.get("_hist_confirm_delete", "")
        for item in history:
            rid = item.get("id", "")
            prompt = item.get("prompt") or ""
            status = (item.get("status") or "").lower()
            icon = _STATUS_ICON.get(status, "•")
            prompt_short = (prompt[:34] + "…") if len(prompt) > 34 else prompt
            ago = _relative_time(item.get("created_at") or "")
            is_selected = rid == current_id

            if confirm_id == rid:
                st.caption(f"🗑 {prompt_short}")
                col_yes, col_no = st.columns(2)
                if col_yes.button(_t("history_delete_yes"), key=f"hist-del-yes-{rid}", use_container_width=True, type="primary"):
                    _safe_api_call(_api_delete, f"/v1/research/{rid}")
                    st.session_state.pop("_hist_confirm_delete", None)
                    st.session_state.pop("_history_cache", None)  # invalidate cache
                    if st.session_state.get("selected_research_id") == rid:
                        st.session_state["selected_research_id"] = ""
                        _sync_query_params()
                    st.rerun(scope="app")
                if col_no.button(_t("history_delete_no"), key=f"hist-del-no-{rid}", use_container_width=True):
                    st.session_state.pop("_hist_confirm_delete", None)
                    st.rerun(scope="app")
            else:
                btn_label = f"{icon} {prompt_short}\n{ago}"
                col_btn, col_del = st.columns([7, 1])
                if col_btn.button(
                    btn_label,
                    key=f"hist-{rid}",
                    use_container_width=True,
                    type="primary" if is_selected else "secondary",
                ):
                    st.session_state["selected_research_id"] = rid
                    _sync_query_params()
                    st.rerun(scope="app")
                if col_del.button("🗑", key=f"hist-trash-{rid}", use_container_width=True):
                    st.session_state["_hist_confirm_delete"] = rid
                    st.rerun()

    with st.sidebar:
        _history_inner()


def _render_sidebar() -> None:
    st.sidebar.header(_t("control"))
    st.session_state["ui_language"] = st.sidebar.selectbox(
        _t("language"),
        options=list(LANGUAGE_OPTIONS.keys()),
        index=list(LANGUAGE_OPTIONS.keys()).index(st.session_state.get("ui_language", "en")),
        format_func=lambda value: LANGUAGE_OPTIONS[value],
    )
    _sync_query_params()

    selected_research_id = st.sidebar.text_input(
        _t("research_id"),
        value=st.session_state.get("selected_research_id", ""),
        placeholder=_t("research_id_placeholder"),
    ).strip()
    st.session_state["selected_research_id"] = selected_research_id
    _sync_query_params()

    if st.sidebar.button(_t("refresh_now"), use_container_width=True):
        st.rerun()

    st.sidebar.divider()
    _render_sidebar_history()

    st.sidebar.divider()
    st.sidebar.subheader(_t("worker"))
    heartbeat = _safe_api_call(
        _api_get,
        f"/health/workers/{WORKER_NAME}",
        ignore_status_codes={404},
    )
    if heartbeat is None:
        st.sidebar.caption(_t("no_heartbeat", worker_name=WORKER_NAME))
        return

    st.sidebar.markdown(_status_badge(heartbeat["status"]), unsafe_allow_html=True)
    st.sidebar.caption(_t("last_seen", timestamp=_format_timestamp(heartbeat["last_seen_at"])))
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
        st.session_state.pop("_history_cache", None)  # force history refresh
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


def _render_graph_step_metrics(graph_metrics: dict) -> None:
    steps = graph_metrics.get("steps") or {}
    for step_name in ("collect_context", "replan", "analyze", "verify", "tie_break"):
        metrics = steps.get(step_name) or {}
        st.caption(
            _t(
                "graph_step_summary",
                step=step_name,
                runs=int(metrics.get("run_count", 0) or 0),
                failures=int(metrics.get("failure_count", 0) or 0),
                avg_ms=round(float(metrics.get("avg_ms", 0.0) or 0.0), 2),
            )
        )


def _render_graph_alerts(graph_alerts: list[dict]) -> None:
    if not graph_alerts:
        return
    for alert in graph_alerts:
        key = f"graph_alert_{alert.get('code')}"
        message = _t(
            key,
            step=alert.get("step") or "-",
            current_value=round(float(alert.get("current_value", 0.0) or 0.0), 2),
            threshold=round(float(alert.get("threshold", 0.0) or 0.0), 2),
        )
        if (alert.get("severity") or "warning").lower() == "critical":
            st.error(message)
        else:
            st.warning(message)
        if alert.get("hint"):
            st.caption(_t("graph_alert_hint", hint=alert["hint"]))


def _render_graph_alert_trend(graph_alert_trend: dict) -> None:
    if not graph_alert_trend:
        return
    worsening = ", ".join(graph_alert_trend.get("worsening_steps") or []) or "-"
    improving = ", ".join(graph_alert_trend.get("improving_steps") or []) or "-"
    repeated_alerts = graph_alert_trend.get("repeated_alerts") or {}
    repeated_value = ", ".join(f"{code}={count}" for code, count in repeated_alerts.items()) or "-"
    top_research_ids = ", ".join(graph_alert_trend.get("top_research_ids") or []) or "-"
    top_worker_names = ", ".join(graph_alert_trend.get("top_worker_names") or []) or "-"
    st.caption(_t("graph_worsening_steps", value=worsening))
    st.caption(_t("graph_improving_steps", value=improving))
    st.caption(_t("graph_repeated_alerts", value=repeated_value))
    st.caption(_t("graph_top_researches", value=top_research_ids))
    st.caption(_t("graph_top_workers", value=top_worker_names))
    recent_alerts = graph_alert_trend.get("recent_alerts") or []
    if recent_alerts:
        st.caption(_t("graph_recent_alerts"))
        for event in recent_alerts[-5:]:
            st.caption(
                _t(
                    "graph_recent_alert_line",
                    timestamp=_format_timestamp(event.get("timestamp")),
                    step=event.get("step") or "-",
                    code=event.get("code") or "-",
                    research_id=event.get("research_id") or "-",
                    worker_name=event.get("worker_name") or "-",
                )
            )


def _render_maintenance_summary(summary: dict) -> None:
    if not summary:
        return
    st.caption(_t("maintenance_last_run", timestamp=_format_timestamp(summary.get("last_run_at"))))
    st.caption(_t("maintenance_compacted_count", count=int(summary.get("compacted_count", 0) or 0)))
    worker_names = ", ".join(summary.get("compacted_graph_event_worker_names") or []) or "-"
    research_ids = ", ".join(summary.get("compacted_graph_trail_research_ids") or []) or "-"
    st.caption(_t("maintenance_compacted_workers", value=worker_names))
    st.caption(_t("maintenance_compacted_researches", value=research_ids))
    alerts = summary.get("alerts") or []
    if alerts:
        st.caption(_t("maintenance_alerts"))
        for alert in alerts:
            key = f"maintenance_alert_{alert.get('code')}"
            message = _t(
                key,
                current_value=round(float(alert.get("current_value", 0.0) or 0.0), 2),
                threshold=round(float(alert.get("threshold", 0.0) or 0.0), 2),
            )
            if (alert.get("severity") or "warning").lower() == "critical":
                st.error(message)
            else:
                st.warning(message)
            if alert.get("hint"):
                st.caption(_t("maintenance_alert_hint", hint=alert["hint"]))
    trend = summary.get("trend") or {}
    if trend:
        st.caption(_t("maintenance_trend"))
        st.caption(_t("maintenance_cleanup_direction", value=trend.get("cleanup_volume_direction") or "stable"))
        st.caption(_t("maintenance_avg_compacted", value=trend.get("average_compacted_count", 0.0)))
        total_sparkline = " ".join(str(item) for item in (trend.get("recent_total_counts") or [])) or "-"
        compacted_sparkline = " ".join(str(item) for item in (trend.get("recent_compacted_counts") or [])) or "-"
        st.caption(_t("maintenance_total_sparkline", value=total_sparkline))
        st.caption(_t("maintenance_compacted_sparkline", value=compacted_sparkline))
    recent_runs = summary.get("recent_runs") or []
    if recent_runs:
        st.caption(_t("maintenance_recent_runs"))
        for run in recent_runs[-5:]:
            st.caption(
                _t(
                    "maintenance_recent_run_line",
                    timestamp=_format_timestamp(run.get("last_run_at")),
                    recovered=int(run.get("recovered_count", 0) or 0),
                    deleted=int(run.get("deleted_count", 0) or 0),
                    compacted=int(run.get("compacted_count", 0) or 0),
                    total=int(run.get("total_count", 0) or 0),
                )
            )
    recommendation_events = summary.get("recent_operational_recommendation_events") or []
    recommendations = summary.get("recent_operational_recommendations") or []
    recommendation_analytics = summary.get("recommendation_analytics") or {}
    if recommendations:
        recommendation_events_by_code: dict[str, list[dict]] = {}
        for event in recommendation_events:
            code = str(event.get("code") or "")
            if not code:
                continue
            recommendation_events_by_code.setdefault(code, []).append(event)

        needs_action_count = sum(
            1
            for item in recommendations
            if bool(item.get("active", True))
            and not bool(item.get("resolved", False))
            and not bool(item.get("acknowledged", False))
        )
        acknowledged_count = sum(
            1
            for item in recommendations
            if bool(item.get("active", True))
            and not bool(item.get("resolved", False))
            and bool(item.get("acknowledged", False))
        )
        resolved_count = sum(1 for item in recommendations if bool(item.get("resolved", False)))
        reappeared_count = sum(
            1 for event in recommendation_events if str(event.get("event_type") or "") == "reappeared"
        )
        st.caption(_t("maintenance_recommendation_counters"))
        counter_row = st.columns(4)
        counter_row[0].metric(_t("maintenance_recommendation_counter_needs_action", count=needs_action_count), needs_action_count)
        counter_row[1].metric(_t("maintenance_recommendation_counter_acknowledged", count=acknowledged_count), acknowledged_count)
        counter_row[2].metric(_t("maintenance_recommendation_counter_resolved", count=resolved_count), resolved_count)
        counter_row[3].metric(_t("maintenance_recommendation_counter_reappeared", count=reappeared_count), reappeared_count)

        st.caption(_t("maintenance_recommendation_analytics"))
        analytics_row = st.columns(3)
        top_recurring = ", ".join(recommendation_analytics.get("top_recurring_codes") or []) or "-"
        analytics_row[0].metric(_t("maintenance_recommendation_top_recurring", value=top_recurring), top_recurring)
        analytics_row[1].metric(
            _t("maintenance_recommendation_avg_ack_hours", value=recommendation_analytics.get("average_time_to_ack_hours", 0.0)),
            recommendation_analytics.get("average_time_to_ack_hours", 0.0),
        )
        analytics_row[2].metric(
            _t("maintenance_recommendation_oldest_unresolved_hours", value=recommendation_analytics.get("oldest_unresolved_hours", 0.0)),
            recommendation_analytics.get("oldest_unresolved_hours", 0.0),
        )
        analytics_row_2 = st.columns(1)
        analytics_row_2[0].metric(
            _t("maintenance_recommendation_avg_resolution_hours", value=recommendation_analytics.get("average_time_to_resolve_hours", 0.0)),
            recommendation_analytics.get("average_time_to_resolve_hours", 0.0),
        )

        st.caption(_t("maintenance_recommendation_summary"))
        sorted_recommendations = sorted(
            recommendations,
            key=lambda item: (
                0
                if bool(item.get("active", True)) and not bool(item.get("resolved", False)) and not bool(item.get("acknowledged", False))
                else 1
                if bool(item.get("active", True)) and not bool(item.get("resolved", False)) and bool(item.get("acknowledged", False))
                else 2
                if not bool(item.get("active", True)) and not bool(item.get("resolved", False))
                else 3,
                -int(item.get("shown_count", 1) or 1),
                str(item.get("code") or ""),
            ),
        )
        for item in sorted_recommendations:
            code = str(item.get("code") or "-")
            shown_count = int(item.get("shown_count", 1) or 1)
            code_events = recommendation_events_by_code.get(code, [])
            reappeared_count = sum(1 for event in code_events if str(event.get("event_type") or "") == "reappeared")
            resolved_events = [
                event for event in code_events if str(event.get("event_type") or "") == "resolved"
            ]
            last_resolved = _format_timestamp(resolved_events[-1].get("timestamp")) if resolved_events else "-"
            active = bool(item.get("active", True))
            acknowledged = bool(item.get("acknowledged", False))
            resolved = bool(item.get("resolved", False))
            badge_key = "resolved"
            badge_label = _t("maintenance_recommendation_badge_resolved")
            if active and not resolved and not acknowledged:
                badge_key = "needs_action"
                badge_label = _t("maintenance_recommendation_badge_needs_action")
            elif active and not resolved and acknowledged:
                badge_key = "acknowledged"
                badge_label = _t("maintenance_recommendation_badge_acknowledged")
            elif not active and not resolved:
                badge_key = "monitor"
                badge_label = _t("maintenance_recommendation_badge_monitor")
            st.markdown(
                f"{_runbook_badge(badge_key, badge_label)} <strong>{escape(code)}</strong>",
                unsafe_allow_html=True,
            )
            st.caption(
                _t(
                    "maintenance_recommendation_summary_line",
                    code=code,
                    repeats=max(shown_count - 1, 0),
                    reappeared=reappeared_count,
                    last_resolved=last_resolved,
                )
            )
    if recommendation_events:
        st.caption(_t("maintenance_recommendation_history"))
        for event in recommendation_events[-8:]:
            st.caption(
                _t(
                    "maintenance_recommendation_event_line",
                    timestamp=_format_timestamp(event.get("timestamp")),
                    code=event.get("code") or "-",
                    event_type=event.get("event_type") or "-",
                )
            )
            if event.get("note"):
                st.caption(_t("maintenance_recommendation_event_note", value=event["note"]))


def _render_operational_health(health: dict, scope_key: str = "global", enable_ack: bool = False) -> None:
    if not health:
        return
    status = str(health.get("status") or "healthy").lower()
    if status == "critical":
        st.error(f"{_t('operational_health')}: {_t('health_critical')}")
    elif status == "warning":
        st.warning(f"{_t('operational_health')}: {_t('health_warning')}")
    else:
        st.success(f"{_t('operational_health')}: {_t('health_healthy')}")
    st.caption(_t("operational_health_score", score=int(health.get("score", 100) or 100)))
    reasons = ", ".join(health.get("reasons") or []) or "-"
    st.caption(_t("operational_health_reasons", value=reasons))
    alerts = health.get("alerts") or []
    if alerts:
        st.caption(_t("operational_health_alerts"))
        for alert in alerts:
            key = f"operational_health_alert_{alert.get('code')}"
            message = _t(
                key,
                current_value=round(float(alert.get("current_value", 0.0) or 0.0), 2),
                threshold=round(float(alert.get("threshold", 0.0) or 0.0), 2),
            )
            if (alert.get("severity") or "warning").lower() == "critical":
                st.error(message)
            else:
                st.warning(message)
            if alert.get("hint"):
                st.caption(_t("operational_health_alert_hint", hint=alert["hint"]))
    recommendations = health.get("recommendations") or []
    if recommendations:
        st.caption(_t("operational_recommendations"))
        for item in recommendations:
            message = str(item.get("message") or item.get("code") or "-")
            shown_count = int(item.get("shown_count", 1) or 1)
            last_shown_at = _format_timestamp(item.get("last_shown_at"))
            acknowledged = bool(item.get("acknowledged", False))
            acknowledged_at = _format_timestamp(item.get("acknowledged_at"))
            resolved = bool(item.get("resolved", False))
            resolved_at = _format_timestamp(item.get("resolved_at"))
            resolution_note = str(item.get("resolution_note") or "").strip()
            active = bool(item.get("active", True))
            st.caption(f"- {message}")
            meta_parts = [
                _t("operational_recommendation_repeat", count=shown_count),
                _t("operational_recommendation_last_shown", timestamp=last_shown_at),
            ]
            if acknowledged:
                meta_parts.append(_t("operational_recommendation_acknowledged", timestamp=acknowledged_at))
            if resolved:
                meta_parts.append(_t("operational_recommendation_resolved", timestamp=resolved_at))
            if not active:
                meta_parts.append(_t("operational_recommendation_inactive"))
            st.caption(" | ".join(meta_parts))
            if resolution_note:
                st.caption(_t("operational_recommendation_note", value=resolution_note))
            if enable_ack and active and not resolved:
                if not acknowledged:
                    button_key = f"ack-operational-recommendation-{scope_key}-{item.get('code')}"
                    if st.button(
                        _t("operational_recommendation_ack"),
                        key=button_key,
                        use_container_width=False,
                    ):
                        response = _safe_api_call(
                            _api_post,
                            f"/health/queues/operational-health/recommendations/{item.get('code')}/ack",
                        )
                        if response is not None:
                            st.rerun()
                note_key = f"resolve-operational-recommendation-note-{scope_key}-{item.get('code')}"
                resolution_value = st.text_input(
                    _t("operational_recommendation_note_input"),
                    key=note_key,
                    value=resolution_note,
                )
                resolve_button_key = f"resolve-operational-recommendation-{scope_key}-{item.get('code')}"
                if st.button(
                    _t("operational_recommendation_resolve"),
                    key=resolve_button_key,
                    use_container_width=False,
                ):
                    response = _safe_api_call(
                        _api_post,
                        f"/health/queues/operational-health/recommendations/{item.get('code')}/resolve",
                        {"note": resolution_value},
                    )
                    if response is not None:
                        st.rerun()
    trend = health.get("trend") or {}
    if trend:
        st.caption(_t("operational_health_trend"))
        st.caption(_t("operational_health_direction", value=trend.get("score_direction") or "stable"))
        st.caption(_t("operational_health_average", value=trend.get("average_score", 100.0)))
        scores = " ".join(str(item) for item in (trend.get("recent_scores") or [])) or "-"
        statuses = " ".join(str(item) for item in (trend.get("recent_statuses") or [])) or "-"
        st.caption(_t("operational_health_scores", value=scores))
        st.caption(_t("operational_health_statuses", value=statuses))


def _render_job_card(job: dict, job_kind: str, key_prefix: str = "") -> None:
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
        button_key = f"{key_prefix}requeue-{job_kind}-{job['id']}"
        path = (
            f"/v1/search-jobs/{job['id']}/requeue"
            if job_kind == "search"
            else f"/v1/research/finalize-jobs/{job['id']}/requeue"
        )
        if st.button(_t("requeue_job", job_id=job["id"]), key=button_key, use_container_width=True):
            _requeue_job(path, _t("requeued_job", job_kind=job_kind))


def _render_job_section(title: str, jobs: list[dict], job_kind: str, key_prefix: str = "") -> None:
    st.markdown(f"**{title}**")
    if not jobs:
        st.caption(_t("no_jobs_bucket"))
        return
    for job in jobs:
        _render_job_card(job, job_kind, key_prefix=key_prefix)


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

    # Cache active state for adaptive queue interval
    st.session_state["_queue_has_active"] = (
        metrics.get("running_search_jobs", 0) > 0
        or metrics.get("running_finalize_jobs", 0) > 0
        or metrics.get("pending_search_jobs", 0) > 0
        or metrics.get("pending_finalize_jobs", 0) > 0
    )

    top = st.columns(3)
    top[0].metric(_t("pending_search"), metrics["pending_search_jobs"])
    top[1].metric(_t("running_search"), metrics["running_search_jobs"])
    top[2].metric(_t("dead_search"), metrics["dead_letter_search_jobs"])

    bottom = st.columns(3)
    bottom[0].metric(_t("pending_finalize"), metrics["pending_finalize_jobs"])
    bottom[1].metric(_t("running_finalize"), metrics["running_finalize_jobs"])
    bottom[2].metric(_t("dead_finalize"), metrics["dead_letter_finalize_jobs"])

    with st.expander(_t("queue_actions"), expanded=False):
        c1, c2 = st.columns(2)
        if c1.button(_t("recover_stale_search"), use_container_width=True):
            _run_queue_action(_t("recovered_stale_search_jobs"), "/v1/search-jobs/recover-stale")
        if c2.button(_t("recover_stale_finalize"), use_container_width=True):
            _run_queue_action(_t("recovered_stale_finalize_jobs"), "/v1/research/finalize-jobs/recover-stale")
        c3, c4 = st.columns(2)
        if c3.button(_t("cleanup_search_jobs"), use_container_width=True):
            _run_queue_action(_t("cleaned_search_jobs"), "/v1/search-jobs/cleanup")
        if c4.button(_t("cleanup_finalize_jobs"), use_container_width=True):
            _run_queue_action(_t("cleaned_finalize_jobs"), "/v1/research/finalize-jobs/cleanup")
        if st.button(_t("run_full_maintenance"), use_container_width=True):
            _run_queue_action(_t("queue_maintenance"), "/health/queues/maintenance")

    with st.expander(_t("operational_view"), expanded=False):
        job_filter = st.text_input(
            _t("job_filter"),
            value="",
            placeholder=_t("job_filter_placeholder"),
        )
        search_running = _filter_jobs(_safe_api_call(_api_get, "/v1/search-jobs?status=running") or [], job_filter)
        search_dead = _filter_jobs(_safe_api_call(_api_get, "/v1/search-jobs?status=dead_letter") or [], job_filter)
        finalize_running = _filter_jobs(_safe_api_call(_api_get, "/v1/research/finalize-jobs?status=running") or [], job_filter)
        finalize_dead = _filter_jobs(_safe_api_call(_api_get, "/v1/research/finalize-jobs?status=dead_letter") or [], job_filter)
        left, right = st.columns(2, gap="large")
        with left:
            _render_job_section(_t("running_search_jobs"), search_running, "search", key_prefix="ops-")
            _render_job_section(_t("dead_letter_search_jobs"), search_dead, "search", key_prefix="ops-")
        with right:
            _render_job_section(_t("running_finalize_jobs"), finalize_running, "finalize", key_prefix="ops-")
            _render_job_section(_t("dead_letter_finalize_jobs"), finalize_dead, "finalize", key_prefix="ops-")


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
    if search_job and search_job.get("status") not in ("completed", None):
        status_line = f"{status_line} {_status_badge(search_job['status'])}"

    source_count = _task_source_count(task)
    label = f"{index}. {task['description']}"
    with st.expander(label, expanded=False):
        st.markdown(status_line, unsafe_allow_html=True)
        st.caption(_t("collected_sources", count=source_count))

        if search_job and search_job.get("error"):
            st.warning(search_job["error"])
        if search_job and search_job.get("status") == "dead_letter":
            if st.button(
                _t("requeue_dead_search_job"),
                key=f"requeue-dead-search-job-{search_job['id']}",
                use_container_width=True,
            ):
                _requeue_job(
                    f"/v1/search-jobs/{search_job['id']}/requeue",
                    _t("requeued_job", job_kind="search"),
                )

        st.code("\n".join(task["queries"]) or "-", language="text")

        logs = task.get("recent_logs") or task.get("logs") or []
        if logs:
            st.caption("\n".join(logs[-5:]))

        results = task.get("source_preview") or task.get("result") or []
        if not results:
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
    _render_job_card(latest_finalize_job, "finalize", key_prefix="latest-")
    return latest_finalize_job


def _fix_source_links(report: str) -> str:
    """Patch reports stored in DB before the analyzer fixes:

    1. Convert '- [S2] https://url' to proper markdown links (fixes 'bullet' text).
    2. Strip LLM meta-commentary preamble before the first heading.
    3. Remove the '## Примечания к отчёту / Report Notes' section.
    """
    import re as _re

    # 1. Strip LLM meta-commentary before the first # heading
    _PREAMBLE = _re.compile(
        r"(?i)^(ниже\s+представлен|ниже\s+приведён|below\s+is\s+(the|a)\s+|"
        r"the\s+following\s+is|вот\s+синтезирован|данный\s+отчёт\s+объединяет|"
        r"here\s+is\s+(the|a)\s+)[^\n]*\n+",
    )
    report = _PREAMBLE.sub("", report).strip()

    # 2. Remove Report Notes section
    _NOTES_SECTION = _re.compile(
        r"\n+##\s+(Примечания к отчёту|Примечания к отчету|Report Notes)\s*\n.*?(?=\n## |\Z)",
        _re.DOTALL,
    )
    report = _NOTES_SECTION.sub("", report)

    # 3. Convert old bare-URL source lines to clickable markdown links
    _OLD_SOURCE = _re.compile(
        r"^- \[S(\d+)\] (https?://\S+)$",
        _re.MULTILINE,
    )
    def _replace(m: _re.Match) -> str:
        sid = f"S{m.group(1)}"
        url = m.group(2)
        try:
            from urllib.parse import urlparse as _up
            label = _up(url).netloc.removeprefix("www.") or url
        except Exception:
            label = url
        return f"- **\\[{sid}\\]** [{label}]({url})"

    return _OLD_SOURCE.sub(_replace, report)


def _render_research_details() -> None:
    research_id = st.session_state.get("selected_research_id", "").strip()
    st.subheader(_t("research_details"))

    if not research_id:
        st.info(_t("no_research_selected"))
        return

    # Fetch fresh data; fall back to cache so the panel never goes blank
    # between fragment ticks (prevents the visual flicker).
    fresh = _safe_api_call(_api_get, f"/v1/research/{research_id}/summary", ignore_status_codes={404})
    cache_key = f"_rc_{research_id}"
    if fresh is not None:
        st.session_state[cache_key] = fresh
    research = st.session_state.get(cache_key)
    if not research:
        st.info(_t("no_research_selected"))
        return

    tasks = research.get("tasks") or []
    completed_tasks = int(research.get("completed_tasks") or 0)
    failed_tasks = int(research.get("failed_tasks") or 0)
    finalize_ready = bool(research.get("finalize_ready"))
    total_sources = int(research.get("collected_sources") or 0)
    latest_finalize_job = research.get("latest_finalize_job")
    latest_finalize_status = (latest_finalize_job or {}).get("status")
    research_status = (research.get("status") or "").strip().lower()

    # Cache status for adaptive polling interval
    prev_status = st.session_state.get(f"_rs_{research_id}", "")
    st.session_state[f"_rs_{research_id}"] = research_status
    if research_status in _TERMINAL_STATUSES and prev_status not in _TERMINAL_STATUSES and prev_status:
        st.rerun(scope="app")  # full-page rerun so _get_live_refresh_interval() returns None

    st.markdown(
        f"""
        <div class="mas-panel">
            <div class="mas-accent">{escape(_t('prompt'))}</div>
            <div>{escape(research['prompt'])}</div>
            <div style="margin-top:0.5rem; display:flex; gap:0.5rem; align-items:center;">
                {_status_badge(research['status'])}
                <span class="mas-kv">{escape(_t('depth', value=research['depth']))} &nbsp;·&nbsp;
                {escape(_t('tasks', count=len(research.get('task_ids', []))))} &nbsp;·&nbsp;
                {escape(_t('collected_sources', count=total_sources))}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    progress_total = len(tasks)
    progress_value = (completed_tasks + failed_tasks) / progress_total if progress_total else 0.0
    st.progress(
        progress_value,
        text=_t("progress_caption", completed=completed_tasks + failed_tasks, total=progress_total),
    )

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
            st.success(_t("finalize_job_queued", job_id=result.get("finalize_job_id") or _t("already_queued")))
            st.rerun()

    if latest_finalize_status == "running" or research_status == "analyzing":
        st.warning(_t("finalize_running"))
    elif latest_finalize_status == "pending":
        st.info(_t("finalize_pending"))
    elif finalize_ready and not latest_finalize_job:
        st.success(_t("all_tasks_completed"))

    # Graph trail — live execution log of the finalize graph
    if latest_finalize_job or research_status in _ACTIVE_STATUSES | _TERMINAL_STATUSES:
        with st.expander(_t("graph_trail"), expanded=(research_status in _ACTIVE_STATUSES)):
            _render_graph_trail(research_id)

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
        partial = research.get("partial_report") or ""
        if partial:
            is_sections = "=== SECTION" in partial or "---" in partial[:200]
            banner = _t("partial_report_sections_banner") if is_sections else _t("partial_report_banner")
            st.info(f"⏳ {banner}")
            st.markdown(partial)
        else:
            st.info(_t("final_report_not_ready"))
        return

    report_payload = _safe_api_call(_api_get, f"/v1/research/{research_id}/report", ignore_status_codes={404})
    if not report_payload:
        return
    final_report = report_payload.get("final_report")
    if not final_report:
        st.info(_t("final_report_not_ready"))
        return

    rendered_tab, raw_tab = st.tabs([_t("rendered"), _t("raw_markdown")])
    with rendered_tab:
        st.markdown(_fix_source_links(final_report))
    with raw_tab:
        st.code(final_report, language="markdown")

    # ── download buttons ─────────────────────────────────────────────────────
    st.divider()
    _prompt  = research.get("prompt", "")
    _depth   = research.get("depth", "")
    _created = research.get("created_at", "")

    col_pdf, col_docx = st.columns(2)
    with col_pdf:
        try:
            pdf_bytes = generate_pdf(final_report, _prompt, _depth, _created)
            st.download_button(
                label=_t("download_pdf"),
                data=pdf_bytes,
                file_name="research_report.pdf",
                mime="application/pdf",
                use_container_width=True,
            )
        except Exception as _exc:
            st.error(f"PDF error: {_exc}")
    with col_docx:
        try:
            docx_bytes = generate_docx(final_report, _prompt, _depth, _created)
            st.download_button(
                label=_t("download_docx"),
                data=docx_bytes,
                file_name="research_report.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                use_container_width=True,
            )
        except Exception as _exc:
            st.error(f"DOCX error: {_exc}")


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
    research_interval = _get_live_refresh_interval()

    left, right = st.columns([1.05, 1.35], gap="large")
    with left:
        _render_create_research()
        st.divider()
        _render_live_queue_fragment()
    with right:
        _render_live_research_fragment(research_interval)


if __name__ == "__main__":
    main()

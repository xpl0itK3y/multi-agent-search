# -*- coding: utf-8 -*-
"""A seeded demo backend for looking at the SPA: the real app, fake data, no LLM, no database.

Builds the real FastAPI app (src.api.app.create_app) on the in-memory task store, seeds it
through the real store/service methods once the app's lifespan has created the service, and
serves it with uvicorn on 127.0.0.1:8000 (the vite dev server's proxy target). Seeded:

- a regular user and an admin (credentials below), five more users with sessions/telemetry;
- a completed Russian research (hard depth, replan loop, conflicts, red team, stance, chat,
  a public share link), a completed English one (comparison table), a RUNNING one (analyzing,
  partial report, live worker heartbeats), a history of completed/failed/cancelled ones;
- LLM usage for two weeks, user events, admin audit rows, a password-reset link.

Nothing in the repository is written. The ids, share token, reset link and credentials go
to <out>/seed_info.json (default <tempdir>/veris-demo), which shoot.cjs reads.

    python demo_server.py [--port 8000] [--out DIR]

The content (demo_content.py) is invented for screenshots: the quotes attributed to real
sites are not real quotes.
"""
import argparse
import os
import sys
import json
import secrets
import tempfile
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
# .claude/skills/ui-screenshots/scripts -> the repository root
REPO = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))

_parser = argparse.ArgumentParser(description="Seeded demo backend for UI screenshots")
_parser.add_argument("--port", type=int, default=8000)
_parser.add_argument("--out", default=os.environ.get("DEMO_OUT") or os.path.join(tempfile.gettempdir(), "veris-demo"))
ARGS, _ = _parser.parse_known_args()
OUT = os.path.abspath(ARGS.out)
os.makedirs(OUT, exist_ok=True)
# A previous run's ids are dead in this fresh in-memory store: never leave them for shoot.cjs.
try:
    os.remove(os.path.join(OUT, "seed_info.json"))
except FileNotFoundError:
    pass

ADMIN_EMAIL = "admin@example.com"
ADMIN_PASSWORD = "Admin-Demo-2026!"
USER_EMAIL = "anna.petrova@example.com"
USER_PASSWORD = "Anna-Demo-2026!"

os.environ.update(
    {
        "TASK_STORE_BACKEND": "memory",
        "ALLOW_MEMORY_TASK_STORE": "true",
        "AUTH_DISABLED": "false",
        "AUTH_SECRET_KEY": secrets.token_urlsafe(48),
        "ADMIN_EMAILS": ADMIN_EMAIL,
        "EMAIL_BACKEND": "console",
        "PUBLIC_APP_URL": "http://localhost:5173",
        # Dummy OAuth client so the login page renders its Google button (never clicked).
        "GOOGLE_CLIENT_ID": "demo-client-id.apps.googleusercontent.com",
        "GOOGLE_CLIENT_SECRET": "demo-client-secret",
        "GOOGLE_REDIRECT_URI": "http://localhost:5173/v1/auth/google/callback",
        "DEEPSEEK_API_KEY": "",
        "TAVILY_API_KEY": "",
        "RETRACTION_CHECK_ENABLED": "false",
        "USE_REDIS_BROKER": "false",
        "AUTH_RATE_LIMIT_PER_MINUTE": "60",
        "CORS_ALLOW_ORIGINS": "http://localhost:5173,http://127.0.0.1:5173",
    }
)
os.environ.pop("REDIS_URL", None)
os.environ.pop("DATABASE_URL", None)
sys.path.insert(0, REPO)
sys.path.insert(0, HERE)
os.chdir(REPO)

import uvicorn  # noqa: E402

from src.api.app import create_app  # noqa: E402
from src.agents.trail_text import trail_detail  # noqa: E402
from src.graph.research_graph import GRAPH_STEP_METADATA  # noqa: E402
from src.domain import (  # noqa: E402
    AuthActionPurpose,
    RedTeamReport,
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    SearchSourcePreview,
    TaskStatus,
)
from src.services.account_recovery_mixin import EMAIL_VERIFICATION_PATH  # noqa: E402

import demo_content as C  # noqa: E402

NOW = datetime.now(timezone.utc)
FINALIZE_META = {
    "redteam": {"agent": "RedTeamAgent", "phase": "verify", "action": "stress_test"},
    "audit": {"agent": "CitationAuditAgent", "phase": "verify", "action": "audit_citations"},
    "viewpoints": {"agent": "StanceAgent", "phase": "verify", "action": "stance_detection"},
    "completed": {"agent": "System", "phase": "complete", "action": "finish"},
}


class Clock:
    """Monotonic fake clock for spacing trail timestamps."""

    def __init__(self, start: datetime):
        self.t = start

    def tick(self, seconds: float) -> datetime:
        self.t = self.t + timedelta(seconds=seconds)
        return self.t


def ev(store, rid, clock, gap, step, detail, agent, phase, action, sources=None, metrics=None):
    event = {
        "timestamp": clock.tick(gap).isoformat(),
        "step": step,
        "agent": agent,
        "phase": phase,
        "action": action,
        "detail": detail,
    }
    if sources:
        event["sources"] = sources[:6]
    if metrics:
        event["metrics"] = metrics
    store.append_research_graph_event(rid, event)


def graph_ev(store, rid, clock, gap, step, detail, metrics=None):
    meta = GRAPH_STEP_METADATA.get(step, {})
    ev(store, rid, clock, gap, step, detail, meta.get("agent", "FinalizeRunner"),
       meta.get("phase", "synthesis"), meta.get("action", step), metrics=metrics)


def fin_ev(store, rid, clock, gap, step, lang, **params):
    meta = FINALIZE_META[step]
    ev(store, rid, clock, gap, step, trail_detail(step, lang, **params), meta["agent"], meta["phase"], meta["action"])


def plan_ev(store, rid, clock, gap, step, action, detail, agent="OrchestratorAgent", metrics=None):
    ev(store, rid, clock, gap, step, detail, agent, "plan", action, metrics=metrics)


def search_ev(store, rid, clock, gap, action, detail, sources=None, metrics=None):
    ev(store, rid, clock, gap, "search", detail, "SearchAgent", "search", action, sources=sources, metrics=metrics)


def result_row(src: dict) -> dict:
    content = src["content"]
    return {
        "url": src["url"],
        "title": src["title"],
        "domain": src["domain"],
        "content": content,
        "snippet": content[:280],
        "source_quality": src.get("source_quality", "medium"),
        "extraction_status": "success",
    }


def add_task(store, rid, spec, results, status=TaskStatus.COMPLETED, logs=None):
    task_id = str(uuid.uuid4())
    store.add_task(
        {
            "id": task_id,
            "research_id": rid,
            "description": spec["description"],
            "queries": spec["queries"],
            "status": status,
            "result": results,
            "logs": logs or [f"Queries: {len(spec['queries'])}", f"Selected sources: {len(results)}"],
            "search_metrics": {
                "candidate_count": 8 + 3 * len(results),
                "extraction_attempts": 4 + 2 * len(results),
                "extraction_success_count": 3 + 2 * len(results),
                "extraction_failure_count": 1,
                "selected_source_count": len(results),
                "avg_content_chars": 5400.0 + 250 * len(results),
            },
        }
    )
    return task_id


def search_block(store, rid, clock, lang, spec, srcs):
    search_ev(store, rid, clock, 2.1, "task_start", trail_detail("search_task_start", lang, task=spec["description"]))
    for q_index, query in enumerate(spec["queries"]):
        found = [{"domain": s["domain"], "title": s["title"]} for s in srcs] or [
            {"domain": "google.com", "title": query}
        ]
        search_ev(store, rid, clock, 3.4 + q_index, "query",
                  trail_detail("search_query", lang, query=query, count=7 + 2 * q_index + len(srcs)),
                  sources=found)
    for s in srcs:
        search_ev(store, rid, clock, 2.6, "scrape",
                  trail_detail("search_scrape", lang, domain=s["domain"], chars=len(s["content"]) * 9),
                  sources=[{"domain": s["domain"], "title": s["title"]}])
    search_ev(store, rid, clock, 1.2, "task_complete",
              trail_detail("search_task_complete", lang, task=spec["description"], count=len(srcs)),
              metrics={"selected_source_count": len(srcs)})


def backdate_trail(research, start: datetime) -> datetime:
    """Re-time events stamped 'now' by the trust mixins so they follow the seeded run.
    Returns the timestamp of the last re-timed event."""
    cursor = start
    fixed = []
    for entry in research.graph_trail:
        ts = datetime.fromisoformat(entry["timestamp"])
        if ts > start:
            cursor = cursor + timedelta(seconds=2.5)
            entry = {**entry, "timestamp": cursor.isoformat()}
        fixed.append(entry)
    fixed.sort(key=lambda e: e["timestamp"])
    research.graph_trail = fixed
    return cursor


def seed_completed(service, *, user_id, prompt, title, language, depth, sources, tasks, replan_task,
                   report, conflicts, red_team, stance, cross_language, comparison, usage,
                   started, model="deepseek-v4-pro", thread_id=None, messages=None, share=False):
    store = service.task_store
    record = store.add_research(ResearchRequest(prompt=prompt, depth=depth), [], user_id=user_id, language=language)
    rid = record.id
    store.merge_research_graph_state(rid, {"thread_id": thread_id or rid, "model": model, "depth": depth.value,
                                           **({"title": title} if title else {})})
    by_id = {s["source_id"]: s for s in sources}
    clock = Clock(started)
    lang = language

    plan_ev(store, rid, clock, 0.5, "plan_start", "analyze_prompt", trail_detail("plan_start", lang))
    plan_ev(store, rid, clock, 4.2, "decompose", "decompose_topics", trail_detail("decompose", lang, depth=depth.value))
    all_tasks = list(tasks)
    plan_ev(store, rid, clock, 6.8, "plan_ready", "tasks_enqueued", trail_detail("plan_ready", lang, count=len(all_tasks)),
            metrics={"task_count": len(all_tasks)})

    task_ids = []
    for spec in tasks:
        srcs = [by_id[s] for s in spec["sources"]]
        search_block(store, rid, clock, lang, spec, srcs)
        task_ids.append(add_task(store, rid, spec, [result_row(s) for s in srcs]))

    graph_ev(store, rid, clock, 3.0, "collect_context", trail_detail("collect_context", lang))
    replan_attempts = 0
    if replan_task:
        graph_ev(store, rid, clock, 5.5, "collect_context",
                 trail_detail("collect_context_done", lang, count=len(sources), replan=True))
        graph_ev(store, rid, clock, 1.5, "replan", trail_detail("replan", lang))
        graph_ev(store, rid, clock, 4.0, "replan", trail_detail("replan_done", lang, tasks=1, recommendations=2))
        search_block(store, rid, clock, lang, replan_task, [])
        task_ids.append(add_task(store, rid, replan_task, []))
        graph_ev(store, rid, clock, 2.0, "collect_context", trail_detail("collect_context", lang))
        replan_attempts = 1
    graph_ev(store, rid, clock, 4.5, "collect_context",
             trail_detail("collect_context_done", lang, count=len(sources), replan=False))
    graph_ev(store, rid, clock, 1.0, "analyze", trail_detail("analyze", lang), metrics={"attempt": 1})
    graph_ev(store, rid, clock, 48.0, "analyze", trail_detail("analyze_done", lang, attempt=1))
    graph_ev(store, rid, clock, 1.0, "verify", trail_detail("verify", lang))
    graph_ev(store, rid, clock, 9.0, "verify",
             trail_detail("verify_done", lang, weak_support=False, conflicts=len(conflicts), retry=False, tie_break=False))
    ev(store, rid, clock, 0.5, "complete", trail_detail("complete", lang, count=1), "FinalizeRunner", "synthesis", "complete")
    fin_ev(store, rid, clock, 1.0, "redteam", lang)
    fin_ev(store, rid, clock, 14.0, "audit", lang)
    store.set_research_task_ids(rid, task_ids)

    research = store.get_research(rid)
    tasks_objs = store.get_tasks_by_research(rid)
    aggregated = [dict(s) for s in sources]
    store.merge_research_graph_state(
        rid,
        {
            "effective_prompt": prompt,
            "task_ids": task_ids,
            "canonical_sources": service._canonical_source_table(aggregated),
            "detected_conflicts": conflicts,
            "analyze_attempts": 1,
            "replan_attempts": replan_attempts,
            "tie_break_attempts": 0,
            "step": "complete",
        },
    )
    research = store.get_research(rid)

    red = RedTeamReport.model_validate({**red_team, "research_id": rid})
    service._store_red_team(rid, red)
    full_report = report.rstrip() + "\n\n" + service._render_red_team_section(red, lang)

    service._audit_citations(full_report, research, tasks_objs, aggregated=aggregated)
    service._analyze_source_independence(research, tasks_objs, aggregated=aggregated)
    service._assess_source_reputation(research, tasks_objs, aggregated=aggregated)
    service._check_numbers(full_report, research, tasks_objs, aggregated=aggregated)
    if comparison:
        store.merge_research_graph_state(rid, {"comparison": {**comparison, "research_id": rid}})
    clock.t = backdate_trail(store.get_research(rid), clock.t)
    fin_ev(store, rid, clock, 3.0, "viewpoints", lang)
    store.merge_research_graph_state(
        rid,
        {
            "stance_balance": {**stance, "research_id": rid},
            "cross_language": {**cross_language, "research_id": rid},
            "llm_token_usage": usage,
        },
    )
    full_report = service._inject_graph_execution_trail(full_report, rid)
    from src.ui.report_utils import clean_report

    full_report = clean_report(full_report)
    store.update_research_status(rid, ResearchStatus.COMPLETED, full_report)
    fin_ev(store, rid, clock, 1.0, "completed", lang)

    for role, content in messages or []:
        cited = [by_id[s] for s in ("S1", "S3", "S4", "S5", "S6") if f"[{s}]" in content]
        service.append_research_message(
            rid,
            role,
            content,
            sources=[
                SearchSourcePreview(url=s["url"], source_id=s["source_id"], title=s["title"], domain=s["domain"],
                                    source_quality=s["source_quality"], snippet=s["content"][:200])
                for s in cited
            ] if role == "assistant" else None,
        )

    token = service.create_share_link(rid).token if share else None
    research = store.get_research(rid)
    research.created_at = started
    research.updated_at = clock.t
    return rid, token


def seed_running(service, *, user_id):
    store = service.task_store
    lang = "ru"
    started = NOW - timedelta(minutes=6, seconds=40)
    record = store.add_research(ResearchRequest(prompt=C.RUN_PROMPT, depth=SearchDepth.HARD), [], user_id=user_id, language=lang)
    rid = record.id
    store.merge_research_graph_state(rid, {"thread_id": rid, "model": "deepseek-v4-pro", "depth": "hard"})
    clock = Clock(started)
    plan_ev(store, rid, clock, 0.5, "plan_start", "analyze_prompt", trail_detail("plan_start", lang))
    plan_ev(store, rid, clock, 5.0, "decompose", "decompose_topics", trail_detail("decompose", lang, depth="hard"))
    plan_ev(store, rid, clock, 3.0, "cross_language", "expand_languages",
            trail_detail("cross_language", lang, languages="английском, французском"), agent="CrossLanguageAgent")
    plan_ev(store, rid, clock, 6.0, "plan_ready", "tasks_enqueued", trail_detail("plan_ready", lang, count=len(C.RUN_TASKS)),
            metrics={"task_count": len(C.RUN_TASKS)})

    def fake_sources(pairs):
        return [
            {"domain": d, "title": t, "url": f"https://{d}/article/{abs(hash(t)) % 100000}",
             "content": (t + ". ") * 40, "source_quality": "high" if d.endswith((".org", ".gov", ".eu")) else "medium"}
            for d, t in pairs
        ]

    task_ids = []
    for spec in C.RUN_TASKS:
        srcs = fake_sources(spec["sources"])
        for s in srcs:
            s["source_id"] = ""
        search_block(store, rid, clock, lang, spec, srcs)
        task_ids.append(add_task(store, rid, spec, [result_row(s) for s in srcs]))

    graph_ev(store, rid, clock, 3.0, "collect_context", trail_detail("collect_context", lang))
    graph_ev(store, rid, clock, 6.0, "collect_context", trail_detail("collect_context_done", lang, count=9, replan=True))
    graph_ev(store, rid, clock, 1.2, "replan", trail_detail("replan", lang))
    graph_ev(store, rid, clock, 5.0, "replan", trail_detail("replan_done", lang, tasks=2, recommendations=3))

    first = C.RUN_REPLAN_TASKS[0]
    srcs = fake_sources(first["sources"])
    search_block(store, rid, clock, lang, first, srcs)
    task_ids.append(add_task(store, rid, first, [result_row(s) for s in srcs]))

    second = C.RUN_REPLAN_TASKS[1]
    srcs = fake_sources(second["sources"])
    search_ev(store, rid, clock, 2.0, "task_start", trail_detail("search_task_start", lang, task=second["description"]))
    search_ev(store, rid, clock, 3.1, "query", trail_detail("search_query", lang, query=second["queries"][0], count=6),
              sources=[{"domain": s["domain"], "title": s["title"]} for s in srcs])
    running_task = add_task(store, rid, second, [result_row(s) for s in srcs], status=TaskStatus.RUNNING,
                            logs=["Queries: 2", "Scraping tengrinews.kz…"])
    task_ids.append(running_task)
    graph_ev(store, rid, clock, 4.0, "collect_context", trail_detail("collect_context", lang))
    graph_ev(store, rid, clock, 1.0, "analyze", trail_detail("analyze", lang), metrics={"attempt": 1})

    store.set_research_task_ids(rid, task_ids)
    store.merge_research_graph_state(rid, {"replan_attempts": 1, "analyze_attempts": 1, "step": "replan"})
    store.update_research_status(rid, ResearchStatus.ANALYZING)
    store.save_partial_report(rid, C.RUN_PARTIAL_REPORT)
    store.save_partial_reasoning(rid, C.RUN_REASONING)
    job = store.add_search_task_job(running_task, "hard", 3)
    research = store.get_research(rid)
    research.created_at = started
    return rid, job.id


def seed_history(service, *, user_id):
    store = service.task_store
    ids = []
    for item in C.HISTORY:
        depth = SearchDepth(item["depth"])
        rec = store.add_research(ResearchRequest(prompt=item["prompt"], depth=depth), [], user_id=user_id,
                                 language=item["language"])
        rid = rec.id
        state = {"thread_id": rid, "model": "deepseek-v4-pro", "depth": item["depth"]}
        if item.get("title"):
            state["title"] = item["title"]
        store.merge_research_graph_state(rid, state)
        started = NOW - timedelta(days=item["days_ago"], hours=3)
        clock = Clock(started)
        lang = item["language"]
        plan_ev(store, rid, clock, 0.5, "plan_start", "analyze_prompt", trail_detail("plan_start", lang))
        plan_ev(store, rid, clock, 4.0, "decompose", "decompose_topics", trail_detail("decompose", lang, depth=item["depth"]))
        if item["status"] == "completed":
            srcs = [
                {"source_id": f"S{i}", "domain": d, "url": u, "title": t, "source_quality": q,
                 "content": f"{t}. " * 30}
                for i, (d, u, t, q) in enumerate(item["sources"], start=1)
            ]
            tid = add_task(store, rid, {"description": item["title"] or item["prompt"], "queries": [item["prompt"][:60]]},
                           [result_row(s) for s in srcs])
            store.set_research_task_ids(rid, [tid])
            store.merge_research_graph_state(rid, {"canonical_sources": service._canonical_source_table(srcs)})
            store.update_research_status(rid, ResearchStatus.COMPLETED, item["report"])
            store.merge_research_graph_state(rid, {"llm_token_usage": {"prompt_tokens": 38112, "completion_tokens": 5120,
                                                                       "total_tokens": 43232, "estimated_cost_usd": 0.0141}})
        elif item["status"] == "failed":
            plan_ev(store, rid, clock, 6.0, "plan_ready", "tasks_enqueued", trail_detail("plan_ready", lang, count=5))
            store.update_research_status(rid, ResearchStatus.FAILED)
        else:
            store.update_research_status(rid, ResearchStatus.CANCELLED)
        research = store.get_research(rid)
        research.created_at = started
        research.updated_at = started + timedelta(minutes=9)
        ids.append(rid)
    return ids


def seed_users(service):
    store = service.task_store
    admin, _ = service.provision_admin_account(ADMIN_EMAIL, ADMIN_PASSWORD)
    service.update_profile(admin.id, name="Ирина Админова")
    user = service.register_user(USER_EMAIL, USER_PASSWORD)
    service.update_profile(user.id, name="Анна Петрова")
    # Verify Anna's address through the real one-time-link path.
    link = service._issue_link(store.get_user_by_id(user.id), AuthActionPurpose.EMAIL_VERIFICATION,
                               EMAIL_VERIFICATION_PATH, 3600)
    service.verify_email_with_token(link.split("#token=", 1)[1], user.id)

    extra = [
        ("d.ivanov@example.com", "Дмитрий Иванов", ("Windows", "Chrome", "desktop", "Kazakhstan", "Almaty"), 0.2),
        ("maria.garcia@example.es", "María García", ("macOS", "Safari", "desktop", "Spain", "Valencia"), 26),
        ("j.smith@example.co.uk", "James Smith", ("iOS", "Safari", "mobile", "United Kingdom", "London"), 74),
        ("a.nurlanov@example.kz", None, ("Android", "Chrome", "mobile", "Kazakhstan", "Astana"), 240),
        ("olga.k@example.com", "Ольга Ковалёва", ("Windows", "Edge", "desktop", "Russia", "Moscow"), 700),
    ]
    extra_ids = []
    for email, name, (os_name, browser, device, country, city), hours_ago in extra:
        u = service.register_user(email, secrets.token_urlsafe(12))
        if name:
            service.update_profile(u.id, name=name)
        extra_ids.append((u.id, hours_ago, (os_name, browser, device, country, city)))

    sessions = [
        (admin.id, 0.01, ("Windows", "Chrome", "desktop", "Kazakhstan", "Almaty")),
        (user.id, 0.05, ("macOS", "Chrome", "desktop", "Kazakhstan", "Almaty")),
        (user.id, 20, ("iOS", "Safari", "mobile", "Kazakhstan", "Almaty")),
        *[(uid, h, meta) for uid, h, meta in extra_ids],
    ]
    ua = {
        ("Windows", "Chrome"): "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36",
        ("macOS", "Chrome"): "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36",
        ("macOS", "Safari"): "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
        ("iOS", "Safari"): "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
        ("Android", "Chrome"): "Mozilla/5.0 (Linux; Android 15; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Mobile Safari/537.36",
        ("Windows", "Edge"): "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36 Edg/140.0",
    }
    for i, (uid, hours_ago, (os_name, browser, device, country, city)) in enumerate(sessions):
        agent = ua[(os_name, browser)]
        ip = f"95.56.{10 + i}.{40 + 3 * i}"
        store.record_user_session(uid, f"sess-{i}-{uuid.uuid4().hex[:8]}", ip_address=ip, user_agent=agent,
                                  device_type=device, browser=browser, os=os_name,
                                  screen_res="1920x1080" if device == "desktop" else "390x844",
                                  viewport="1440x900" if device == "desktop" else "390x844",
                                  language="ru-RU" if country in ("Kazakhstan", "Russia") else "en-GB",
                                  client_timezone="Asia/Almaty", country=country, city=city)
        store.user_sessions[-1]["started_at"] = NOW - timedelta(hours=hours_ago + 1)
        store.user_sessions[-1]["last_active_at"] = NOW - timedelta(hours=hours_ago)
        store.touch_user_activity(uid, ip_address=ip, user_agent=agent, device=device)
        store.user_telemetry[uid]["last_seen_at"] = NOW - timedelta(hours=hours_ago)
    return admin, user, [uid for uid, _, _ in extra_ids]


def seed_usage(service, user_id, admin_id, extra_ids, research_ids):
    store = service.task_store
    rows = []
    for day in range(14):
        for k in range(2 + (day * 7) % 5):
            owner = [user_id, user_id, admin_id, *extra_ids][(day + k) % (3 + len(extra_ids))]
            rid = research_ids[(day + k) % len(research_ids)]
            model = "deepseek-v4-pro" if k % 3 else "deepseek-chat"
            pt = 9000 + (day * 1370 + k * 4410) % 42000
            ct = 900 + (day * 311 + k * 977) % 6200
            cost = round(pt * 0.00000027 + ct * 0.0000011, 5)
            store.record_llm_usage(rid, owner, model, pt, ct, pt + ct, cost, cache_hit_tokens=pt // 3)
            store.llm_usage_logs[-1]["created_at"] = NOW - timedelta(days=day, hours=(k * 5) % 23)
            rows.append(pt + ct)
    return len(rows)


def seed_events(service, user_id, research_id):
    store = service.task_store
    store.record_user_event("chat_prompt", "chat", user_id=user_id,
                            details={"prompt": C.RU_CHAT[0][1], "research_id": research_id})
    for name, cat in [("page_view", "navigation"), ("research_created", "research"), ("export_report", "research"),
                      ("theme_changed", "settings"), ("page_view", "navigation")]:
        store.record_user_event(name, cat, user_id=user_id, details={"path": "/"})
    store.record_admin_audit(ADMIN_EMAIL, "maintenance.cleanup_search_jobs", "queue", details={"deleted": 14})
    store.record_admin_audit(ADMIN_EMAIL, "users.export", "user", details={"rows": 7})
    store.record_admin_audit(ADMIN_EMAIL, "maintenance.recover_stale_finalize_jobs", "queue", details={"recovered": 1})


def heartbeat_loop(store):
    n = 0
    while True:
        n += 1
        for name, status, processed in [("search-worker-1", "running", 1284 + n), ("search-worker-2", "idle", 1190),
                                        ("finalize-worker-1", "running", 342)]:
            store.upsert_worker_heartbeat(
                name, processed, status,
                extraction_metrics={"attempts": 5120, "success_count": 4712, "empty_count": 188, "failure_count": 220,
                                    "success_rate_percent": 92.0, "avg_download_ms": 640.0, "avg_extract_ms": 85.0}
                if name.startswith("search") else {},
            )
        time.sleep(15)


def seed(service):
    store = service.task_store
    admin, user, extra_ids = seed_users(service)
    history = seed_history(service, user_id=user.id)
    en_id, _ = seed_completed(
        service, user_id=user.id, prompt=C.EN_PROMPT, title=C.EN_TITLE, language="en", depth=SearchDepth.MEDIUM,
        sources=C.EN_SOURCES, tasks=C.EN_TASKS, replan_task=None, report=C.EN_REPORT, conflicts=C.EN_CONFLICTS,
        red_team=C.EN_RED_TEAM,
        stance={"applicable": False, "sources": []},
        cross_language={"query_language": "en", "languages": [{"lang": "en", "count": 6}, {"lang": "es", "count": 1}],
                        "target_languages": ["es"], "foreign_source_count": 1, "monolingual": False,
                        "unique_findings": [{"lang": "es", "finding": "Valencia's city-wide trial measured emissions, not output."}]},
        comparison=C.EN_COMPARISON, usage=C.EN_USAGE, started=NOW - timedelta(days=1, hours=2, minutes=14),
    )
    ru_id, share_token = seed_completed(
        service, user_id=user.id, prompt=C.RU_PROMPT, title=C.RU_TITLE, language="ru", depth=SearchDepth.HARD,
        sources=C.RU_SOURCES, tasks=C.RU_TASKS, replan_task=C.RU_REPLAN_TASK, report=C.RU_REPORT,
        conflicts=C.RU_CONFLICTS, red_team=C.RU_RED_TEAM, stance=C.RU_STANCE, cross_language=C.RU_CROSS_LANGUAGE,
        comparison=None, usage=C.RU_USAGE, started=NOW - timedelta(hours=2, minutes=37), messages=C.RU_CHAT, share=True,
    )
    running_id, _ = seed_running(service, user_id=user.id)
    # One research owned by the admin so its own sidebar is not empty.
    admin_rid, _ = seed_completed(
        service, user_id=admin.id, prompt=C.EN_PROMPT, title=C.EN_TITLE, language="en", depth=SearchDepth.EASY,
        sources=C.EN_SOURCES, tasks=C.EN_TASKS, replan_task=None, report=C.EN_REPORT, conflicts=[],
        red_team={"findings": [], "challenged": 0, "held": 0}, stance={"applicable": False, "sources": []},
        cross_language={"query_language": "en", "languages": [{"lang": "en", "count": 4}], "monolingual": True},
        comparison=None, usage=C.EN_USAGE, started=NOW - timedelta(days=4),
    )
    seed_usage(service, user.id, admin.id, extra_ids, [ru_id, en_id, running_id, admin_rid, *history])
    seed_events(service, user.id, ru_id)
    _, reset_link = service.issue_password_reset_link(USER_EMAIL)
    service.llm_available = True  # health: agents stay None, so nothing can reach a real LLM
    threading.Thread(target=heartbeat_loop, args=(store,), daemon=True).start()

    info = {
        "admin": {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD, "id": admin.id},
        "user": {"email": USER_EMAIL, "password": USER_PASSWORD, "id": user.id},
        "ru_completed": ru_id,
        "en_completed": en_id,
        "running": running_id,
        "admin_research": admin_rid,
        "history": history,
        "share_token": share_token,
        "reset_link": reset_link,
    }
    with open(os.path.join(OUT, "seed_info.json"), "w", encoding="utf-8") as fh:
        json.dump(info, fh, ensure_ascii=False, indent=2)
    print("SEED_OK", json.dumps(info, ensure_ascii=False), flush=True)


app = create_app()
_orig_lifespan = app.router.lifespan_context


@asynccontextmanager
async def seeded_lifespan(a):
    async with _orig_lifespan(a) as state:
        seed(a.state.research_service)
        yield state


app.router.lifespan_context = seeded_lifespan

if __name__ == "__main__":
    print("seed_info.json ->", OUT, flush=True)
    uvicorn.run(app, host="127.0.0.1", port=ARGS.port, log_level="warning", access_log=False)

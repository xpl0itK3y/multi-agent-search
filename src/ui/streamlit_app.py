import os
from datetime import datetime
from html import escape
from typing import Any

import httpx
import streamlit as st
import streamlit.components.v1 as components


API_BASE_URL = os.getenv("STREAMLIT_API_BASE_URL", "http://localhost:8000").rstrip("/")
REQUEST_TIMEOUT_SECONDS = 30.0
WORKER_NAME = os.getenv("STREAMLIT_WORKER_NAME", "job-worker")


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
                radial-gradient(circle at top left, rgba(249, 216, 168, 0.38), transparent 28%),
                radial-gradient(circle at top right, rgba(132, 173, 222, 0.22), transparent 24%),
                linear-gradient(180deg, #f6f0e5 0%, #efe5d3 100%);
            color: #1f2933;
        }
        h1, h2, h3 {
            font-family: Georgia, "Times New Roman", serif;
            letter-spacing: -0.02em;
        }
        .stMarkdown, .stText, .stCaption, label, p, li, div[data-testid="stMetricValue"] {
            font-family: "Trebuchet MS", "Segoe UI", sans-serif;
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #1f3a5f 0%, #13263f 100%);
        }
        [data-testid="stSidebar"] * {
            color: #f8f4eb;
        }
        .mas-panel {
            border: 1px solid rgba(26, 61, 94, 0.12);
            background: rgba(255, 252, 246, 0.82);
            border-radius: 18px;
            padding: 1rem 1.1rem;
            box-shadow: 0 10px 30px rgba(31, 58, 95, 0.08);
            margin-bottom: 0.8rem;
        }
        .mas-accent {
            color: #8a3b12;
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
            color: #52606d;
            font-size: 0.92rem;
            margin-top: 0.25rem;
        }
        .mas-source {
            border-top: 1px solid rgba(31, 58, 95, 0.08);
            padding-top: 0.8rem;
            margin-top: 0.8rem;
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


def _truncate(value: str | None, limit: int = 220) -> str:
    if not value:
        return ""
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit - 3]}..."


def _safe_api_call(method, *args, ignore_status_codes: set[int] | None = None, **kwargs) -> Any | None:
    ignore_status_codes = ignore_status_codes or set()
    try:
        return method(*args, **kwargs)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in ignore_status_codes:
            return None
        detail = exc.response.text
        st.error(f"API error {exc.response.status_code}: {detail}")
    except httpx.HTTPError as exc:
        st.error(f"Connection error: {exc}")
    return None


def _initialize_state() -> None:
    query_research_id = st.query_params.get("research_id", "")
    if isinstance(query_research_id, list):
        query_research_id = query_research_id[0] if query_research_id else ""

    st.session_state.setdefault("selected_research_id", query_research_id)
    st.session_state.setdefault("auto_refresh_enabled", False)
    st.session_state.setdefault("auto_refresh_seconds", 10)


def _sync_query_params() -> None:
    research_id = st.session_state.get("selected_research_id", "").strip()
    if research_id:
        st.query_params["research_id"] = research_id
    elif "research_id" in st.query_params:
        del st.query_params["research_id"]


def _render_auto_refresh() -> None:
    if not st.session_state.get("auto_refresh_enabled"):
        return

    interval_seconds = max(int(st.session_state.get("auto_refresh_seconds", 10)), 3)
    components.html(
        f"""
        <script>
        window.setTimeout(function() {{
            window.parent.location.reload();
        }}, {interval_seconds * 1000});
        </script>
        """,
        height=0,
    )


def _render_header() -> None:
    st.markdown('<div class="mas-accent">Research Console</div>', unsafe_allow_html=True)
    st.title("Multi-Agent Search")
    st.caption("Run research flows, inspect queue state, and operate the worker/runtime from one screen.")


def _render_sidebar() -> None:
    st.sidebar.header("Control")
    st.sidebar.caption(f"API: `{API_BASE_URL}`")

    selected_research_id = st.sidebar.text_input(
        "Research ID",
        value=st.session_state.get("selected_research_id", ""),
        placeholder="Paste or launch a research ID",
    ).strip()
    st.session_state["selected_research_id"] = selected_research_id
    _sync_query_params()

    st.sidebar.divider()
    st.session_state["auto_refresh_enabled"] = st.sidebar.checkbox(
        "Auto refresh",
        value=st.session_state.get("auto_refresh_enabled", False),
    )
    st.session_state["auto_refresh_seconds"] = st.sidebar.slider(
        "Refresh interval (sec)",
        min_value=3,
        max_value=30,
        value=int(st.session_state.get("auto_refresh_seconds", 10)),
        disabled=not st.session_state["auto_refresh_enabled"],
    )
    if st.sidebar.button("Refresh now", use_container_width=True):
        st.rerun()

    st.sidebar.divider()
    st.sidebar.subheader("Worker")
    heartbeat = _safe_api_call(
        _api_get,
        f"/health/workers/{WORKER_NAME}",
        ignore_status_codes={404},
    )
    if heartbeat is None:
        st.sidebar.info(f"No heartbeat found for `{WORKER_NAME}`.")
        return

    st.sidebar.markdown(_status_badge(heartbeat["status"]), unsafe_allow_html=True)
    st.sidebar.caption(f"Processed jobs: {heartbeat['processed_jobs']}")
    st.sidebar.caption(f"Last seen: {_format_timestamp(heartbeat['last_seen_at'])}")
    if heartbeat.get("last_error"):
        st.sidebar.error(heartbeat["last_error"])


def _render_create_research() -> None:
    st.subheader("Start Research")
    with st.form("start_research_form", clear_on_submit=False):
        prompt = st.text_area(
            "Research prompt",
            height=140,
            placeholder="Compare the latest battery storage trends, costs, and deployment constraints.",
        )
        depth = st.selectbox("Depth", options=["easy", "medium", "hard"], index=1)
        submitted = st.form_submit_button("Launch Research", use_container_width=True)

    if not submitted:
        return

    payload = {"prompt": prompt.strip(), "depth": depth}
    if not payload["prompt"]:
        st.warning("Research prompt is required.")
        return

    result = _safe_api_call(_api_post, "/v1/research", payload)
    if result:
        st.session_state["selected_research_id"] = result["research_id"]
        _sync_query_params()
        st.success(f"Research created: {result['research_id']}")
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
    st.markdown(
        f"""
        <div class="mas-panel">
            <div style="display:flex; justify-content:space-between; gap:1rem; align-items:center;">
                <strong>{escape(job['id'])}</strong>
                {status_html}
            </div>
            <div class="mas-kv">Attempts: {job['attempt_count']} / {job['max_attempts']}</div>
            <div class="mas-kv">Updated: {_format_timestamp(job.get('updated_at'))}</div>
            <div class="mas-kv">{'Task' if job_kind == 'search' else 'Research'} ID: {escape(job['task_id'] if job_kind == 'search' else job['research_id'])}</div>
            {f"<div class='mas-kv'>Error: {escape(job['error'])}</div>" if job.get('error') else ""}
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
        if st.button(f"Requeue {job['id']}", key=button_key, use_container_width=True):
            _requeue_job(path, f"Requeued {job_kind} job")


def _render_job_section(title: str, jobs: list[dict], job_kind: str) -> None:
    st.markdown(f"**{title}**")
    if not jobs:
        st.caption("No jobs in this bucket.")
        return
    for job in jobs:
        _render_job_card(job, job_kind)


def _render_queue_overview() -> None:
    st.subheader("Queue Overview")
    metrics = _safe_api_call(_api_get, "/health/queues")
    if not metrics:
        return

    top = st.columns(3)
    top[0].metric("Pending Search", metrics["pending_search_jobs"])
    top[1].metric("Running Search", metrics["running_search_jobs"])
    top[2].metric("Dead Search", metrics["dead_letter_search_jobs"])

    bottom = st.columns(3)
    bottom[0].metric("Pending Finalize", metrics["pending_finalize_jobs"])
    bottom[1].metric("Running Finalize", metrics["running_finalize_jobs"])
    bottom[2].metric("Dead Finalize", metrics["dead_letter_finalize_jobs"])

    st.markdown("**Queue Actions**")
    action_rows = [st.columns(2), st.columns(2), st.columns(1)]
    if action_rows[0][0].button("Recover Stale Search", use_container_width=True):
        _run_queue_action("Recovered stale search jobs", "/v1/search-jobs/recover-stale")
    if action_rows[0][1].button("Recover Stale Finalize", use_container_width=True):
        _run_queue_action("Recovered stale finalize jobs", "/v1/research/finalize-jobs/recover-stale")
    if action_rows[1][0].button("Cleanup Search Jobs", use_container_width=True):
        _run_queue_action("Cleaned search jobs", "/v1/search-jobs/cleanup")
    if action_rows[1][1].button("Cleanup Finalize Jobs", use_container_width=True):
        _run_queue_action("Cleaned finalize jobs", "/v1/research/finalize-jobs/cleanup")
    if action_rows[2][0].button("Run Full Maintenance", use_container_width=True):
        _run_queue_action("Queue maintenance", "/health/queues/maintenance")

    with st.expander("Operational View", expanded=False):
        search_running = _safe_api_call(_api_get, "/v1/search-jobs?status=running") or []
        search_dead = _safe_api_call(_api_get, "/v1/search-jobs?status=dead_letter") or []
        finalize_running = _safe_api_call(_api_get, "/v1/research/finalize-jobs?status=running") or []
        finalize_dead = _safe_api_call(_api_get, "/v1/research/finalize-jobs?status=dead_letter") or []

        left, right = st.columns(2, gap="large")
        with left:
            _render_job_section("Running Search Jobs", search_running, "search")
            _render_job_section("Dead-Letter Search Jobs", search_dead, "search")
        with right:
            _render_job_section("Running Finalize Jobs", finalize_running, "finalize")
            _render_job_section("Dead-Letter Finalize Jobs", finalize_dead, "finalize")


def _render_source(result: dict, task_id: str, source_index: int) -> None:
    title = result.get("title") or result.get("url") or "Untitled source"
    url = result.get("url") or ""
    snippet = result.get("snippet") or result.get("content") or ""
    metadata = " | ".join(
        filter(
            None,
            [
                result.get("domain") or "unknown-domain",
                result.get("source_quality") or "unknown-quality",
                result.get("extraction_status") or "unknown-extraction",
            ],
        )
    )

    st.markdown(f"**{source_index}. {title}**")
    st.caption(metadata)
    if url:
        st.link_button("Open Source", url, key=f"source-link-{task_id}-{source_index}", use_container_width=False)
    if snippet:
        st.write(_truncate(snippet, 320))


def _render_task(task: dict, index: int) -> None:
    search_job = _safe_api_call(
        _api_get,
        f"/v1/tasks/{task['id']}/search-job",
        ignore_status_codes={404},
    )
    status_line = _status_badge(task["status"])
    if search_job:
        status_line = f"{status_line} {_status_badge(search_job['status'])}"

    with st.expander(f"{index}. {task['description']}", expanded=index == 1):
        st.markdown(status_line, unsafe_allow_html=True)
        st.caption(f"Task ID: {task['id']}")

        if search_job:
            st.caption(
                f"Search job: {search_job['id']} | attempts {search_job['attempt_count']}/{search_job['max_attempts']} | "
                f"updated {_format_timestamp(search_job.get('updated_at'))}"
            )
            if search_job.get("error"):
                st.warning(search_job["error"])

        st.markdown("**Queries**")
        st.code("\n".join(task["queries"]) or "-", language="text")

        logs = task.get("logs") or []
        if logs:
            st.markdown("**Recent Logs**")
            st.code("\n".join(logs[-10:]), language="text")

        results = task.get("result") or []
        if not results:
            st.info("No sources collected for this task yet.")
            return

        st.markdown("**Selected Sources**")
        for source_index, result in enumerate(results, start=1):
            _render_source(result, task["id"], source_index)


def _render_latest_finalize_job(research_id: str) -> None:
    latest_finalize_job = _safe_api_call(
        _api_get,
        f"/v1/research/{research_id}/finalize-job",
        ignore_status_codes={404},
    )
    if latest_finalize_job is None:
        st.info("No finalize job has been created for this research yet.")
        return

    st.markdown("**Latest Finalize Job**")
    _render_job_card(latest_finalize_job, "finalize")


def _render_research_details() -> None:
    research_id = st.session_state.get("selected_research_id", "").strip()
    st.subheader("Research Details")

    if not research_id:
        st.info("Create a research job or paste an existing research ID in the sidebar.")
        return

    research = _safe_api_call(_api_get, f"/v1/research/{research_id}")
    if not research:
        return

    top_left, top_right = st.columns([2, 1], gap="large")
    with top_left:
        st.markdown(
            f"""
            <div class="mas-panel">
                <div class="mas-accent">Prompt</div>
                <div>{escape(research['prompt'])}</div>
                <div class="mas-kv">Created: {_format_timestamp(research.get('created_at'))}</div>
                <div class="mas-kv">Updated: {_format_timestamp(research.get('updated_at'))}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with top_right:
        st.markdown(
            f"""
            <div class="mas-panel">
                <div class="mas-accent">Research Status</div>
                <div>{_status_badge(research['status'])}</div>
                <div class="mas-kv">Depth: {escape(research['depth'])}</div>
                <div class="mas-kv">Tasks: {len(research.get('task_ids', []))}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    action_col1, action_col2 = st.columns([1, 1])
    if action_col1.button("Refresh Research", use_container_width=True):
        st.rerun()
    if action_col2.button("Enqueue Finalize", use_container_width=True):
        result = _safe_api_call(_api_post, f"/v1/research/{research_id}/finalize")
        if result:
            st.success(f"Finalize job queued: {result.get('finalize_job_id') or 'already queued'}")
            st.rerun()

    _render_latest_finalize_job(research_id)

    tasks = []
    for task_id in research.get("task_ids", []):
        task = _safe_api_call(_api_get, f"/v1/tasks/{task_id}")
        if task:
            tasks.append(task)

    st.subheader("Task Pipeline")
    if not tasks:
        st.info("No tasks found for this research yet.")
    else:
        for index, task in enumerate(tasks, start=1):
            _render_task(task, index)

    st.subheader("Final Report")
    final_report = research.get("final_report")
    if not final_report:
        st.info("Final report is not ready yet.")
        return

    rendered_tab, raw_tab = st.tabs(["Rendered", "Raw Markdown"])
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

    left, right = st.columns([1.05, 1.35], gap="large")
    with left:
        _render_create_research()
        st.divider()
        _render_queue_overview()
    with right:
        _render_research_details()


if __name__ == "__main__":
    main()

import os
from typing import Any

import httpx
import streamlit as st


API_BASE_URL = os.getenv("STREAMLIT_API_BASE_URL", "http://localhost:8000").rstrip("/")
REQUEST_TIMEOUT_SECONDS = 30.0


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
            background: rgba(255, 252, 246, 0.78);
            border-radius: 18px;
            padding: 1rem 1.1rem;
            box-shadow: 0 10px 30px rgba(31, 58, 95, 0.08);
        }
        .mas-accent {
            color: #8a3b12;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-size: 0.75rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_header() -> None:
    st.markdown('<div class="mas-accent">Research Console</div>', unsafe_allow_html=True)
    st.title("Multi-Agent Search")
    st.caption("Launch research jobs, watch the queue, and inspect final reports from one screen.")


def _safe_api_call(method, *args, **kwargs) -> Any | None:
    try:
        return method(*args, **kwargs)
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text
        st.error(f"API error {exc.response.status_code}: {detail}")
    except httpx.HTTPError as exc:
        st.error(f"Connection error: {exc}")
    return None


def _render_sidebar() -> None:
    st.sidebar.header("Control")
    st.sidebar.caption(f"API: `{API_BASE_URL}`")
    selected_research_id = st.sidebar.text_input(
        "Research ID",
        value=st.session_state.get("selected_research_id", ""),
        placeholder="Paste an existing research ID",
    ).strip()
    st.session_state["selected_research_id"] = selected_research_id

    st.sidebar.divider()
    if st.sidebar.button("Run Queue Maintenance", use_container_width=True):
        result = _safe_api_call(_api_post, "/health/queues/maintenance")
        if result:
            st.sidebar.success(
                f"Recovered {result['recovered_count']} jobs, cleaned {result['deleted_count']} jobs."
            )


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

    if submitted:
        payload = {"prompt": prompt.strip(), "depth": depth}
        if not payload["prompt"]:
            st.warning("Research prompt is required.")
            return
        result = _safe_api_call(_api_post, "/v1/research", payload)
        if result:
            st.session_state["selected_research_id"] = result["research_id"]
            st.success(f"Research created: {result['research_id']}")


def _render_queue_overview() -> None:
    st.subheader("Queue Overview")
    metrics = _safe_api_call(_api_get, "/health/queues")
    if not metrics:
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("Pending Search", metrics["pending_search_jobs"])
    col2.metric("Running Search", metrics["running_search_jobs"])
    col3.metric("Dead Search", metrics["dead_letter_search_jobs"])

    col4, col5, col6 = st.columns(3)
    col4.metric("Pending Finalize", metrics["pending_finalize_jobs"])
    col5.metric("Running Finalize", metrics["running_finalize_jobs"])
    col6.metric("Dead Finalize", metrics["dead_letter_finalize_jobs"])

    with st.expander("Admin Lists", expanded=False):
        search_running = _safe_api_call(_api_get, "/v1/search-jobs?status=running") or []
        search_dead = _safe_api_call(_api_get, "/v1/search-jobs?status=dead_letter") or []
        finalize_running = _safe_api_call(_api_get, "/v1/research/finalize-jobs?status=running") or []
        finalize_dead = _safe_api_call(_api_get, "/v1/research/finalize-jobs?status=dead_letter") or []

        st.write("Running search jobs")
        st.json(search_running)
        st.write("Dead-letter search jobs")
        st.json(search_dead)
        st.write("Running finalize jobs")
        st.json(finalize_running)
        st.write("Dead-letter finalize jobs")
        st.json(finalize_dead)


def _render_tasks(tasks: list[dict]) -> None:
    st.subheader("Task Pipeline")
    if not tasks:
        st.info("No tasks found for this research yet.")
        return

    for index, task in enumerate(tasks, start=1):
        title = f"{index}. {task['description']}"
        with st.expander(title, expanded=index == 1):
            st.caption(f"Status: {task['status']} | Queries: {len(task['queries'])}")
            st.write("Queries")
            st.code("\n".join(task["queries"]) or "-", language="text")

            if task.get("logs"):
                st.write("Logs")
                st.code("\n".join(task["logs"][-8:]), language="text")

            results = task.get("result") or []
            if results:
                st.write("Top Sources")
                for source in results:
                    source_title = source.get("title") or source.get("url")
                    st.markdown(
                        f"- **{source_title}**  \n"
                        f"  `{source.get('domain') or 'unknown-domain'}` | "
                        f"`{source.get('source_quality', 'unknown')}` | "
                        f"`{source.get('extraction_status', 'unknown')}`"
                    )
                    if source.get("snippet"):
                        st.caption(source["snippet"])


def _render_research_details() -> None:
    research_id = st.session_state.get("selected_research_id", "").strip()
    st.subheader("Research Details")

    if not research_id:
        st.info("Create a research job or paste an existing research ID in the sidebar.")
        return

    research = _safe_api_call(_api_get, f"/v1/research/{research_id}")
    if not research:
        return

    top_left, top_right = st.columns([2, 1])
    top_left.markdown(
        f"""
        <div class="mas-panel">
            <div class="mas-accent">Prompt</div>
            <div>{research['prompt']}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    top_right.markdown(
        f"""
        <div class="mas-panel">
            <div class="mas-accent">Status</div>
            <div><strong>{research['status']}</strong></div>
            <div>Depth: {research['depth']}</div>
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

    tasks = []
    for task_id in research.get("task_ids", []):
        task = _safe_api_call(_api_get, f"/v1/tasks/{task_id}")
        if task:
            tasks.append(task)
    _render_tasks(tasks)

    st.subheader("Final Report")
    final_report = research.get("final_report")
    if final_report:
        st.markdown(final_report)
    else:
        st.info("Final report is not ready yet.")


def main() -> None:
    st.set_page_config(
        page_title="Multi-Agent Search",
        page_icon="M",
        layout="wide",
    )
    _render_styles()
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

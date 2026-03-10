import json
import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
APP_PORT = int(os.environ.get("SMOKE_APP_PORT", "18021"))
SMOKE_REPORT = "smoke final report"
BASE_URL = ""


def get_base_url() -> str:
    return BASE_URL


def pick_app_port() -> int:
    configured_port = os.environ.get("SMOKE_APP_PORT")
    if configured_port:
        return int(configured_port)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http_request(method: str, path: str, payload: dict | None = None) -> tuple[int, dict | list]:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(
        url=f"{get_base_url()}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))


def http_json(method: str, path: str, payload: dict | None = None) -> dict | list:
    status_code, body = http_request(method, path, payload)
    if status_code >= 400:
        raise RuntimeError(f"{method} {path} returned {status_code}: {body}")
    return body


def wait_for_health() -> None:
    for _ in range(40):
        try:
            payload = http_json("GET", "/health")
            if payload == {"status": "ok"}:
                return
        except URLError:
            time.sleep(0.25)
    raise RuntimeError("API did not become healthy in time")


def wait_for_research_status(research_id: str, expected_status: str) -> dict:
    for _ in range(40):
        research = http_json("GET", f"/v1/research/{research_id}")
        if research["status"] == expected_status:
            return research
        time.sleep(0.25)
    raise RuntimeError(
        f"Research {research_id} did not reach status {expected_status!r} in time"
    )


def run_finalize_worker_once(env: dict[str, str]) -> None:
    completed = subprocess.run(
        [sys.executable, "scripts/run_finalize_worker.py", "--once"],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    if "processed=1" not in completed.stdout:
        raise RuntimeError(f"Unexpected worker output: {completed.stdout}")


def run_migrations(env: dict[str, str]) -> None:
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def seed_task() -> tuple[str, str]:
    os.environ.setdefault("TASK_STORE_BACKEND", "postgres")
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    os.environ.setdefault("POSTGRES_PORT", "5433")

    from src.api.schemas import ResearchRequest, SearchDepth, TaskStatus
    from src.repositories import create_task_store

    store = create_task_store()
    research = store.add_research(
        ResearchRequest(prompt="smoke research", depth=SearchDepth.EASY),
        task_ids=[],
    )
    task_id = str(uuid.uuid4())
    store.add_task(
        {
            "id": task_id,
            "research_id": research.id,
            "description": "smoke task",
            "queries": ["smoke query"],
            "status": TaskStatus.PENDING,
        }
    )
    return research.id, task_id


def verify_db_rows(research_id: str, task_id: str) -> None:
    from sqlalchemy import create_engine, text

    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql+psycopg://app:app@localhost:5433/multi_agent_search",
    )
    engine = create_engine(database_url, pool_pre_ping=True)
    with engine.connect() as connection:
        task_row = connection.execute(
            text("select status, logs from search_tasks where id = :task_id"),
            {"task_id": task_id},
        ).one()
        research_row = connection.execute(
            text("select status, final_report from researches where id = :research_id"),
            {"research_id": research_id},
        ).one()
    assert task_row.status == "completed", task_row
    assert "patched via smoke script" in task_row.logs
    assert research_row.status == "completed", research_row
    assert research_row.final_report == SMOKE_REPORT, research_row


def main() -> int:
    global BASE_URL

    env = os.environ.copy()
    env.setdefault("TASK_STORE_BACKEND", "postgres")
    env.setdefault("POSTGRES_HOST", "localhost")
    env.setdefault("POSTGRES_PORT", "5433")
    env.setdefault("SMOKE_ANALYZER_REPORT", SMOKE_REPORT)
    run_migrations(env)
    app_port = pick_app_port()
    BASE_URL = f"http://127.0.0.1:{app_port}"

    server = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "src.api.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(app_port),
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        wait_for_health()
        research_id, task_id = seed_task()

        tasks = http_json("GET", "/v1/tasks")
        assert any(task["id"] == task_id for task in tasks), tasks

        task = http_json("GET", f"/v1/tasks/{task_id}")
        assert task["status"] == "pending", task

        research = http_json("GET", f"/v1/research/{research_id}")
        assert research["status"] == "processing", research

        updated = http_json(
            "PATCH",
            f"/v1/tasks/{task_id}",
            {
                "status": "completed",
                "result": [
                    {
                        "url": "https://smoke.example",
                        "title": "Smoke",
                        "content": "Patched from smoke script",
                    }
                ],
                "log": "patched via smoke script",
            },
        )
        assert updated["status"] == "completed", updated
        assert updated["result"][0]["url"] == "https://smoke.example", updated

        finalize = http_json("POST", f"/v1/research/{research_id}/finalize")
        assert finalize["status"] == "analyzing", finalize
        assert finalize["final_report"] is None, finalize

        run_finalize_worker_once(env)
        completed = wait_for_research_status(research_id, "completed")
        assert completed["final_report"] == SMOKE_REPORT, completed

        verify_db_rows(research_id, task_id)
        print("smoke-postgres-runtime: ok")
        return 0
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())

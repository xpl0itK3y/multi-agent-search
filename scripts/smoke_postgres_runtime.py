import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
APP_PORT = int(os.environ.get("SMOKE_APP_PORT", "18021"))
BASE_URL = f"http://127.0.0.1:{APP_PORT}"


def http_json(method: str, path: str, payload: dict | None = None) -> dict | list:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(
        url=f"{BASE_URL}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_for_health() -> None:
    for _ in range(40):
        try:
            payload = http_json("GET", "/health")
            if payload == {"status": "ok"}:
                return
        except URLError:
            time.sleep(0.25)
    raise RuntimeError("API did not become healthy in time")


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


def verify_db_row(task_id: str) -> None:
    from sqlalchemy import create_engine, text

    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql+psycopg://app:app@localhost:5433/multi_agent_search",
    )
    engine = create_engine(database_url, pool_pre_ping=True)
    with engine.connect() as connection:
        row = connection.execute(
            text("select status, logs from search_tasks where id = :task_id"),
            {"task_id": task_id},
        ).one()
    assert row.status == "completed", row
    assert "patched via smoke script" in row.logs


def main() -> int:
    env = os.environ.copy()
    env.setdefault("TASK_STORE_BACKEND", "postgres")
    env.setdefault("POSTGRES_HOST", "localhost")
    env.setdefault("POSTGRES_PORT", "5433")

    server = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "src.api.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(APP_PORT),
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        wait_for_health()
        _, task_id = seed_task()

        tasks = http_json("GET", "/v1/tasks")
        assert any(task["id"] == task_id for task in tasks), tasks

        task = http_json("GET", f"/v1/tasks/{task_id}")
        assert task["status"] == "pending", task

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
        verify_db_row(task_id)
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

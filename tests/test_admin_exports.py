"""EXPORTS: admin CSV exports stream page by page (no 10k-row, blob-loading single
request) and neutralize spreadsheet formula injection in every text cell."""
import csv
import io

import pytest

from src.api.app import csv_safe, stream_csv
from src.api.schemas import ResearchRequest, SearchDepth


def _rows(text: str) -> list[list[str]]:
    return list(csv.reader(io.StringIO(text)))


@pytest.mark.parametrize("value", ["=1+1", "+1", "-2+3", "@SUM(A1)", "\tcmd", "\rcmd"])
def test_csv_safe_quotes_formula_like_text(value):
    assert csv_safe(value) == "'" + value


@pytest.mark.parametrize("value", ["plain", "a=1", "", 42, -1, 0.5, None])
def test_csv_safe_leaves_other_cells_alone(value):
    assert csv_safe(value) == value


def test_stream_csv_pages_until_a_short_page_and_skips_repeats(monkeypatch):
    monkeypatch.setattr("src.api.app.ADMIN_EXPORT_PAGE_SIZE", 2)
    pages = {1: ["a", "b"], 2: ["b", "c"], 3: ["d"]}  # "b" slid onto page 2
    fetched = []

    def fetch(page):
        fetched.append(page)
        return pages[page]

    chunks = list(stream_csv(["id"], fetch, lambda item: [item], key=lambda item: item))

    assert fetched == [1, 2, 3]
    assert len(chunks) == 3  # one chunk per DB page, never the whole export at once
    assert _rows("".join(chunks)) == [["id"], ["a"], ["b"], ["c"], ["d"]]


def test_stream_csv_reads_the_first_page_before_the_response_starts():
    def failing_fetch(page):
        raise RuntimeError("db down")

    # Raised by the route itself (a 500), not half-way through a 200 response body.
    with pytest.raises(RuntimeError):
        stream_csv(["id"], failing_fetch, lambda item: [item], key=lambda item: item)


@pytest.mark.anyio
async def test_users_export_streams_every_page_and_quotes_a_formula_name(client, monkeypatch):
    monkeypatch.setattr("src.api.app.ADMIN_EXPORT_PAGE_SIZE", 2)
    store = client._transport.app.state.research_service.task_store
    for index in range(5):
        store.create_user(f"export-{index}", f"export-{index}@example.com", None)
    store.update_user_profile("export-3", "=1+1", None)
    pages = []
    real_list = store.get_admin_users_list

    def spy(**kwargs):
        pages.append((kwargs["page"], kwargs["page_size"]))
        return real_list(**kwargs)

    monkeypatch.setattr(store, "get_admin_users_list", spy)

    response = await client.get("/v1/admin/users/export")

    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "users_telemetry.csv" in response.headers["content-disposition"]
    rows = _rows(response.text)
    assert rows[0][:3] == ["user_id", "email", "name"]
    assert sorted(row[0] for row in rows[1:]) == [f"export-{index}" for index in range(5)]
    assert next(row for row in rows if row[0] == "export-3")[2] == "'=1+1"
    assert pages == [(1, 2), (2, 2), (3, 2)]


@pytest.mark.anyio
async def test_prompts_export_quotes_research_and_chat_prompts(client):
    store = client._transport.app.state.research_service.task_store
    research = store.add_research(
        ResearchRequest(prompt="=cmd|' /C calc'!A0", depth=SearchDepth.EASY), task_ids=[]
    )
    store.record_user_event(
        event_name="chat_prompt",
        event_category="prompt",
        details={"research_id": research.id, "prompt": "@SUM(1+1)*cmd"},
    )

    response = await client.get("/v1/admin/prompts/export")

    assert response.status_code == 200
    prompts = {row[1]: row[6] for row in _rows(response.text)[1:]}
    assert prompts == {"research": "'=cmd|' /C calc'!A0", "chat": "'@SUM(1+1)*cmd"}


@pytest.mark.anyio
async def test_tokens_export_quotes_prompts_and_pages(client, monkeypatch):
    monkeypatch.setattr("src.api.app.ADMIN_EXPORT_PAGE_SIZE", 1)
    store = client._transport.app.state.research_service.task_store
    for prompt in ("-2+3 is the prompt", "an ordinary prompt", "+SUM(A1:A9)"):
        store.add_research(ResearchRequest(prompt=prompt, depth=SearchDepth.EASY), task_ids=[])

    response = await client.get("/v1/admin/tokens/export")

    assert response.status_code == 200
    rows = _rows(response.text)
    assert rows[0] == ["research_id", "prompt", "depth", "status", "total_tokens", "estimated_cost_usd", "created_at"]
    assert sorted(row[1] for row in rows[1:]) == ["'+SUM(A1:A9)", "'-2+3 is the prompt", "an ordinary prompt"]

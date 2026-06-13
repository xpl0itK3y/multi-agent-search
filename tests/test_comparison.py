import json

from src.agents.comparison import ComparisonAgent
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.services.research_service import ResearchService


class _LLM:
    def __init__(self, payload):
        self.payload = payload
        self.models = []

    def generate(self, system_prompt, user_prompt, **kwargs):
        self.models.append(kwargs.get("model"))
        return json.dumps(self.payload)


def test_comparison_builds_table():
    payload = {
        "options": ["Postgres", "MySQL"],
        "rows": [
            {
                "criterion": "Лицензия",
                "cells": [
                    {"option": "Postgres", "value": "PostgreSQL License", "source_ids": ["S1"]},
                    {"option": "MySQL", "value": "GPL", "source_ids": ["S2"]},
                ],
            }
        ],
        "recommendation": "Postgres для сложных запросов.",
    }
    table = ComparisonAgent(_LLM(payload)).build("сравни Postgres и MySQL", "Report [S1] [S2].", model="deepseek-chat")
    assert table.has_table and table.options == ["Postgres", "MySQL"]
    assert table.rows[0].criterion == "Лицензия"
    assert table.rows[0].cells[0].source_ids == ["S1"]
    assert "Postgres" in table.recommendation


def test_comparison_empty_when_not_a_comparison():
    assert not ComparisonAgent(_LLM({"options": [], "rows": []})).build("p", "report").has_table


def test_comparison_no_llm_returns_empty():
    assert not ComparisonAgent(None).build("p", "r").has_table


def test_comparison_drops_rows_without_cells():
    payload = {"options": ["A", "B"], "rows": [{"criterion": "X", "cells": []}]}
    assert not ComparisonAgent(_LLM(payload)).build("compare A and B", "r").has_table


def test_looks_like_comparison_heuristic():
    svc = ResearchService(task_store=InMemoryTaskStore())
    assert svc._looks_like_comparison("Сравни X и Y")
    assert svc._looks_like_comparison("Python vs Go performance")
    assert not svc._looks_like_comparison("История квантовых компьютеров")


def test_get_research_comparison_empty_without_data():
    svc = ResearchService(task_store=InMemoryTaskStore())
    assert not svc.get_research_comparison("nope").has_table

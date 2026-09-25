"""LLM-ROUTE-GUARD: every route that makes the service call an LLM agent consumes the
per-user LLM allowance (enforce_llm_rate_limit), so a new route cannot open unmetered
DeepSeek spend.

Routes are found by what their handler calls: one that invokes a ResearchService entry
point below (each reaches the optimizer, orchestrator, clarifier, analyzer or chat LLM,
directly or by starting the work that does) must carry the limiter. The named routes are
pinned as well, so renaming a service method cannot quietly drop them from the scan.
Admin re-dispatch routes (requeue/recover/maintenance) are throttled by
enforce_admin_rate_limit instead; tests/test_admin_audit.py covers them.
"""
import inspect
import re

import pytest
from fastapi.routing import APIRoute

from src.api.app import create_app
from src.auth.llm_rate_limit import enforce_llm_rate_limit

LLM_ENTRY_POINTS = frozenset({
    "optimize_prompt",  # optimizer
    "decompose_prompt",  # orchestrator
    "start_research",  # queues decompose_and_enqueue
    "decompose_and_enqueue",  # clarifier, orchestrator
    "generate_research_answer",  # chat answer (and its mini-search)
    "retry_research",  # re-runs decomposition, search or finalization
    "get_research_summary",  # follow-up recommendations
    "submit_clarifications",  # orchestrator, via decompose_and_enqueue
    "approve_research_plan",  # search jobs, then the analyzer
    "enqueue_research_finalization",  # analyzer
})

NAMED_LLM_ROUTES = [
    ("POST", "/v1/optimize"),
    ("POST", "/v1/decompose"),
    ("POST", "/v1/research"),
    ("POST", "/v1/research/{research_id}/messages"),
    ("POST", "/v1/research/{research_id}/messages/stream"),
    ("POST", "/v1/research/{research_id}/retry"),
    ("GET", "/v1/research/{research_id}/summary"),
]

# Start LLM work without the limiter. Each is a one-shot, admission-gated transition of a
# research whose creation was rate-limited, so the spend stays bounded per research, but
# they should be metered like the rest. Remove an entry once its route depends on
# enforce_llm_rate_limit (this test fails until you do).
KNOWN_UNMETERED_LLM_ROUTES = {
    ("POST", "/v1/research/{research_id}/clarify"),
    ("POST", "/v1/research/{research_id}/plan/approve"),
    ("POST", "/v1/research/{research_id}/finalize"),
}


def _depends_on(dependant, call) -> bool:
    return any(dep.call is call or _depends_on(dep, call) for dep in dependant.dependencies)


def _llm_entry_points_called(route: APIRoute) -> set[str]:
    source = inspect.getsource(route.endpoint)
    return {name for name in LLM_ENTRY_POINTS if re.search(rf"\.{name}\b", source)}


@pytest.fixture(scope="module")
def routes() -> dict[tuple[str, str], APIRoute]:
    return {
        (method, route.path): route
        for route in create_app().routes
        if isinstance(route, APIRoute)
        for method in route.methods
    }


@pytest.mark.parametrize(("method", "path"), NAMED_LLM_ROUTES)
def test_named_llm_routes_are_rate_limited_and_found_by_the_scan(routes, method, path):
    route = routes[(method, path)]

    assert _depends_on(route.dependant, enforce_llm_rate_limit)
    assert _llm_entry_points_called(route), f"{method} {path} calls none of LLM_ENTRY_POINTS"


def test_every_route_that_reaches_an_llm_is_rate_limited(routes):
    unmetered = {
        key
        for key, route in routes.items()
        if _llm_entry_points_called(route) and not _depends_on(route.dependant, enforce_llm_rate_limit)
    }

    assert unmetered == KNOWN_UNMETERED_LLM_ROUTES


def test_every_llm_entry_point_is_a_service_method():
    from src.services import ResearchService

    assert {name for name in LLM_ENTRY_POINTS if not callable(getattr(ResearchService, name, None))} == set()

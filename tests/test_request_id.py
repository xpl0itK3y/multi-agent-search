"""SEC2-10: a client's X-Request-ID is kept (bound into every log record of the request and
echoed back) only if it matches ^[A-Za-z0-9._-]{1,64}$; any other value is replaced by a
generated id."""
import re

import httpx
import pytest

from src.api.app import accepted_request_id, create_app
from src.observability import get_observability_context

GENERATED = re.compile(r"[0-9a-f]{32}")
VALID = ["abc-123", "req.id_1-2", "A" * 64, "0", "trace.7f3a-b9_c"]
INVALID = [
    "",
    "A" * 65,
    "has space",
    "x research_id=forged",
    "k=v",
    "id\tb",
    "ünïcode",
    "1٠",  # Arabic-Indic digit: \d would take it, the ASCII class does not
    "abc\n",  # $ would match before a trailing newline; fullmatch does not
    "a/b",
    "id;drop",
]


@pytest.mark.parametrize("value", VALID)
def test_well_formed_client_id_is_kept(value):
    assert accepted_request_id(value) == value


@pytest.mark.parametrize("value", [*INVALID, None])
def test_other_values_get_a_generated_id(value):
    generated = accepted_request_id(value)
    assert generated != value
    assert GENERATED.fullmatch(generated)


@pytest.fixture
async def probe_client():
    app = create_app()

    @app.get("/__request_id_probe")
    def probe():
        return {"logged": get_observability_context().get("request_id")}

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client


@pytest.mark.anyio
async def test_middleware_echoes_and_logs_a_valid_id(probe_client):
    response = await probe_client.get("/__request_id_probe", headers={"X-Request-ID": "client-id.42"})

    assert response.headers["X-Request-ID"] == "client-id.42"
    assert response.json() == {"logged": "client-id.42"}


@pytest.mark.anyio
@pytest.mark.parametrize("value", ["x research_id=forged", "A" * 65, "a/b", "k=v"])
async def test_middleware_replaces_a_malformed_id_everywhere(probe_client, value):
    response = await probe_client.get("/__request_id_probe", headers={"X-Request-ID": value})

    echoed = response.headers["X-Request-ID"]
    assert GENERATED.fullmatch(echoed)
    assert response.json() == {"logged": echoed}


@pytest.mark.anyio
async def test_middleware_generates_an_id_when_none_is_sent(probe_client):
    first = await probe_client.get("/__request_id_probe")
    second = await probe_client.get("/__request_id_probe")

    assert GENERATED.fullmatch(first.headers["X-Request-ID"])
    assert first.headers["X-Request-ID"] != second.headers["X-Request-ID"]

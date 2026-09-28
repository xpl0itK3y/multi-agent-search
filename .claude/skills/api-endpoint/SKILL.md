---
name: api-endpoint
description: How to add or change a backend HTTP endpoint or service method in multi-agent-search (FastAPI app in src/api/app.py, ResearchService mixins in src/services/) without reopening the holes this codebase has already closed — IDOR via research ownership, CSRF double-submit, admin authorization/audit/throttling, LLM route metering, error-detail leaks, the frontend error-contract test, metric cardinality and log redaction. Use it for any work touching src/api/, src/services/, src/auth/, src/domain/errors.py, a new route, a new user-facing error message, or a review of such a change, even if the user only says "add an endpoint", "expose X in the API" or "сделай ручку".
---

# Backend endpoints and service methods

## Where code goes

- **Routes.** All routes are closures inside `register_routes(app)` in `src/api/app.py`. There is no APIRouter. Put a new route next to its siblings: research routes (`/v1/research/{research_id}/…`), admin, auth, public share.
- **Handlers are sync `def`**, so they run in the threadpool. The only `async` handlers are the SSE streams, and those wrap every blocking store call in `run_in_threadpool`. Never block inside an `async def`.
- **Guards.** Reuse the aliases defined at the top of `register_routes`:
  - `research_guard` (ownership, via `verify_research_access` → `service._ensure_research_access`)
  - `auth_required` (`get_current_user`)
  - `admin_guard` (`require_admin`)
  - `auth_rate_limit`
- **Service.** Reach it with `get_research_service(request)`, not with `Depends`. `ResearchService` (`src/services/research_service.py`) is composed of mixins: auth, account recovery, operational health, export, job queue, trust report and share. A new concern gets a new `src/services/<concern>_mixin.py` added to the bases, and it uses `self.task_store`.
- **Layering.** `src/services/`, `src/domain/`, `src/repositories/` and `src/agents/` never import FastAPI.
  - Services raise `src/domain/errors.py` types: `BadRequestError` 400, `UnauthorizedError` 401, `ForbiddenError` 403, `NotFoundError` 404, `ConflictError` 409, `UnprocessableError` 422, `ServiceUnavailableError` 503. A handler in `create_app` turns them into `{"detail": …}`.
  - `HTTPException` lives only in `src/api/` (`app.py`, `dependencies.py`) and in the `src/auth/` dependencies.
- **Models.**
  - Request and response models live in `src/domain/models.py`. Add every public name to **both** the import block and `__all__` in `src/domain/__init__.py`, or `tests/test_domain_exports.py` fails.
  - `src/api/schemas.py` is only a re-export shim.
  - Bound every request field (`Field(max_length=…)`, ranges).
- **Persistence.** New persistence means a TaskStore protocol method, both stores, and a conformance test (plus a migration if the schema changes). Follow the `data-layer` skill.

## Authorization: the rules that were bugs once

- **Research ownership.** Use one of two patterns:
  - `dependencies=research_guard` on the route. The path parameter must be named `research_id`.
  - `owner: str | None = Depends(scope_user_id)`, passed as `user_id=` into a service method that calls `_ensure_research_access` itself.
- A stranger gets **404 "Research not found"**, never 403. Legacy rows with a NULL owner are refused while auth is on.
- **Service getters trust the guard.** Many service getters check no ownership: status, the trust artifacts, share create/revoke/info, export. Calling one from a route without `research_guard` is an IDOR.
- **Store-scoped routes.** `/v1/tasks/{task_id}` (plus `/summary` and `/search-job`), `/v1/search-jobs/{job_id}` and `/v1/research/finalize-jobs/{job_id}` are scoped in the store by `user_id`, not by the guard. A new route of that kind needs the same scoping. The `GET /v1/tasks` list is admin-only.
- **Record stripping.** Wrap any `ResearchRecord` you return in `_public_record(...)`, including one nested in a response model, and pass any other `graph_state` you return through `_public_graph_state(...)` (as the `/graph` route does). Both strip `_SENSITIVE_GRAPH_STATE_KEYS` (`share_token`, `webhook_url`, `decompose_payload`); `tests/test_public_record.py` covers the routes. A new secret `graph_state` key goes into that set.
- **Admin rights.** `require_admin` decides. `is_admin` always comes from `has_admin_rights` (in `AuthMixin._to_auth_user` and in both stores' admin user list): the email must be in `ADMIN_EMAILS` **and** the account must be Google-linked or provisioned by `scripts/create_admin.py`. Never test `is_admin_email` alone.
- **Admin mutations.**
  - Take `admin_user: AuthUser = Depends(enforce_admin_rate_limit)`, which is the admin check plus a per-admin budget.
  - Call `record_admin_action(...)` before the destructive write.
  - Register the route in `tests/test_admin_audit.py`, which fails otherwise:
    - **An admin POST/PATCH/DELETE goes in `AUDITED_ROUTES`**, as `(method, path): (setup(service) -> (url, json), "audit_action")`. That test calls the route and asserts **status 200** and an audit row with that action, actor `local@local` and IP `127.0.0.1`, so return 200, not 201 or 204.
    - A POST that changes nothing (like `/v1/admin/operations/preview`) goes in `READ_ONLY_ADMIN_POSTS` instead.
    - **An admin GET with side effects** (it audits or spends the admin budget, like the CSV exports) goes in both `AUDITED_EXPORTS` and `_CSRF_CHECKED_GET_PATHS`.
  - Read-only admin routes use `dependencies=admin_guard`.
- **Sensitive account actions.**
  - Pass `fresh_google_auth=request_has_fresh_google_auth(request)` to the service, which raises `ForbiddenError("reauth_required: …")` when the Google sign-in is not fresh.
  - Password-checking routes use `enforce_password_check_rate_limit`.
  - Write with `expected_token_version` (compare-and-set).
- **Public share.** `GET /v1/public/research/{token}` deliberately has no auth. `ShareMixin.get_public_report` builds a strict field whitelist. A new trust artifact is private until you add it there on purpose.

## CSRF

- **Mechanism.** Double-submit. `_issue_session` sets an httpOnly `access_token` cookie and a readable `csrf_token` cookie. `csrf_middleware` rejects a cookie-authenticated unsafe request whose `X-CSRF-Token` header does not match the cookie.
- **Skipped for:**
  - safe methods (GET/HEAD/OPTIONS), except `_CSRF_CHECKED_GET_PATHS`;
  - requests with a non-blank Bearer token;
  - `_CSRF_EXEMPT_PATHS` (login, register, password forgot/reset);
  - `_CSRF_SESSION_COOKIE_PATHS` (logout, email verify) when no session cookie comes with the request;
  - AUTH_DISABLED setups with no `ADMIN_EMAILS`, or with no session cookie.
- **A new POST/PATCH/DELETE for signed-in users needs nothing extra.** The frontend's `request()` sends the header already.
- **A new anonymous mutation needs care.** With no cookie, the check refuses every caller with 403 (like forgot/reset before they were exempted). Add it to `_CSRF_EXEMPT_PATHS` only if it acts on nothing a session could ride on.
- **Never put side effects on a GET.** If a GET must be protected (the admin CSV exports are), add its exact path to `_CSRF_CHECKED_GET_PATHS`. All these path sets are exact strings, not templates.
- **Middleware order.** CSRF is checked **before** routing and authentication, so an anonymous mutation gets 403 "CSRF token missing or invalid", not 401.

## Rate limits and LLM metering

- **Limiters.** All are in-process sliding windows (`src/auth/sliding_window.py`), except the LLM limiter, which uses Redis when a broker is configured. Several dependencies authenticate *and* throttle, and return the user; use them in place of `get_current_user`:
  - `enforce_llm_rate_limit`
  - `enforce_admin_rate_limit`
  - `enforce_password_check_rate_limit`
  - `enforce_verification_email_rate_limit`
- **LLM routes.** A route whose handler reaches an LLM must depend on `enforce_llm_rate_limit`. `tests/test_llm_route_guard.py` finds such routes by scanning handler source for the `ResearchService` method names in `LLM_ENTRY_POINTS`. For a new route:
  1. Add any new service entry point that reaches an LLM to `LLM_ENTRY_POINTS`.
  2. Pin the route itself in `NAMED_LLM_ROUTES`, so a rename cannot drop it from the scan.
  3. Keep `KNOWN_UNMETERED_LLM_ROUTES` empty.
- **New module-level limiters** must be bounded. Add them to `test_auth_rate_limit.py::test_every_process_limiter_is_bounded`, and reset them between tests: extend the module's `reset_*()` hook, or add a hook and call it from `tests/conftest.py::_isolate_auth_settings`. The LLM limiter is not reset there, so tests replace it themselves.

## Errors: what the client may see

- **Never let `str(exc)` reach a response, an SSE payload or a stored job error.**
  - Log it with `logger.exception("snake_case_event research_id=%s", rid)` and return a generic or typed message. The chat stream handler shows the pattern.
  - Owner job views go through `_owner_job_view` and `ResearchService._failure_message`.
  - `tests/test_error_leak.py` injects a secret-looking exception and asserts that it stays in the logs only.
- **The API sends English details, not codes.** The web client (`web/src/lib/api.ts` `apiErrorMessage`) maps them to i18n keys in this order:
  1. `API_DETAIL_KEYS[status]`: regexes against the detail;
  2. `API_STATUS_KEYS`: a generic text for 401/403/404/409/422/429/500;
  3. the raw detail. Unmapped **400 and 503 details reach users verbatim**, so keep them user-safe English.
- **`tests/test_frontend_error_contract.py`** parses `api.ts` and `stream.ts` (`SERVER_STREAM_ERRORS`) and requires every web pattern to match a detail that `src/` actually raises with the same status.
  - It reads backend details from literals, f-strings, module-level string constants and helper return values.
  - **A detail built dynamically is invisible to it** and silently breaks the mapping.
  - Rewording a mapped detail means changing the web regex in the same change.
- **A new keyed error** (for example a link refusal the UI must explain):
  1. Define a module constant `FOO_DETAIL = "foo_code: English explanation"` and raise `XError(FOO_DETAIL)`.
  2. Add `[/^foo_code/, "fooCode"]` under the status in `API_DETAIL_KEYS`. Key on the code before the colon, never on the prose.
  3. Add `errors.api.fooCode` to the ru, en and es locales in `web/src/i18n/index.ts`.
  4. Add a case to `web/src/lib/api.test.ts`. If the text is load-bearing, also pin it in the contract test.

## Observability constraints

- **Metric labels are bounded.** `correlation_middleware` labels requests with the route template (`metric_route_template`) and a bounded method. Never add a label carrying a user id, research id, token or raw path; `tests/test_metrics_cardinality.py` fails if series grow with traffic.
- **Logs.**
  - Log `user_id`, never an email, token or emailed link. The account-recovery tests assert this.
  - Log user-supplied URLs through `net_safety.log_safe_url`.
  - A new route with a secret in its path must extend `ShareTokenRedactionFilter`'s regex (`src/observability/logging.py`) **and** the nginx `map` blocks (`web/nginx.conf`). Their tests are `test_access_log_redaction.py` and `test_nginx_log_redaction.py`.
- **Outbound HTTP to a user-supplied URL** goes through `src/net_safety.py`:
  - `safe_fetch_document` for page fetches: every redirect hop is re-validated, but the IP is not pinned (its docstring names the DNS-rebinding residual).
  - `safe_post_json` for webhooks: the validated IP is pinned, and it makes one hop with no redirects.
- **Client IP** comes from `extract_client_ip`. Never read forwarding headers yourself.
- **Log context.** Tag logs with `bind_observability_context(research_id=…)`.

## Twins to update together

This codebase's recurring bug is "fixed here, forgot in the twin":

- `/messages` and `/messages/stream`: both log the prompt event, are LLM-limited and append messages.
- The research routes and the tasks/jobs route family.
- A new trust artifact touches the public share whitelist (`get_public_report`), the JSON export (`_export_json_payload`), the scorecard (`_export_scorecard`) and `_RETRY_RESET_GRAPH_STATE_KEYS`.
- Every service path that leaves a job PENDING must also push it to the broker (see the `data-layer` skill).

## Tests

- **Default test mode.** `tests/conftest.py::_isolate_auth_settings` (autouse) sets `auth_disabled=True` and no admins. In that mode ownership, CSRF and admin checks are off, so a test that does not turn auth on proves nothing about them.
- **The `client` fixture** is `create_app()` + lifespan + `httpx.AsyncClient(ASGITransport)`; mark tests `@pytest.mark.anyio`. The service is `client._transport.app.state.research_service`.
- **Auth-on fixture.** Set the settings *before* `create_app()`, because the lifespan refuses a weak secret. Then swap in a fresh in-memory service (see `tests/test_health_access.py`). The skeleton below runs as-is against the current app; change the route to yours:

```python
import uuid

import httpx
import pytest

from src.api.app import create_app
from src.auth.security import create_token
from src.config import settings
from src.domain import ResearchRequest, SearchDepth
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


@pytest.fixture
async def auth_client(monkeypatch):
    # Before create_app(): the lifespan refuses a weak secret while auth is on.
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "feature-" + "x" * 40, raising=False)
    app = create_app()
    service = ResearchService(task_store=InMemoryTaskStore())
    async with app.router.lifespan_context(app):
        app.state.research_service = service
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as c:
            yield c, service


def _bearer(user):
    return {"Authorization": f"Bearer {create_token(user.id, token_version=user.token_version)}"}


def _user(service, tag):
    return service.register_user(f"{tag}-{uuid.uuid4().hex[:8]}@example.com", "secret123")


@pytest.mark.anyio
async def test_research_is_owner_only(auth_client):
    client, service = auth_client
    owner, other = _user(service, "owner"), _user(service, "other")
    rec = service.task_store.add_research(   # prompt: min_length=5
        ResearchRequest(prompt="owner only topic", depth=SearchDepth.EASY), task_ids=[], user_id=owner.id
    )
    url = f"/v1/research/{rec.id}"
    assert (await client.get(url)).status_code == 401                          # anonymous
    assert (await client.get(url, headers=_bearer(other))).status_code == 404  # stranger: 404, never 403
    assert (await client.get(url, headers=_bearer(owner))).status_code == 200


@pytest.mark.anyio
async def test_anonymous_mutation_meets_csrf_first(auth_client):
    client, service = auth_client
    rec = service.task_store.add_research(
        ResearchRequest(prompt="owner only topic", depth=SearchDepth.EASY), task_ids=[], user_id=_user(service, "o").id
    )
    assert (await client.delete(f"/v1/research/{rec.id}")).status_code == 403  # CSRF runs before auth
    bogus = {"Authorization": "Bearer bogus"}
    assert (await client.delete(f"/v1/research/{rec.id}", headers=bogus)).status_code == 401
```

- **Test matrix for reads.** Owner 2xx, stranger 404, anonymous 401.
- **Test matrix for mutations:**
  - no credential: 403 (CSRF);
  - a bogus Bearer: 401;
  - a cookie session without `X-CSRF-Token`: 403;
  - with `X-CSRF-Token: client.cookies["csrf_token"]`: 2xx.
  The client keeps the cookies from `POST /v1/auth/register`, so run the anonymous checks before registering, or use a second client.
- **Admins.** Set `admin_emails` and create the user with `admin, _ = service.get_or_create_oauth_user(email, google_subject="sub-x")`; it returns a tuple.
- **LLM routes.** Replace the limiter with a fresh `SlidingWindowLimiter()` (see `tests/test_error_leak.py`).
- **The Postgres Smoke job runs the whole suite on one real database.** Use uuid-unique ids and emails, and assert only on your own rows.

## Before you call it done

Walk this list. Then run the checks (`run-checks` skill): a targeted pytest, then `lint`, `backend` and `guards`, plus `frontend` if `api.ts` or the locales changed.

- Ownership guard or owner scoping on every id; 404 for a stranger.
- No side-effecting GET.
- Admin mutations are throttled, audited and listed in `AUDITED_ROUTES`.
- LLM routes are metered; `LLM_ENTRY_POINTS` / `NAMED_LLM_ROUTES` are updated.
- No exception text reaches the client.
- Error contract and the three locales are updated together.
- Metric labels are bounded; nothing identifying goes into logs.
- No blocking I/O in async handlers.
- Postgres parity (store changes).
- The twins above are updated.

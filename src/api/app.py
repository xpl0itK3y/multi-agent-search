import asyncio
import contextlib
import hmac
import json
import logging
import secrets
import uuid
import time
from typing import List

from starlette.concurrency import run_in_threadpool
from urllib.parse import quote

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse

from src.domain.errors import ConflictError, ServiceError
from src.api.dependencies import (
    get_current_user,
    get_research_service,
    require_admin,
    scope_user_id,
    verify_research_access,
)
from src.auth.login_rate_limit import (
    enforce_auth_rate_limit,
    enforce_login_account_rate_limit,
    enforce_password_check_rate_limit,
)
from src.auth.llm_rate_limit import enforce_llm_rate_limit
from src.auth.admin_rate_limit import enforce_admin_rate_limit
from src.auth.security import create_token, decode_token
from src.auth.google_oauth import build_authorization_url, fetch_userinfo
from src.model_catalog import list_models as list_model_catalog
from src.api.schemas import (
    AdminAuditLogItem,
    AdminDryRunResult,
    AdminEventLogResponse,
    AdminOverviewResponse,
    AdminPromptsResponse,
    AdminTelemetrySummaryResponse,
    AdminTokenAnalyticsResponse,
    AdminUserDetailResponse,
    AdminUserListResponse,
    AgentMetadataItem,
    UserTelemetryEventInput,
    AuthUser,
    UpdateProfileRequest,
    AuthSession,
    SetPasswordRequest,
    DeleteAccountRequest,
    DecomposeRequest,
    DecomposeResponse,
    LoginRequest,
    RegisterRequest,
    JobCleanupResponse,
    JobRecoveryResponse,
    OperationalHealth,
    OperationalRecommendationResolveRequest,
    OptimizeRequest,
    OptimizeResponse,
    QueueMetrics,
    QueueMaintenanceResponse,
    ResearchFinalizeJob,
    ResearchFinalizeResponse,
    ResearchGraphResponse,
    ResearchHistoryItem,
    ResearchRecord,
    ResearchRename,
    ResearchReportResponse,
    ResearchRequest,
    ChatAsk,
    ChatMessage,
    CitationAudit,
    SourceIndependence,
    SourceReputation,
    SourceIntegrity,
    CrossLanguageReport,
    StanceBalance,
    NumericCheck,
    ConfidenceReport,
    AuditTrail,
    ShareInfo,
    PublicReport,
    Clarification,
    ClarifyAnswers,
    ComparisonTable,
    RedTeamReport,
    ResearchConflict,
    VerificationReport,
    ResearchPlan,
    ResearchPlanUpdate,
    ResearchResponse,
    ResearchSummary,
    ResearchStatusSummary,
    SearchTaskJob,
    SearchTask,
    SearchSourcePreview,
    SearchTaskSummary,
    TaskUpdate,
    WorkerHeartbeat,
)
from src.bootstrap import lifespan
from src.config import settings
from src.observability import bind_observability_context, metric_route_template, observe_api_request, render_metrics


logger = logging.getLogger(__name__)

_CSRF_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_CSRF_EXEMPT_PATHS = frozenset({"/v1/auth/login", "/v1/auth/register", "/v1/telemetry/event"})


def _is_csrf_violation(request: Request) -> bool:
    """Double-submit CSRF check for cookie-authenticated mutations. Bearer-token requests are
    exempt (the header can't be forged cross-site); the check is off when auth is disabled."""
    if settings.auth_disabled or request.method in _CSRF_SAFE_METHODS:
        return False
    if request.url.path in _CSRF_EXEMPT_PATHS:
        return False
    if request.headers.get("authorization", "").lower().startswith("bearer "):
        return False
    cookie = request.cookies.get(settings.csrf_cookie_name)
    header = request.headers.get("x-csrf-token")
    return not cookie or not header or not hmac.compare_digest(cookie, header)


_SENSITIVE_GRAPH_STATE_KEYS = frozenset({"share_token", "webhook_url", "decompose_payload"})


def _public_record(record: ResearchRecord | None) -> ResearchRecord | None:
    """Strip internal/sensitive graph_state keys before returning a ResearchRecord to a client
    (AUD-012). Returns a copy — the internal record and persistence keep the full state."""
    if record is None or not record.graph_state:
        return record
    cleaned = {k: v for k, v in record.graph_state.items() if k not in _SENSITIVE_GRAPH_STATE_KEYS}
    return record.model_copy(update={"graph_state": cleaned})


def extract_client_ip(request: Request) -> str | None:
    """The caller's address for audit rows, last_ip and telemetry.

    Only the peer address uvicorn resolved (``--proxy-headers`` against the trusted nginx
    hop, which overwrites X-Forwarded-For with $remote_addr) is used. Raw
    CF-Connecting-IP / X-Forwarded-For / X-Real-IP headers are client-supplied and would
    let any caller forge the IP recorded in the admin audit log."""
    client = request.client
    return client.host if client and client.host else None


def parse_client_ua(ua_string: str | None) -> dict[str, str]:
    if not ua_string:
        return {"browser": "Unknown", "os": "Unknown", "device_type": "desktop"}
    ua = ua_string.lower()

    if any(k in ua for k in ["ipad", "tablet"]):
        device_type = "tablet"
    elif any(k in ua for k in ["mobile", "android", "iphone", "ipod"]):
        device_type = "mobile"
    else:
        device_type = "desktop"

    if "macintosh" in ua or "mac os" in ua:
        os_name = "macOS"
    elif "windows" in ua:
        os_name = "Windows"
    elif "iphone" in ua or "ipad" in ua or "ios" in ua:
        os_name = "iOS"
    elif "android" in ua:
        os_name = "Android"
    elif "linux" in ua:
        os_name = "Linux"
    else:
        os_name = "Other"

    if "edg/" in ua or "edge" in ua:
        browser = "Edge"
    elif "chrome" in ua and "safari" in ua and "edg" not in ua and "opr" not in ua:
        browser = "Chrome"
    elif "safari" in ua and "chrome" not in ua:
        browser = "Safari"
    elif "firefox" in ua:
        browser = "Firefox"
    elif "opera" in ua or "opr" in ua:
        browser = "Opera"
    else:
        browser = "Other"

    return {"browser": browser, "os": os_name, "device_type": device_type}


def create_app() -> FastAPI:
    app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)

    @app.exception_handler(ServiceError)
    async def _service_error_handler(request: Request, exc: ServiceError) -> JSONResponse:
        # Translate framework-agnostic service errors (ARCH-004) into HTTP responses, matching
        # the {"detail": ...} shape FastAPI produced for the old HTTPException raises.
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    allowed_origins = [origin.strip() for origin in settings.cors_allow_origins.split(",") if origin.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["Content-Disposition"],
    )

    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = request_id
        started_at = time.perf_counter()
        with bind_observability_context(
            request_id=request_id,
            method=request.method,
            path=str(request.url.path),
        ):
            response = await call_next(request)
        observe_api_request(
            request.method,
            metric_route_template(request.scope.get("route")),
            response.status_code,
            time.perf_counter() - started_at,
        )
        response.headers["X-Request-ID"] = request_id

        # Automatic server-side user activity telemetry
        path_str = str(request.url.path)
        if not path_str.startswith("/metrics") and not path_str.startswith("/health"):
            auth_header = request.headers.get("authorization")
            token_str = None
            if auth_header and auth_header.startswith("Bearer "):
                token_str = auth_header.split(" ", 1)[1]
            elif settings.auth_cookie_name in request.cookies:
                token_str = request.cookies[settings.auth_cookie_name]

            if token_str:
                try:
                    dec = decode_token(token_str)
                    uid = dec.get("sub")
                    if uid:
                        service = getattr(request.app.state, "research_service", None)
                        if service and hasattr(service, "task_store"):
                            c_ip = extract_client_ip(request)
                            u_agent = request.headers.get("user-agent")
                            dev_info = parse_client_ua(u_agent)
                            service.task_store.touch_user_activity(
                                user_id=uid,
                                ip_address=c_ip,
                                user_agent=u_agent,
                                device=dev_info["device_type"],
                            )
                except Exception:
                    pass
        # Baseline security headers (SEC-009). HSTS only when cookies are Secure (i.e. served
        # over HTTPS). CSP is left to the SPA's own server — this API is JSON-first.
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        if settings.auth_cookie_secure:
            response.headers.setdefault(
                "Strict-Transport-Security", "max-age=31536000; includeSubDomains"
            )
        return response

    @app.middleware("http")
    async def csrf_middleware(request: Request, call_next):
        if _is_csrf_violation(request):
            return JSONResponse(status_code=403, content={"detail": "CSRF token missing or invalid"})
        return await call_next(request)

    register_routes(app)
    return app


def _issue_session(response: Response, user: AuthUser) -> str:
    """Mint a JWT for the user, set it as an httpOnly cookie, and return it (Bearer)."""
    token = create_token(user.id, email=user.email, token_version=user.token_version)
    response.set_cookie(
        key=settings.auth_cookie_name,
        value=token,
        httponly=True,
        secure=settings.auth_cookie_secure,
        samesite="lax",
        max_age=settings.auth_token_ttl_seconds,
        path="/",
    )
    # Double-submit CSRF token: readable by JS so the SPA can echo it in a header. The
    # csrf_middleware requires header==cookie for cookie-authenticated mutations.
    response.set_cookie(
        key=settings.csrf_cookie_name,
        value=secrets.token_urlsafe(32),
        httponly=False,
        secure=settings.auth_cookie_secure,
        samesite="lax",
        max_age=settings.auth_token_ttl_seconds,
        path="/",
    )
    return token


def register_routes(app: FastAPI) -> None:
    # Ownership guard for per-research routes (no-op when auth is disabled).
    research_guard = [Depends(verify_research_access)]
    auth_required = [Depends(get_current_user)]   # requires login (no-op when auth disabled)
    admin_guard = [Depends(require_admin)]         # login + admin email (no-op when auth disabled)
    auth_rate_limit = [Depends(enforce_auth_rate_limit)]  # per-IP throttle (no-op when auth disabled)

    @app.get("/health")
    def health_check(request: Request):
        # Cheap pings-only payload: this is the compose healthcheck and LB probe path.
        return get_research_service(request).get_health_summary()

    @app.get("/health/detail", dependencies=admin_guard)
    def health_detail(request: Request):
        """Full operational payload (queues, graph alerts, trends) — admin only."""
        return get_research_service(request).get_health_status()

    @app.post("/v1/auth/register", response_model=AuthSession, dependencies=auth_rate_limit)
    def register(payload: RegisterRequest, response: Response, request: Request):
        user = get_research_service(request).register_user(payload.email, payload.password)
        token = _issue_session(response, user)
        return AuthSession(access_token=token, user=user)

    @app.post("/v1/auth/login", response_model=AuthSession, dependencies=auth_rate_limit)
    def login(payload: LoginRequest, response: Response, request: Request):
        enforce_login_account_rate_limit(payload.email)  # per-account, on top of per-IP
        user = get_research_service(request).authenticate_user(payload.email, payload.password)
        token = _issue_session(response, user)
        return AuthSession(access_token=token, user=user)

    @app.post("/v1/auth/logout")
    def logout(response: Response):
        response.delete_cookie(settings.auth_cookie_name, path="/")
        response.delete_cookie(settings.csrf_cookie_name, path="/")
        return {"status": "ok"}

    @app.get("/v1/auth/me", response_model=AuthUser)
    def me(user: AuthUser = Depends(get_current_user)):
        return user

    @app.patch("/v1/auth/profile", response_model=AuthUser)
    def update_profile(
        payload: UpdateProfileRequest,
        request: Request,
        user: AuthUser = Depends(get_current_user),
    ):
        return get_research_service(request).update_profile(
            user.id,
            name=payload.name,
            avatar_url=payload.avatar_url,
        )

    @app.get("/v1/auth/token-stats")
    def get_user_token_stats(
        request: Request,
        user: AuthUser = Depends(get_current_user),
    ):
        return get_research_service(request).task_store.get_user_token_analytics(user.id)

    @app.get("/v1/auth/config")
    def auth_config():
        """Which auth options the SPA should offer (e.g. show the Google button)."""
        return {"google_oauth": settings.oauth_enabled}

    @app.get("/v1/auth/google/login")
    def google_login():
        if not settings.oauth_enabled:
            raise HTTPException(status_code=404, detail="Google OAuth is not configured")
        # CSRF state bound to the browser: a short-lived signed token carried in BOTH the URL
        # and an httpOnly SameSite=Lax cookie. The callback requires them to match, so a state
        # minted for one browser can't be used to complete a login in another (login-CSRF).
        state = create_token(secrets.token_urlsafe(8), ttl_seconds=600)
        redirect = RedirectResponse(build_authorization_url(state), status_code=302)
        redirect.set_cookie(
            "oauth_state", state, max_age=600, httponly=True,
            secure=settings.auth_cookie_secure, samesite="lax", path="/",
        )
        return redirect

    def _oauth_failure(reason: str) -> RedirectResponse:
        # The callback is a full-page navigation: send the browser back to the SPA's login
        # page with a stable code instead of a raw JSON error (or provider exception text).
        redirect = RedirectResponse(f"/login?error={reason}", status_code=302)
        redirect.delete_cookie("oauth_state", path="/")
        return redirect

    @app.get("/v1/auth/google/callback")
    def google_callback(request: Request, code: str = "", state: str = ""):
        if not settings.oauth_enabled:
            raise HTTPException(status_code=404, detail="Google OAuth is not configured")
        cookie_state = request.cookies.get("oauth_state")
        if not code or decode_token(state) is None or not cookie_state or cookie_state != state:
            logger.warning("google_oauth_invalid_state")
            return _oauth_failure("oauth_failed")
        try:
            userinfo = fetch_userinfo(code)
        except Exception:
            logger.exception("google_oauth_userinfo_failed")
            return _oauth_failure("oauth_failed")
        email = userinfo.get("email")
        verified = userinfo.get("email_verified")
        if not email or verified not in (True, "true"):
            logger.warning("google_oauth_email_unverified")
            return _oauth_failure("oauth_failed")
        try:
            user, created = get_research_service(request).get_or_create_oauth_user(
                email,
                google_subject=userinfo.get("sub") or "",
                name=userinfo.get("name"),
                avatar_url=userinfo.get("picture"),
            )
        except ConflictError:
            # The email already belongs to a local account that is not linked to this Google
            # identity; it is never merged silently (SEC-ACCOUNT).
            logger.warning("google_oauth_conflict_unlinked_local_account")
            return _oauth_failure("oauth_conflict")
        except Exception:
            logger.exception("google_oauth_account_resolution_failed")
            return _oauth_failure("oauth_failed")
        # New users are offered a password to set; returning users go straight in.
        target = settings.oauth_new_user_redirect if created else settings.oauth_post_login_redirect
        redirect = RedirectResponse(target, status_code=302)
        redirect.delete_cookie("oauth_state", path="/")
        _issue_session(redirect, user)  # sets the JWT session cookie
        return redirect

    @app.post("/v1/auth/set-password", response_model=AuthSession)
    def set_password(
        payload: SetPasswordRequest,
        response: Response,
        request: Request,
        user: AuthUser = Depends(enforce_password_check_rate_limit),
    ):
        updated_user = get_research_service(request).set_user_password(
            user.id,
            payload.password,
            current_password=payload.current_password,
        )
        token = _issue_session(response, updated_user)
        return AuthSession(access_token=token, user=updated_user)

    @app.delete("/v1/auth/account")
    def delete_account(
        payload: DeleteAccountRequest,
        response: Response,
        request: Request,
        user: AuthUser = Depends(enforce_password_check_rate_limit),
    ):
        """Delete the account and all owned data (researches, results, share links).

        Throttled per user like set-password: whoever holds a stolen session could
        otherwise guess current_password here without limit."""
        get_research_service(request).delete_user_account(
            user.id,
            current_password=payload.current_password,
            confirm=payload.confirm,
        )
        response.delete_cookie(settings.auth_cookie_name, path="/")
        response.delete_cookie(settings.csrf_cookie_name, path="/")
        return {"status": "deleted"}

    @app.get("/metrics")
    def metrics_endpoint(request: Request):
        # Optional shared-secret guard: Prometheus authenticates via the scrape job's
        # authorization config; a literal admin login would not work for scraping.
        token = settings.metrics_token
        if token:
            supplied = request.headers.get("x-metrics-token", "")
            if not supplied:
                authorization = request.headers.get("authorization", "")
                if authorization.startswith("Bearer "):
                    supplied = authorization[len("Bearer "):]
            if not hmac.compare_digest(supplied, token):
                raise HTTPException(status_code=401, detail="Valid metrics token required")
        payload, content_type = render_metrics()
        return Response(content=payload, media_type=content_type)

    @app.get("/health/queues", response_model=QueueMetrics, dependencies=auth_required)
    def queue_health(request: Request):
        return get_research_service(request).get_queue_metrics()

    @app.post("/health/queues/maintenance", response_model=QueueMaintenanceResponse, dependencies=admin_guard)
    def run_queue_maintenance(request: Request):
        return get_research_service(request).run_queue_maintenance()

    @app.post(
        "/health/queues/operational-health/recommendations/{code}/ack",
        response_model=OperationalHealth.RecommendationEntry,
        dependencies=admin_guard,
    )
    def acknowledge_operational_recommendation(code: str, request: Request):
        return get_research_service(request).acknowledge_operational_recommendation(code)

    @app.post(
        "/health/queues/operational-health/recommendations/{code}/resolve",
        response_model=OperationalHealth.RecommendationEntry,
        dependencies=admin_guard,
    )
    def resolve_operational_recommendation(
        code: str,
        payload: OperationalRecommendationResolveRequest,
        request: Request,
    ):
        return get_research_service(request).resolve_operational_recommendation(code, payload.note)

    @app.get("/health/workers/{worker_name}", response_model=WorkerHeartbeat, dependencies=auth_required)
    def worker_health(worker_name: str, request: Request):
        heartbeat = get_research_service(request).get_worker_heartbeat(worker_name)
        if not heartbeat:
            raise HTTPException(status_code=404, detail="Worker heartbeat not found")
        return heartbeat

    @app.get("/v1/models")
    def list_models():
        return list_model_catalog()

    @app.post("/v1/optimize", response_model=OptimizeResponse)
    def optimize_prompt(
        request: Request,
        payload: OptimizeRequest,
        user: AuthUser = Depends(enforce_llm_rate_limit),
    ):
        optimized = get_research_service(request).optimize_prompt(payload.prompt, user_id=user.id)
        return OptimizeResponse(optimized_prompt=optimized)

    @app.post("/v1/decompose", response_model=DecomposeResponse)
    def decompose_prompt(
        request: Request,
        payload: DecomposeRequest,
        user: AuthUser = Depends(enforce_llm_rate_limit),
    ):
        return get_research_service(request).decompose_prompt(
            payload.prompt,
            payload.depth,
            user_id=user.id,
        )

    @app.get("/v1/tasks", response_model=List[SearchTask], dependencies=admin_guard)
    def list_tasks(request: Request, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
        return get_research_service(request).list_tasks(limit=limit, offset=offset)

    @app.get("/v1/tasks/{task_id}", response_model=SearchTask, dependencies=auth_required)
    def get_task(task_id: str, request: Request, owner: str | None = Depends(scope_user_id)):
        task = get_research_service(request).get_task(task_id, user_id=owner)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        return task

    @app.get("/v1/tasks/{task_id}/summary", response_model=SearchTaskSummary, dependencies=auth_required)
    def get_task_summary(task_id: str, request: Request, owner: str | None = Depends(scope_user_id)):
        return get_research_service(request).get_task_summary(task_id, user_id=owner)

    @app.patch("/v1/tasks/{task_id}", response_model=SearchTask, dependencies=auth_required)
    def update_task(
        task_id: str,
        update: TaskUpdate,
        request: Request,
        owner: str | None = Depends(scope_user_id),
    ):
        task = get_research_service(request).update_task(task_id, update, user_id=owner)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        return task

    @app.get("/v1/tasks/{task_id}/search-job", response_model=SearchTaskJob, dependencies=auth_required)
    def get_latest_search_job(
        task_id: str,
        request: Request,
        owner: str | None = Depends(scope_user_id),
    ):
        job = get_research_service(request).get_latest_search_task_job(task_id, user_id=owner)
        if not job:
            raise HTTPException(status_code=404, detail="Search job not found")
        return job

    @app.get("/v1/search-jobs/{job_id}", response_model=SearchTaskJob, dependencies=auth_required)
    def get_search_job(
        job_id: str,
        request: Request,
        owner: str | None = Depends(scope_user_id),
    ):
        job = get_research_service(request).get_search_task_job(job_id, user_id=owner)
        if not job:
            raise HTTPException(status_code=404, detail="Search job not found")
        return job

    @app.get("/v1/search-jobs", response_model=List[SearchTaskJob], dependencies=admin_guard)
    def list_search_jobs(status: str, request: Request):
        service = get_research_service(request)
        if status == "running":
            return service.list_running_search_task_jobs()
        if status == "dead_letter":
            return service.list_dead_letter_search_task_jobs()
        raise HTTPException(status_code=422, detail="Unsupported search job status filter")

    @app.post("/v1/search-jobs/{job_id}/requeue", response_model=SearchTaskJob, dependencies=admin_guard)
    def requeue_search_job(job_id: str, request: Request):
        return get_research_service(request).requeue_search_task_job(job_id)

    @app.post("/v1/search-jobs/recover-stale", response_model=JobRecoveryResponse, dependencies=admin_guard)
    def recover_stale_search_jobs(request: Request):
        return get_research_service(request).recover_stale_search_task_jobs()

    @app.post("/v1/search-jobs/cleanup", response_model=JobCleanupResponse, dependencies=admin_guard)
    def cleanup_search_jobs(request: Request):
        return get_research_service(request).cleanup_old_search_task_jobs()

    @app.get("/v1/research", response_model=List[ResearchHistoryItem])
    def list_researches(request: Request, limit: int = Query(20, ge=1, le=200), owner: str | None = Depends(scope_user_id)):
        return get_research_service(request).list_researches(limit=limit, user_id=owner)

    @app.get("/v1/threads/{thread_id}", response_model=List[ResearchHistoryItem])
    def list_thread(thread_id: str, request: Request, owner: str | None = Depends(scope_user_id)):
        return get_research_service(request).list_thread(thread_id, user_id=owner)

    @app.post("/v1/research", response_model=ResearchResponse)
    def start_research(
        request: Request,
        payload: ResearchRequest,
        background_tasks: BackgroundTasks,
        _rate_user: AuthUser = Depends(enforce_llm_rate_limit),
        owner: str | None = Depends(scope_user_id),
    ):
        service = get_research_service(request)
        response, research_id = service.start_research(payload, user_id=owner)
        try:
            service.task_store.record_user_event(
                event_name="research_prompt",
                event_category="prompt",
                details={
                    "research_id": research_id,
                    "prompt": payload.prompt,
                    "depth": payload.depth.value if hasattr(payload.depth, "value") else str(payload.depth),
                },
                user_id=owner,
                ip_address=extract_client_ip(request),
                user_agent=request.headers.get("user-agent"),
            )
        except Exception:
            pass
        # LLM decompose runs after response is sent — user gets research_id instantly
        background_tasks.add_task(service.decompose_and_enqueue, research_id, payload)
        return response

    @app.get("/v1/research/finalize-jobs", response_model=List[ResearchFinalizeJob], dependencies=admin_guard)
    def list_finalize_jobs(status: str, request: Request):
        service = get_research_service(request)
        if status == "running":
            return service.list_running_research_finalize_jobs()
        if status == "dead_letter":
            return service.list_dead_letter_research_finalize_jobs()
        raise HTTPException(status_code=422, detail="Unsupported finalize job status filter")

    @app.get("/v1/research/finalize-jobs/{job_id}", response_model=ResearchFinalizeJob, dependencies=auth_required)
    def get_finalize_job(
        job_id: str,
        request: Request,
        owner: str | None = Depends(scope_user_id),
    ):
        job = get_research_service(request).get_research_finalize_job(job_id, user_id=owner)
        if not job:
            raise HTTPException(status_code=404, detail="Finalize job not found")
        return job

    @app.get("/v1/research/{research_id}/finalize-job", response_model=ResearchFinalizeJob, dependencies=research_guard)
    def get_latest_finalize_job(research_id: str, request: Request):
        job = get_research_service(request).get_latest_research_finalize_job(research_id)
        if not job:
            raise HTTPException(status_code=404, detail="Finalize job not found")
        return job

    @app.post("/v1/research/finalize-jobs/{job_id}/requeue", response_model=ResearchFinalizeJob, dependencies=admin_guard)
    def requeue_finalize_job(job_id: str, request: Request):
        return get_research_service(request).requeue_research_finalize_job(job_id)

    @app.post("/v1/research/finalize-jobs/recover-stale", response_model=JobRecoveryResponse, dependencies=admin_guard)
    def recover_stale_finalize_jobs(request: Request):
        return get_research_service(request).recover_stale_research_finalize_jobs()

    @app.post("/v1/research/finalize-jobs/cleanup", response_model=JobCleanupResponse, dependencies=admin_guard)
    def cleanup_finalize_jobs(request: Request):
        return get_research_service(request).cleanup_old_research_finalize_jobs()

    # ── Client Telemetry Ingestion ────────────────────────────────────────────
    @app.post("/v1/telemetry/event")
    def record_telemetry_event(
        payload: UserTelemetryEventInput,
        request: Request,
    ):
        service = get_research_service(request)
        c_ip = extract_client_ip(request)
        u_agent = request.headers.get("user-agent")

        user_id = None
        auth_header = request.headers.get("authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ", 1)[1]
            try:
                decoded = decode_token(token)
                user_id = decoded.get("sub")
            except Exception:
                pass
        elif settings.auth_cookie_name in request.cookies:
            try:
                decoded = decode_token(request.cookies[settings.auth_cookie_name])
                user_id = decoded.get("sub")
            except Exception:
                pass

        if payload.device_info and user_id:
            dev = payload.device_info
            ua_parsed = parse_client_ua(u_agent)
            service.task_store.record_user_session(
                user_id=user_id,
                session_id=payload.session_id or str(uuid.uuid4()),
                ip_address=c_ip,
                user_agent=u_agent,
                device_type=dev.get("device_type") or ua_parsed["device_type"],
                browser=dev.get("browser") or ua_parsed["browser"],
                os=dev.get("os") or ua_parsed["os"],
                screen_res=dev.get("screen_res"),
                viewport=dev.get("viewport"),
                language=dev.get("language"),
                client_timezone=dev.get("timezone"),
                country=request.headers.get("cf-ipcountry"),
                city=request.headers.get("cf-ipcity"),
            )

        event_id = service.task_store.record_user_event(
            event_name=payload.event_name,
            event_category=payload.event_category,
            user_id=user_id,
            session_id=payload.session_id,
            details=payload.details,
            ip_address=c_ip,
            user_agent=u_agent,
        )
        return {"status": "ok", "event_id": event_id}

    # ── Admin Panel Dedicated Endpoints ───────────────────────────────────────
    @app.get("/v1/admin/users", response_model=AdminUserListResponse, dependencies=admin_guard)
    def admin_users_list(
        request: Request,
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        search: str | None = None,
        role: str | None = None,
        online_only: bool = False,
        sort_by: str = "last_seen",
    ):
        return get_research_service(request).task_store.get_admin_users_list(
            page=page,
            page_size=page_size,
            search=search,
            role=role,
            online_only=online_only,
            sort_by=sort_by,
        )

    @app.get("/v1/admin/users/analytics/summary", response_model=AdminTelemetrySummaryResponse, dependencies=admin_guard)
    def admin_telemetry_summary(request: Request):
        return get_research_service(request).task_store.get_admin_telemetry_summary()

    @app.get("/v1/admin/users/events", response_model=AdminEventLogResponse, dependencies=admin_guard)
    def admin_user_events(
        request: Request,
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
        category: str | None = None,
        event_name: str | None = None,
        user_id: str | None = None,
    ):
        return get_research_service(request).task_store.get_admin_event_logs(
            limit=limit,
            offset=offset,
            category=category,
            event_name=event_name,
            user_id=user_id,
        )

    @app.get("/v1/admin/users/export", dependencies=admin_guard)
    def admin_users_export(request: Request):
        import csv
        import io
        users_resp = get_research_service(request).task_store.get_admin_users_list(page=1, page_size=10000)
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "user_id", "email", "name", "role", "is_online", "last_seen_at",
            "last_ip", "last_device", "last_browser", "last_os",
            "researches_count", "total_tokens", "total_cost_usd", "created_at"
        ])
        for u in users_resp.users:
            writer.writerow([
                u.id,
                u.email,
                u.name or "",
                "admin" if u.is_admin else "user",
                "yes" if u.is_online else "no",
                u.last_seen_at or "",
                u.last_ip or "",
                u.last_device or "",
                u.last_browser or "",
                u.last_os or "",
                u.researches_count,
                u.total_tokens,
                u.total_cost_usd,
                u.created_at,
            ])
        csv_content = output.getvalue()
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=users_telemetry.csv"},
        )

    @app.get("/v1/admin/prompts", response_model=AdminPromptsResponse, dependencies=admin_guard)
    def admin_prompts(
        request: Request,
        page: int = Query(1, ge=1),
        page_size: int = Query(25, ge=1, le=100),
        search: str | None = None,
        user_id: str | None = None,
        prompt_type: str | None = None,
    ):
        return get_research_service(request).task_store.get_admin_prompts(
            page=page,
            page_size=page_size,
            search=search,
            user_id=user_id,
            prompt_type=prompt_type,
        )

    @app.get("/v1/admin/prompts/export", dependencies=admin_guard)
    def admin_prompts_export(request: Request):
        import csv
        import io
        resp = get_research_service(request).task_store.get_admin_prompts(page=1, page_size=10000)
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "id", "prompt_type", "research_id", "user_id", "user_email", "user_name",
            "prompt", "depth", "status", "total_tokens", "cost_usd", "created_at"
        ])
        for p in resp.prompts:
            writer.writerow([
                p.id,
                p.prompt_type,
                p.research_id,
                p.user_id or "",
                p.user_email or "",
                p.user_name or "",
                p.prompt,
                p.depth or "",
                p.status or "",
                p.total_tokens,
                p.cost_usd,
                p.created_at,
            ])
        csv_content = output.getvalue()
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=user_prompts.csv"},
        )

    @app.get("/v1/admin/users/{user_id}", response_model=AdminUserDetailResponse, dependencies=admin_guard)
    def admin_user_detail(user_id: str, request: Request):
        detail = get_research_service(request).task_store.get_admin_user_detail(user_id)
        if not detail:
            raise HTTPException(status_code=404, detail="User not found")
        return detail

    @app.delete("/v1/admin/users/{user_id}")
    def admin_delete_user(
        user_id: str,
        request: Request,
        admin_user: AuthUser = Depends(require_admin),
    ):
        if admin_user.id == user_id:
            raise HTTPException(status_code=400, detail="Cannot delete your own admin account")
        service = get_research_service(request)
        deleted = service.task_store.delete_user(user_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="User not found")
        client_ip = extract_client_ip(request)
        service.task_store.record_admin_audit(
            actor_email=admin_user.email,
            action="delete_user",
            target_type="user",
            target_id=user_id,
            details={"deleted_user_id": user_id},
            ip_address=client_ip,
        )
        return {"status": "ok", "deleted_user_id": user_id}

    @app.get("/v1/admin/overview", response_model=AdminOverviewResponse, dependencies=admin_guard)
    def admin_overview(request: Request):
        return get_research_service(request).get_admin_overview()

    @app.get("/v1/admin/tokens", response_model=AdminTokenAnalyticsResponse, dependencies=admin_guard)
    def admin_tokens(
        request: Request,
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
    ):
        return get_research_service(request).get_admin_token_analytics(page=page, page_size=page_size)

    @app.get("/v1/admin/tokens/export", dependencies=admin_guard)
    def admin_tokens_export(request: Request):
        import csv
        import io
        analytics = get_research_service(request).get_admin_token_analytics(page=1, page_size=10000)
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["research_id", "prompt", "depth", "status", "total_tokens", "estimated_cost_usd", "created_at"])
        for item in analytics.researches:
            clean_prompt = item.prompt.replace("\n", " ").replace("\r", "")
            writer.writerow([
                item.research_id,
                clean_prompt,
                item.depth,
                item.status,
                item.total_tokens,
                item.estimated_cost_usd,
                item.created_at.isoformat(),
            ])
        csv_content = output.getvalue()
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=token_usage.csv"},
        )

    @app.get("/v1/admin/agents", response_model=List[AgentMetadataItem], dependencies=admin_guard)
    def admin_agents(request: Request):
        return get_research_service(request).get_agents_catalog()

    @app.get("/v1/admin/audit", response_model=List[AdminAuditLogItem], dependencies=admin_guard)
    def admin_audit_logs(
        request: Request,
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
        action: str | None = None,
    ):
        return get_research_service(request).get_admin_audit_logs(
            limit=limit, offset=offset, action=action
        )

    @app.post("/v1/admin/operations/preview", response_model=AdminDryRunResult, dependencies=admin_guard)
    def admin_operations_preview(payload: dict, request: Request):
        action = payload.get("action", "")
        params = payload.get("params", {})
        return get_research_service(request).preview_maintenance_action(action=action, params=params)

    @app.post("/v1/admin/operations/execute", response_model=AdminDryRunResult)
    def admin_operations_execute(payload: dict, request: Request, admin_user: AuthUser = Depends(enforce_admin_rate_limit)):
        action = payload.get("action", "")
        params = payload.get("params", {})
        client_ip = extract_client_ip(request)
        return get_research_service(request).execute_maintenance_action(
            action=action,
            actor_email=admin_user.email,
            params=params,
            ip_address=client_ip,
        )

    @app.get("/v1/admin/stream")
    async def admin_stream(request: Request):
        require_admin(request)
        service = get_research_service(request)

        async def event_generator():
            import asyncio
            while True:
                if await request.is_disconnected():
                    break
                try:
                    overview = service.get_admin_overview()
                    data = overview.model_dump_json()
                    yield f"event: overview\ndata: {data}\n\n"
                except Exception as err:
                    yield f"event: error\ndata: {json.dumps({'error': str(err)})}\n\n"
                await asyncio.sleep(2.0)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.delete("/v1/research/{research_id}", status_code=204)
    def delete_research(research_id: str, request: Request, owner: str | None = Depends(scope_user_id)):
        deleted = get_research_service(request).delete_research(research_id, user_id=owner)
        if not deleted:
            raise HTTPException(status_code=404, detail="Research not found")

    @app.post("/v1/research/{research_id}/cancel", response_model=ResearchRecord)
    def cancel_research(research_id: str, request: Request, owner: str | None = Depends(scope_user_id)):
        return _public_record(get_research_service(request).cancel_research(research_id, user_id=owner))

    @app.post("/v1/research/{research_id}/retry", response_model=ResearchRecord)
    def retry_research(
        research_id: str,
        request: Request,
        background_tasks: BackgroundTasks,
        owner: str | None = Depends(scope_user_id),
    ):
        return _public_record(
            get_research_service(request).retry_research(
                research_id, user_id=owner, background_tasks=background_tasks
            )
        )

    @app.patch("/v1/research/{research_id}", response_model=ResearchRecord)
    def rename_research(
        research_id: str, payload: ResearchRename, request: Request, owner: str | None = Depends(scope_user_id)
    ):
        return _public_record(get_research_service(request).rename_research(research_id, payload.title, user_id=owner))

    @app.get("/v1/research/{research_id}", response_model=ResearchRecord, dependencies=research_guard)
    def get_research_status(research_id: str, request: Request):
        return _public_record(get_research_service(request).get_research_status(research_id))

    @app.get("/v1/research/{research_id}/summary", response_model=ResearchSummary, dependencies=research_guard)
    def get_research_summary(research_id: str, request: Request):
        return get_research_service(request).get_research_summary(research_id)

    @app.get("/v1/research/{research_id}/status", response_model=ResearchStatusSummary, dependencies=research_guard)
    def get_research_status_summary(research_id: str, request: Request):
        # Cheap polling endpoint: no heavy analysis/LLM (unlike /summary).
        return get_research_service(request).get_research_status_summary(research_id)

    @app.get("/v1/research/{research_id}/report", response_model=ResearchReportResponse, dependencies=research_guard)
    def get_research_report(research_id: str, request: Request):
        return get_research_service(request).get_research_report(research_id)

    @app.get("/v1/research/{research_id}/sources", response_model=List[SearchSourcePreview], dependencies=research_guard)
    def get_research_sources(research_id: str, request: Request):
        return get_research_service(request).get_research_sources(research_id)

    @app.get("/v1/research/{research_id}/export", dependencies=research_guard)
    def export_research(
        research_id: str, request: Request, format: str = "pdf",
        theme: str | None = None, accent: str | None = None, base: str | None = None,
    ):
        # sync def -> threadpool (PDF/DOCX generation is blocking)
        data, media_type, filename = get_research_service(request).export_research_report(
            research_id, format, theme=theme, accent=accent, base=base,
        )
        ascii_name = filename.encode("ascii", "ignore").decode() or "research"
        disposition = f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(filename)}"
        return Response(content=data, media_type=media_type, headers={"Content-Disposition": disposition})

    @app.get("/v1/research/{research_id}/conflicts", response_model=List[ResearchConflict], dependencies=research_guard)
    def get_research_conflicts(research_id: str, request: Request):
        return get_research_service(request).get_research_conflicts(research_id)

    @app.get("/v1/research/{research_id}/verification", response_model=VerificationReport, dependencies=research_guard)
    def get_research_verification(research_id: str, request: Request):
        return get_research_service(request).get_research_verification(research_id)

    @app.get("/v1/research/{research_id}/red-team", response_model=RedTeamReport, dependencies=research_guard)
    def get_research_red_team(research_id: str, request: Request):
        return get_research_service(request).get_research_red_team(research_id)

    @app.get("/v1/research/{research_id}/citations", response_model=CitationAudit, dependencies=research_guard)
    def get_research_citations(research_id: str, request: Request):
        return get_research_service(request).get_research_citation_audit(research_id)

    @app.get("/v1/research/{research_id}/source-independence", response_model=SourceIndependence, dependencies=research_guard)
    def get_research_source_independence(research_id: str, request: Request):
        return get_research_service(request).get_research_source_independence(research_id)

    @app.get("/v1/research/{research_id}/source-reputation", response_model=SourceReputation, dependencies=research_guard)
    def get_research_source_reputation(research_id: str, request: Request):
        return get_research_service(request).get_research_source_reputation(research_id)

    @app.get("/v1/research/{research_id}/source-integrity", response_model=SourceIntegrity, dependencies=research_guard)
    def get_research_source_integrity(research_id: str, request: Request):
        return get_research_service(request).get_research_source_integrity(research_id)

    @app.get("/v1/research/{research_id}/cross-language", response_model=CrossLanguageReport, dependencies=research_guard)
    def get_research_cross_language(research_id: str, request: Request):
        return get_research_service(request).get_research_cross_language(research_id)

    @app.get("/v1/research/{research_id}/stance", response_model=StanceBalance, dependencies=research_guard)
    def get_research_stance(research_id: str, request: Request):
        return get_research_service(request).get_research_stance(research_id)

    @app.get("/v1/research/{research_id}/confidence", response_model=ConfidenceReport, dependencies=research_guard)
    def get_research_confidence(research_id: str, request: Request):
        return get_research_service(request).get_research_confidence(research_id)

    @app.get("/v1/research/{research_id}/numeric-check", response_model=NumericCheck, dependencies=research_guard)
    def get_research_numeric_check(research_id: str, request: Request):
        return get_research_service(request).get_research_numeric_check(research_id)

    @app.get("/v1/research/{research_id}/audit-trail", response_model=AuditTrail, dependencies=research_guard)
    def get_research_audit_trail(research_id: str, request: Request):
        return get_research_service(request).get_research_audit_trail(research_id)

    @app.get("/v1/research/{research_id}/share", response_model=ShareInfo, dependencies=research_guard)
    def get_research_share(research_id: str, request: Request):
        return get_research_service(request).get_share_info(research_id)

    @app.post("/v1/research/{research_id}/share", response_model=ShareInfo, dependencies=research_guard)
    def create_research_share(research_id: str, request: Request):
        return get_research_service(request).create_share_link(research_id)

    @app.delete("/v1/research/{research_id}/share", response_model=ShareInfo, dependencies=research_guard)
    def revoke_research_share(research_id: str, request: Request):
        return get_research_service(request).revoke_share_link(research_id)

    # PUBLIC — deliberately NO auth and NO research_guard. Security rests on the unguessable
    # token and the strict field whitelist in get_public_report; only shared researches resolve.
    @app.get("/v1/public/research/{token}", response_model=PublicReport)
    def get_public_research(token: str, request: Request):
        return get_research_service(request).get_public_report(token)

    @app.get("/v1/research/{research_id}/comparison", response_model=ComparisonTable, dependencies=research_guard)
    def get_research_comparison(research_id: str, request: Request):
        return get_research_service(request).get_research_comparison(research_id)

    @app.get("/v1/research/{research_id}/clarifications", response_model=Clarification, dependencies=research_guard)
    def get_research_clarifications(research_id: str, request: Request):
        return get_research_service(request).get_research_clarifications(research_id)

    @app.post("/v1/research/{research_id}/clarify", response_model=ResearchRecord, dependencies=research_guard)
    def submit_clarifications(research_id: str, payload: ClarifyAnswers, request: Request):
        return _public_record(get_research_service(request).submit_clarifications(research_id, payload.answers))

    @app.get("/v1/research/{research_id}/plan", response_model=ResearchPlan, dependencies=research_guard)
    def get_research_plan(research_id: str, request: Request):
        return get_research_service(request).get_research_plan(research_id)

    @app.put("/v1/research/{research_id}/plan", response_model=ResearchPlan, dependencies=research_guard)
    def update_research_plan(research_id: str, payload: ResearchPlanUpdate, request: Request):
        return get_research_service(request).update_research_plan(research_id, payload)

    @app.post("/v1/research/{research_id}/plan/approve", response_model=ResearchRecord, dependencies=research_guard)
    def approve_research_plan(research_id: str, request: Request):
        return _public_record(get_research_service(request).approve_research_plan(research_id))

    @app.get("/v1/research/{research_id}/messages", response_model=List[ChatMessage], dependencies=research_guard)
    def list_research_messages(research_id: str, request: Request):
        return get_research_service(request).list_research_messages(research_id)

    @app.post("/v1/research/{research_id}/messages", response_model=ChatMessage, dependencies=research_guard)
    def ask_research(
        research_id: str,
        payload: ChatAsk,
        request: Request,
        _rate_user: AuthUser = Depends(enforce_llm_rate_limit),
    ):
        # sync def -> runs in a threadpool so the blocking LLM call doesn't stall the event loop
        service = get_research_service(request)
        try:
            service.task_store.record_user_event(
                event_name="chat_prompt",
                event_category="prompt",
                details={
                    "research_id": research_id,
                    "prompt": payload.question,
                },
                user_id=_rate_user.id if _rate_user else None,
                ip_address=extract_client_ip(request),
                user_agent=request.headers.get("user-agent"),
            )
        except Exception:
            pass
        answer = service.generate_research_answer(research_id, payload.question)
        service.append_research_message(research_id, "user", payload.question)
        service.append_research_message(
            research_id,
            "assistant",
            answer.content,
            answer.sources,
        )
        return answer

    @app.post("/v1/research/{research_id}/messages/stream", dependencies=research_guard)
    async def ask_research_stream(
        research_id: str,
        payload: ChatAsk,
        request: Request,
        _rate_user: AuthUser = Depends(enforce_llm_rate_limit),
    ):
        """Stream a grounded follow-up answer token-by-token via SSE, then persist the turn.

        Async generator: the blocking LLM call runs on a worker thread bridged into an
        asyncio.Queue, so an open stream parks zero threadpool tokens (PERF-SSE)."""
        service = get_research_service(request)
        question = payload.question
        try:
            service.task_store.record_user_event(
                event_name="chat_prompt",
                event_category="prompt",
                details={
                    "research_id": research_id,
                    "prompt": question,
                },
                user_id=_rate_user.id if _rate_user else None,
                ip_address=extract_client_ip(request),
                user_agent=request.headers.get("user-agent"),
            )
        except Exception:
            pass

        def sse(event: str, data: dict) -> str:
            return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

        async def event_stream():
            import threading

            loop = asyncio.get_running_loop()
            channel: asyncio.Queue = asyncio.Queue()

            def emit(kind: str, value) -> None:
                loop.call_soon_threadsafe(channel.put_nowait, (kind, value))

            def worker() -> None:
                try:
                    answer = service.generate_research_answer(
                        research_id,
                        question,
                        streaming_callback=lambda partial: emit("delta", partial),
                        status_callback=lambda status: emit("status", status),
                    )
                    emit("final", answer)
                except ServiceError as exc:
                    emit("error", str(exc.detail))
                except Exception as exc:  # pragma: no cover - defensive
                    emit("error", str(exc))

            threading.Thread(target=worker, daemon=True, name=f"chat-{research_id[:8]}").start()
            yield ": connected\n\n"
            while True:
                kind, value = await channel.get()
                if kind == "delta":
                    yield sse("delta", {"answer": value})
                elif kind == "status":
                    yield sse("searching", {"status": value})
                elif kind == "final":
                    await run_in_threadpool(
                        service.append_research_message, research_id, "user", question
                    )
                    await run_in_threadpool(
                        service.append_research_message,
                        research_id,
                        "assistant",
                        value.content,
                        value.sources,
                    )
                    yield sse(
                        "done",
                        {
                            "answer": value.content,
                            "sources": [source.model_dump() for source in value.sources],
                        },
                    )
                    return
                else:
                    yield sse("stream_error", {"detail": value})
                    return

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
        )

    @app.get("/v1/research/{research_id}/graph", response_model=ResearchGraphResponse, dependencies=research_guard)
    def get_research_graph(research_id: str, request: Request):
        return get_research_service(request).get_research_graph(research_id)

    @app.get("/v1/research/{research_id}/events", dependencies=research_guard)
    async def research_events(research_id: str, request: Request):
        """Server-Sent Events stream: live status, graph trace and report deltas (F1).

        Async generator (PERF-SSE): DB reads hop to the threadpool and the Redis
        pub/sub wait runs on redis.asyncio, so each open stream costs zero
        threadpool tokens — /health stays responsive under many viewers."""
        service = get_research_service(request)

        def sse(event: str, data: dict) -> str:
            return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

        async def event_stream():
            last_status: str | None = None
            last_report: str | None = None
            last_reasoning: str | None = None
            last_trail_len = 0
            # Sized for long deep-research workloads (HARD can take 30-45+ minutes);
            # dynamically bumped whenever new steps/reasoning/reports arrive.
            deadline = time.monotonic() + 3600
            # Wake on a Redis pub/sub change ping instead of polling Postgres every second;
            # a heartbeat timeout still re-reads + keeps the connection alive (and is the
            # fallback when no broker is configured).
            broker = getattr(service, "broker", None)
            async with contextlib.AsyncExitStack() as stack:
                listener = None
                if broker is not None:
                    try:
                        listener = await stack.enter_async_context(
                            broker.research_listener_async(research_id)
                        )
                    except Exception:
                        listener = None
                yield ": connected\n\n"
                while time.monotonic() < deadline:
                    research = await run_in_threadpool(
                        service.task_store.get_research, research_id
                    )
                    if research is None:
                        yield sse("stream_error", {"detail": "Research not found"})
                        yield sse("done", {"status": "failed"})
                        return

                    status = getattr(research.status, "value", str(research.status))
                    if status != last_status:
                        last_status = status
                        yield sse("status_change", {"status": status})

                    trail = research.graph_trail or []
                    if len(trail) > last_trail_len:
                        for entry in trail[last_trail_len:]:
                            yield sse("trace_step", {
                                "step": entry.get("step"),
                                "detail": entry.get("detail"),
                                "sources": entry.get("sources") or [],
                                "agent": entry.get("agent"),
                                "phase": entry.get("phase"),
                                "action": entry.get("action"),
                                "metrics": entry.get("metrics"),
                                "timestamp": entry.get("timestamp"),
                            })
                        last_trail_len = len(trail)
                        deadline = max(deadline, time.monotonic() + 1800)

                    reasoning = research.partial_reasoning
                    if reasoning and reasoning != last_reasoning:
                        last_reasoning = reasoning
                        yield sse("reasoning_delta", {"reasoning": reasoning, "phase": "analyze"})
                        deadline = max(deadline, time.monotonic() + 1800)

                    report = research.final_report or research.partial_report
                    if report and report != last_report:
                        last_report = report
                        yield sse("report", {"report": report, "final": bool(research.final_report)})
                        deadline = max(deadline, time.monotonic() + 1800)

                    if status in ("completed", "failed", "cancelled"):
                        yield sse("done", {"status": status})
                        return

                    if listener is not None:
                        try:
                            woke = await listener.get_message(15.0)
                        except Exception:
                            woke = None
                            await asyncio.sleep(1.0)
                        if woke is None:  # heartbeat — no change this interval
                            yield ": ping\n\n"
                    else:
                        await asyncio.sleep(1.0)

                if last_status in ("completed", "failed", "cancelled"):
                    yield sse("done", {"status": last_status})
                else:
                    # Stream timed out on transport, but research is still running in background.
                    # Send stream_error so client auto-reconnects, rather than marking research as dead.
                    yield sse("stream_error", {"detail": "stream_timeout"})

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.post("/v1/research/{research_id}/finalize", response_model=ResearchFinalizeResponse, dependencies=research_guard)
    def finalize_research(research_id: str, request: Request):
        research, job = get_research_service(request).enqueue_research_finalization(research_id)
        return ResearchFinalizeResponse(
            research=research,
            finalize_job_id=job.id if job else None,
        )


app = create_app()

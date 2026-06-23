import hmac
import json
import secrets
import uuid
import time
from typing import List
from urllib.parse import quote

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse

from src.domain.errors import ServiceError
from src.api.dependencies import (
    get_current_user,
    get_research_service,
    require_admin,
    scope_user_id,
    verify_research_access,
)
from src.auth.login_rate_limit import enforce_auth_rate_limit
from src.auth.security import create_token, decode_token
from src.auth.google_oauth import build_authorization_url, fetch_userinfo
from src.model_catalog import list_models as list_model_catalog
from src.api.schemas import (
    AuthUser,
    AuthSession,
    SetPasswordRequest,
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
from src.observability import bind_observability_context, observe_api_request, render_metrics


_CSRF_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_CSRF_EXEMPT_PATHS = frozenset({"/v1/auth/login", "/v1/auth/register"})


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
            str(request.url.path),
            response.status_code,
            time.perf_counter() - started_at,
        )
        response.headers["X-Request-ID"] = request_id
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
    token = create_token(user.id, email=user.email)
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
        return get_research_service(request).get_health_status()

    @app.post("/v1/auth/register", response_model=AuthSession, dependencies=auth_rate_limit)
    def register(payload: RegisterRequest, response: Response, request: Request):
        user = get_research_service(request).register_user(payload.email, payload.password)
        token = _issue_session(response, user)
        return AuthSession(access_token=token, user=user)

    @app.post("/v1/auth/login", response_model=AuthSession, dependencies=auth_rate_limit)
    def login(payload: LoginRequest, response: Response, request: Request):
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

    @app.get("/v1/auth/google/callback")
    def google_callback(request: Request, code: str = "", state: str = ""):
        if not settings.oauth_enabled:
            raise HTTPException(status_code=404, detail="Google OAuth is not configured")
        cookie_state = request.cookies.get("oauth_state")
        if not code or decode_token(state) is None or not cookie_state or cookie_state != state:
            raise HTTPException(status_code=400, detail="Invalid OAuth state")
        try:
            userinfo = fetch_userinfo(code)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Google sign-in failed: {exc}")
        email = userinfo.get("email")
        verified = userinfo.get("email_verified")
        if not email or verified not in (True, "true"):
            raise HTTPException(status_code=400, detail="Google account email is not verified")
        user, created = get_research_service(request).get_or_create_oauth_user(
            email, name=userinfo.get("name"), avatar_url=userinfo.get("picture")
        )
        # New users are offered a password to set; returning users go straight in.
        target = settings.oauth_new_user_redirect if created else settings.oauth_post_login_redirect
        redirect = RedirectResponse(target, status_code=302)
        redirect.delete_cookie("oauth_state", path="/")
        _issue_session(redirect, user)  # sets the JWT session cookie
        return redirect

    @app.post("/v1/auth/set-password")
    def set_password(
        payload: SetPasswordRequest, request: Request, user: AuthUser = Depends(get_current_user)
    ):
        get_research_service(request).set_user_password(user.id, payload.password)
        return {"status": "ok"}

    @app.get("/metrics")
    def metrics_endpoint():
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

    @app.post("/v1/optimize", response_model=OptimizeResponse, dependencies=auth_required)
    def optimize_prompt(request: Request, payload: OptimizeRequest):
        try:
            optimized = get_research_service(request).optimize_prompt(payload.prompt)
            return OptimizeResponse(optimized_prompt=optimized)
        except Exception as e:
            if isinstance(e, HTTPException):
                raise e
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/v1/decompose", response_model=DecomposeResponse, dependencies=auth_required)
    def decompose_prompt(request: Request, payload: DecomposeRequest):
        try:
            return get_research_service(request).decompose_prompt(
                payload.prompt,
                payload.depth,
            )
        except Exception as e:
            if isinstance(e, HTTPException):
                raise e
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/v1/tasks", response_model=List[SearchTask], dependencies=auth_required)
    def list_tasks(request: Request, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
        return get_research_service(request).list_tasks(limit=limit, offset=offset)

    @app.get("/v1/tasks/{task_id}", response_model=SearchTask, dependencies=auth_required)
    def get_task(task_id: str, request: Request):
        task = get_research_service(request).get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        return task

    @app.get("/v1/tasks/{task_id}/summary", response_model=SearchTaskSummary, dependencies=auth_required)
    def get_task_summary(task_id: str, request: Request):
        return get_research_service(request).get_task_summary(task_id)

    @app.patch("/v1/tasks/{task_id}", response_model=SearchTask, dependencies=auth_required)
    def update_task(task_id: str, update: TaskUpdate, request: Request):
        task = get_research_service(request).update_task(task_id, update)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        return task

    @app.get("/v1/tasks/{task_id}/search-job", response_model=SearchTaskJob, dependencies=auth_required)
    def get_latest_search_job(task_id: str, request: Request):
        job = get_research_service(request).get_latest_search_task_job(task_id)
        if not job:
            raise HTTPException(status_code=404, detail="Search job not found")
        return job

    @app.get("/v1/search-jobs/{job_id}", response_model=SearchTaskJob, dependencies=auth_required)
    def get_search_job(job_id: str, request: Request):
        job = get_research_service(request).get_search_task_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Search job not found")
        return job

    @app.get("/v1/search-jobs", response_model=List[SearchTaskJob], dependencies=auth_required)
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
        owner: str | None = Depends(scope_user_id),
    ):
        try:
            service = get_research_service(request)
            response, research_id = service.start_research(payload, user_id=owner)
            # LLM decompose runs after response is sent — user gets research_id instantly
            background_tasks.add_task(service.decompose_and_enqueue, research_id, payload)
            return response
        except Exception as e:
            if isinstance(e, HTTPException):
                raise e
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/v1/research/finalize-jobs", response_model=List[ResearchFinalizeJob], dependencies=auth_required)
    def list_finalize_jobs(status: str, request: Request):
        service = get_research_service(request)
        if status == "running":
            return service.list_running_research_finalize_jobs()
        if status == "dead_letter":
            return service.list_dead_letter_research_finalize_jobs()
        raise HTTPException(status_code=422, detail="Unsupported finalize job status filter")

    @app.get("/v1/research/finalize-jobs/{job_id}", response_model=ResearchFinalizeJob, dependencies=auth_required)
    def get_finalize_job(job_id: str, request: Request):
        job = get_research_service(request).get_research_finalize_job(job_id)
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

    @app.delete("/v1/research/{research_id}", status_code=204)
    def delete_research(research_id: str, request: Request, owner: str | None = Depends(scope_user_id)):
        deleted = get_research_service(request).delete_research(research_id, user_id=owner)
        if not deleted:
            raise HTTPException(status_code=404, detail="Research not found")

    @app.post("/v1/research/{research_id}/cancel", response_model=ResearchRecord)
    def cancel_research(research_id: str, request: Request, owner: str | None = Depends(scope_user_id)):
        return _public_record(get_research_service(request).cancel_research(research_id, user_id=owner))

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
    def ask_research(research_id: str, payload: ChatAsk, request: Request):
        # sync def -> runs in a threadpool so the blocking LLM call doesn't stall the event loop
        service = get_research_service(request)
        answer = service.generate_research_answer(research_id, payload.question)
        service.append_research_message(research_id, "user", payload.question)
        service.append_research_message(research_id, "assistant", answer)
        return ChatMessage(role="assistant", content=answer)

    @app.post("/v1/research/{research_id}/messages/stream", dependencies=research_guard)
    def ask_research_stream(research_id: str, payload: ChatAsk, request: Request):
        """Stream a grounded follow-up answer token-by-token via SSE, then persist the turn."""
        service = get_research_service(request)
        question = payload.question

        def sse(event: str, data: dict) -> str:
            return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

        def event_stream():
            import queue
            import threading

            channel: "queue.Queue" = queue.Queue()

            def on_delta(partial: str) -> None:
                channel.put(("delta", partial))

            def on_status(status: str) -> None:
                channel.put(("status", status))

            def worker() -> None:
                try:
                    answer = service.generate_research_answer(
                        research_id, question, streaming_callback=on_delta, status_callback=on_status
                    )
                    channel.put(("final", answer))
                except ServiceError as exc:
                    channel.put(("error", str(exc.detail)))
                except Exception as exc:  # pragma: no cover - defensive
                    channel.put(("error", str(exc)))

            threading.Thread(target=worker, daemon=True, name=f"chat-{research_id[:8]}").start()
            yield ": connected\n\n"
            while True:
                kind, value = channel.get()
                if kind == "delta":
                    yield sse("delta", {"answer": value})
                elif kind == "status":
                    yield sse("searching", {"status": value})
                elif kind == "final":
                    service.append_research_message(research_id, "user", question)
                    service.append_research_message(research_id, "assistant", value)
                    yield sse("done", {"answer": value})
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
    def research_events(research_id: str, request: Request):
        """Server-Sent Events stream: live status, graph trace and report deltas (F1)."""
        service = get_research_service(request)

        def sse(event: str, data: dict) -> str:
            return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

        def event_stream():
            last_status: str | None = None
            last_report: str | None = None
            last_reasoning: str | None = None
            last_trail_len = 0
            deadline = time.monotonic() + 900  # 10-minute safety cap
            # Wake on a Redis pub/sub change ping instead of polling Postgres every second;
            # a heartbeat timeout still re-reads + keeps the connection alive (and is the
            # fallback when no broker is configured).
            broker = getattr(service, "broker", None)
            try:
                listener = broker.research_listener(research_id) if broker is not None else None
            except Exception:
                listener = None
            yield ": connected\n\n"
            try:
                while time.monotonic() < deadline:
                    research = service.task_store.get_research(research_id)
                    if research is None:
                        yield sse("stream_error", {"detail": "Research not found"})
                        yield sse("done", {"status": "failed"})
                        return

                    graph_state = research.graph_state or {}
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
                            })
                        last_trail_len = len(trail)

                    reasoning = graph_state.get("partial_reasoning")
                    if reasoning and reasoning != last_reasoning:
                        last_reasoning = reasoning
                        yield sse("reasoning_delta", {"reasoning": reasoning, "phase": "analyze"})

                    report = research.final_report or graph_state.get("partial_report")
                    if report and report != last_report:
                        last_report = report
                        yield sse("report", {"report": report, "final": bool(research.final_report)})

                    if status in ("completed", "failed", "cancelled"):
                        yield sse("done", {"status": status})
                        return

                    if listener is not None:
                        try:
                            woke = listener.get_message(timeout=15.0)
                        except Exception:
                            woke = None
                            time.sleep(1.0)
                        if woke is None:  # heartbeat — no change this interval
                            yield ": ping\n\n"
                    else:
                        time.sleep(1.0)

                yield sse("done", {"status": "timeout"})
            finally:
                if listener is not None:
                    try:
                        listener.close()
                    except Exception:
                        pass

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

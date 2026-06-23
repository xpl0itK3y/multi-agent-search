from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    deepseek_api_key: Optional[str] = None
    deepseek_model: str = "deepseek-v4-pro"
    deepseek_repair_model: Optional[str] = None
    # Reasoning-capable model for planner/gap-analysis steps; must expose `reasoning_content`
    # so the "thinking" stream (frontend §4.1) can be shown. Falls back to deepseek_model if unset.
    deepseek_reasoner_model: Optional[str] = None
    # Adversarial "red-team" pass: after the report, search for counter-evidence to its
    # key claims and append a "weaknesses & counter-arguments" section (HARD only).
    red_team_enabled: bool = True
    red_team_model: str = "deepseek-chat"  # mechanical judging — use the fast/cheap model
    red_team_max_claims: int = 5
    # Max concurrent section-writer LLM calls in the multi-stage analyzer (model-agnostic
    # speed knob — higher writes more report sections at once; watch provider rate limits).
    analyzer_section_concurrency: int = 6
    # DB connection pool (per process). The SQLAlchemy default (5 + 10 overflow = 15) is
    # far too small once many SSE/status pollers hit the API — tune these under load and
    # keep (api_procs + workers) * (pool+overflow) under Postgres max_connections / PgBouncer.
    db_pool_size: int = 10
    db_max_overflow: int = 20
    db_pool_timeout: int = 30
    db_pool_recycle: int = 1800
    # Global admission control: max researches RUNNING at once across all users. Excess are
    # QUEUED (with a position) and promoted as slots free — graceful backpressure under load.
    max_global_active_researches: int = 20
    # LLM call rate-limiting: cap concurrent in-flight LLM calls across ALL processes
    # (global Redis semaphore; in-process fallback). 0 disables. Prevents 429 storms.
    llm_max_concurrent: int = 16
    llm_acquire_timeout_seconds: int = 120  # max wait for a slot before proceeding anyway
    llm_retry_max_attempts: int = 3
    llm_retry_base_delay: float = 1.0
    # Report quality: a final editorial LLM pass (answer-first structure, tighter prose, dedupe)
    # on MEDIUM/HARD reports. One extra call — disable to trade quality for latency/cost.
    report_editor_enabled: bool = True
    # Retraction check: look up cited DOIs against Crossref/Retraction Watch at finalize and flag
    # sources backed by a retracted paper. Network — degrades gracefully, never breaks finalize.
    retraction_check_enabled: bool = True
    retraction_check_timeout: float = 5.0
    # Cross-language: also search the question in other relevant languages and surface what
    # non-query-language sources uniquely add. Adds one decompose-time call + one extra task.
    cross_language_enabled: bool = True
    cross_language_max_targets: int = 2
    langsmith_tracing: bool = False
    langsmith_api_key: Optional[str] = None
    langsmith_endpoint: str = "https://api.smith.langchain.com"
    langsmith_project: Optional[str] = None
    log_format: str = "text"
    prometheus_metrics_enabled: bool = True

    app_name: str = "Prompt Optimizer API"
    debug: bool = False
    # Auth (on by default — set AUTH_DISABLED=true only for trusted single-tenant / dev).
    auth_disabled: bool = False
    auth_secret_key: str = "dev-insecure-secret-change-in-production"
    auth_token_ttl_seconds: int = 604800  # 7 days
    auth_cookie_name: str = "access_token"
    csrf_cookie_name: str = "csrf_token"
    auth_cookie_secure: bool = False  # set True when served over HTTPS
    # CSV of emails granted admin access to job/queue maintenance endpoints (auth-enabled mode).
    admin_emails: str = ""
    # Per-IP login/register attempts allowed per minute (auth-enabled mode; 0 disables).
    auth_rate_limit_per_minute: int = 10
    # Google OAuth (Sign in with Google). Create an OAuth 2.0 Web client in Google
    # Cloud Console; set the client id/secret and the EXACT redirect URI you registered.
    google_client_id: str = ""
    google_client_secret: str = ""
    google_redirect_uri: str = "http://localhost:8501/v1/auth/google/callback"
    oauth_post_login_redirect: str = "/"
    # New OAuth users land here to optionally set a password for email/password login.
    oauth_new_user_redirect: str = "/set-password"

    @property
    def oauth_enabled(self) -> bool:
        return bool(self.google_client_id and self.google_client_secret)
    # Comma-separated list of allowed CORS origins for the Vue SPA (dev: Vite 5173 / preview 4173).
    cors_allow_origins: str = "http://localhost:5173,http://localhost:4173"
    # SSRF guard: when False, user webhooks must resolve to public IPs only.
    # Set True only for trusted internal deployments that intentionally call private hosts.
    webhook_allow_private_targets: bool = False
    task_store_backend: str = "postgres"
    allow_memory_task_store: bool = False
    smoke_analyzer_report: Optional[str] = None
    job_max_attempts: int = 3
    worker_heartbeat_ttl_seconds: int = 30
    search_job_timeout_seconds: int = 300
    finalize_job_timeout_seconds: int = 600
    search_job_retention_seconds: int = 86400
    finalize_job_retention_seconds: int = 86400
    search_extraction_concurrency: int = 4
    search_extraction_timeout_seconds: int = 12
    search_extraction_max_redirects: int = 1
    search_domain_fail_threshold: int = 2
    search_domain_cooldown_seconds: int = 600
    # Search backend (P2): Tavily as the primary backend with DuckDuckGo as fallback.
    # `search_backend`: "auto" (Tavily when a key is set, else DDG) | "tavily" | "duckduckgo".
    # Tavily returns relevant results plus raw page content in one call, which removes the
    # fragile per-URL fetch/extract step and the DuckDuckGo rate-limit ceiling.
    tavily_api_key: Optional[str] = None
    search_backend: str = "auto"
    tavily_search_depth: str = "advanced"  # "basic" | "advanced"
    tavily_timeout_seconds: float = 20.0
    tavily_include_raw_content: bool = True
    # Shared TTL cache for search results (P2 / 3.2) — a repeated query within the
    # TTL reuses stored results instead of re-hitting the (paid) search API.
    search_cache_enabled: bool = True
    search_cache_ttl_seconds: int = 86400
    # Cap concurrently in-flight (processing/analyzing) researches per user to avoid
    # overloading search/LLM. 0 disables the guard. A busy research only counts while
    # it's making progress (updated within research_stale_active_seconds) — a stalled
    # one (dead worker, hung provider) stops blocking instead of locking the user out.
    max_concurrent_researches: int = 1
    research_stale_active_seconds: int = 300
    analyzer_max_sources: int = 24
    analyzer_max_sources_per_domain: int = 3
    analyzer_max_sources_per_task: int = 6
    analyzer_payload_char_budget: int = 28000
    analyzer_conflict_source_limit: int = 12
    analyzer_evidence_source_limit: int = 12
    analyzer_local_repair_issue_threshold: int = 20
    use_langgraph_finalize_graph: bool = True
    # Deep-research loop is on by default but bounded by the finalize budget below.
    # Set any of these to 0 to disable that branch (faster, shallower finalize).
    langgraph_replan_max_loops: int = 1
    langgraph_verification_max_retries: int = 1
    langgraph_tie_break_max_loops: int = 1
    # Budget guard so the revived loop can't run away on cost/latency. Sized for the
    # deep HARD tier (120 sources → ~6 writer sections + synthesis per analyze pass);
    # EASY/MEDIUM finalize well under this, so the larger ceiling only benefits HARD.
    finalize_budget_max_seconds: int = 480
    finalize_budget_max_analyze_passes: int = 3
    graph_step_event_history_limit: int = 250
    graph_step_event_retention_seconds: int = 86400
    graph_trail_history_limit: int = 200
    graph_trail_retention_seconds: int = 604800

    postgres_user: str = "app"
    postgres_password: str = "app"
    postgres_db: str = "multi_agent_search"
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    database_url: Optional[str] = None

    redis_url: Optional[str] = None
    use_redis_broker: bool = False
    redis_broker_pop_timeout_seconds: int = 2
    decompose_recovery_minutes: int = 10

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @property
    def resolved_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()

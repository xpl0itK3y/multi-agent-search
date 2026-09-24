from datetime import datetime
from typing import Protocol

from src.domain import (
    AdminAuditLogItem,
    AdminDryRunResult,
    AdminEventLogResponse,
    AdminOverviewResponse,
    AdminPromptsResponse,
    AdminTelemetrySummaryResponse,
    AdminTokenAnalyticsResponse,
    AdminTokenResearchUsageItem,
    AdminUserDetailResponse,
    AdminUserListResponse,
    FinalizeJobStatus,
    QueueMetrics,
    ResearchFinalizeJob,
    ResearchHistoryItem,
    SearchJobStatus,
    SearchTaskJob,
    WorkerHeartbeat,
    ResearchRecord,
    ResearchRequest,
    ResearchStatus,
    SearchTask,
    TaskUpdate,
    UserRecord,
)


class TaskStore(Protocol):
    def add_research(
        self,
        request: ResearchRequest,
        task_ids: list[str],
        user_id: str | None = None,
        language: str = "unknown",
    ) -> ResearchRecord: ...

    def add_research_if_under_limit(
        self,
        request: ResearchRequest,
        task_ids: list[str],
        user_id: str | None,
        graph_state: dict,
        per_user_limit: int,
        global_limit: int,
        stale_before: datetime,
        language: str = "unknown",
    ) -> ResearchRecord | None: ...

    def get_research(self, research_id: str) -> ResearchRecord | None: ...

    def list_researches(self, limit: int = 20, user_id: str | None = None) -> list[ResearchHistoryItem]: ...

    def list_thread_researches(self, thread_id: str, user_id: str | None = None) -> list[ResearchHistoryItem]: ...

    def get_research_by_share_token(self, token: str) -> ResearchRecord | None: ...

    def delete_research(self, research_id: str) -> bool: ...

    # ── users (auth) ──────────────────────────────────────────────────────────
    def create_user(
        self,
        user_id: str,
        email: str,
        password_hash: str | None,
        google_subject: str | None = None,
    ) -> UserRecord: ...

    def get_user_by_email(self, email: str) -> UserRecord | None: ...

    def get_user_by_id(self, user_id: str) -> UserRecord | None: ...

    def get_user_by_google_subject(self, google_subject: str) -> UserRecord | None: ...

    # Account deletion (DATA-LIFECYCLE): removes the user; researches cascade via FK.
    def delete_user(self, user_id: str) -> bool: ...

    def update_user_password(self, user_id: str, password_hash: str) -> UserRecord | None: ...

    def update_user_profile(self, user_id: str, name: str | None, avatar_url: str | None) -> UserRecord | None: ...

    def get_user_token_analytics(self, user_id: str) -> dict: ...

    def update_research_status(
        self,
        research_id: str,
        status: ResearchStatus,
        report: str | None = None,
    ) -> ResearchRecord | None: ...

    # Status-guarded reset for a retry, under the row lock: clears the reports and drops
    # `remove_graph_state_keys`, only while the research is in `expected_status`.
    def reset_research_for_retry(
        self,
        research_id: str,
        expected_status: ResearchStatus,
        remove_graph_state_keys: list[str],
    ) -> ResearchRecord | None: ...

    def try_admit_research(
        self,
        research_id: str,
        expected_status: ResearchStatus,
        per_user_limit: int,
        global_limit: int,
        stale_before: datetime,
    ) -> bool: ...

    def try_claim_queued_research(
        self,
        research_id: str,
        global_limit: int = 0,
        stale_before: datetime | None = None,
    ) -> bool: ...

    def try_begin_finalization(self, research_id: str) -> bool: ...

    def set_research_task_ids(
        self,
        research_id: str,
        task_ids: list[str],
    ) -> ResearchRecord | None: ...

    def merge_research_graph_state(
        self,
        research_id: str,
        patch: dict | None = None,
        *,
        remove_keys: list[str] | None = None,
    ) -> ResearchRecord | None: ...

    # Append `item` to the graph_state list under `key` (keeping the last `max_items`)
    # under the same row lock as merges, so concurrent appends cannot drop each other.
    # Returns the new list, or None when the research does not exist.
    def append_research_graph_state_item(
        self,
        research_id: str,
        key: str,
        item: dict,
        *,
        max_items: int | None = None,
    ) -> list[dict] | None: ...

    def save_partial_report(self, research_id: str, partial: str) -> None: ...

    def save_partial_reasoning(self, research_id: str, partial: str) -> None: ...

    # Atomic under the research row lock; returns the new trail, not the whole record.
    def append_research_graph_event(
        self,
        research_id: str,
        event: dict,
    ) -> list[dict] | None: ...

    def compact_research_graph_trails(self) -> list[str]: ...

    def add_research_finalize_job(
        self,
        research_id: str,
        max_attempts: int = 3,
    ) -> ResearchFinalizeJob: ...

    def get_research_finalize_job(
        self,
        job_id: str,
        user_id: str | None = None,
    ) -> ResearchFinalizeJob | None: ...

    def get_latest_research_finalize_job(
        self,
        research_id: str,
        user_id: str | None = None,
    ) -> ResearchFinalizeJob | None: ...

    def get_pending_research_finalize_jobs(self) -> list[ResearchFinalizeJob]: ...

    def get_running_research_finalize_jobs(self) -> list[ResearchFinalizeJob]: ...

    def get_dead_letter_research_finalize_jobs(self) -> list[ResearchFinalizeJob]: ...

    def claim_next_research_finalize_job(self) -> ResearchFinalizeJob | None: ...

    def claim_research_finalize_job_by_id(self, job_id: str) -> ResearchFinalizeJob | None: ...

    def renew_research_finalize_job_lease(self, job_id: str, lease_epoch: int) -> bool: ...

    def complete_research_finalize_job(
        self,
        job_id: str,
        research_id: str,
        lease_epoch: int,
        report: str,
    ) -> ResearchFinalizeJob | None: ...

    def update_research_finalize_job(
        self,
        job_id: str,
        status: FinalizeJobStatus,
        error: str | None = None,
        lease_epoch: int | None = None,
    ) -> ResearchFinalizeJob | None: ...

    def record_research_finalize_job_failure(
        self,
        job_id: str,
        error: str,
        lease_epoch: int | None = None,
    ) -> ResearchFinalizeJob | None: ...

    # Requeue only a DEAD_LETTER/FAILED job (None otherwise), resetting its attempts;
    # the finalize variant also bumps lease_epoch to fence a runner on the old lease.
    def requeue_research_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None: ...

    def recover_stale_research_finalize_jobs(
        self,
        stale_before: datetime,
    ) -> list[ResearchFinalizeJob]: ...

    def cleanup_old_research_finalize_jobs(
        self,
        older_than: datetime,
    ) -> list[str]: ...

    def add_search_task_job(
        self,
        task_id: str,
        depth: str,
        max_attempts: int = 3,
    ) -> SearchTaskJob: ...

    def get_search_task_job(
        self,
        job_id: str,
        user_id: str | None = None,
    ) -> SearchTaskJob | None: ...

    def get_latest_search_task_job(
        self,
        task_id: str,
        user_id: str | None = None,
    ) -> SearchTaskJob | None: ...

    def get_pending_search_task_jobs(self) -> list[SearchTaskJob]: ...

    def get_running_search_task_jobs(self) -> list[SearchTaskJob]: ...

    def get_dead_letter_search_task_jobs(self) -> list[SearchTaskJob]: ...

    def claim_next_search_task_job(self) -> SearchTaskJob | None: ...

    def claim_search_task_job_by_id(self, job_id: str) -> SearchTaskJob | None: ...

    def update_search_task_job(
        self,
        job_id: str,
        status: SearchJobStatus,
        error: str | None = None,
    ) -> SearchTaskJob | None: ...

    def record_search_task_job_failure(
        self,
        job_id: str,
        error: str,
    ) -> SearchTaskJob | None: ...

    def requeue_search_task_job(self, job_id: str) -> SearchTaskJob | None: ...

    def recover_stale_search_task_jobs(
        self,
        stale_before: datetime,
    ) -> list[SearchTaskJob]: ...

    def cleanup_old_search_task_jobs(
        self,
        older_than: datetime,
    ) -> list[str]: ...

    def upsert_worker_heartbeat(
        self,
        worker_name: str,
        processed_jobs: int,
        status: str,
        last_error: str | None = None,
        extraction_metrics: dict | None = None,
        graph_metrics: dict | None = None,
        graph_step_events: list[dict] | None = None,
        maintenance_summary: dict | None = None,
    ) -> WorkerHeartbeat: ...

    def get_worker_heartbeat(self, worker_name: str) -> WorkerHeartbeat | None: ...

    def get_graph_step_events(self, worker_name: str | None = None) -> list[dict]: ...

    def compact_worker_graph_step_events(self) -> list[str]: ...

    def get_queue_metrics(self) -> QueueMetrics: ...

    def add_task(self, task_data: dict) -> SearchTask: ...

    def get_task(self, task_id: str, user_id: str | None = None) -> SearchTask | None: ...

    def get_all_tasks(
        self,
        limit: int = 100,
        offset: int = 0,
        user_id: str | None = None,
    ) -> list[SearchTask]: ...

    def get_tasks_by_research(self, research_id: str) -> list[SearchTask]: ...

    def update_task(
        self,
        task_id: str,
        update: TaskUpdate,
        user_id: str | None = None,
    ) -> SearchTask | None: ...

    # search result cache (P2) — shared TTL cache so repeated queries skip the API
    def ping(self) -> bool: ...

    def get_cached_search(self, cache_key: str, max_age_seconds: int) -> list[dict] | None: ...

    def put_cached_search(self, cache_key: str, payload: list[dict]) -> None: ...

    def cleanup_search_cache(self, older_than: datetime) -> int: ...

    # Retention: cascade-delete terminal researches past the retention window (OPS-RETENTION).
    def cleanup_old_researches(self, older_than: datetime) -> list[str]: ...

    # Telemetry retention: delete rows older than the cutoff (events and audit rows by
    # created_at, sessions by last_active_at) in short batches; returns how many went.
    def cleanup_old_user_events(self, older_than: datetime) -> int: ...

    def cleanup_old_user_sessions(self, older_than: datetime) -> int: ...

    def cleanup_old_admin_audit_logs(self, older_than: datetime) -> int: ...

    # ── admin & token tracking ────────────────────────────────────────────────
    # One row per LLM call (the provider's usage sink): the model actually sent and the
    # unrounded cost; cache_hit_tokens is the part of prompt_tokens served from cache.
    def record_llm_usage(
        self,
        research_id: str | None,
        user_id: str | None,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
        estimated_cost_usd: float,
        cache_hit_tokens: int = 0,
    ) -> str: ...

    def record_admin_audit(
        self,
        actor_email: str,
        action: str,
        target_type: str,
        target_id: str | None = None,
        details: dict | None = None,
        ip_address: str | None = None,
    ) -> str: ...

    def get_admin_audit_logs(
        self,
        limit: int = 50,
        offset: int = 0,
        action: str | None = None,
        actor_email: str | None = None,
    ) -> list[AdminAuditLogItem]: ...

    def get_admin_token_analytics(
        self,
        page: int = 1,
        page_size: int = 20,
    ) -> AdminTokenAnalyticsResponse: ...

    # Just the per-research page of get_admin_token_analytics (newest first), without the
    # totals and breakdowns: what the streamed CSV export pages through.
    def get_admin_token_research_usage(
        self,
        page: int = 1,
        page_size: int = 20,
    ) -> list[AdminTokenResearchUsageItem]: ...

    def get_admin_overview(self) -> AdminOverviewResponse: ...

    # Dry-run counting only: executing an action goes through the service's job paths
    # (broker re-dispatch, dead-letter guard, status resets), then record_admin_audit.
    def preview_maintenance_action(
        self,
        action: str,
        params: dict | None = None,
    ) -> AdminDryRunResult: ...

    # ── user telemetry & activity tracking ───────────────────────────────────
    # Upsert keyed on (session_id, user_id): a repeat bumps last_active_at (and IP/UA);
    # another user's row with the same client-chosen session_id is never touched.
    def record_user_session(
        self,
        user_id: str,
        session_id: str,
        ip_address: str | None = None,
        user_agent: str | None = None,
        device_type: str = "desktop",
        browser: str | None = None,
        os: str | None = None,
        screen_res: str | None = None,
        viewport: str | None = None,
        language: str | None = None,
        client_timezone: str | None = None,
        country: str | None = None,
        city: str | None = None,
    ) -> str: ...

    def record_user_event(
        self,
        event_name: str,
        event_category: str = "general",
        user_id: str | None = None,
        session_id: str | None = None,
        details: dict | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
    ) -> str: ...

    # users.last_seen_at/last_ip/...; skipped when the user was seen less than
    # USER_ACTIVITY_TOUCH_INTERVAL_SECONDS ago. Events and sessions do not call it.
    def touch_user_activity(
        self,
        user_id: str,
        ip_address: str | None = None,
        user_agent: str | None = None,
        device: str | None = None,
    ) -> None: ...

    def get_admin_users_list(
        self,
        page: int = 1,
        page_size: int = 20,
        search: str | None = None,
        role: str | None = None,
        online_only: bool = False,
        sort_by: str = "last_seen",
    ) -> AdminUserListResponse: ...

    def get_admin_user_detail(self, user_id: str) -> AdminUserDetailResponse | None: ...

    def get_admin_telemetry_summary(self) -> AdminTelemetrySummaryResponse: ...

    def get_admin_event_logs(
        self,
        limit: int = 50,
        offset: int = 0,
        category: str | None = None,
        event_name: str | None = None,
        user_id: str | None = None,
    ) -> AdminEventLogResponse: ...

    def get_admin_prompts(
        self,
        page: int = 1,
        page_size: int = 25,
        search: str | None = None,
        user_id: str | None = None,
        prompt_type: str | None = None,
    ) -> AdminPromptsResponse: ...



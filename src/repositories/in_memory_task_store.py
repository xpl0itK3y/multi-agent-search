from datetime import datetime, timedelta, timezone
import threading
import uuid

from src.core.graph_history import compact_graph_step_events, compact_graph_trail
from src.domain import (
    AdminAuditLogItem,
    AdminDryRunResult,
    AdminEventLogItem,
    AdminEventLogResponse,
    AdminOverviewResponse,
    AdminPromptItem,
    AdminPromptsResponse,
    AdminTelemetrySummaryResponse,
    AdminTokenAnalyticsResponse,
    AdminTokenDepthBreakdown,
    AdminTokenModelBreakdown,
    AdminTokenResearchUsageItem,
    AdminUserDetailResponse,
    AdminUserListItem,
    AdminUserListResponse,
    AdminWorkerFleetItem,
    ExtractionMetrics,
    FinalizeJobStatus,
    GraphMetrics,
    PROMPT_EVENT_NAMES,
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
    TELEMETRY_IP_MAX_LENGTH,
    TELEMETRY_USER_AGENT_MAX_LENGTH,
    TaskUpdate,
    USER_ACTIVITY_TOUCH_INTERVAL_SECONDS,
    UserRecord,
    clip_text,
)


def _admin_emails() -> set[str]:
    """ADMIN_EMAILS, lower-cased: the same parsing as the SQL store's."""
    from src.config import settings

    raw = getattr(settings, "admin_emails", "")
    if not raw:
        return set()
    if isinstance(raw, str):
        return {e.strip().lower() for e in raw.split(",") if e.strip()}
    return {str(e).strip().lower() for e in raw if str(e).strip()}


class InMemoryTaskStore:
    def __init__(self):
        self.tasks: dict[str, SearchTask] = {}
        self.researches: dict[str, ResearchRecord] = {}
        self.finalize_jobs: dict[str, ResearchFinalizeJob] = {}
        self.search_jobs: dict[str, SearchTaskJob] = {}
        self.worker_heartbeats: dict[str, WorkerHeartbeat] = {}
        self.worker_graph_step_events: dict[str, list[dict]] = {}
        self.users: dict[str, UserRecord] = {}
        # users.created_at: UserRecord does not carry it, the admin views sort by it.
        self._user_created_at: dict[str, datetime] = {}
        self.search_cache: dict[str, tuple[datetime, list[dict]]] = {}
        self.llm_usage_logs: list[dict] = []
        self.admin_audit_logs: list[AdminAuditLogItem] = []
        self.user_sessions: list[dict] = []
        self.user_events: list[dict] = []
        self.user_telemetry: dict[str, dict] = {}
        self._admission_lock = threading.RLock()
        # Serializes graph_state merges, mirroring the SQL store's FOR UPDATE row lock.
        self._state_lock = threading.RLock()

    def ping(self) -> bool:
        return True

    def _research_belongs_to(self, research_id: str | None, user_id: str | None) -> bool:
        if user_id is None:
            return True
        research = self.researches.get(research_id or "")
        return research is not None and research.user_id == user_id

    def get_cached_search(self, cache_key: str, max_age_seconds: int) -> list[dict] | None:
        entry = self.search_cache.get(cache_key)
        if entry is None:
            return None
        created_at, payload = entry
        # An entry exactly max_age old is already expired, so max_age_seconds=0 always
        # misses even when the clock has not ticked since the write.
        if (datetime.now(timezone.utc) - created_at).total_seconds() >= max_age_seconds:
            return None
        return [dict(item) for item in payload]

    def put_cached_search(self, cache_key: str, payload: list[dict]) -> None:
        self.search_cache[cache_key] = (datetime.now(timezone.utc), [dict(item) for item in payload])

    def cleanup_search_cache(self, older_than: datetime) -> int:
        stale = [key for key, (created, _) in self.search_cache.items() if created < older_than]
        for key in stale:
            del self.search_cache[key]
        return len(stale)

    def add_research(
        self,
        request: ResearchRequest,
        task_ids: list[str],
        user_id: str | None = None,
        language: str = "unknown",
    ) -> ResearchRecord:
        research_id = str(uuid.uuid4())
        record = ResearchRecord(
            id=research_id,
            prompt=request.prompt,
            language=language,
            user_id=user_id,
            depth=request.depth,
            task_ids=task_ids,
        )
        self.researches[research_id] = record
        return record

    @staticmethod
    def _is_fresh_running(research: ResearchRecord, stale_before: datetime) -> bool:
        updated_at = research.updated_at
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        return (
            research.status in (ResearchStatus.PROCESSING, ResearchStatus.ANALYZING)
            and updated_at >= stale_before
        )

    # Parked plan-first researches (waiting on the user) hold the user's slot while
    # fresh, so a user cannot park N uncounted plans and then activate them all at once.
    _PARKED_STATUSES = (ResearchStatus.CLARIFYING, ResearchStatus.PLAN_REVIEW)

    @classmethod
    def _is_fresh_parked(cls, research: ResearchRecord, stale_before: datetime) -> bool:
        updated_at = research.updated_at
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        return research.status in cls._PARKED_STATUSES and updated_at >= stale_before

    def _active_count(
        self,
        user_id: str | None,
        stale_before: datetime,
        *,
        include_queued: bool,
        exclude_id: str | None = None,
        include_parked: bool = True,
    ) -> int:
        records = self.researches.values()
        if user_id is not None:
            records = (record for record in records if record.user_id == user_id)
        if exclude_id is not None:
            records = (record for record in records if record.id != exclude_id)
        return sum(
            1
            for record in records
            if self._is_fresh_running(record, stale_before)
            or (
                include_queued
                and (
                    record.status == ResearchStatus.QUEUED
                    or (include_parked and self._is_fresh_parked(record, stale_before))
                )
            )
        )

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
    ) -> ResearchRecord | None:
        """Atomically reserve user capacity and either a running slot or a queue place."""
        with self._admission_lock:
            if (
                per_user_limit > 0
                and self._active_count(user_id, stale_before, include_queued=True) >= per_user_limit
            ):
                return None
            queued = (
                global_limit > 0
                and self._active_count(None, stale_before, include_queued=False) >= global_limit
            )
            record = ResearchRecord(
                id=str(uuid.uuid4()),
                prompt=request.prompt,
                language=language,
                user_id=user_id,
                depth=request.depth,
                status=ResearchStatus.QUEUED if queued else ResearchStatus.PROCESSING,
                task_ids=task_ids,
                graph_state=graph_state,
            )
            self.researches[record.id] = record
            return record

    def try_admit_research(
        self,
        research_id: str,
        expected_status: ResearchStatus,
        per_user_limit: int,
        global_limit: int,
        stale_before: datetime,
    ) -> bool:
        """Atomically move a parked research back into a capacity-consuming state."""
        with self._admission_lock:
            research = self.researches.get(research_id)
            if research is None or research.status != expected_status:
                return False
            if (
                per_user_limit > 0
                and self._active_count(
                    research.user_id,
                    stale_before,
                    include_queued=True,
                    exclude_id=research_id,
                    # Activation needs running capacity only: parked siblings wait on
                    # the user and consume nothing, so they must not block each other.
                    include_parked=False,
                )
                >= per_user_limit
            ):
                return False
            if (
                global_limit > 0
                and self._active_count(None, stale_before, include_queued=False) >= global_limit
            ):
                return False
            research.status = ResearchStatus.PROCESSING
            research.updated_at = datetime.now(timezone.utc)
            self._emit_change(research_id)
            return True

    def create_user(
        self,
        user_id: str,
        email: str,
        password_hash: str | None,
        google_subject: str | None = None,
    ) -> UserRecord:
        user = UserRecord(
            id=user_id,
            email=email,
            password_hash=password_hash,
            google_subject=google_subject,
        )
        self.users[user_id] = user
        self._user_created_at[user_id] = datetime.now(timezone.utc)
        return user

    def get_user_by_email(self, email: str) -> UserRecord | None:
        target = email.strip().lower()
        return next((u for u in self.users.values() if u.email.lower() == target), None)

    def get_user_by_id(self, user_id: str) -> UserRecord | None:
        return self.users.get(user_id)

    def get_user_by_google_subject(self, google_subject: str) -> UserRecord | None:
        return next(
            (user for user in self.users.values() if user.google_subject == google_subject),
            None,
        )

    def delete_user(self, user_id: str) -> bool:
        if user_id not in self.users:
            return False
        del self.users[user_id]
        self._user_created_at.pop(user_id, None)
        # Mirror the SQL FK cascade: the user's researches (and their tasks) go too,
        # which also revokes every public share token they had minted.
        for research in [
            record for record in self.researches.values() if record.user_id == user_id
        ]:
            self.delete_research(research.id)
        self.user_sessions = [s for s in self.user_sessions if s.get("user_id") != user_id]
        self.user_events = [e for e in self.user_events if e.get("user_id") != user_id]
        return True

    def update_user_password(self, user_id: str, password_hash: str) -> UserRecord | None:
        user = self.users.get(user_id)
        if user is None:
            return None
        updated = user.model_copy(
            update={
                "password_hash": password_hash,
                "token_version": user.token_version + 1,
            }
        )
        self.users[user_id] = updated
        return updated

    def update_user_profile(self, user_id: str, name: str | None, avatar_url: str | None) -> UserRecord | None:
        user = self.users.get(user_id)
        if user:
            patch = {}
            if name is not None:
                patch["name"] = name
            if avatar_url is not None:
                patch["avatar_url"] = avatar_url
            if patch:
                updated = user.model_copy(update=patch)
                self.users[user_id] = updated
                return updated
            return user
        return None

    def get_research(self, research_id: str) -> ResearchRecord | None:
        return self.researches.get(research_id)

    def delete_research(self, research_id: str) -> bool:
        if research_id not in self.researches:
            return False
        del self.researches[research_id]
        task_ids = [tid for tid, t in self.tasks.items() if t.research_id == research_id]
        for tid in task_ids:
            del self.tasks[tid]
        # Like SQL: the prompt copies in user_events go with their research.
        self.user_events = [
            e for e in self.user_events
            if not (
                e.get("event_name") in PROMPT_EVENT_NAMES
                and (e.get("details") or {}).get("research_id") == research_id
            )
        ]
        return True

    _TERMINAL_RESEARCH_STATUSES = (
        ResearchStatus.COMPLETED,
        ResearchStatus.FAILED,
        ResearchStatus.CANCELLED,
    )

    def cleanup_old_researches(self, older_than: datetime) -> list[str]:
        deleted_ids = [
            research.id
            for research in self.researches.values()
            if research.status in self._TERMINAL_RESEARCH_STATUSES
            and (research.updated_at or research.created_at) < older_than
        ]
        for research_id in deleted_ids:
            self.delete_research(research_id)
        return deleted_ids

    def cleanup_old_user_events(self, older_than: datetime) -> int:
        kept = [e for e in self.user_events if e["created_at"] >= older_than]
        deleted, self.user_events = len(self.user_events) - len(kept), kept
        return deleted

    def cleanup_old_user_sessions(self, older_than: datetime) -> int:
        kept = [s for s in self.user_sessions if s["last_active_at"] >= older_than]
        deleted, self.user_sessions = len(self.user_sessions) - len(kept), kept
        return deleted

    def cleanup_old_admin_audit_logs(self, older_than: datetime) -> int:
        kept = [entry for entry in self.admin_audit_logs if entry.created_at >= older_than]
        deleted, self.admin_audit_logs = len(self.admin_audit_logs) - len(kept), kept
        return deleted

    def list_researches(self, limit: int = 20, user_id: str | None = None) -> list[ResearchHistoryItem]:
        records = self.researches.values()
        if user_id is not None:
            records = [r for r in records if r.user_id == user_id]
        sorted_records = sorted(
            records,
            key=lambda r: r.created_at,
            reverse=True,
        )
        return [
            ResearchHistoryItem(
                id=r.id,
                prompt=r.prompt,
                title=(r.graph_state or {}).get("title"),
                thread_id=(r.graph_state or {}).get("thread_id"),
                depth=r.depth,
                status=r.status,
                created_at=r.created_at,
                updated_at=r.updated_at,
                has_final_report=bool(r.final_report),
            )
            for r in sorted_records[:max(1, min(limit, 100))]
        ]

    def get_research_by_share_token(self, token: str) -> ResearchRecord | None:
        if not token:
            return None
        for r in self.researches.values():
            if (r.graph_state or {}).get("share_token") == token:
                return r
        return None

    def list_thread_researches(
        self, thread_id: str, user_id: str | None = None
    ) -> list[ResearchHistoryItem]:
        records = [
            r for r in self.researches.values()
            if (r.graph_state or {}).get("thread_id") == thread_id or r.id == thread_id
        ]
        if user_id is not None:
            records = [r for r in records if r.user_id == user_id]
        records.sort(key=lambda r: r.created_at)  # chronological within the thread
        return [
            ResearchHistoryItem(
                id=r.id,
                prompt=r.prompt,
                title=(r.graph_state or {}).get("title"),
                thread_id=(r.graph_state or {}).get("thread_id"),
                depth=r.depth,
                status=r.status,
                created_at=r.created_at,
                updated_at=r.updated_at,
                has_final_report=bool(r.final_report),
            )
            for r in records[:200]
        ]

    def set_event_notifier(self, notifier) -> None:
        """Optional callback(research_id) fired on live state changes (SSE pub/sub)."""
        self._event_notifier = notifier

    def _emit_change(self, research_id: str) -> None:
        notifier = getattr(self, "_event_notifier", None)
        if notifier:
            try:
                notifier(research_id)
            except Exception:  # best-effort — never break a write on a notify failure
                pass

    def update_research_status(
        self,
        research_id: str,
        status: ResearchStatus,
        report: str | None = None,
    ) -> ResearchRecord | None:
        research = self.researches.get(research_id)
        if research:
            research.status = status
            if report is not None:
                research.final_report = report
                research.partial_report = None
                research.partial_reasoning = None
            research.updated_at = datetime.now(timezone.utc)
            self._emit_change(research_id)
        return research

    def reset_research_for_retry(
        self,
        research_id: str,
        expected_status: ResearchStatus,
        remove_graph_state_keys: list[str],
    ) -> ResearchRecord | None:
        with self._state_lock:
            research = self.researches.get(research_id)
            if research is None or research.status != expected_status:
                return None
            research.final_report = None
            research.partial_report = None
            research.partial_reasoning = None
            research.graph_state = {
                key: value
                for key, value in (research.graph_state or {}).items()
                if key not in remove_graph_state_keys
            }
            research.updated_at = datetime.now(timezone.utc)
        self._emit_change(research_id)
        return research

    def try_claim_queued_research(
        self,
        research_id: str,
        global_limit: int = 0,
        stale_before: datetime | None = None,
    ) -> bool:
        """Atomically claim QUEUED only while a global running slot is available."""
        with self._admission_lock:
            research = self.researches.get(research_id)
            if research is None or research.status != ResearchStatus.QUEUED:
                return False
            cutoff = stale_before or datetime.min.replace(tzinfo=timezone.utc)
            if (
                global_limit > 0
                and self._active_count(None, cutoff, include_queued=False) >= global_limit
            ):
                return False
            research.status = ResearchStatus.PROCESSING
            research.updated_at = datetime.now(timezone.utc)
            self._emit_change(research_id)
            return True

    def try_begin_finalization(self, research_id: str) -> bool:
        """Atomically flip into ANALYZING unless already terminal/finalizing. True if this
        caller won the transition (single-winner finalize enqueue)."""
        research = self.researches.get(research_id)
        if research is None or research.status in (
            ResearchStatus.ANALYZING,
            ResearchStatus.COMPLETED,
            ResearchStatus.FAILED,
            ResearchStatus.CANCELLED,
        ):
            return False
        research.status = ResearchStatus.ANALYZING
        research.updated_at = datetime.now(timezone.utc)
        self._emit_change(research_id)
        return True

    def add_task(self, task_data: dict) -> SearchTask:
        task = SearchTask(**task_data)
        self.tasks[task.id] = task
        return task

    def set_research_task_ids(
        self,
        research_id: str,
        task_ids: list[str],
    ) -> ResearchRecord | None:
        research = self.researches.get(research_id)
        if research is None:
            return None

        research.task_ids = task_ids
        research.updated_at = datetime.now(timezone.utc)
        return research

    def merge_research_graph_state(
        self,
        research_id: str,
        patch: dict | None = None,
        *,
        remove_keys: list[str] | None = None,
    ) -> ResearchRecord | None:
        with self._state_lock:
            research = self.researches.get(research_id)
            if research is None:
                return None
            patch = patch or {}
            remove_keys = remove_keys or []
            if patch or remove_keys:
                merged = {**(research.graph_state or {}), **patch}
                for key in remove_keys:
                    merged.pop(key, None)
                research.graph_state = merged
                research.updated_at = datetime.now(timezone.utc)
            return research

    def append_research_graph_state_item(
        self,
        research_id: str,
        key: str,
        item: dict,
        *,
        max_items: int | None = None,
    ) -> list[dict] | None:
        with self._state_lock:
            research = self.researches.get(research_id)
            if research is None:
                return None
            items = [*((research.graph_state or {}).get(key) or []), item]
            if max_items is not None:
                items = items[-max_items:]
            research.graph_state = {**(research.graph_state or {}), key: items}
            research.updated_at = datetime.now(timezone.utc)
            return items

    def save_partial_report(self, research_id: str, partial: str) -> None:
        research = self.researches.get(research_id)
        if research is None:
            return
        research.partial_report = partial
        self._emit_change(research_id)

    def save_partial_reasoning(self, research_id: str, partial: str) -> None:
        research = self.researches.get(research_id)
        if research is None:
            return
        research.partial_reasoning = partial
        self._emit_change(research_id)

    def append_research_graph_event(
        self,
        research_id: str,
        event: dict,
    ) -> list[dict] | None:
        # Same lock as graph_state merges, mirroring the SQL store's row lock: concurrent
        # appenders must not overwrite each other's events.
        with self._state_lock:
            research = self.researches.get(research_id)
            if research is None:
                return None
            now = datetime.now(timezone.utc)
            normalized_event = {"timestamp": now.isoformat(), **event}
            research.graph_trail = compact_graph_trail(research.graph_trail, [normalized_event])
            research.updated_at = now
            trail = research.graph_trail
        self._emit_change(research_id)
        return trail

    def compact_research_graph_trails(self) -> list[str]:
        compacted_ids: list[str] = []
        for research in self.researches.values():
            compacted_trail = compact_graph_trail(research.graph_trail, [])
            if compacted_trail != (research.graph_trail or []):
                research.graph_trail = compacted_trail
                research.updated_at = datetime.now(timezone.utc)
                compacted_ids.append(research.id)
        return compacted_ids

    def add_research_finalize_job(
        self,
        research_id: str,
        max_attempts: int = 3,
    ) -> ResearchFinalizeJob:
        job_id = str(uuid.uuid4())
        job = ResearchFinalizeJob(id=job_id, research_id=research_id, max_attempts=max_attempts)
        self.finalize_jobs[job_id] = job
        return job

    def get_research_finalize_job(
        self,
        job_id: str,
        user_id: str | None = None,
    ) -> ResearchFinalizeJob | None:
        job = self.finalize_jobs.get(job_id)
        if job is None or not self._research_belongs_to(job.research_id, user_id):
            return None
        return job

    @staticmethod
    def _latest_job(jobs: list):
        """Newest job by created_at, then updated_at, like the SQL ordering. Jobs created
        in the same clock tick (common on Windows) tie on both; the last inserted wins."""
        if not jobs:
            return None
        return max(
            enumerate(jobs),
            key=lambda pair: (pair[1].created_at, pair[1].updated_at, pair[0]),
        )[1]

    def get_latest_research_finalize_job(
        self,
        research_id: str,
        user_id: str | None = None,
    ) -> ResearchFinalizeJob | None:
        if not self._research_belongs_to(research_id, user_id):
            return None
        matching_jobs = [
            job
            for job in self.finalize_jobs.values()
            if job.research_id == research_id
        ]
        return self._latest_job(matching_jobs)

    def get_pending_research_finalize_jobs(self) -> list[ResearchFinalizeJob]:
        return [
            job
            for job in self.finalize_jobs.values()
            if job.status == FinalizeJobStatus.PENDING
        ]

    def get_running_research_finalize_jobs(self) -> list[ResearchFinalizeJob]:
        return [
            job
            for job in self.finalize_jobs.values()
            if job.status == FinalizeJobStatus.RUNNING
        ]

    def get_dead_letter_research_finalize_jobs(self) -> list[ResearchFinalizeJob]:
        return [
            job
            for job in self.finalize_jobs.values()
            if job.status == FinalizeJobStatus.DEAD_LETTER
        ]

    def claim_next_research_finalize_job(self) -> ResearchFinalizeJob | None:
        pending_jobs = sorted(
            (
                job
                for job in self.finalize_jobs.values()
                if job.status == FinalizeJobStatus.PENDING
            ),
            key=lambda item: item.created_at,
        )
        if not pending_jobs:
            return None

        job = pending_jobs[0]
        job.status = FinalizeJobStatus.RUNNING
        job.attempt_count += 1
        job.updated_at = datetime.now(timezone.utc)
        return job

    def claim_research_finalize_job_by_id(self, job_id: str) -> ResearchFinalizeJob | None:
        job = self.finalize_jobs.get(job_id)
        if job is None or job.status != FinalizeJobStatus.PENDING:
            return None
        job.status = FinalizeJobStatus.RUNNING
        job.attempt_count += 1
        job.updated_at = datetime.now(timezone.utc)
        return job

    def renew_research_finalize_job_lease(self, job_id: str, lease_epoch: int) -> bool:
        job = self.finalize_jobs.get(job_id)
        if (
            job is None
            or job.status != FinalizeJobStatus.RUNNING
            or job.lease_epoch != lease_epoch
        ):
            return False
        job.updated_at = datetime.now(timezone.utc)
        return True

    def complete_research_finalize_job(
        self,
        job_id: str,
        research_id: str,
        lease_epoch: int,
        report: str,
    ) -> ResearchFinalizeJob | None:
        job = self.finalize_jobs.get(job_id)
        research = self.researches.get(research_id)
        if (
            job is None
            or research is None
            or job.research_id != research_id
            or job.status != FinalizeJobStatus.RUNNING
            or job.lease_epoch != lease_epoch
        ):
            return None
        if research.status != ResearchStatus.CANCELLED:
            self.update_research_status(research_id, ResearchStatus.COMPLETED, report)
        job.status = FinalizeJobStatus.COMPLETED
        job.error = None
        job.updated_at = datetime.now(timezone.utc)
        return job

    def update_research_finalize_job(
        self,
        job_id: str,
        status: FinalizeJobStatus,
        error: str | None = None,
        lease_epoch: int | None = None,
    ) -> ResearchFinalizeJob | None:
        job = self.finalize_jobs.get(job_id)
        if job is None or (
            lease_epoch is not None
            and (
                job.status != FinalizeJobStatus.RUNNING
                or job.lease_epoch != lease_epoch
            )
        ):
            return None

        job.status = status
        job.error = error
        job.updated_at = datetime.now(timezone.utc)
        return job

    def record_research_finalize_job_failure(
        self,
        job_id: str,
        error: str,
        lease_epoch: int | None = None,
    ) -> ResearchFinalizeJob | None:
        job = self.finalize_jobs.get(job_id)
        if job is None or (
            lease_epoch is not None
            and (
                job.status != FinalizeJobStatus.RUNNING
                or job.lease_epoch != lease_epoch
            )
        ):
            return None

        job.error = error
        job.status = (
            FinalizeJobStatus.DEAD_LETTER
            if job.attempt_count >= job.max_attempts
            else FinalizeJobStatus.PENDING
        )
        job.updated_at = datetime.now(timezone.utc)
        return job

    # Only a stopped job may be requeued (a RUNNING one would get a second runner).
    _REQUEUEABLE_FINALIZE_STATUSES = (FinalizeJobStatus.DEAD_LETTER, FinalizeJobStatus.FAILED)
    _REQUEUEABLE_SEARCH_STATUSES = (SearchJobStatus.DEAD_LETTER, SearchJobStatus.FAILED)

    def requeue_research_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None:
        job = self.finalize_jobs.get(job_id)
        if job is None or job.status not in self._REQUEUEABLE_FINALIZE_STATUSES:
            return None

        job.status = FinalizeJobStatus.PENDING
        job.attempt_count = 0
        job.error = None
        job.lease_epoch += 1  # fence any runner still holding the old lease
        job.updated_at = datetime.now(timezone.utc)
        return job

    def recover_stale_research_finalize_jobs(
        self,
        stale_before: datetime,
    ) -> list[ResearchFinalizeJob]:
        recovered_jobs = []
        for job in self.finalize_jobs.values():
            if job.status == FinalizeJobStatus.RUNNING and job.updated_at < stale_before:
                job.status = FinalizeJobStatus.PENDING
                job.lease_epoch += 1
                job.error = None
                job.updated_at = datetime.now(timezone.utc)
                recovered_jobs.append(job)
        return recovered_jobs

    def cleanup_old_research_finalize_jobs(
        self,
        older_than: datetime,
    ) -> list[str]:
        deleted_ids = []
        for job_id, job in list(self.finalize_jobs.items()):
            if job.status in (FinalizeJobStatus.COMPLETED, FinalizeJobStatus.DEAD_LETTER) and job.updated_at < older_than:
                deleted_ids.append(job_id)
                del self.finalize_jobs[job_id]
        return deleted_ids

    def add_search_task_job(
        self,
        task_id: str,
        depth: str,
        max_attempts: int = 3,
    ) -> SearchTaskJob:
        job_id = str(uuid.uuid4())
        job = SearchTaskJob(id=job_id, task_id=task_id, depth=depth, max_attempts=max_attempts)
        self.search_jobs[job_id] = job
        return job

    def get_search_task_job(
        self,
        job_id: str,
        user_id: str | None = None,
    ) -> SearchTaskJob | None:
        job = self.search_jobs.get(job_id)
        if job is None or self.get_task(job.task_id, user_id=user_id) is None:
            return None
        return job

    def get_latest_search_task_job(
        self,
        task_id: str,
        user_id: str | None = None,
    ) -> SearchTaskJob | None:
        if self.get_task(task_id, user_id=user_id) is None:
            return None
        matching_jobs = [
            job
            for job in self.search_jobs.values()
            if job.task_id == task_id
        ]
        return self._latest_job(matching_jobs)

    def get_pending_search_task_jobs(self) -> list[SearchTaskJob]:
        return [
            job
            for job in self.search_jobs.values()
            if job.status == SearchJobStatus.PENDING
        ]

    def get_running_search_task_jobs(self) -> list[SearchTaskJob]:
        return [
            job
            for job in self.search_jobs.values()
            if job.status == SearchJobStatus.RUNNING
        ]

    def get_dead_letter_search_task_jobs(self) -> list[SearchTaskJob]:
        return [
            job
            for job in self.search_jobs.values()
            if job.status == SearchJobStatus.DEAD_LETTER
        ]

    def claim_next_search_task_job(self) -> SearchTaskJob | None:
        pending_jobs = sorted(
            (
                job
                for job in self.search_jobs.values()
                if job.status == SearchJobStatus.PENDING
            ),
            key=lambda item: item.created_at,
        )
        if not pending_jobs:
            return None

        job = pending_jobs[0]
        job.status = SearchJobStatus.RUNNING
        job.attempt_count += 1
        job.updated_at = datetime.now(timezone.utc)
        return job

    def claim_search_task_job_by_id(self, job_id: str) -> SearchTaskJob | None:
        job = self.search_jobs.get(job_id)
        if job is None or job.status != SearchJobStatus.PENDING:
            return None
        job.status = SearchJobStatus.RUNNING
        job.attempt_count += 1
        job.updated_at = datetime.now(timezone.utc)
        return job

    def update_search_task_job(
        self,
        job_id: str,
        status: SearchJobStatus,
        error: str | None = None,
    ) -> SearchTaskJob | None:
        job = self.search_jobs.get(job_id)
        if job is None:
            return None

        job.status = status
        job.error = error
        job.updated_at = datetime.now(timezone.utc)
        return job

    def record_search_task_job_failure(
        self,
        job_id: str,
        error: str,
    ) -> SearchTaskJob | None:
        job = self.search_jobs.get(job_id)
        if job is None:
            return None

        job.error = error
        job.status = (
            SearchJobStatus.DEAD_LETTER
            if job.attempt_count >= job.max_attempts
            else SearchJobStatus.PENDING
        )
        job.updated_at = datetime.now(timezone.utc)
        return job

    def requeue_search_task_job(self, job_id: str) -> SearchTaskJob | None:
        job = self.search_jobs.get(job_id)
        if job is None or job.status not in self._REQUEUEABLE_SEARCH_STATUSES:
            return None

        job.status = SearchJobStatus.PENDING
        job.attempt_count = 0
        job.error = None
        job.updated_at = datetime.now(timezone.utc)
        return job

    def recover_stale_search_task_jobs(
        self,
        stale_before: datetime,
    ) -> list[SearchTaskJob]:
        recovered_jobs = []
        for job in self.search_jobs.values():
            if job.status == SearchJobStatus.RUNNING and job.updated_at < stale_before:
                job.status = SearchJobStatus.PENDING
                job.error = None
                job.updated_at = datetime.now(timezone.utc)
                recovered_jobs.append(job)
        return recovered_jobs

    def cleanup_old_search_task_jobs(
        self,
        older_than: datetime,
    ) -> list[str]:
        deleted_ids = []
        for job_id, job in list(self.search_jobs.items()):
            if job.status in (SearchJobStatus.COMPLETED, SearchJobStatus.DEAD_LETTER) and job.updated_at < older_than:
                deleted_ids.append(job_id)
                del self.search_jobs[job_id]
        return deleted_ids

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
    ) -> WorkerHeartbeat:
        heartbeat = WorkerHeartbeat(
            worker_name=worker_name,
            processed_jobs=processed_jobs,
            status=status,
            last_error=last_error,
            extraction_metrics=extraction_metrics or {},
            graph_metrics=graph_metrics or {},
            maintenance_summary=maintenance_summary or {},
        )
        self.worker_heartbeats[worker_name] = heartbeat
        self.worker_graph_step_events[worker_name] = compact_graph_step_events(
            self.worker_graph_step_events.get(worker_name, []),
            list(graph_step_events or []),
        )
        return heartbeat

    def get_worker_heartbeat(self, worker_name: str) -> WorkerHeartbeat | None:
        return self.worker_heartbeats.get(worker_name)

    def get_graph_step_events(self, worker_name: str | None = None) -> list[dict]:
        events: list[dict] = []
        heartbeats = (
            [self.worker_heartbeats[worker_name]]
            if worker_name and worker_name in self.worker_heartbeats
            else self.worker_heartbeats.values()
        )
        for heartbeat in heartbeats:
            events.extend(self.worker_graph_step_events.get(heartbeat.worker_name, []))
        return events

    def compact_worker_graph_step_events(self) -> list[str]:
        compacted_workers: list[str] = []
        for worker_name, events in list(self.worker_graph_step_events.items()):
            compacted = compact_graph_step_events(events, [])
            if compacted != events:
                self.worker_graph_step_events[worker_name] = compacted
                compacted_workers.append(worker_name)
        return compacted_workers

    def get_queue_metrics(self) -> QueueMetrics:
        extraction_metrics = ExtractionMetrics()
        graph_metrics = GraphMetrics()
        for heartbeat in self.worker_heartbeats.values():
            extraction_metrics.attempts += heartbeat.extraction_metrics.attempts
            extraction_metrics.success_count += heartbeat.extraction_metrics.success_count
            extraction_metrics.empty_count += heartbeat.extraction_metrics.empty_count
            extraction_metrics.failure_count += heartbeat.extraction_metrics.failure_count
            extraction_metrics.downloaded_bytes += heartbeat.extraction_metrics.downloaded_bytes
            extraction_metrics.content_chars += heartbeat.extraction_metrics.content_chars
            extraction_metrics.total_download_ms += heartbeat.extraction_metrics.total_download_ms
            extraction_metrics.total_extract_ms += heartbeat.extraction_metrics.total_extract_ms
            extraction_metrics.total_post_process_ms += heartbeat.extraction_metrics.total_post_process_ms
            extraction_metrics.total_total_ms += heartbeat.extraction_metrics.total_total_ms
            graph_metrics.resume_count += heartbeat.graph_metrics.resume_count
            graph_metrics.replan_pass_count += heartbeat.graph_metrics.replan_pass_count
            graph_metrics.tie_break_pass_count += heartbeat.graph_metrics.tie_break_pass_count
            graph_metrics.analyze_pass_count += heartbeat.graph_metrics.analyze_pass_count
            graph_metrics.completed_run_count += heartbeat.graph_metrics.completed_run_count
            for step_name, step_metrics in heartbeat.graph_metrics.steps.items():
                aggregate_step = graph_metrics.steps[step_name]
                aggregate_step.run_count += step_metrics.run_count
                aggregate_step.failure_count += step_metrics.failure_count
                aggregate_step.total_ms += step_metrics.total_ms
                aggregate_step.avg_ms = round(aggregate_step.total_ms / aggregate_step.run_count, 2) if aggregate_step.run_count > 0 else 0.0
        return QueueMetrics(
            pending_search_jobs=sum(1 for job in self.search_jobs.values() if job.status == SearchJobStatus.PENDING),
            running_search_jobs=sum(1 for job in self.search_jobs.values() if job.status == SearchJobStatus.RUNNING),
            dead_letter_search_jobs=sum(1 for job in self.search_jobs.values() if job.status == SearchJobStatus.DEAD_LETTER),
            pending_finalize_jobs=sum(1 for job in self.finalize_jobs.values() if job.status == FinalizeJobStatus.PENDING),
            running_finalize_jobs=sum(1 for job in self.finalize_jobs.values() if job.status == FinalizeJobStatus.RUNNING),
            dead_letter_finalize_jobs=sum(1 for job in self.finalize_jobs.values() if job.status == FinalizeJobStatus.DEAD_LETTER),
            extraction_metrics=extraction_metrics,
            graph_metrics=graph_metrics,
        )

    def get_task(self, task_id: str, user_id: str | None = None) -> SearchTask | None:
        task = self.tasks.get(task_id)
        if task is None or not self._research_belongs_to(task.research_id, user_id):
            return None
        return task

    def get_all_tasks(
        self,
        limit: int = 100,
        offset: int = 0,
        user_id: str | None = None,
    ) -> list[SearchTask]:
        tasks = list(self.tasks.values())
        if user_id is not None:
            tasks = [
                task
                for task in tasks
                if self._research_belongs_to(task.research_id, user_id)
            ]
        return tasks[offset : offset + limit]

    def get_tasks_by_research(self, research_id: str) -> list[SearchTask]:
        return [task for task in self.tasks.values() if task.research_id == research_id]

    def update_task(
        self,
        task_id: str,
        update: TaskUpdate,
        user_id: str | None = None,
    ) -> SearchTask | None:
        task = self.get_task(task_id, user_id=user_id)
        if task is None:
            return None

        if update.status is not None:
            task.status = update.status
        if update.result is not None:
            task.result = update.result
        if update.search_metrics is not None:
            task.search_metrics = update.search_metrics
        if update.log:
            task.logs.append(update.log)

        task.updated_at = datetime.now(timezone.utc)
        return task

    # ── admin & token tracking ────────────────────────────────────────────────
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
    ) -> str:
        usage_id = str(uuid.uuid4())
        self.llm_usage_logs.append({
            "id": usage_id,
            "research_id": research_id,
            "user_id": user_id,
            "model": model,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "cache_hit_tokens": cache_hit_tokens,
            "estimated_cost_usd": estimated_cost_usd,
            "created_at": datetime.now(timezone.utc),
        })
        return usage_id

    def record_admin_audit(
        self,
        actor_email: str,
        action: str,
        target_type: str,
        target_id: str | None = None,
        details: dict | None = None,
        ip_address: str | None = None,
    ) -> str:
        audit_id = str(uuid.uuid4())
        item = AdminAuditLogItem(
            id=audit_id,
            actor_email=actor_email,
            action=action,
            target_type=target_type,
            target_id=target_id,
            details=details or {},
            ip_address=ip_address,
            created_at=datetime.now(timezone.utc),
        )
        self.admin_audit_logs.append(item)
        return audit_id

    def get_admin_audit_logs(
        self,
        limit: int = 50,
        offset: int = 0,
        action: str | None = None,
        actor_email: str | None = None,
    ) -> list[AdminAuditLogItem]:
        logs = self.admin_audit_logs
        if action:
            logs = [entry for entry in logs if entry.action == action]
        if actor_email:
            logs = [entry for entry in logs if entry.actor_email == actor_email]
        sorted_logs = sorted(logs, key=lambda x: x.created_at, reverse=True)
        return sorted_logs[offset : offset + limit]

    def get_admin_token_analytics(
        self,
        page: int = 1,
        page_size: int = 20,
    ) -> AdminTokenAnalyticsResponse:
        total_prompt = sum(u["prompt_tokens"] for u in self.llm_usage_logs)
        total_comp = sum(u["completion_tokens"] for u in self.llm_usage_logs)
        total_tok = sum(u["total_tokens"] for u in self.llm_usage_logs)
        total_cost = sum(u["estimated_cost_usd"] for u in self.llm_usage_logs)

        models_map: dict[str, dict] = {}
        depths_map: dict[str, dict] = {}
        for u in self.llm_usage_logs:
            m = u["model"]
            if m not in models_map:
                models_map[m] = {"prompt": 0, "comp": 0, "tok": 0, "cost": 0.0, "count": 0}
            models_map[m]["prompt"] += u["prompt_tokens"]
            models_map[m]["comp"] += u["completion_tokens"]
            models_map[m]["tok"] += u["total_tokens"]
            models_map[m]["cost"] += u["estimated_cost_usd"]
            models_map[m]["count"] += 1
            research = self.researches.get(u["research_id"] or "")
            if research is not None:  # the SQL join on researches
                d = depths_map.setdefault(research.depth.value, {"tok": 0, "cost": 0.0, "ids": set()})
                d["tok"] += u["total_tokens"]
                d["cost"] += u["estimated_cost_usd"]
                d["ids"].add(research.id)

        # Same order as SQL: by total tokens, then name.
        by_model = [
            AdminTokenModelBreakdown(
                model=m,
                prompt_tokens=d["prompt"],
                completion_tokens=d["comp"],
                total_tokens=d["tok"],
                estimated_cost_usd=round(d["cost"], 4),
                calls_count=d["count"],
            )
            for m, d in sorted(models_map.items(), key=lambda item: (-item[1]["tok"], item[0]))
        ]
        by_depth = [
            AdminTokenDepthBreakdown(
                depth=depth,
                total_tokens=d["tok"],
                estimated_cost_usd=round(d["cost"], 4),
                researches_count=len(d["ids"]),
            )
            for depth, d in sorted(depths_map.items(), key=lambda item: (-item[1]["tok"], item[0]))
        ]

        return AdminTokenAnalyticsResponse(
            total_prompt_tokens=total_prompt,
            total_completion_tokens=total_comp,
            total_tokens=total_tok,
            total_cost_usd=round(total_cost, 4),
            by_model=by_model,
            by_depth=by_depth,
            researches=self.get_admin_token_research_usage(page=page, page_size=page_size),
            total_researches=len(self.researches),
            page=page,
            page_size=page_size,
        )

    def get_admin_token_research_usage(
        self,
        page: int = 1,
        page_size: int = 20,
    ) -> list[AdminTokenResearchUsageItem]:
        newest_first = sorted(self.researches.values(), key=lambda r: (r.created_at, r.id), reverse=True)
        offset = max(0, (page - 1) * page_size)
        items: list[AdminTokenResearchUsageItem] = []
        for r in newest_first[offset : offset + page_size]:
            tok, cost, calls = self._usage_by_research(r.id)
            if calls == 0:
                # Researches finalized before per-call usage rows existed.
                legacy = (r.graph_state or {}).get("llm_token_usage") or {}
                tok = int(legacy.get("total_tokens", 0) or 0)
                cost = float(legacy.get("estimated_cost_usd", 0.0) or 0.0)
            items.append(
                AdminTokenResearchUsageItem(
                    research_id=r.id,
                    prompt=r.prompt,
                    depth=r.depth.value,
                    status=r.status.value,
                    total_tokens=tok,
                    estimated_cost_usd=round(cost, 4),
                    created_at=r.created_at,
                )
            )
        return items

    def _usage_by_research(self, research_id: str) -> tuple[int, float, int]:
        """(total_tokens, unrounded cost, calls) logged for one research."""
        logs = [u for u in self.llm_usage_logs if u["research_id"] == research_id]
        return (
            sum(u["total_tokens"] for u in logs),
            sum(u["estimated_cost_usd"] for u in logs),
            len(logs),
        )

    def get_admin_overview(self) -> AdminOverviewResponse:
        from src.config import settings

        active_count = sum(1 for r in self.researches.values() if r.status == ResearchStatus.PROCESSING)
        pending_count = sum(1 for j in self.search_jobs.values() if j.status == SearchJobStatus.PENDING)
        failed_count = sum(1 for j in self.search_jobs.values() if j.status in (SearchJobStatus.DEAD_LETTER, "failed"))

        now = datetime.now(timezone.utc)
        worker_items: list[AdminWorkerFleetItem] = []
        for hb in self.worker_heartbeats.values():
            is_alive = (now - hb.last_seen_at).total_seconds() < 60.0
            worker_items.append(
                AdminWorkerFleetItem(
                    worker_name=hb.worker_name,
                    status=hb.status,
                    processed_jobs=hb.processed_jobs,
                    last_error=hb.last_error,
                    last_seen_at=hb.last_seen_at,
                    is_alive=is_alive,
                    extraction_metrics=hb.extraction_metrics or {},
                    graph_metrics=hb.graph_metrics or {},
                    maintenance_summary=hb.maintenance_summary or {},
                )
            )

        # system_health is the service's: it adds the LLM/broker probes to these counters.
        return AdminOverviewResponse(
            active_researches_count=active_count,
            pending_tasks_count=pending_count,
            failed_tasks_count=failed_count,
            workers=worker_items,
            is_dev_mode=settings.auth_disabled,
        )

    def preview_maintenance_action(
        self,
        action: str,
        params: dict | None = None,
    ) -> AdminDryRunResult:
        params = params or {}
        now = datetime.now(timezone.utc)
        if action == "recover_stale_finalize_jobs":
            stale_seconds = int(params.get("stale_seconds", 300))
            stale_dt = datetime.fromtimestamp(now.timestamp() - stale_seconds, tz=timezone.utc)
            stale = [j for j in self.finalize_jobs.values() if j.status == FinalizeJobStatus.RUNNING and j.updated_at < stale_dt]
            return AdminDryRunResult(
                action=action,
                dry_run=True,
                affected_count=len(stale),
                sample_affected_ids=[j.id for j in stale[:10]],
                summary=f"Would recover {len(stale)} stale finalize jobs",
            )
        elif action == "recover_stale_search_jobs":
            stale_seconds = int(params.get("stale_seconds", 300))
            stale_dt = datetime.fromtimestamp(now.timestamp() - stale_seconds, tz=timezone.utc)
            stale = [j for j in self.search_jobs.values() if j.status == SearchJobStatus.RUNNING and j.updated_at < stale_dt]
            return AdminDryRunResult(
                action=action,
                dry_run=True,
                affected_count=len(stale),
                sample_affected_ids=[j.id for j in stale[:10]],
                summary=f"Would recover {len(stale)} stale search jobs",
            )
        elif action == "cleanup_old_jobs":
            days = int(params.get("days", 7))
            cutoff = datetime.fromtimestamp(now.timestamp() - days * 86400, tz=timezone.utc)
            old_f = [j for j in self.finalize_jobs.values() if j.status in (FinalizeJobStatus.COMPLETED, FinalizeJobStatus.DEAD_LETTER) and j.updated_at < cutoff]
            old_s = [j for j in self.search_jobs.values() if j.status in (SearchJobStatus.COMPLETED, SearchJobStatus.DEAD_LETTER) and j.updated_at < cutoff]
            tot = len(old_f) + len(old_s)
            sample = [j.id for j in (old_f + old_s)[:10]]
            return AdminDryRunResult(
                action=action,
                dry_run=True,
                affected_count=tot,
                sample_affected_ids=sample,
                summary=f"Would delete {tot} old jobs",
            )
        elif action == "cleanup_search_cache":
            days = int(params.get("days", 3))
            cutoff = datetime.fromtimestamp(now.timestamp() - days * 86400, tz=timezone.utc)
            count = sum(1 for created, _ in self.search_cache.values() if created < cutoff)
            return AdminDryRunResult(
                action=action,
                dry_run=True,
                affected_count=count,
                sample_affected_ids=[],
                summary=f"Would delete {count} cached search entries older than {days} days",
            )
        elif action in ("requeue_finalize_job", "requeue_search_job"):
            tid = params.get("target_id")
            return AdminDryRunResult(
                action=action,
                dry_run=True,
                affected_count=1 if tid else 0,
                sample_affected_ids=[tid] if tid else [],
                summary=f"Would requeue job {tid}",
            )
        return AdminDryRunResult(action=action, dry_run=True, affected_count=0, sample_affected_ids=[], summary="Unknown action")

    # ── user telemetry & activity tracking ───────────────────────────────────
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
    ) -> str:
        ip_address = clip_text(ip_address, TELEMETRY_IP_MAX_LENGTH)
        user_agent = clip_text(user_agent, TELEMETRY_USER_AGENT_MAX_LENGTH)
        now = datetime.now(timezone.utc)
        # Mirrors the SQL upsert on (session_id, user_id): a repeat bumps the caller's own
        # row; another account's row with the same client-chosen id is left alone.
        existing = next(
            (
                s for s in self.user_sessions
                if s.get("session_id") == session_id and s.get("user_id") == user_id
            ),
            None,
        )
        if existing is not None:
            existing["last_active_at"] = now
            if ip_address:
                existing["ip_address"] = ip_address
            if user_agent:
                existing["user_agent"] = user_agent
            record_id = existing["id"]
        else:
            record_id = str(uuid.uuid4())
            self.user_sessions.append({
                "id": record_id,
                "user_id": user_id,
                "session_id": session_id,
                "ip_address": ip_address,
                "user_agent": user_agent,
                "device_type": device_type,
                "browser": browser,
                "os": os,
                "screen_res": screen_res,
                "viewport": viewport,
                "language": language,
                "timezone": client_timezone,
                "country": country,
                "city": city,
                "started_at": now,
                "last_active_at": now,
            })
        return record_id

    def record_user_event(
        self,
        event_name: str,
        event_category: str = "general",
        user_id: str | None = None,
        session_id: str | None = None,
        details: dict | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
    ) -> str:
        ip_address = clip_text(ip_address, TELEMETRY_IP_MAX_LENGTH)
        user_agent = clip_text(user_agent, TELEMETRY_USER_AGENT_MAX_LENGTH)
        record_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        self.user_events.append({
            "id": record_id,
            "user_id": user_id,
            "session_id": session_id,
            "event_name": event_name,
            "event_category": event_category,
            "details": details or {},
            "ip_address": ip_address,
            "user_agent": user_agent,
            "created_at": now,
        })
        # Like SQL: users activity is the middleware's (throttled) job, not every event's.
        return record_id

    def touch_user_activity(
        self,
        user_id: str,
        ip_address: str | None = None,
        user_agent: str | None = None,
        device: str | None = None,
    ) -> None:
        ip_address = clip_text(ip_address, TELEMETRY_IP_MAX_LENGTH)
        user_agent = clip_text(user_agent, TELEMETRY_USER_AGENT_MAX_LENGTH)
        now = datetime.now(timezone.utc)
        if user_id not in self.users:
            return  # SQL updates no row for an unknown user
        entry = self.user_telemetry.setdefault(user_id, {})
        # Same throttle as the SQL WHERE clause: seen within the interval -> no rewrite.
        last_seen = entry.get("last_seen_at")
        if last_seen is not None and (now - last_seen).total_seconds() < USER_ACTIVITY_TOUCH_INTERVAL_SECONDS:
            return
        entry["last_seen_at"] = now
        if ip_address:
            entry["last_ip"] = ip_address
        if user_agent:
            entry["last_user_agent"] = user_agent
        if device:
            entry["last_device"] = device

    def _admin_user_row(self, user_id: str, admin_emails: set[str], online_threshold: datetime) -> tuple[dict, AdminUserListItem]:
        """The SQL store's per-user row: its sort keys plus the list item."""
        u = self.users[user_id]
        telem = self.user_telemetry.get(user_id, {})
        last_seen = telem.get("last_seen_at")
        created_at = self._user_created_at.get(user_id) or datetime.now(timezone.utc)
        user_logs = [log_item for log_item in self.llm_usage_logs if log_item.get("user_id") == user_id]
        tot_tokens = sum(log_item.get("total_tokens", 0) for log_item in user_logs)
        tot_cost = sum(log_item.get("estimated_cost_usd", 0.0) for log_item in user_logs)
        researches_count = sum(1 for r in self.researches.values() if r.user_id == user_id)
        last_sess = self._latest_sessions(user_id, 1)
        last_sess = last_sess[0] if last_sess else {}
        item = AdminUserListItem(
            id=user_id,
            email=u.email,
            name=u.name,
            avatar_url=u.avatar_url,
            is_admin=bool(u.email and u.email.lower() in admin_emails),
            created_at=created_at.isoformat(),
            last_seen_at=last_seen.isoformat() if last_seen else None,
            is_online=bool(last_seen and last_seen >= online_threshold),
            last_ip=telem.get("last_ip") or last_sess.get("ip_address"),
            last_device=telem.get("last_device") or last_sess.get("device_type"),
            last_browser=last_sess.get("browser"),
            last_os=last_sess.get("os"),
            researches_count=researches_count,
            total_tokens=tot_tokens,
            total_cost_usd=round(tot_cost, 4),
        )
        keys = {
            "created_at": created_at,
            "last_seen_at": last_seen,
            "last_ip": telem.get("last_ip") or "",
            "total_tokens": tot_tokens,
            "total_cost": tot_cost,
            "researches_count": researches_count,
        }
        return keys, item

    def _latest_sessions(self, user_id: str, limit: int) -> list[dict]:
        sessions = [s for s in self.user_sessions if s.get("user_id") == user_id]
        sessions.sort(key=lambda s: (s["last_active_at"], s["id"]), reverse=True)
        return sessions[:limit]

    def get_admin_users_list(
        self,
        page: int = 1,
        page_size: int = 20,
        search: str | None = None,
        role: str | None = None,
        online_only: bool = False,
        sort_by: str = "last_seen",
    ) -> AdminUserListResponse:
        online_threshold = datetime.now(timezone.utc) - timedelta(minutes=2)
        admin_emails = _admin_emails()
        term = (search or "").strip().lower()

        rows: list[tuple[dict, AdminUserListItem]] = []
        for uid in self.users:
            keys, item = self._admin_user_row(uid, admin_emails, online_threshold)
            if term and not (
                term in item.email.lower()
                or term in (item.name or "").lower()
                or term in keys["last_ip"].lower()
            ):
                continue
            if role == "admin" and not item.is_admin:
                continue
            if role == "user" and item.is_admin:
                continue
            if online_only and not item.is_online:
                continue
            rows.append((keys, item))

        # The SQL ORDER BY: the metric (or recency) first, then newest account, then id.
        def newest_first(row):
            return (row[0]["created_at"], row[1].id)

        metric = {"tokens": "total_tokens", "cost": "total_cost", "researches": "researches_count"}.get(sort_by)
        if metric:
            rows.sort(key=lambda row: (row[0][metric], *newest_first(row)), reverse=True)
        elif sort_by == "registered":
            rows.sort(key=newest_first, reverse=True)
        else:  # last_seen, never-seen users last
            epoch = datetime.min.replace(tzinfo=timezone.utc)
            rows.sort(
                key=lambda row: (row[0]["last_seen_at"] is not None, row[0]["last_seen_at"] or epoch, *newest_first(row)),
                reverse=True,
            )

        online_users = sum(
            1
            for entry in self.user_telemetry.values()
            if entry.get("last_seen_at") and entry["last_seen_at"] >= online_threshold
        )
        start = (page - 1) * page_size
        return AdminUserListResponse(
            users=[item for _keys, item in rows[start : start + page_size]],
            total_users=len(rows),
            online_users=online_users,
            page=page,
            page_size=page_size,
        )

    def get_admin_user_detail(self, user_id: str) -> AdminUserDetailResponse | None:
        if user_id not in self.users:
            return None
        online_threshold = datetime.now(timezone.utc) - timedelta(minutes=2)
        _keys, user_item = self._admin_user_row(user_id, _admin_emails(), online_threshold)

        sessions = [
            {
                "id": s["id"],
                "session_id": s["session_id"],
                "ip_address": s.get("ip_address"),
                "device_type": s.get("device_type"),
                "browser": s.get("browser"),
                "os": s.get("os"),
                "screen_res": s.get("screen_res"),
                "language": s.get("language"),
                "timezone": s.get("timezone"),
                "country": s.get("country"),
                "city": s.get("city"),
                "started_at": s["started_at"].isoformat(),
                "last_active_at": s["last_active_at"].isoformat(),
            }
            for s in self._latest_sessions(user_id, 10)
        ]
        owned = sorted(
            (r for r in self.researches.values() if r.user_id == user_id),
            key=lambda r: (r.created_at, r.id),
            reverse=True,
        )[:20]
        researches = []
        for r in owned:
            tokens, cost, _calls = self._usage_by_research(r.id)
            researches.append(
                {
                    "id": r.id,
                    "prompt": r.prompt,
                    "depth": r.depth.value,
                    "status": r.status.value,
                    "total_tokens": tokens,
                    "cost_usd": round(cost, 4),
                    "created_at": r.created_at.isoformat(),
                }
            )
        events = [
            {
                "id": e["id"],
                "event_name": e["event_name"],
                "event_category": e["event_category"],
                "details": e["details"] or {},
                "created_at": e["created_at"].isoformat(),
            }
            for e in sorted(
                (e for e in self.user_events if e.get("user_id") == user_id),
                key=lambda e: (e["created_at"], e["id"]),
                reverse=True,
            )[:20]
        ]
        by_model: dict[str, dict] = {}
        for log_item in sorted(self.llm_usage_logs, key=lambda log_item: log_item["model"]):
            if log_item.get("user_id") != user_id:
                continue
            entry = by_model.setdefault(log_item["model"], {"tokens": 0, "cost_usd": 0.0})
            entry["tokens"] += log_item["total_tokens"]
            entry["cost_usd"] += log_item["estimated_cost_usd"]
        for entry in by_model.values():
            entry["cost_usd"] = round(entry["cost_usd"], 4)

        return AdminUserDetailResponse(
            user=user_item,
            sessions=sessions,
            researches=researches,
            recent_researches=researches,
            recent_events=events,
            token_breakdown={"by_model": by_model},
        )

    def get_admin_telemetry_summary(self) -> AdminTelemetrySummaryResponse:
        now = datetime.now(timezone.utc)
        total_users = len(self.users)
        online_now = 0
        dau_set = set()
        wau_set = set()
        mau_set = set()

        os_counts: dict[str, int] = {}
        browser_counts: dict[str, int] = {}
        device_counts: dict[str, int] = {}
        country_counts: dict[str, int] = {}

        for s in self.user_sessions:
            uid = s.get("user_id")
            active = s.get("last_active_at", now)
            diff = (now - active).total_seconds()
            if diff < 120:
                online_now += 1
            if diff < 86400 and uid:
                dau_set.add(uid)
            if diff < 7 * 86400 and uid:
                wau_set.add(uid)
            if diff < 30 * 86400 and uid:
                mau_set.add(uid)

            os_name = s.get("os") or "macOS"
            os_counts[os_name] = os_counts.get(os_name, 0) + 1

            b_name = s.get("browser") or "Chrome"
            browser_counts[b_name] = browser_counts.get(b_name, 0) + 1

            d_name = s.get("device_type") or "desktop"
            device_counts[d_name] = device_counts.get(d_name, 0) + 1

            c_name = s.get("country") or "Local"
            country_counts[c_name] = country_counts.get(c_name, 0) + 1

        total_tokens = sum(log_item.get("total_tokens", 0) for log_item in self.llm_usage_logs)
        total_cost = sum(log_item.get("estimated_cost_usd", 0.0) for log_item in self.llm_usage_logs)

        depth_counts: dict[str, int] = {}
        prompt_lens: list[int] = []
        for r in self.researches.values():
            d = r.depth or "medium"
            depth_counts[d] = depth_counts.get(d, 0) + 1
            if r.prompt:
                prompt_lens.append(len(r.prompt))

        avg_prompt_len = round(sum(prompt_lens) / len(prompt_lens), 1) if prompt_lens else 0.0

        return AdminTelemetrySummaryResponse(
            total_users=total_users,
            online_now=online_now,
            online_users_now=online_now,
            dau=len(dau_set),
            dau_today=len(dau_set),
            wau=len(wau_set),
            wau_7d=len(wau_set),
            mau=len(mau_set),
            mau_30d=len(mau_set),
            total_researches=len(self.researches),
            total_tokens=total_tokens,
            total_cost_usd=round(total_cost, 4),
            by_os=[{"name": k, "count": v} for k, v in os_counts.items()],
            by_browser=[{"name": k, "count": v} for k, v in browser_counts.items()],
            by_device=[{"name": k, "count": v} for k, v in device_counts.items()],
            by_country=[{"name": k, "count": v} for k, v in country_counts.items()],
            os_breakdown=os_counts,
            browser_breakdown=browser_counts,
            device_breakdown=device_counts,
            depth_distribution=depth_counts,
            popular_depths=[{"depth": k, "count": v} for k, v in depth_counts.items()],
            popular_models=[],
            avg_prompt_len=avg_prompt_len,
        )

    def get_admin_event_logs(
        self,
        limit: int = 50,
        offset: int = 0,
        category: str | None = None,
        event_name: str | None = None,
        user_id: str | None = None,
    ) -> AdminEventLogResponse:
        filtered = list(self.user_events)
        if category:
            filtered = [e for e in filtered if e.get("event_category") == category]
        if event_name:
            filtered = [e for e in filtered if e.get("event_name") == event_name]
        if user_id:
            filtered = [e for e in filtered if e.get("user_id") == user_id]

        filtered.reverse()
        paged = filtered[offset : offset + limit]

        items = []
        for e in paged:
            u_email = None
            uid = e.get("user_id")
            if uid and uid in self.users:
                u_email = self.users[uid].email
            items.append(
                AdminEventLogItem(
                    id=e["id"],
                    user_id=uid,
                    user_email=u_email,
                    session_id=e.get("session_id"),
                    event_name=e["event_name"],
                    event_category=e["event_category"],
                    details=e.get("details", {}),
                    ip_address=e.get("ip_address"),
                    user_agent=e.get("user_agent"),
                    created_at=e["created_at"].isoformat() if hasattr(e["created_at"], "isoformat") else str(e["created_at"]),
                )
            )

        return AdminEventLogResponse(
            events=items,
            total_count=len(filtered),
            page=(offset // limit) + 1 if limit > 0 else 1,
            page_size=limit,
        )

    def get_admin_prompts(
        self,
        page: int = 1,
        page_size: int = 25,
        search: str | None = None,
        user_id: str | None = None,
        prompt_type: str | None = None,
    ) -> AdminPromptsResponse:
        rows: list[tuple[datetime, AdminPromptItem]] = []
        if not prompt_type or prompt_type in ("all", "research"):
            for r in self.researches.values():
                if user_id and r.user_id != user_id:
                    continue
                user = self.users.get(r.user_id) if r.user_id else None
                tokens, cost, _calls = self._usage_by_research(r.id)
                rows.append((r.created_at, AdminPromptItem(
                    id=f"res_{r.id}",
                    prompt_type="research",
                    prompt=r.prompt,
                    research_id=r.id,
                    user_id=r.user_id,
                    user_email=user.email if user else None,
                    user_name=user.name if user else None,
                    depth=r.depth.value,
                    status=r.status.value,
                    total_tokens=tokens,
                    cost_usd=round(cost, 4),
                    created_at=r.created_at.isoformat(),
                )))

        if not prompt_type or prompt_type in ("all", "chat"):
            for ev in self.user_events:
                if ev.get("event_name") != "chat_prompt":
                    continue
                ev_user_id = ev.get("user_id")
                if user_id and ev_user_id != user_id:
                    continue
                details = ev.get("details") or {}
                user = self.users.get(ev_user_id) if ev_user_id else None
                rows.append((ev["created_at"], AdminPromptItem(
                    id=f"chat_{ev['id']}",
                    prompt_type="chat",
                    prompt=str(details.get("prompt") or ""),
                    research_id=str(details.get("research_id") or ""),
                    user_id=ev_user_id,
                    user_email=user.email if user else None,
                    user_name=user.name if user else None,
                    depth=None,
                    status=None,
                    total_tokens=0,
                    cost_usd=0.0,
                    created_at=ev["created_at"].isoformat(),
                )))

        term = (search or "").strip().lower()
        if term:
            rows = [
                (created_at, it) for created_at, it in rows
                if term in it.prompt.lower()
                or (it.user_email and term in it.user_email.lower())
                or (it.user_name and term in it.user_name.lower())
            ]

        # The SQL ORDER BY created_at DESC, item id DESC.
        rows.sort(key=lambda row: (row[0], row[1].id), reverse=True)
        start = (page - 1) * page_size
        return AdminPromptsResponse(
            prompts=[item for _created_at, item in rows[start : start + page_size]],
            total_count=len(rows),
            page=page,
            page_size=page_size,
        )

    def get_user_token_analytics(self, user_id: str) -> dict:
        logs = [u for u in self.llm_usage_logs if u.get("user_id") == user_id]
        total_prompt = sum(u.get("prompt_tokens", 0) for u in logs)
        total_comp = sum(u.get("completion_tokens", 0) for u in logs)
        total_tok = sum(u.get("total_tokens", 0) for u in logs)
        total_cost = round(sum(u.get("estimated_cost_usd", 0.0) for u in logs), 4)
        researches = [r for r in self.researches.values() if r.user_id == user_id]

        models_map: dict[str, dict] = {}
        for u in logs:
            m = u.get("model", "unknown")
            if m not in models_map:
                models_map[m] = {
                    "model": m,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "estimated_cost_usd": 0.0,
                    "calls_count": 0,
                }
            models_map[m]["prompt_tokens"] += u.get("prompt_tokens", 0)
            models_map[m]["completion_tokens"] += u.get("completion_tokens", 0)
            models_map[m]["total_tokens"] += u.get("total_tokens", 0)
            models_map[m]["estimated_cost_usd"] += u.get("estimated_cost_usd", 0.0)
            models_map[m]["calls_count"] += 1

        for v in models_map.values():
            v["estimated_cost_usd"] = round(v["estimated_cost_usd"], 4)

        recent = [
            {
                "id": r.id,
                "prompt": r.prompt,
                "depth": r.depth,
                "status": r.status,
                "total_tokens": sum(u.get("total_tokens", 0) for u in logs if u.get("research_id") == r.id),
                "estimated_cost_usd": round(sum(u.get("estimated_cost_usd", 0.0) for u in logs if u.get("research_id") == r.id), 4),
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in sorted(researches, key=lambda x: x.created_at or datetime.min, reverse=True)[:10]
        ]

        return {
            "total_tokens": total_tok,
            "prompt_tokens": total_prompt,
            "completion_tokens": total_comp,
            "estimated_cost_usd": total_cost,
            "calls_count": len(logs),
            "researches_count": len(researches),
            "by_model": list(models_map.values()),
            "recent": recent,
        }

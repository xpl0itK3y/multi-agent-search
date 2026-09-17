from datetime import datetime, timezone
import threading
import uuid

from src.core.graph_history import compact_graph_step_events, compact_graph_trail
from src.domain import (
    AdminAuditLogItem,
    AdminDryRunResult,
    AdminOverviewResponse,
    AdminTokenAnalyticsResponse,
    AdminTokenDepthBreakdown,
    AdminTokenModelBreakdown,
    AdminTokenResearchUsageItem,
    AdminWorkerFleetItem,
    ExtractionMetrics,
    FinalizeJobStatus,
    GraphMetrics,
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


class InMemoryTaskStore:
    def __init__(self):
        self.tasks: dict[str, SearchTask] = {}
        self.researches: dict[str, ResearchRecord] = {}
        self.finalize_jobs: dict[str, ResearchFinalizeJob] = {}
        self.search_jobs: dict[str, SearchTaskJob] = {}
        self.worker_heartbeats: dict[str, WorkerHeartbeat] = {}
        self.worker_graph_step_events: dict[str, list[dict]] = {}
        self.users: dict[str, UserRecord] = {}
        self.search_cache: dict[str, tuple[datetime, list[dict]]] = {}
        self.llm_usage_logs: list[dict] = []
        self.admin_audit_logs: list[AdminAuditLogItem] = []
        self._admission_lock = threading.RLock()

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
        if (datetime.now(timezone.utc) - created_at).total_seconds() > max_age_seconds:
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

    def _active_count(
        self,
        user_id: str | None,
        stale_before: datetime,
        *,
        include_queued: bool,
    ) -> int:
        records = self.researches.values()
        if user_id is not None:
            records = (record for record in records if record.user_id == user_id)
        return sum(
            1
            for record in records
            if self._is_fresh_running(record, stale_before)
            or (include_queued and record.status == ResearchStatus.QUEUED)
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
                and self._active_count(research.user_id, stale_before, include_queued=True)
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

    def update_user_profile(self, user_id: str, name: str | None, avatar_url: str | None) -> None:
        user = self.users.get(user_id)
        if user:
            patch = {}
            if name:
                patch["name"] = name
            if avatar_url:
                patch["avatar_url"] = avatar_url
            if patch:
                self.users[user_id] = user.model_copy(update=patch)

    def get_research(self, research_id: str) -> ResearchRecord | None:
        return self.researches.get(research_id)

    def delete_research(self, research_id: str) -> bool:
        if research_id not in self.researches:
            return False
        del self.researches[research_id]
        task_ids = [tid for tid, t in self.tasks.items() if t.research_id == research_id]
        for tid in task_ids:
            del self.tasks[tid]
        return True

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

    def update_research_graph_state(
        self,
        research_id: str,
        graph_state: dict,
    ) -> ResearchRecord | None:
        research = self.researches.get(research_id)
        if research is None:
            return None
        research.graph_state = graph_state or {}
        research.updated_at = datetime.now(timezone.utc)
        return research

    def merge_research_graph_state(self, research_id: str, patch: dict) -> ResearchRecord | None:
        research = self.researches.get(research_id)
        if research is None:
            return None
        if patch:
            research.graph_state = {**(research.graph_state or {}), **patch}
            research.updated_at = datetime.now(timezone.utc)
        return research

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
    ) -> ResearchRecord | None:
        research = self.researches.get(research_id)
        if research is None:
            return None
        normalized_event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **event,
        }
        research.graph_trail = compact_graph_trail(research.graph_trail, [normalized_event])
        research.updated_at = datetime.now(timezone.utc)
        self._emit_change(research_id)
        return research

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
        if not matching_jobs:
            return None
        return max(matching_jobs, key=lambda item: item.created_at)

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

    def requeue_research_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None:
        job = self.finalize_jobs.get(job_id)
        if job is None:
            return None

        job.status = FinalizeJobStatus.PENDING
        job.attempt_count = 0
        job.error = None
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
        if not matching_jobs:
            return None
        return max(matching_jobs, key=lambda item: item.created_at)

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
        if job is None:
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
            logs = [l for l in logs if l.action == action]
        if actor_email:
            logs = [l for l in logs if l.actor_email == actor_email]
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
        for u in self.llm_usage_logs:
            m = u["model"]
            if m not in models_map:
                models_map[m] = {"prompt": 0, "comp": 0, "tok": 0, "cost": 0.0, "count": 0}
            models_map[m]["prompt"] += u["prompt_tokens"]
            models_map[m]["comp"] += u["completion_tokens"]
            models_map[m]["tok"] += u["total_tokens"]
            models_map[m]["cost"] += u["estimated_cost_usd"]
            models_map[m]["count"] += 1

        by_model = [
            AdminTokenModelBreakdown(
                model=m,
                prompt_tokens=d["prompt"],
                completion_tokens=d["comp"],
                total_tokens=d["tok"],
                estimated_cost_usd=round(d["cost"], 4),
                calls_count=d["count"],
            )
            for m, d in models_map.items()
        ]

        all_researches = sorted(self.researches.values(), key=lambda r: r.created_at, reverse=True)
        total_researches = len(all_researches)
        offset = max(0, (page - 1) * page_size)
        paged_researches = all_researches[offset : offset + page_size]

        research_items: list[AdminTokenResearchUsageItem] = []
        for r in paged_researches:
            r_logs = [u for u in self.llm_usage_logs if u["research_id"] == r.id]
            if r_logs:
                tok = sum(u["total_tokens"] for u in r_logs)
                cost = sum(u["estimated_cost_usd"] for u in r_logs)
            else:
                legacy = (r.graph_state or {}).get("llm_token_usage") or {}
                tok = legacy.get("total_tokens", 0)
                cost = legacy.get("estimated_cost_usd", 0.0)
            research_items.append(
                AdminTokenResearchUsageItem(
                    research_id=r.id,
                    prompt=r.prompt,
                    depth=r.depth,
                    status=r.status,
                    total_tokens=tok,
                    estimated_cost_usd=round(cost, 4),
                    created_at=r.created_at,
                )
            )

        return AdminTokenAnalyticsResponse(
            total_prompt_tokens=total_prompt,
            total_completion_tokens=total_comp,
            total_tokens=total_tok,
            total_cost_usd=round(total_cost, 4),
            by_model=by_model,
            by_depth=[],
            researches=research_items,
            total_researches=total_researches,
            page=page,
            page_size=page_size,
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

        return AdminOverviewResponse(
            system_health={"overall": "healthy"},
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

    def execute_maintenance_action(
        self,
        action: str,
        actor_email: str,
        params: dict | None = None,
        ip_address: str | None = None,
    ) -> AdminDryRunResult:
        res = self.preview_maintenance_action(action, params)
        res.dry_run = False
        self.record_admin_audit(
            actor_email=actor_email,
            action=action,
            target_type="maintenance",
            target_id=(params or {}).get("target_id"),
            details={"params": params, "affected_count": res.affected_count},
            ip_address=ip_address,
        )
        return res

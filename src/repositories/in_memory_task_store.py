from datetime import datetime, timezone
import uuid

from src.graph.history import compact_graph_step_events, compact_graph_trail
from src.domain import (
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
        self, request: ResearchRequest, task_ids: list[str], user_id: str | None = None
    ) -> ResearchRecord:
        research_id = str(uuid.uuid4())
        record = ResearchRecord(
            id=research_id,
            prompt=request.prompt,
            user_id=user_id,
            depth=request.depth,
            task_ids=task_ids,
        )
        self.researches[research_id] = record
        return record

    def create_user(self, user_id: str, email: str, password_hash: str) -> UserRecord:
        user = UserRecord(id=user_id, email=email, password_hash=password_hash)
        self.users[user_id] = user
        return user

    def get_user_by_email(self, email: str) -> UserRecord | None:
        target = email.strip().lower()
        return next((u for u in self.users.values() if u.email.lower() == target), None)

    def get_user_by_id(self, user_id: str) -> UserRecord | None:
        return self.users.get(user_id)

    def update_user_password(self, user_id: str, password_hash: str) -> None:
        user = self.users.get(user_id)
        if user:
            self.users[user_id] = user.model_copy(update={"password_hash": password_hash})

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

    def list_active_watch_research_ids(self) -> list[str]:
        return [
            r.id
            for r in self.researches.values()
            if ((r.graph_state or {}).get("watch") or {}).get("enabled")
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
            for r in records
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
            if report:
                research.final_report = report
            research.updated_at = datetime.now(timezone.utc)
            self._emit_change(research_id)
        return research

    def try_claim_queued_research(self, research_id: str) -> bool:
        """Atomically flip QUEUED -> PROCESSING (only if still queued)."""
        research = self.researches.get(research_id)
        if research is None or research.status != ResearchStatus.QUEUED:
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
        state = dict(research.graph_state or {})
        state["partial_report"] = partial
        research.graph_state = state
        self._emit_change(research_id)

    def save_partial_reasoning(self, research_id: str, partial: str) -> None:
        research = self.researches.get(research_id)
        if research is None:
            return
        state = dict(research.graph_state or {})
        state["partial_reasoning"] = partial
        research.graph_state = state
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

    def get_research_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None:
        return self.finalize_jobs.get(job_id)

    def get_latest_research_finalize_job(
        self,
        research_id: str,
    ) -> ResearchFinalizeJob | None:
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

    def update_research_finalize_job(
        self,
        job_id: str,
        status: FinalizeJobStatus,
        error: str | None = None,
    ) -> ResearchFinalizeJob | None:
        job = self.finalize_jobs.get(job_id)
        if job is None:
            return None

        job.status = status
        job.error = error
        job.updated_at = datetime.now(timezone.utc)
        return job

    def record_research_finalize_job_failure(
        self,
        job_id: str,
        error: str,
    ) -> ResearchFinalizeJob | None:
        job = self.finalize_jobs.get(job_id)
        if job is None:
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

    def get_search_task_job(self, job_id: str) -> SearchTaskJob | None:
        return self.search_jobs.get(job_id)

    def get_latest_search_task_job(self, task_id: str) -> SearchTaskJob | None:
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

    def get_task(self, task_id: str) -> SearchTask | None:
        return self.tasks.get(task_id)

    def get_all_tasks(self, limit: int = 100, offset: int = 0) -> list[SearchTask]:
        return list(self.tasks.values())[offset : offset + limit]

    def get_tasks_by_research(self, research_id: str) -> list[SearchTask]:
        return [task for task in self.tasks.values() if task.research_id == research_id]

    def update_task(self, task_id: str, update: TaskUpdate) -> SearchTask | None:
        task = self.tasks.get(task_id)
        if not task:
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

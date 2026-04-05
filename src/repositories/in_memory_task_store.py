from datetime import datetime, timezone
import uuid

from src.api.schemas import (
    ExtractionMetrics,
    FinalizeJobStatus,
    GraphMetrics,
    QueueMetrics,
    ResearchFinalizeJob,
    SearchJobStatus,
    SearchTaskJob,
    WorkerHeartbeat,
    ResearchRecord,
    ResearchRequest,
    ResearchStatus,
    SearchTask,
    TaskUpdate,
)


class InMemoryTaskStore:
    def __init__(self):
        self.tasks: dict[str, SearchTask] = {}
        self.researches: dict[str, ResearchRecord] = {}
        self.finalize_jobs: dict[str, ResearchFinalizeJob] = {}
        self.search_jobs: dict[str, SearchTaskJob] = {}
        self.worker_heartbeats: dict[str, WorkerHeartbeat] = {}

    def add_research(self, request: ResearchRequest, task_ids: list[str]) -> ResearchRecord:
        research_id = str(uuid.uuid4())
        record = ResearchRecord(
            id=research_id,
            prompt=request.prompt,
            depth=request.depth,
            task_ids=task_ids,
        )
        self.researches[research_id] = record
        return record

    def get_research(self, research_id: str) -> ResearchRecord | None:
        return self.researches.get(research_id)

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
        return research

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

    def append_research_graph_event(
        self,
        research_id: str,
        event: dict,
    ) -> ResearchRecord | None:
        research = self.researches.get(research_id)
        if research is None:
            return None
        research.graph_trail.append(event)
        research.updated_at = datetime.now(timezone.utc)
        return research

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
    ) -> WorkerHeartbeat:
        heartbeat = WorkerHeartbeat(
            worker_name=worker_name,
            processed_jobs=processed_jobs,
            status=status,
            last_error=last_error,
            extraction_metrics=extraction_metrics or {},
            graph_metrics=graph_metrics or {},
        )
        self.worker_heartbeats[worker_name] = heartbeat
        return heartbeat

    def get_worker_heartbeat(self, worker_name: str) -> WorkerHeartbeat | None:
        return self.worker_heartbeats.get(worker_name)

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

    def get_all_tasks(self) -> list[SearchTask]:
        return list(self.tasks.values())

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

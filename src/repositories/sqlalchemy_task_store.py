import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable

from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.orm import Session, selectinload

from src.api.schemas import (
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
from src.db.models import (
    ResearchFinalizeJobORM,
    ResearchORM,
    SearchCacheORM,
    SearchTaskJobORM,
    SearchTaskORM,
    UserORM,
    WorkerHeartbeatORM,
)
from src.graph.history import compact_graph_step_events, compact_graph_trail
from src.repositories.mappers import (
    research_finalize_job_orm_to_schema,
    research_orm_to_record,
    search_task_job_orm_to_schema,
    search_result_dicts_to_orm,
    search_task_orm_to_schema,
    worker_heartbeat_orm_to_schema,
)


class SQLAlchemyTaskStore:
    def __init__(self, session_factory: Callable[[], Session]):
        self.session_factory = session_factory

    @contextmanager
    def session_scope(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def add_research(
        self, request: ResearchRequest, task_ids: list[str], user_id: str | None = None
    ) -> ResearchRecord:
        research = ResearchORM(
            id=str(uuid.uuid4()),
            prompt=request.prompt,
            user_id=user_id,
            depth=request.depth.value,
            status=ResearchStatus.PROCESSING.value,
            task_ids=task_ids,
        )
        with self.session_scope() as session:
            session.add(research)
            session.flush()
            session.refresh(research)
            return research_orm_to_record(research)

    def create_user(self, user_id: str, email: str, password_hash: str) -> UserRecord:
        user = UserORM(id=user_id, email=email.strip().lower(), password_hash=password_hash)
        with self.session_scope() as session:
            session.add(user)
            session.flush()
            return UserRecord(id=user.id, email=user.email, password_hash=user.password_hash, name=user.name, avatar_url=user.avatar_url)

    def get_user_by_email(self, email: str) -> UserRecord | None:
        with self.session_scope() as session:
            user = session.execute(
                select(UserORM).where(UserORM.email == email.strip().lower())
            ).scalar_one_or_none()
            if user is None:
                return None
            return UserRecord(id=user.id, email=user.email, password_hash=user.password_hash, name=user.name, avatar_url=user.avatar_url)

    def get_user_by_id(self, user_id: str) -> UserRecord | None:
        with self.session_scope() as session:
            user = session.get(UserORM, user_id)
            if user is None:
                return None
            return UserRecord(id=user.id, email=user.email, password_hash=user.password_hash, name=user.name, avatar_url=user.avatar_url)

    def update_user_password(self, user_id: str, password_hash: str) -> None:
        with self.session_scope() as session:
            user = session.get(UserORM, user_id)
            if user is not None:
                user.password_hash = password_hash

    def update_user_profile(self, user_id: str, name: str | None, avatar_url: str | None) -> None:
        with self.session_scope() as session:
            user = session.get(UserORM, user_id)
            if user is not None:
                if name:
                    user.name = name
                if avatar_url:
                    user.avatar_url = avatar_url

    def get_cached_search(self, cache_key: str, max_age_seconds: int) -> list[dict] | None:
        with self.session_scope() as session:
            row = session.get(SearchCacheORM, cache_key)
            if row is None:
                return None
            age = (datetime.now(timezone.utc) - row.created_at).total_seconds()
            if age > max_age_seconds:
                return None
            return [dict(item) for item in (row.payload or [])]

    def put_cached_search(self, cache_key: str, payload: list[dict]) -> None:
        now = datetime.now(timezone.utc)
        with self.session_scope() as session:
            row = session.get(SearchCacheORM, cache_key)
            if row is None:
                session.add(SearchCacheORM(cache_key=cache_key, payload=payload, created_at=now))
            else:
                row.payload = payload
                row.created_at = now

    def get_research(self, research_id: str) -> ResearchRecord | None:
        with self.session_scope() as session:
            research = session.get(ResearchORM, research_id)
            if research is None:
                return None
            return research_orm_to_record(research)

    def delete_research(self, research_id: str) -> bool:
        with self.session_scope() as session:
            research = session.get(ResearchORM, research_id)
            if research is None:
                return False
            session.delete(research)
            return True

    def list_researches(self, limit: int = 20, user_id: str | None = None) -> list[ResearchHistoryItem]:
        with self.session_scope() as session:
            stmt = select(ResearchORM).order_by(ResearchORM.created_at.desc())
            if user_id is not None:
                stmt = stmt.where(ResearchORM.user_id == user_id)
            rows = session.execute(stmt.limit(max(1, min(limit, 100)))).scalars().all()
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
                for r in rows
            ]

    def list_active_watch_research_ids(self) -> list[str]:
        """Ids of researches with an enabled watch (server-side JSONB filter)."""
        with self.session_scope() as session:
            stmt = select(ResearchORM.id).where(
                ResearchORM.graph_state["watch"]["enabled"].astext == "true"
            )
            return [row[0] for row in session.execute(stmt).all()]

    def get_research_by_share_token(self, token: str) -> ResearchRecord | None:
        """Resolve a research from its (unguessable) public share token. Exact match only."""
        if not token:
            return None
        with self.session_scope() as session:
            stmt = select(ResearchORM).where(ResearchORM.graph_state["share_token"].astext == token)
            research = session.execute(stmt).scalars().first()
            return research_orm_to_record(research) if research else None

    def list_thread_researches(
        self, thread_id: str, user_id: str | None = None
    ) -> list[ResearchHistoryItem]:
        with self.session_scope() as session:
            stmt = (
                select(ResearchORM)
                # Match by thread_id, or by research id (a single-research thread whose
                # thread_id was never set / got lost still resolves via /thread/<id>).
                .where(or_(ResearchORM.graph_state["thread_id"].astext == thread_id, ResearchORM.id == thread_id))
                .order_by(ResearchORM.created_at.asc())
            )
            if user_id is not None:
                stmt = stmt.where(ResearchORM.user_id == user_id)
            rows = session.execute(stmt).scalars().all()
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
                for r in rows
            ]

    def set_event_notifier(self, notifier) -> None:
        """Optional callback(research_id) fired after a committed state change (SSE pub/sub)."""
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
        with self.session_scope() as session:
            research = session.get(ResearchORM, research_id)
            if research is None:
                return None

            research.status = status.value
            if report is not None:
                research.final_report = report
            research.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(research)
            result = research_orm_to_record(research)
        self._emit_change(research_id)  # after commit so SSE re-reads the new state
        return result

    def try_claim_queued_research(self, research_id: str) -> bool:
        """Atomically flip QUEUED -> PROCESSING (only if still queued). True if this caller
        won the slot — prevents two processes from promoting the same research."""
        with self.session_scope() as session:
            outcome = session.execute(
                update(ResearchORM)
                .where(ResearchORM.id == research_id, ResearchORM.status == ResearchStatus.QUEUED.value)
                .values(status=ResearchStatus.PROCESSING.value, updated_at=datetime.now(timezone.utc))
            )
            claimed = outcome.rowcount == 1
        if claimed:
            self._emit_change(research_id)
        return claimed

    def try_begin_finalization(self, research_id: str) -> bool:
        """Atomically flip into ANALYZING unless already terminal/finalizing. True if this caller
        won — prevents two replicas from enqueueing duplicate finalize jobs for one research."""
        terminal = [
            ResearchStatus.ANALYZING.value,
            ResearchStatus.COMPLETED.value,
            ResearchStatus.FAILED.value,
            ResearchStatus.CANCELLED.value,
        ]
        with self.session_scope() as session:
            outcome = session.execute(
                update(ResearchORM)
                .where(ResearchORM.id == research_id, ResearchORM.status.notin_(terminal))
                .values(status=ResearchStatus.ANALYZING.value, updated_at=datetime.now(timezone.utc))
            )
            claimed = outcome.rowcount == 1
        if claimed:
            self._emit_change(research_id)
        return claimed

    def add_task(self, task_data: dict) -> SearchTask:
        task = SearchTaskORM(
            id=task_data["id"],
            research_id=task_data.get("research_id"),
            description=task_data["description"],
            queries=task_data.get("queries", []),
            status=getattr(task_data.get("status"), "value", task_data.get("status", "pending")),
            logs=task_data.get("logs", []),
            search_metrics=task_data.get("search_metrics") or {},
        )
        with self.session_scope() as session:
            session.add(task)
            session.flush()
            statement = (
                select(SearchTaskORM)
                .options(selectinload(SearchTaskORM.results))
                .where(SearchTaskORM.id == task.id)
            )
            persisted = session.execute(statement).scalar_one()
            return search_task_orm_to_schema(persisted)

    def set_research_task_ids(
        self,
        research_id: str,
        task_ids: list[str],
    ) -> ResearchRecord | None:
        with self.session_scope() as session:
            research = session.get(ResearchORM, research_id)
            if research is None:
                return None

            research.task_ids = task_ids
            research.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(research)
            return research_orm_to_record(research)

    def update_research_graph_state(
        self,
        research_id: str,
        graph_state: dict,
    ) -> ResearchRecord | None:
        with self.session_scope() as session:
            research = session.get(ResearchORM, research_id)
            if research is None:
                return None

            research.graph_state = graph_state or {}
            research.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(research)
            return research_orm_to_record(research)

    def save_partial_report(self, research_id: str, partial: str) -> None:
        with self.session_scope() as session:
            research = session.get(ResearchORM, research_id)
            if research is None:
                return
            state = dict(research.graph_state or {})
            state["partial_report"] = partial
            research.graph_state = state
            session.flush()
        self._emit_change(research_id)

    def save_partial_reasoning(self, research_id: str, partial: str) -> None:
        with self.session_scope() as session:
            research = session.get(ResearchORM, research_id)
            if research is None:
                return
            state = dict(research.graph_state or {})
            state["partial_reasoning"] = partial
            research.graph_state = state
            session.flush()
        self._emit_change(research_id)

    def append_research_graph_event(
        self,
        research_id: str,
        event: dict,
    ) -> ResearchRecord | None:
        with self.session_scope() as session:
            research = session.get(ResearchORM, research_id)
            if research is None:
                return None

            normalized_event = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                **event,
            }
            research.graph_trail = compact_graph_trail(research.graph_trail or [], [normalized_event])
            research.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(research)
            result = research_orm_to_record(research)
        self._emit_change(research_id)
        return result

    def compact_research_graph_trails(self) -> list[str]:
        with self.session_scope() as session:
            researches = session.execute(select(ResearchORM)).scalars().all()
            compacted_ids: list[str] = []
            for research in researches:
                compacted_trail = compact_graph_trail(research.graph_trail or [], [])
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
        job = ResearchFinalizeJobORM(
            id=str(uuid.uuid4()),
            research_id=research_id,
            attempt_count=0,
            max_attempts=max_attempts,
            status=FinalizeJobStatus.PENDING.value,
        )
        with self.session_scope() as session:
            session.add(job)
            session.flush()
            session.refresh(job)
            return research_finalize_job_orm_to_schema(job)

    def get_research_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            job = session.get(ResearchFinalizeJobORM, job_id)
            if job is None:
                return None
            return research_finalize_job_orm_to_schema(job)

    def get_latest_research_finalize_job(
        self,
        research_id: str,
    ) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            statement = (
                select(ResearchFinalizeJobORM)
                .where(ResearchFinalizeJobORM.research_id == research_id)
                .order_by(ResearchFinalizeJobORM.created_at.desc())
            )
            job = session.execute(statement).scalars().first()
            if job is None:
                return None
            return research_finalize_job_orm_to_schema(job)

    def get_pending_research_finalize_jobs(self) -> list[ResearchFinalizeJob]:
        with self.session_scope() as session:
            statement = (
                select(ResearchFinalizeJobORM)
                .where(ResearchFinalizeJobORM.status == FinalizeJobStatus.PENDING.value)
                .order_by(ResearchFinalizeJobORM.created_at.asc())
            )
            jobs = session.execute(statement).scalars().all()
            return [research_finalize_job_orm_to_schema(job) for job in jobs]

    def get_running_research_finalize_jobs(self) -> list[ResearchFinalizeJob]:
        with self.session_scope() as session:
            statement = (
                select(ResearchFinalizeJobORM)
                .where(ResearchFinalizeJobORM.status == FinalizeJobStatus.RUNNING.value)
                .order_by(ResearchFinalizeJobORM.updated_at.asc())
            )
            jobs = session.execute(statement).scalars().all()
            return [research_finalize_job_orm_to_schema(job) for job in jobs]

    def get_dead_letter_research_finalize_jobs(self) -> list[ResearchFinalizeJob]:
        with self.session_scope() as session:
            statement = (
                select(ResearchFinalizeJobORM)
                .where(ResearchFinalizeJobORM.status == FinalizeJobStatus.DEAD_LETTER.value)
                .order_by(ResearchFinalizeJobORM.updated_at.asc())
            )
            jobs = session.execute(statement).scalars().all()
            return [research_finalize_job_orm_to_schema(job) for job in jobs]

    def claim_next_research_finalize_job(self) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            statement = (
                select(ResearchFinalizeJobORM)
                .where(ResearchFinalizeJobORM.status == FinalizeJobStatus.PENDING.value)
                .order_by(ResearchFinalizeJobORM.created_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            job = session.execute(statement).scalars().first()
            if job is None:
                return None

            job.status = FinalizeJobStatus.RUNNING.value
            job.attempt_count += 1
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return research_finalize_job_orm_to_schema(job)

    def claim_research_finalize_job_by_id(self, job_id: str) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            statement = (
                select(ResearchFinalizeJobORM)
                .where(
                    ResearchFinalizeJobORM.id == job_id,
                    ResearchFinalizeJobORM.status == FinalizeJobStatus.PENDING.value,
                )
                .with_for_update(skip_locked=True)
            )
            job = session.execute(statement).scalars().first()
            if job is None:
                return None

            job.status = FinalizeJobStatus.RUNNING.value
            job.attempt_count += 1
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return research_finalize_job_orm_to_schema(job)

    def update_research_finalize_job(
        self,
        job_id: str,
        status: FinalizeJobStatus,
        error: str | None = None,
    ) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            job = session.get(ResearchFinalizeJobORM, job_id)
            if job is None:
                return None

            job.status = status.value
            job.error = error
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return research_finalize_job_orm_to_schema(job)

    def record_research_finalize_job_failure(
        self,
        job_id: str,
        error: str,
    ) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            job = session.get(ResearchFinalizeJobORM, job_id)
            if job is None:
                return None

            job.error = error
            job.status = (
                FinalizeJobStatus.DEAD_LETTER.value
                if job.attempt_count >= job.max_attempts
                else FinalizeJobStatus.PENDING.value
            )
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return research_finalize_job_orm_to_schema(job)

    def requeue_research_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            job = session.get(ResearchFinalizeJobORM, job_id)
            if job is None:
                return None

            job.status = FinalizeJobStatus.PENDING.value
            job.attempt_count = 0
            job.error = None
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return research_finalize_job_orm_to_schema(job)

    def recover_stale_research_finalize_jobs(
        self,
        stale_before: datetime,
    ) -> list[ResearchFinalizeJob]:
        with self.session_scope() as session:
            statement = (
                select(ResearchFinalizeJobORM)
                .where(ResearchFinalizeJobORM.status == FinalizeJobStatus.RUNNING.value)
                .where(ResearchFinalizeJobORM.updated_at < stale_before)
            )
            jobs = session.execute(statement).scalars().all()
            recovered = []
            for job in jobs:
                job.status = FinalizeJobStatus.PENDING.value
                job.error = None
                job.updated_at = datetime.now(timezone.utc)
                recovered.append(job)
            session.flush()
            return [research_finalize_job_orm_to_schema(job) for job in recovered]

    def cleanup_old_research_finalize_jobs(
        self,
        older_than: datetime,
    ) -> list[str]:
        with self.session_scope() as session:
            statement = (
                select(ResearchFinalizeJobORM.id)
                .where(ResearchFinalizeJobORM.status.in_(
                    [FinalizeJobStatus.COMPLETED.value, FinalizeJobStatus.DEAD_LETTER.value]
                ))
                .where(ResearchFinalizeJobORM.updated_at < older_than)
            )
            job_ids = list(session.execute(statement).scalars().all())
            if not job_ids:
                return []
            session.execute(
                delete(ResearchFinalizeJobORM).where(ResearchFinalizeJobORM.id.in_(job_ids))
            )
            return job_ids

    def add_search_task_job(
        self,
        task_id: str,
        depth: str,
        max_attempts: int = 3,
    ) -> SearchTaskJob:
        job = SearchTaskJobORM(
            id=str(uuid.uuid4()),
            task_id=task_id,
            depth=depth,
            attempt_count=0,
            max_attempts=max_attempts,
            status=SearchJobStatus.PENDING.value,
        )
        with self.session_scope() as session:
            session.add(job)
            session.flush()
            session.refresh(job)
            return search_task_job_orm_to_schema(job)

    def get_search_task_job(self, job_id: str) -> SearchTaskJob | None:
        with self.session_scope() as session:
            job = session.get(SearchTaskJobORM, job_id)
            if job is None:
                return None
            return search_task_job_orm_to_schema(job)

    def get_latest_search_task_job(self, task_id: str) -> SearchTaskJob | None:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskJobORM)
                .where(SearchTaskJobORM.task_id == task_id)
                .order_by(SearchTaskJobORM.created_at.desc())
            )
            job = session.execute(statement).scalars().first()
            if job is None:
                return None
            return search_task_job_orm_to_schema(job)

    def get_pending_search_task_jobs(self) -> list[SearchTaskJob]:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskJobORM)
                .where(SearchTaskJobORM.status == SearchJobStatus.PENDING.value)
                .order_by(SearchTaskJobORM.created_at.asc())
            )
            jobs = session.execute(statement).scalars().all()
            return [search_task_job_orm_to_schema(job) for job in jobs]

    def get_running_search_task_jobs(self) -> list[SearchTaskJob]:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskJobORM)
                .where(SearchTaskJobORM.status == SearchJobStatus.RUNNING.value)
                .order_by(SearchTaskJobORM.updated_at.asc())
            )
            jobs = session.execute(statement).scalars().all()
            return [search_task_job_orm_to_schema(job) for job in jobs]

    def get_dead_letter_search_task_jobs(self) -> list[SearchTaskJob]:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskJobORM)
                .where(SearchTaskJobORM.status == SearchJobStatus.DEAD_LETTER.value)
                .order_by(SearchTaskJobORM.updated_at.asc())
            )
            jobs = session.execute(statement).scalars().all()
            return [search_task_job_orm_to_schema(job) for job in jobs]

    def claim_next_search_task_job(self) -> SearchTaskJob | None:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskJobORM)
                .where(SearchTaskJobORM.status == SearchJobStatus.PENDING.value)
                .order_by(SearchTaskJobORM.created_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            job = session.execute(statement).scalars().first()
            if job is None:
                return None

            job.status = SearchJobStatus.RUNNING.value
            job.attempt_count += 1
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return search_task_job_orm_to_schema(job)

    def claim_search_task_job_by_id(self, job_id: str) -> SearchTaskJob | None:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskJobORM)
                .where(
                    SearchTaskJobORM.id == job_id,
                    SearchTaskJobORM.status == SearchJobStatus.PENDING.value,
                )
                .with_for_update(skip_locked=True)
            )
            job = session.execute(statement).scalars().first()
            if job is None:
                return None

            job.status = SearchJobStatus.RUNNING.value
            job.attempt_count += 1
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return search_task_job_orm_to_schema(job)

    def update_search_task_job(
        self,
        job_id: str,
        status: SearchJobStatus,
        error: str | None = None,
    ) -> SearchTaskJob | None:
        with self.session_scope() as session:
            job = session.get(SearchTaskJobORM, job_id)
            if job is None:
                return None

            job.status = status.value
            job.error = error
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return search_task_job_orm_to_schema(job)

    def record_search_task_job_failure(
        self,
        job_id: str,
        error: str,
    ) -> SearchTaskJob | None:
        with self.session_scope() as session:
            job = session.get(SearchTaskJobORM, job_id)
            if job is None:
                return None

            job.error = error
            job.status = (
                SearchJobStatus.DEAD_LETTER.value
                if job.attempt_count >= job.max_attempts
                else SearchJobStatus.PENDING.value
            )
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return search_task_job_orm_to_schema(job)

    def requeue_search_task_job(self, job_id: str) -> SearchTaskJob | None:
        with self.session_scope() as session:
            job = session.get(SearchTaskJobORM, job_id)
            if job is None:
                return None

            job.status = SearchJobStatus.PENDING.value
            job.attempt_count = 0
            job.error = None
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return search_task_job_orm_to_schema(job)

    def recover_stale_search_task_jobs(
        self,
        stale_before: datetime,
    ) -> list[SearchTaskJob]:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskJobORM)
                .where(SearchTaskJobORM.status == SearchJobStatus.RUNNING.value)
                .where(SearchTaskJobORM.updated_at < stale_before)
            )
            jobs = session.execute(statement).scalars().all()
            recovered = []
            for job in jobs:
                job.status = SearchJobStatus.PENDING.value
                job.error = None
                job.updated_at = datetime.now(timezone.utc)
                recovered.append(job)
            session.flush()
            return [search_task_job_orm_to_schema(job) for job in recovered]

    def cleanup_old_search_task_jobs(
        self,
        older_than: datetime,
    ) -> list[str]:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskJobORM.id)
                .where(SearchTaskJobORM.status.in_(
                    [SearchJobStatus.COMPLETED.value, SearchJobStatus.DEAD_LETTER.value]
                ))
                .where(SearchTaskJobORM.updated_at < older_than)
            )
            job_ids = list(session.execute(statement).scalars().all())
            if not job_ids:
                return []
            session.execute(
                delete(SearchTaskJobORM).where(SearchTaskJobORM.id.in_(job_ids))
            )
            return job_ids

    def cleanup_search_cache(self, older_than: datetime) -> int:
        with self.session_scope() as session:
            result = session.execute(
                delete(SearchCacheORM).where(SearchCacheORM.created_at < older_than)
            )
            return result.rowcount or 0

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
        with self.session_scope() as session:
            heartbeat = session.get(WorkerHeartbeatORM, worker_name)
            if heartbeat is None:
                heartbeat = WorkerHeartbeatORM(worker_name=worker_name)
                session.add(heartbeat)

            heartbeat.processed_jobs = processed_jobs
            heartbeat.status = status
            heartbeat.last_error = last_error
            heartbeat.extraction_metrics = extraction_metrics or {}
            heartbeat.graph_metrics = graph_metrics or {}
            heartbeat.graph_step_events = compact_graph_step_events(
                heartbeat.graph_step_events or [],
                graph_step_events or [],
            )
            heartbeat.maintenance_summary = maintenance_summary or {}
            heartbeat.last_seen_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(heartbeat)
            return worker_heartbeat_orm_to_schema(heartbeat)

    def get_worker_heartbeat(self, worker_name: str) -> WorkerHeartbeat | None:
        with self.session_scope() as session:
            heartbeat = session.get(WorkerHeartbeatORM, worker_name)
            if heartbeat is None:
                return None
            return worker_heartbeat_orm_to_schema(heartbeat)

    def get_graph_step_events(self, worker_name: str | None = None) -> list[dict]:
        with self.session_scope() as session:
            statement = select(WorkerHeartbeatORM)
            if worker_name:
                statement = statement.where(WorkerHeartbeatORM.worker_name == worker_name)
            heartbeats = session.execute(statement).scalars().all()
            events: list[dict] = []
            for heartbeat in heartbeats:
                events.extend(heartbeat.graph_step_events or [])
            return events

    def compact_worker_graph_step_events(self) -> list[str]:
        with self.session_scope() as session:
            heartbeats = session.execute(select(WorkerHeartbeatORM)).scalars().all()
            compacted_workers: list[str] = []
            for heartbeat in heartbeats:
                compacted = compact_graph_step_events(heartbeat.graph_step_events or [], [])
                if compacted != (heartbeat.graph_step_events or []):
                    heartbeat.graph_step_events = compacted
                    compacted_workers.append(heartbeat.worker_name)
            return compacted_workers

    def get_queue_metrics(self) -> QueueMetrics:
        with self.session_scope() as session:
            def count_for(model, status_value: str) -> int:
                statement = select(func.count()).select_from(model).where(model.status == status_value)
                return session.execute(statement).scalar_one()

            extraction_metrics = ExtractionMetrics()
            graph_metrics = GraphMetrics()
            heartbeats = session.execute(select(WorkerHeartbeatORM)).scalars().all()
            for heartbeat in heartbeats:
                metrics = ExtractionMetrics.model_validate(heartbeat.extraction_metrics or {})
                extraction_metrics.attempts += metrics.attempts
                extraction_metrics.success_count += metrics.success_count
                extraction_metrics.empty_count += metrics.empty_count
                extraction_metrics.failure_count += metrics.failure_count
                extraction_metrics.downloaded_bytes += metrics.downloaded_bytes
                extraction_metrics.content_chars += metrics.content_chars
                extraction_metrics.total_download_ms += metrics.total_download_ms
                extraction_metrics.total_extract_ms += metrics.total_extract_ms
                extraction_metrics.total_post_process_ms += metrics.total_post_process_ms
                extraction_metrics.total_total_ms += metrics.total_total_ms
                runtime_metrics = GraphMetrics.model_validate(heartbeat.graph_metrics or {})
                graph_metrics.resume_count += runtime_metrics.resume_count
                graph_metrics.replan_pass_count += runtime_metrics.replan_pass_count
                graph_metrics.tie_break_pass_count += runtime_metrics.tie_break_pass_count
                graph_metrics.analyze_pass_count += runtime_metrics.analyze_pass_count
                graph_metrics.completed_run_count += runtime_metrics.completed_run_count
                for step_name, step_metrics in runtime_metrics.steps.items():
                    aggregate_step = graph_metrics.steps[step_name]
                    aggregate_step.run_count += step_metrics.run_count
                    aggregate_step.failure_count += step_metrics.failure_count
                    aggregate_step.total_ms += step_metrics.total_ms
                    aggregate_step.avg_ms = round(aggregate_step.total_ms / aggregate_step.run_count, 2) if aggregate_step.run_count > 0 else 0.0

            return QueueMetrics(
                pending_search_jobs=count_for(SearchTaskJobORM, SearchJobStatus.PENDING.value),
                running_search_jobs=count_for(SearchTaskJobORM, SearchJobStatus.RUNNING.value),
                dead_letter_search_jobs=count_for(SearchTaskJobORM, SearchJobStatus.DEAD_LETTER.value),
                pending_finalize_jobs=count_for(ResearchFinalizeJobORM, FinalizeJobStatus.PENDING.value),
                running_finalize_jobs=count_for(ResearchFinalizeJobORM, FinalizeJobStatus.RUNNING.value),
                dead_letter_finalize_jobs=count_for(ResearchFinalizeJobORM, FinalizeJobStatus.DEAD_LETTER.value),
                extraction_metrics=extraction_metrics,
                graph_metrics=graph_metrics,
            )

    def get_task(self, task_id: str) -> SearchTask | None:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskORM)
                .options(selectinload(SearchTaskORM.results))
                .where(SearchTaskORM.id == task_id)
            )
            task = session.execute(statement).scalar_one_or_none()
            if task is None:
                return None
            return search_task_orm_to_schema(task)

    def get_all_tasks(self, limit: int = 100, offset: int = 0) -> list[SearchTask]:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskORM)
                .options(selectinload(SearchTaskORM.results))
                .order_by(SearchTaskORM.created_at.desc())
                .limit(limit)
                .offset(offset)
            )
            tasks = session.execute(statement).scalars().all()
            return [search_task_orm_to_schema(task) for task in tasks]

    def get_tasks_by_research(self, research_id: str) -> list[SearchTask]:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskORM)
                .options(selectinload(SearchTaskORM.results))
                .where(SearchTaskORM.research_id == research_id)
            )
            tasks = session.execute(statement).scalars().all()
            return [search_task_orm_to_schema(task) for task in tasks]

    def update_task(self, task_id: str, update: TaskUpdate) -> SearchTask | None:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskORM)
                .options(selectinload(SearchTaskORM.results))
                .where(SearchTaskORM.id == task_id)
            )
            task = session.execute(statement).scalar_one_or_none()
            if task is None:
                return None

            if update.status is not None:
                task.status = update.status.value
            if update.result is not None:
                task.results = search_result_dicts_to_orm(task_id, update.result)
            if update.search_metrics is not None:
                task.search_metrics = update.search_metrics.model_dump()
            if update.log:
                task.logs = [*task.logs, update.log]

            task.updated_at = datetime.now(timezone.utc)
            session.flush()
            # Serialize the in-memory row directly: it was just loaded with its results and
            # mutated here, so a refresh + second selectinload query (AUD-021) is redundant.
            # update_task runs once per extraction log line, so that doubled every log write.
            return search_task_orm_to_schema(task)

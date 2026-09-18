import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Callable

from sqlalchemy import and_, case, delete, func, or_, select, text, update
from sqlalchemy.orm import Session, selectinload

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
from src.db.models import (
    AdminAuditLogORM,
    LLMUsageLogORM,
    ResearchFinalizeJobORM,
    ResearchORM,
    SearchCacheORM,
    SearchTaskJobORM,
    SearchTaskORM,
    UserORM,
    WorkerHeartbeatORM,
)
from src.core.graph_history import compact_graph_step_events, compact_graph_trail
from src.repositories.mappers import (
    research_finalize_job_orm_to_schema,
    research_orm_to_record,
    search_task_job_orm_to_schema,
    search_result_dicts_to_orm,
    search_task_orm_to_schema,
    worker_heartbeat_orm_to_schema,
)


class SQLAlchemyTaskStore:
    _ADMISSION_LOCK_KEY = 1_297_304_387
    _THREAD_HISTORY_LIMIT = 200

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

    def ping(self) -> bool:
        """Lightweight DB connectivity check for /health (AUD-036)."""
        try:
            with self.session_scope() as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def add_research(
        self,
        request: ResearchRequest,
        task_ids: list[str],
        user_id: str | None = None,
        language: str = "unknown",
    ) -> ResearchRecord:
        research = ResearchORM(
            id=str(uuid.uuid4()),
            prompt=request.prompt,
            language=language,
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

    def _lock_admission(self, session: Session) -> None:
        # Serialize count-and-transition across API replicas. The lock is scoped
        # to this transaction, so it also works with transaction-pooled connections.
        session.execute(select(func.pg_advisory_xact_lock(self._ADMISSION_LOCK_KEY)))

    @staticmethod
    def _active_count(
        session: Session,
        user_id: str | None,
        stale_before: datetime,
        *,
        include_queued: bool,
        exclude_id: str | None = None,
        include_parked: bool = True,
    ) -> int:
        running = and_(
            ResearchORM.status.in_(
                [ResearchStatus.PROCESSING.value, ResearchStatus.ANALYZING.value]
            ),
            ResearchORM.updated_at >= stale_before,
        )
        if include_queued:
            status_filter = or_(running, ResearchORM.status == ResearchStatus.QUEUED.value)
            if include_parked:
                # Parked plan-first researches hold the user's slot while fresh, so a
                # user cannot park N uncounted plans and then activate them all at once.
                parked = and_(
                    ResearchORM.status.in_(
                        [ResearchStatus.CLARIFYING.value, ResearchStatus.PLAN_REVIEW.value]
                    ),
                    ResearchORM.updated_at >= stale_before,
                )
                status_filter = or_(status_filter, parked)
        else:
            status_filter = running
        statement = select(func.count()).select_from(ResearchORM).where(status_filter)
        if user_id is not None:
            statement = statement.where(ResearchORM.user_id == user_id)
        if exclude_id is not None:
            statement = statement.where(ResearchORM.id != exclude_id)
        return int(session.execute(statement).scalar_one())

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
        with self.session_scope() as session:
            self._lock_admission(session)
            if (
                per_user_limit > 0
                and self._active_count(
                    session, user_id, stale_before, include_queued=True
                ) >= per_user_limit
            ):
                return None
            queued = (
                global_limit > 0
                and self._active_count(
                    session, None, stale_before, include_queued=False
                ) >= global_limit
            )
            research = ResearchORM(
                id=str(uuid.uuid4()),
                prompt=request.prompt,
                language=language,
                user_id=user_id,
                depth=request.depth.value,
                status=(ResearchStatus.QUEUED if queued else ResearchStatus.PROCESSING).value,
                task_ids=task_ids,
                graph_state=graph_state,
            )
            session.add(research)
            session.flush()
            session.refresh(research)
            return research_orm_to_record(research)

    def try_admit_research(
        self,
        research_id: str,
        expected_status: ResearchStatus,
        per_user_limit: int,
        global_limit: int,
        stale_before: datetime,
    ) -> bool:
        with self.session_scope() as session:
            self._lock_admission(session)
            research = session.get(ResearchORM, research_id)
            if research is None or research.status != expected_status.value:
                return False
            # The parked research being admitted must not count against itself, and
            # activation needs running capacity only — parked siblings wait on the
            # user and consume nothing, so they must not block each other.
            if (
                per_user_limit > 0
                and self._active_count(
                    session,
                    research.user_id,
                    stale_before,
                    include_queued=True,
                    exclude_id=research_id,
                    include_parked=False,
                ) >= per_user_limit
            ):
                return False
            if (
                global_limit > 0
                and self._active_count(
                    session, None, stale_before, include_queued=False, exclude_id=research_id
                ) >= global_limit
            ):
                return False
            research.status = ResearchStatus.PROCESSING.value
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
        user = UserORM(
            id=user_id,
            email=email.strip().lower(),
            password_hash=password_hash,
            google_subject=google_subject,
        )
        with self.session_scope() as session:
            session.add(user)
            session.flush()
            return UserRecord(
                id=user.id,
                email=user.email,
                password_hash=user.password_hash,
                google_subject=user.google_subject,
                token_version=user.token_version,
                name=user.name,
                avatar_url=user.avatar_url,
            )

    def get_user_by_email(self, email: str) -> UserRecord | None:
        with self.session_scope() as session:
            user = session.execute(
                select(UserORM).where(UserORM.email == email.strip().lower())
            ).scalar_one_or_none()
            if user is None:
                return None
            return UserRecord(
                id=user.id,
                email=user.email,
                password_hash=user.password_hash,
                google_subject=user.google_subject,
                token_version=user.token_version,
                name=user.name,
                avatar_url=user.avatar_url,
            )

    def get_user_by_id(self, user_id: str) -> UserRecord | None:
        with self.session_scope() as session:
            user = session.get(UserORM, user_id)
            if user is None:
                return None
            return UserRecord(
                id=user.id,
                email=user.email,
                password_hash=user.password_hash,
                google_subject=user.google_subject,
                token_version=user.token_version,
                name=user.name,
                avatar_url=user.avatar_url,
            )

    def get_user_by_google_subject(self, google_subject: str) -> UserRecord | None:
        with self.session_scope() as session:
            statement = select(UserORM).where(UserORM.google_subject == google_subject)
            user = session.execute(statement).scalar_one_or_none()
            if user is None:
                return None
            return UserRecord(
                id=user.id,
                email=user.email,
                password_hash=user.password_hash,
                google_subject=user.google_subject,
                token_version=user.token_version,
                name=user.name,
                avatar_url=user.avatar_url,
            )

    def delete_user(self, user_id: str) -> bool:
        """Delete a user; researches (and via FK cascades: tasks, results, jobs,
        share tokens inside graph_state) are removed with them (DATA-LIFECYCLE)."""
        with self.session_scope() as session:
            user = session.get(UserORM, user_id)
            if user is None:
                return False
            session.delete(user)
            return True

    def update_user_password(self, user_id: str, password_hash: str) -> UserRecord | None:
        with self.session_scope() as session:
            statement = (
                update(UserORM)
                .where(UserORM.id == user_id)
                .values(
                    password_hash=password_hash,
                    token_version=UserORM.token_version + 1,
                )
                .returning(UserORM)
            )
            user = session.execute(statement).scalar_one_or_none()
            if user is None:
                return None
            return UserRecord(
                id=user.id,
                email=user.email,
                password_hash=user.password_hash,
                google_subject=user.google_subject,
                token_version=user.token_version,
                name=user.name,
                avatar_url=user.avatar_url,
            )

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

    # Retention (OPS-RETENTION): terminal researches past the window are removed
    # with their tasks/results via the existing ON DELETE CASCADEs.
    _TERMINAL_RESEARCH_STATUSES = (
        ResearchStatus.COMPLETED,
        ResearchStatus.FAILED,
        ResearchStatus.CANCELLED,
    )

    def cleanup_old_researches(self, older_than: datetime) -> list[str]:
        terminal = [status.value for status in self._TERMINAL_RESEARCH_STATUSES]
        with self.session_scope() as session:
            research_ids = session.execute(
                select(ResearchORM.id).where(
                    ResearchORM.status.in_(terminal),
                    ResearchORM.updated_at < older_than,
                )
            ).scalars().all()
            if research_ids:
                session.execute(delete(ResearchORM).where(ResearchORM.id.in_(research_ids)))
            return list(research_ids)

    @staticmethod
    def _research_history_select():
        return select(
            ResearchORM.id,
            ResearchORM.prompt,
            ResearchORM.graph_state["title"].astext.label("title"),
            ResearchORM.graph_state["thread_id"].astext.label("thread_id"),
            ResearchORM.depth,
            ResearchORM.status,
            ResearchORM.created_at,
            ResearchORM.updated_at,
            ResearchORM.final_report.is_not(None).label("has_final_report"),
        )

    @staticmethod
    def _history_item(row) -> ResearchHistoryItem:
        return ResearchHistoryItem(
            id=row.id,
            prompt=row.prompt,
            title=row.title,
            thread_id=row.thread_id,
            depth=row.depth,
            status=row.status,
            created_at=row.created_at,
            updated_at=row.updated_at,
            has_final_report=row.has_final_report,
        )

    def list_researches(self, limit: int = 20, user_id: str | None = None) -> list[ResearchHistoryItem]:
        with self.session_scope() as session:
            stmt = self._research_history_select().order_by(ResearchORM.created_at.desc())
            if user_id is not None:
                stmt = stmt.where(ResearchORM.user_id == user_id)
            rows = session.execute(stmt.limit(max(1, min(limit, 100)))).all()
            return [self._history_item(row) for row in rows]

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
                self._research_history_select()
                # Match by thread_id, or by research id (a single-research thread whose
                # thread_id was never set / got lost still resolves via /thread/<id>).
                .where(or_(ResearchORM.graph_state["thread_id"].astext == thread_id, ResearchORM.id == thread_id))
                .order_by(ResearchORM.created_at.asc())
            )
            if user_id is not None:
                stmt = stmt.where(ResearchORM.user_id == user_id)
            rows = session.execute(stmt.limit(self._THREAD_HISTORY_LIMIT)).all()
            return [self._history_item(row) for row in rows]

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
                research.partial_report = None
                research.partial_reasoning = None
            research.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(research)
            result = research_orm_to_record(research)
        self._emit_change(research_id)  # after commit so SSE re-reads the new state
        return result

    def try_claim_queued_research(
        self,
        research_id: str,
        global_limit: int = 0,
        stale_before: datetime | None = None,
    ) -> bool:
        """Atomically claim QUEUED only while a global running slot is available."""
        with self.session_scope() as session:
            self._lock_admission(session)
            research = session.get(ResearchORM, research_id)
            if research is None or research.status != ResearchStatus.QUEUED.value:
                return False
            cutoff = stale_before or datetime.min.replace(tzinfo=timezone.utc)
            if (
                global_limit > 0
                and self._active_count(session, None, cutoff, include_queued=False) >= global_limit
            ):
                return False
            research.status = ResearchStatus.PROCESSING.value
            research.updated_at = datetime.now(timezone.utc)
            claimed = True
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

    def merge_research_graph_state(
        self,
        research_id: str,
        patch: dict | None = None,
        *,
        remove_keys: list[str] | None = None,
    ) -> ResearchRecord | None:
        """Atomically merge `patch` into graph_state (and drop `remove_keys`) under a row
        lock, so concurrent writers can't clobber each other's keys (AUD-014). Replaces
        the read-then-write pattern, which must not be reintroduced."""
        patch = patch or {}
        remove_keys = remove_keys or []
        if not patch and not remove_keys:
            return self.get_research(research_id)
        with self.session_scope() as session:
            research = session.execute(
                select(ResearchORM).where(ResearchORM.id == research_id).with_for_update()
            ).scalar_one_or_none()
            if research is None:
                return None
            merged = {**(research.graph_state or {}), **patch}
            for key in remove_keys:
                merged.pop(key, None)
            research.graph_state = merged
            research.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(research)
            return research_orm_to_record(research)

    def save_partial_report(self, research_id: str, partial: str) -> None:
        with self.session_scope() as session:
            outcome = session.execute(
                update(ResearchORM)
                .where(ResearchORM.id == research_id)
                .values(partial_report=partial)
            )
            if outcome.rowcount == 0:
                return
        self._emit_change(research_id)

    def save_partial_reasoning(self, research_id: str, partial: str) -> None:
        with self.session_scope() as session:
            outcome = session.execute(
                update(ResearchORM)
                .where(ResearchORM.id == research_id)
                .values(partial_reasoning=partial)
            )
            if outcome.rowcount == 0:
                return
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
        # Bounded working set (OPS-RETENTION): only recently-active researches still grow a
        # trail, so older rows are skipped; fetching id+graph_trail in batches avoids
        # loading full rows (report blobs, graph_state) into worker memory each pass.
        from src.config import settings as _settings

        horizon = datetime.now(timezone.utc) - timedelta(
            seconds=_settings.graph_trail_retention_seconds
        )
        compacted_ids: list[str] = []
        with self.session_scope() as session:
            statement = (
                select(ResearchORM.id, ResearchORM.graph_trail)
                .where(ResearchORM.updated_at >= horizon)
                .execution_options(yield_per=200)
            )
            for research_id, graph_trail in session.execute(statement):
                compacted_trail = compact_graph_trail(graph_trail or [], [])
                if compacted_trail != (graph_trail or []):
                    session.execute(
                        update(ResearchORM)
                        .where(ResearchORM.id == research_id)
                        .values(
                            graph_trail=compacted_trail,
                            updated_at=datetime.now(timezone.utc),
                        )
                    )
                    compacted_ids.append(research_id)
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

    def get_research_finalize_job(
        self,
        job_id: str,
        user_id: str | None = None,
    ) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            statement = select(ResearchFinalizeJobORM).where(ResearchFinalizeJobORM.id == job_id)
            if user_id is not None:
                statement = statement.join(
                    ResearchORM,
                    ResearchFinalizeJobORM.research_id == ResearchORM.id,
                ).where(ResearchORM.user_id == user_id)
            job = session.execute(statement).scalar_one_or_none()
            if job is None:
                return None
            return research_finalize_job_orm_to_schema(job)

    def get_latest_research_finalize_job(
        self,
        research_id: str,
        user_id: str | None = None,
    ) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            statement = (
                select(ResearchFinalizeJobORM)
                .where(ResearchFinalizeJobORM.research_id == research_id)
                .order_by(ResearchFinalizeJobORM.created_at.desc())
            )
            if user_id is not None:
                statement = statement.join(
                    ResearchORM,
                    ResearchFinalizeJobORM.research_id == ResearchORM.id,
                ).where(ResearchORM.user_id == user_id)
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

    def renew_research_finalize_job_lease(self, job_id: str, lease_epoch: int) -> bool:
        with self.session_scope() as session:
            statement = (
                update(ResearchFinalizeJobORM)
                .where(
                    ResearchFinalizeJobORM.id == job_id,
                    ResearchFinalizeJobORM.status == FinalizeJobStatus.RUNNING.value,
                    ResearchFinalizeJobORM.lease_epoch == lease_epoch,
                )
                .values(updated_at=datetime.now(timezone.utc))
                .returning(ResearchFinalizeJobORM.id)
            )
            return session.execute(statement).scalar_one_or_none() is not None

    def complete_research_finalize_job(
        self,
        job_id: str,
        research_id: str,
        lease_epoch: int,
        report: str,
    ) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            statement = (
                select(ResearchFinalizeJobORM)
                .where(
                    ResearchFinalizeJobORM.id == job_id,
                    ResearchFinalizeJobORM.research_id == research_id,
                    ResearchFinalizeJobORM.status == FinalizeJobStatus.RUNNING.value,
                    ResearchFinalizeJobORM.lease_epoch == lease_epoch,
                )
                .with_for_update()
            )
            job = session.execute(statement).scalar_one_or_none()
            if job is None:
                return None
            research = session.get(ResearchORM, research_id, with_for_update=True)
            if research is None:
                return None

            if research.status != ResearchStatus.CANCELLED.value:
                research.status = ResearchStatus.COMPLETED.value
                research.final_report = report
                research.partial_report = None
                research.partial_reasoning = None
                research.updated_at = datetime.now(timezone.utc)
            job.status = FinalizeJobStatus.COMPLETED.value
            job.error = None
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            result = research_finalize_job_orm_to_schema(job)
        self._emit_change(research_id)
        return result

    def update_research_finalize_job(
        self,
        job_id: str,
        status: FinalizeJobStatus,
        error: str | None = None,
        lease_epoch: int | None = None,
    ) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            statement = update(ResearchFinalizeJobORM).where(
                ResearchFinalizeJobORM.id == job_id
            )
            if lease_epoch is not None:
                statement = statement.where(
                    ResearchFinalizeJobORM.status == FinalizeJobStatus.RUNNING.value,
                    ResearchFinalizeJobORM.lease_epoch == lease_epoch,
                )
            statement = statement.values(
                status=status.value,
                error=error,
                updated_at=datetime.now(timezone.utc),
            ).returning(ResearchFinalizeJobORM)
            job = session.execute(statement).scalar_one_or_none()
            if job is None:
                return None
            return research_finalize_job_orm_to_schema(job)

    def record_research_finalize_job_failure(
        self,
        job_id: str,
        error: str,
        lease_epoch: int | None = None,
    ) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            statement = update(ResearchFinalizeJobORM).where(
                ResearchFinalizeJobORM.id == job_id
            )
            if lease_epoch is not None:
                statement = statement.where(
                    ResearchFinalizeJobORM.status == FinalizeJobStatus.RUNNING.value,
                    ResearchFinalizeJobORM.lease_epoch == lease_epoch,
                )
            statement = statement.values(
                error=error,
                status=case(
                    (
                        ResearchFinalizeJobORM.attempt_count
                        >= ResearchFinalizeJobORM.max_attempts,
                        FinalizeJobStatus.DEAD_LETTER.value,
                    ),
                    else_=FinalizeJobStatus.PENDING.value,
                ),
                updated_at=datetime.now(timezone.utc),
            ).returning(ResearchFinalizeJobORM)
            job = session.execute(statement).scalar_one_or_none()
            if job is None:
                return None
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
                update(ResearchFinalizeJobORM)
                .where(
                    ResearchFinalizeJobORM.status == FinalizeJobStatus.RUNNING.value,
                    ResearchFinalizeJobORM.updated_at < stale_before,
                )
                .values(
                    status=FinalizeJobStatus.PENDING.value,
                    lease_epoch=ResearchFinalizeJobORM.lease_epoch + 1,
                    error=None,
                    updated_at=datetime.now(timezone.utc),
                )
                .returning(ResearchFinalizeJobORM)
            )
            jobs = session.execute(statement).scalars().all()
            jobs.sort(key=lambda item: item.created_at)
            return [research_finalize_job_orm_to_schema(job) for job in jobs]

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

    def get_search_task_job(
        self,
        job_id: str,
        user_id: str | None = None,
    ) -> SearchTaskJob | None:
        with self.session_scope() as session:
            statement = select(SearchTaskJobORM).where(SearchTaskJobORM.id == job_id)
            if user_id is not None:
                statement = (
                    statement
                    .join(SearchTaskORM, SearchTaskJobORM.task_id == SearchTaskORM.id)
                    .join(ResearchORM, SearchTaskORM.research_id == ResearchORM.id)
                    .where(ResearchORM.user_id == user_id)
                )
            job = session.execute(statement).scalar_one_or_none()
            if job is None:
                return None
            return search_task_job_orm_to_schema(job)

    def get_latest_search_task_job(
        self,
        task_id: str,
        user_id: str | None = None,
    ) -> SearchTaskJob | None:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskJobORM)
                .where(SearchTaskJobORM.task_id == task_id)
                .order_by(SearchTaskJobORM.created_at.desc())
            )
            if user_id is not None:
                statement = (
                    statement
                    .join(SearchTaskORM, SearchTaskJobORM.task_id == SearchTaskORM.id)
                    .join(ResearchORM, SearchTaskORM.research_id == ResearchORM.id)
                    .where(ResearchORM.user_id == user_id)
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

    def get_task(self, task_id: str, user_id: str | None = None) -> SearchTask | None:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskORM)
                .options(selectinload(SearchTaskORM.results))
                .where(SearchTaskORM.id == task_id)
            )
            if user_id is not None:
                statement = statement.join(
                    ResearchORM,
                    SearchTaskORM.research_id == ResearchORM.id,
                ).where(ResearchORM.user_id == user_id)
            task = session.execute(statement).scalar_one_or_none()
            if task is None:
                return None
            return search_task_orm_to_schema(task)

    def get_all_tasks(
        self,
        limit: int = 100,
        offset: int = 0,
        user_id: str | None = None,
    ) -> list[SearchTask]:
        with self.session_scope() as session:
            statement = select(SearchTaskORM).options(selectinload(SearchTaskORM.results))
            if user_id is not None:
                statement = statement.join(
                    ResearchORM,
                    SearchTaskORM.research_id == ResearchORM.id,
                ).where(ResearchORM.user_id == user_id)
            statement = (
                statement
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

    def update_task(
        self,
        task_id: str,
        update: TaskUpdate,
        user_id: str | None = None,
    ) -> SearchTask | None:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskORM)
                .options(selectinload(SearchTaskORM.results))
                .where(SearchTaskORM.id == task_id)
            )
            if user_id is not None:
                statement = statement.join(
                    ResearchORM,
                    SearchTaskORM.research_id == ResearchORM.id,
                ).where(ResearchORM.user_id == user_id)
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
        with self.session_scope() as session:
            usage_id = str(uuid.uuid4())
            record = LLMUsageLogORM(
                id=usage_id,
                research_id=research_id,
                user_id=user_id,
                model=model,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
                estimated_cost_usd=estimated_cost_usd,
                created_at=datetime.now(timezone.utc),
            )
            session.add(record)
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
        with self.session_scope() as session:
            audit_id = str(uuid.uuid4())
            record = AdminAuditLogORM(
                id=audit_id,
                actor_email=actor_email,
                action=action,
                target_type=target_type,
                target_id=target_id,
                details=details or {},
                ip_address=ip_address,
                created_at=datetime.now(timezone.utc),
            )
            session.add(record)
            return audit_id

    def get_admin_audit_logs(
        self,
        limit: int = 50,
        offset: int = 0,
        action: str | None = None,
        actor_email: str | None = None,
    ) -> list[AdminAuditLogItem]:
        with self.session_scope() as session:
            stmt = select(AdminAuditLogORM)
            if action:
                stmt = stmt.where(AdminAuditLogORM.action == action)
            if actor_email:
                stmt = stmt.where(AdminAuditLogORM.actor_email == actor_email)
            stmt = stmt.order_by(AdminAuditLogORM.created_at.desc()).limit(limit).offset(offset)
            records = session.execute(stmt).scalars().all()
            return [
                AdminAuditLogItem(
                    id=r.id,
                    actor_email=r.actor_email,
                    action=r.action,
                    target_type=r.target_type,
                    target_id=r.target_id,
                    details=r.details or {},
                    ip_address=r.ip_address,
                    created_at=r.created_at,
                )
                for r in records
            ]

    def get_admin_token_analytics(
        self,
        page: int = 1,
        page_size: int = 20,
    ) -> AdminTokenAnalyticsResponse:
        with self.session_scope() as session:
            # 1. Totals
            tot_stmt = select(
                func.coalesce(func.sum(LLMUsageLogORM.prompt_tokens), 0),
                func.coalesce(func.sum(LLMUsageLogORM.completion_tokens), 0),
                func.coalesce(func.sum(LLMUsageLogORM.total_tokens), 0),
                func.coalesce(func.sum(LLMUsageLogORM.estimated_cost_usd), 0.0),
            )
            tot_row = session.execute(tot_stmt).one()
            total_prompt = int(tot_row[0])
            total_comp = int(tot_row[1])
            total_tok = int(tot_row[2])
            total_cost = round(float(tot_row[3]), 4)

            # 2. By Model
            model_stmt = (
                select(
                    LLMUsageLogORM.model,
                    func.coalesce(func.sum(LLMUsageLogORM.prompt_tokens), 0),
                    func.coalesce(func.sum(LLMUsageLogORM.completion_tokens), 0),
                    func.coalesce(func.sum(LLMUsageLogORM.total_tokens), 0),
                    func.coalesce(func.sum(LLMUsageLogORM.estimated_cost_usd), 0.0),
                    func.count(LLMUsageLogORM.id),
                )
                .group_by(LLMUsageLogORM.model)
                .order_by(func.sum(LLMUsageLogORM.total_tokens).desc())
            )
            by_model = [
                AdminTokenModelBreakdown(
                    model=row[0],
                    prompt_tokens=int(row[1]),
                    completion_tokens=int(row[2]),
                    total_tokens=int(row[3]),
                    estimated_cost_usd=round(float(row[4]), 4),
                    calls_count=int(row[5]),
                )
                for row in session.execute(model_stmt).all()
            ]

            # 3. By Depth
            depth_stmt = (
                select(
                    ResearchORM.depth,
                    func.coalesce(func.sum(LLMUsageLogORM.total_tokens), 0),
                    func.coalesce(func.sum(LLMUsageLogORM.estimated_cost_usd), 0.0),
                    func.count(func.distinct(ResearchORM.id)),
                )
                .join(ResearchORM, LLMUsageLogORM.research_id == ResearchORM.id)
                .group_by(ResearchORM.depth)
            )
            by_depth = [
                AdminTokenDepthBreakdown(
                    depth=row[0],
                    total_tokens=int(row[1]),
                    estimated_cost_usd=round(float(row[2]), 4),
                    researches_count=int(row[3]),
                )
                for row in session.execute(depth_stmt).all()
            ]

            # 4. Researches list with pagination
            total_researches_stmt = select(func.count()).select_from(ResearchORM)
            total_researches = session.execute(total_researches_stmt).scalar_one()

            offset = max(0, (page - 1) * page_size)
            researches_stmt = (
                select(
                    ResearchORM.id,
                    ResearchORM.prompt,
                    ResearchORM.depth,
                    ResearchORM.status,
                    ResearchORM.created_at,
                    func.coalesce(func.sum(LLMUsageLogORM.total_tokens), 0),
                    func.coalesce(func.sum(LLMUsageLogORM.estimated_cost_usd), 0.0),
                    ResearchORM.graph_state,
                )
                .outerjoin(LLMUsageLogORM, LLMUsageLogORM.research_id == ResearchORM.id)
                .group_by(ResearchORM.id)
                .order_by(ResearchORM.created_at.desc())
                .limit(page_size)
                .offset(offset)
            )
            research_items: list[AdminTokenResearchUsageItem] = []
            for row in session.execute(researches_stmt).all():
                r_id, r_prompt, r_depth, r_status, r_created, r_tokens, r_cost, r_state = row
                tokens = int(r_tokens)
                cost = float(r_cost)
                if tokens == 0 and isinstance(r_state, dict):
                    legacy = r_state.get("llm_token_usage") or {}
                    if legacy:
                        tokens = int(legacy.get("total_tokens", 0) or 0)
                        cost = float(legacy.get("estimated_cost_usd", 0.0) or 0.0)
                research_items.append(
                    AdminTokenResearchUsageItem(
                        research_id=r_id,
                        prompt=r_prompt,
                        depth=r_depth,
                        status=r_status,
                        total_tokens=tokens,
                        estimated_cost_usd=round(cost, 4),
                        created_at=r_created,
                    )
                )

            return AdminTokenAnalyticsResponse(
                total_prompt_tokens=total_prompt,
                total_completion_tokens=total_comp,
                total_tokens=total_tok,
                total_cost_usd=total_cost,
                by_model=by_model,
                by_depth=by_depth,
                researches=research_items,
                total_researches=total_researches,
                page=page,
                page_size=page_size,
            )

    def get_admin_overview(self) -> AdminOverviewResponse:
        from src.config import settings

        with self.session_scope() as session:
            # Active researches
            active_cnt_stmt = (
                select(func.count())
                .select_from(ResearchORM)
                .where(ResearchORM.status == ResearchStatus.PROCESSING.value)
            )
            active_researches = session.execute(active_cnt_stmt).scalar_one()

            # Pending & failed tasks
            pending_tasks_stmt = (
                select(func.count())
                .select_from(SearchTaskJobORM)
                .where(SearchTaskJobORM.status == SearchJobStatus.PENDING.value)
            )
            pending_tasks = session.execute(pending_tasks_stmt).scalar_one()

            failed_tasks_stmt = (
                select(func.count())
                .select_from(SearchTaskJobORM)
                .where(SearchTaskJobORM.status.in_([SearchJobStatus.DEAD_LETTER.value, "failed"]))
            )
            failed_tasks = session.execute(failed_tasks_stmt).scalar_one()

            # Workers
            heartbeats = session.execute(select(WorkerHeartbeatORM)).scalars().all()
            now = datetime.now(timezone.utc)
            worker_items: list[AdminWorkerFleetItem] = []
            for hb in heartbeats:
                age_seconds = (now - hb.last_seen_at).total_seconds()
                is_alive = age_seconds < 60.0
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

            system_health = {
                "postgres": "ok",
                "overall": "healthy" if failed_tasks == 0 else "degraded",
            }

            return AdminOverviewResponse(
                system_health=system_health,
                active_researches_count=active_researches,
                pending_tasks_count=pending_tasks,
                failed_tasks_count=failed_tasks,
                workers=worker_items,
                is_dev_mode=settings.auth_disabled,
            )

    def preview_maintenance_action(
        self,
        action: str,
        params: dict | None = None,
    ) -> AdminDryRunResult:
        params = params or {}
        with self.session_scope() as session:
            now = datetime.now(timezone.utc)
            if action == "recover_stale_finalize_jobs":
                stale_seconds = int(params.get("stale_seconds", 300))
                stale_before = now.timestamp() - stale_seconds
                stale_dt = datetime.fromtimestamp(stale_before, tz=timezone.utc)
                stmt = (
                    select(ResearchFinalizeJobORM.id)
                    .where(ResearchFinalizeJobORM.status == FinalizeJobStatus.RUNNING.value)
                    .where(ResearchFinalizeJobORM.updated_at < stale_dt)
                )
                ids = list(session.execute(stmt).scalars().all())
                return AdminDryRunResult(
                    action=action,
                    dry_run=True,
                    affected_count=len(ids),
                    sample_affected_ids=ids[:10],
                    summary=f"Would recover {len(ids)} stale finalize jobs running before {stale_dt.isoformat()}",
                )

            elif action == "recover_stale_search_jobs":
                stale_seconds = int(params.get("stale_seconds", 300))
                stale_before = now.timestamp() - stale_seconds
                stale_dt = datetime.fromtimestamp(stale_before, tz=timezone.utc)
                stmt = (
                    select(SearchTaskJobORM.id)
                    .where(SearchTaskJobORM.status == SearchJobStatus.RUNNING.value)
                    .where(SearchTaskJobORM.updated_at < stale_dt)
                )
                ids = list(session.execute(stmt).scalars().all())
                return AdminDryRunResult(
                    action=action,
                    dry_run=True,
                    affected_count=len(ids),
                    sample_affected_ids=ids[:10],
                    summary=f"Would recover {len(ids)} stale search jobs running before {stale_dt.isoformat()}",
                )

            elif action == "cleanup_old_jobs":
                days = int(params.get("days", 7))
                cutoff = datetime.fromtimestamp(now.timestamp() - days * 86400, tz=timezone.utc)
                stmt1 = select(ResearchFinalizeJobORM.id).where(
                    ResearchFinalizeJobORM.status.in_([FinalizeJobStatus.COMPLETED.value, FinalizeJobStatus.DEAD_LETTER.value]),
                    ResearchFinalizeJobORM.updated_at < cutoff,
                )
                stmt2 = select(SearchTaskJobORM.id).where(
                    SearchTaskJobORM.status.in_([SearchJobStatus.COMPLETED.value, SearchJobStatus.DEAD_LETTER.value]),
                    SearchTaskJobORM.updated_at < cutoff,
                )
                f_ids = list(session.execute(stmt1).scalars().all())
                s_ids = list(session.execute(stmt2).scalars().all())
                all_ids = f_ids + s_ids
                return AdminDryRunResult(
                    action=action,
                    dry_run=True,
                    affected_count=len(all_ids),
                    sample_affected_ids=all_ids[:10],
                    summary=f"Would delete {len(f_ids)} finalize and {len(s_ids)} search jobs older than {days} days",
                )

            elif action == "cleanup_search_cache":
                days = int(params.get("days", 3))
                cutoff = datetime.fromtimestamp(now.timestamp() - days * 86400, tz=timezone.utc)
                stmt = select(func.count()).select_from(SearchCacheORM).where(SearchCacheORM.created_at < cutoff)
                count = session.execute(stmt).scalar_one()
                return AdminDryRunResult(
                    action=action,
                    dry_run=True,
                    affected_count=count,
                    sample_affected_ids=[],
                    summary=f"Would delete {count} cached search entries older than {days} days",
                )

            elif action in ("requeue_finalize_job", "requeue_search_job"):
                target_id = params.get("target_id")
                return AdminDryRunResult(
                    action=action,
                    dry_run=True,
                    affected_count=1 if target_id else 0,
                    sample_affected_ids=[target_id] if target_id else [],
                    summary=f"Would requeue job {target_id}",
                )

            return AdminDryRunResult(
                action=action,
                dry_run=True,
                affected_count=0,
                sample_affected_ids=[],
                summary=f"Unknown maintenance action: {action}",
            )

    def execute_maintenance_action(
        self,
        action: str,
        actor_email: str,
        params: dict | None = None,
        ip_address: str | None = None,
    ) -> AdminDryRunResult:
        params = params or {}
        now = datetime.now(timezone.utc)
        affected_count = 0
        sample_ids: list[str] = []
        summary = ""

        if action == "recover_stale_finalize_jobs":
            stale_seconds = int(params.get("stale_seconds", 300))
            stale_dt = datetime.fromtimestamp(now.timestamp() - stale_seconds, tz=timezone.utc)
            recovered = self.recover_stale_research_finalize_jobs(stale_dt)
            affected_count = len(recovered)
            sample_ids = [j.id for j in recovered[:10]]
            summary = f"Recovered {affected_count} stale finalize jobs"

        elif action == "recover_stale_search_jobs":
            stale_seconds = int(params.get("stale_seconds", 300))
            stale_dt = datetime.fromtimestamp(now.timestamp() - stale_seconds, tz=timezone.utc)
            recovered = self.recover_stale_search_task_jobs(stale_dt)
            affected_count = len(recovered)
            sample_ids = [j.id for j in recovered[:10]]
            summary = f"Recovered {affected_count} stale search jobs"

        elif action == "cleanup_old_jobs":
            days = int(params.get("days", 7))
            cutoff = datetime.fromtimestamp(now.timestamp() - days * 86400, tz=timezone.utc)
            f_ids = self.cleanup_old_research_finalize_jobs(cutoff)
            s_ids = self.cleanup_old_search_task_jobs(cutoff)
            all_ids = f_ids + s_ids
            affected_count = len(all_ids)
            sample_ids = all_ids[:10]
            summary = f"Deleted {len(f_ids)} finalize and {len(s_ids)} search jobs"

        elif action == "cleanup_search_cache":
            days = int(params.get("days", 3))
            cutoff = datetime.fromtimestamp(now.timestamp() - days * 86400, tz=timezone.utc)
            affected_count = self.cleanup_search_cache(cutoff)
            summary = f"Cleaned up {affected_count} search cache entries"

        elif action == "requeue_finalize_job":
            target_id = params.get("target_id")
            if target_id:
                requeued = self.requeue_research_finalize_job(target_id)
                if requeued:
                    affected_count = 1
                    sample_ids = [target_id]
                    summary = f"Requeued finalize job {target_id}"
                else:
                    summary = f"Finalize job {target_id} not found"

        elif action == "requeue_search_job":
            target_id = params.get("target_id")
            if target_id:
                requeued = self.requeue_search_task_job(target_id)
                if requeued:
                    affected_count = 1
                    sample_ids = [target_id]
                    summary = f"Requeued search job {target_id}"
                else:
                    summary = f"Search job {target_id} not found"

        # Record audit log
        self.record_admin_audit(
            actor_email=actor_email,
            action=action,
            target_type="maintenance",
            target_id=params.get("target_id"),
            details={"params": params, "affected_count": affected_count, "summary": summary},
            ip_address=ip_address,
        )

        return AdminDryRunResult(
            action=action,
            dry_run=False,
            affected_count=affected_count,
            sample_affected_ids=sample_ids,
            summary=summary,
        )

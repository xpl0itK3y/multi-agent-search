import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Callable

from sqlalchemy import and_, case, delete, false, func, literal, null, or_, select, text, true, union_all, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session, selectinload

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
    QueueMetrics,
    ResearchFinalizeJob,
    PROMPT_EVENT_NAMES,
    ResearchHistoryItem,
    SearchJobStatus,
    SearchTaskJob,
    TELEMETRY_IP_MAX_LENGTH,
    TELEMETRY_USER_AGENT_MAX_LENGTH,
    USER_ACTIVITY_TOUCH_INTERVAL_SECONDS,
    WorkerHeartbeat,
    ResearchRecord,
    ResearchRequest,
    ResearchStatus,
    SearchTask,
    TaskUpdate,
    UserRecord,
    clip_text,
)
from src.db.models import (
    AdminAuditLogORM,
    LLMUsageLogORM,
    ResearchFinalizeJobORM,
    ResearchORM,
    SearchCacheORM,
    SearchTaskJobORM,
    SearchTaskORM,
    UserEventORM,
    UserORM,
    UserSessionORM,
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
def _parse_admin_emails() -> set[str]:
    from src.config import settings
    raw = getattr(settings, "admin_emails", "")
    if not raw:
        return set()
    if isinstance(raw, str):
        return {e.strip().lower() for e in raw.split(",") if e.strip()}
    return {str(e).strip().lower() for e in raw if str(e).strip()}



def _contains_pattern(term: str) -> str:
    """ILIKE pattern (with escape '\\') matching ``term`` as a plain substring: its % and _
    are literal, as in the in-memory store's substring match."""
    escaped = term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


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

    def update_user_profile(self, user_id: str, name: str | None, avatar_url: str | None) -> UserRecord | None:
        with self.session_scope() as session:
            user = session.get(UserORM, user_id)
            if user is not None:
                if name is not None:
                    user.name = name
                if avatar_url is not None:
                    user.avatar_url = avatar_url
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
            return None

    def get_cached_search(self, cache_key: str, max_age_seconds: int) -> list[dict] | None:
        with self.session_scope() as session:
            row = session.get(SearchCacheORM, cache_key)
            if row is None:
                return None
            age = (datetime.now(timezone.utc) - row.created_at).total_seconds()
            if age >= max_age_seconds:  # same boundary as the in-memory store
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
            session.execute(self._delete_prompt_events([research_id]))
            return True

    @staticmethod
    def _delete_prompt_events(research_ids: list[str]):
        """The research/chat prompt copies in user_events, which has no FK to researches:
        deleted explicitly, in the same transaction as their research. The IN list keeps
        the ix_user_events_prompt_research_id partial index usable."""
        return (
            delete(UserEventORM)
            .where(
                UserEventORM.event_name.in_(PROMPT_EVENT_NAMES),
                UserEventORM.details["research_id"].astext.in_(research_ids),
            )
            .execution_options(synchronize_session=False)
        )

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
                session.execute(self._delete_prompt_events(list(research_ids)))
            return list(research_ids)

    # Telemetry retention: one short transaction per batch, so the first sweep over a
    # large table never becomes one huge delete holding locks and WAL.
    _RETENTION_BATCH_SIZE = 1000

    def _delete_in_batches(self, model, predicate) -> int:
        deleted = 0
        while True:
            with self.session_scope() as session:
                batch = select(model.id).where(predicate).limit(self._RETENTION_BATCH_SIZE)
                result = session.execute(
                    delete(model)
                    .where(model.id.in_(batch.scalar_subquery()))
                    .execution_options(synchronize_session=False)
                )
            count = result.rowcount or 0
            deleted += count
            if count < self._RETENTION_BATCH_SIZE:
                return deleted

    def cleanup_old_user_events(self, older_than: datetime) -> int:
        return self._delete_in_batches(UserEventORM, UserEventORM.created_at < older_than)

    def cleanup_old_user_sessions(self, older_than: datetime) -> int:
        return self._delete_in_batches(UserSessionORM, UserSessionORM.last_active_at < older_than)

    def cleanup_old_admin_audit_logs(self, older_than: datetime) -> int:
        return self._delete_in_batches(AdminAuditLogORM, AdminAuditLogORM.created_at < older_than)

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

    def reset_research_for_retry(
        self,
        research_id: str,
        expected_status: ResearchStatus,
        remove_graph_state_keys: list[str],
    ) -> ResearchRecord | None:
        """Clear a failed attempt's report and graph_state keys under the row lock, only
        while the research is still in `expected_status` (a retry resets right after its
        admission CAS; a cancel landing in between wins). None when the guard fails."""
        with self.session_scope() as session:
            research = session.execute(
                select(ResearchORM).where(ResearchORM.id == research_id).with_for_update()
            ).scalar_one_or_none()
            if research is None or research.status != expected_status.value:
                return None

            research.final_report = None
            research.partial_report = None
            research.partial_reasoning = None
            graph_state = dict(research.graph_state or {})
            for key in remove_graph_state_keys:
                graph_state.pop(key, None)
            research.graph_state = graph_state
            research.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(research)
            result = research_orm_to_record(research)
        self._emit_change(research_id)
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

    def append_research_graph_state_item(
        self,
        research_id: str,
        key: str,
        item: dict,
        *,
        max_items: int | None = None,
    ) -> list[dict] | None:
        """Append to a graph_state list under the row lock. A merge of a list computed
        from an earlier read would drop whatever another writer appended meanwhile."""
        with self.session_scope() as session:
            row = session.execute(
                select(ResearchORM.graph_state)
                .where(ResearchORM.id == research_id)
                .with_for_update()
            ).one_or_none()
            if row is None:
                return None
            graph_state = dict(row.graph_state or {})
            items = [*(graph_state.get(key) or []), item]
            if max_items is not None:
                items = items[-max_items:]
            graph_state[key] = items
            session.execute(
                update(ResearchORM)
                .where(ResearchORM.id == research_id)
                .values(graph_state=graph_state, updated_at=datetime.now(timezone.utc))
            )
            return items

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
    ) -> list[dict] | None:
        """Append one trail event under the research row lock, reading and writing only
        graph_trail — parallel search workers append to the same research, and the old
        unlocked read-modify-write dropped events. The timestamp is taken inside the
        lock, so trail order follows commit order (the SSE cursor relies on it).
        Returns the new trail (None when the research is gone)."""
        with self.session_scope() as session:
            row = session.execute(
                select(ResearchORM.graph_trail)
                .where(ResearchORM.id == research_id)
                .with_for_update()
            ).one_or_none()
            if row is None:
                return None

            now = datetime.now(timezone.utc)
            normalized_event = {"timestamp": now.isoformat(), **event}
            trail = compact_graph_trail(row.graph_trail or [], [normalized_event])
            session.execute(
                update(ResearchORM)
                .where(ResearchORM.id == research_id)
                .values(graph_trail=trail, updated_at=now)
            )
        self._emit_change(research_id)
        return trail

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
                # Secondary keys keep a created_at tie deterministic.
                .order_by(
                    ResearchFinalizeJobORM.created_at.desc(),
                    ResearchFinalizeJobORM.updated_at.desc(),
                    ResearchFinalizeJobORM.id.desc(),
                )
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

    # Only a job that has stopped may be requeued. Resetting a RUNNING (or PENDING) job
    # would let a second worker claim it next to the live runner.
    _REQUEUEABLE_JOB_STATUSES = (FinalizeJobStatus.DEAD_LETTER.value, FinalizeJobStatus.FAILED.value)

    def requeue_research_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None:
        """Reset a dead-lettered/failed job to PENDING; None when it is not requeueable.
        The lease epoch is bumped so any runner still holding the old lease is fenced."""
        with self.session_scope() as session:
            statement = (
                update(ResearchFinalizeJobORM)
                .where(
                    ResearchFinalizeJobORM.id == job_id,
                    ResearchFinalizeJobORM.status.in_(self._REQUEUEABLE_JOB_STATUSES),
                )
                .values(
                    status=FinalizeJobStatus.PENDING.value,
                    attempt_count=0,
                    error=None,
                    lease_epoch=ResearchFinalizeJobORM.lease_epoch + 1,
                    updated_at=datetime.now(timezone.utc),
                )
                .returning(ResearchFinalizeJobORM)
            )
            job = session.execute(statement).scalar_one_or_none()
            if job is None:
                return None
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
                .order_by(
                    SearchTaskJobORM.created_at.desc(),
                    SearchTaskJobORM.updated_at.desc(),
                    SearchTaskJobORM.id.desc(),
                )
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
        """Reset a dead-lettered/failed job to PENDING; None when it is not requeueable."""
        with self.session_scope() as session:
            statement = (
                update(SearchTaskJobORM)
                .where(
                    SearchTaskJobORM.id == job_id,
                    SearchTaskJobORM.status.in_(self._REQUEUEABLE_JOB_STATUSES),
                )
                .values(
                    status=SearchJobStatus.PENDING.value,
                    attempt_count=0,
                    error=None,
                    updated_at=datetime.now(timezone.utc),
                )
                .returning(SearchTaskJobORM)
            )
            job = session.execute(statement).scalar_one_or_none()
            if job is None:
                return None
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
        cache_hit_tokens: int = 0,
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
                cache_hit_tokens=cache_hit_tokens,
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
            # id breaks created_at ties, so LIMIT/OFFSET pages neither repeat nor skip rows.
            stmt = (
                stmt.order_by(AdminAuditLogORM.created_at.desc(), AdminAuditLogORM.id.desc())
                .limit(limit)
                .offset(offset)
            )
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
                .order_by(func.sum(LLMUsageLogORM.total_tokens).desc(), LLMUsageLogORM.model)
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
                .order_by(func.sum(LLMUsageLogORM.total_tokens).desc(), ResearchORM.depth)
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
            research_items = self._token_research_page(session, page, page_size)

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

    def get_admin_token_research_usage(
        self,
        page: int = 1,
        page_size: int = 20,
    ) -> list[AdminTokenResearchUsageItem]:
        with self.session_scope() as session:
            return self._token_research_page(session, page, page_size)

    def _token_research_page(self, session: Session, page: int, page_size: int) -> list[AdminTokenResearchUsageItem]:
        """The page first, then usage for its ids only, and just graph_state's
        llm_token_usage (the legacy per-research figure), never the whole graph_state."""
        page_rows = session.execute(
            select(
                ResearchORM.id,
                ResearchORM.prompt,
                ResearchORM.depth,
                ResearchORM.status,
                ResearchORM.created_at,
                ResearchORM.graph_state["llm_token_usage"],
            )
            .order_by(ResearchORM.created_at.desc(), ResearchORM.id.desc())
            .limit(page_size)
            .offset(max(0, (page - 1) * page_size))
        ).all()
        usage_by_research = self._usage_by_research(session, [row[0] for row in page_rows])
        items: list[AdminTokenResearchUsageItem] = []
        for r_id, r_prompt, r_depth, r_status, r_created, legacy in page_rows:
            tokens, cost, calls = usage_by_research.get(r_id, (0, 0.0, 0))
            if calls == 0 and isinstance(legacy, dict):
                # Researches finalized before per-call usage rows existed.
                tokens = int(legacy.get("total_tokens", 0) or 0)
                cost = float(legacy.get("estimated_cost_usd", 0.0) or 0.0)
            items.append(
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
        return items

    @staticmethod
    def _usage_by_research(session: Session, research_ids: list[str]) -> dict[str, tuple[int, float, int]]:
        """(total_tokens, unrounded cost, calls) per research, for these ids only."""
        if not research_ids:
            return {}
        rows = session.execute(
            select(
                LLMUsageLogORM.research_id,
                func.coalesce(func.sum(LLMUsageLogORM.total_tokens), 0),
                func.coalesce(func.sum(LLMUsageLogORM.estimated_cost_usd), 0.0),
                func.count(LLMUsageLogORM.id),
            )
            .where(LLMUsageLogORM.research_id.in_(research_ids))
            .group_by(LLMUsageLogORM.research_id)
        ).all()
        return {row[0]: (int(row[1]), float(row[2]), int(row[3])) for row in rows}

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

            # system_health is the service's: it adds the LLM/broker probes to these counters.
            return AdminOverviewResponse(
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
        # One row per (session_id, user_id). session_id is client-chosen, so the key must
        # include the caller: another account's session is never updated. The unique
        # index arbitrates concurrent session_starts instead of a check-then-insert.
        insert_stmt = pg_insert(UserSessionORM).values(
            id=str(uuid.uuid4()),
            user_id=user_id,
            session_id=session_id,
            ip_address=ip_address,
            user_agent=user_agent,
            device_type=device_type,
            browser=browser,
            os=os,
            screen_res=screen_res,
            viewport=viewport,
            language=language,
            timezone=client_timezone,
            country=country,
            city=city,
            started_at=now,
            last_active_at=now,
        )
        upsert = insert_stmt.on_conflict_do_update(
            index_elements=[UserSessionORM.session_id, UserSessionORM.user_id],
            set_={
                "last_active_at": insert_stmt.excluded.last_active_at,
                "ip_address": func.coalesce(insert_stmt.excluded.ip_address, UserSessionORM.ip_address),
                "user_agent": func.coalesce(insert_stmt.excluded.user_agent, UserSessionORM.user_agent),
            },
        ).returning(UserSessionORM.id)
        with self.session_scope() as session:
            return session.execute(upsert).scalar_one()

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
        with self.session_scope() as session:
            event_orm = UserEventORM(
                id=record_id,
                user_id=user_id,
                session_id=session_id,
                event_name=event_name,
                event_category=event_category,
                details=details or {},
                ip_address=ip_address,
                user_agent=user_agent,
                created_at=now,
            )
            session.add(event_orm)
        # users.last_seen_at is the activity middleware's job (throttled); touching it here
        # too cost a second users UPDATE per event.
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
        values: dict = {"last_seen_at": now}
        if ip_address:
            values["last_ip"] = ip_address
        if user_agent:
            values["last_user_agent"] = user_agent
        if device:
            values["last_device"] = device
        # Throttled in SQL as well as by the caller's in-process gate (other API workers
        # have their own): a user seen within the interval is not rewritten, sparing a
        # users-row UPDATE per request.
        stale_before = now - timedelta(seconds=USER_ACTIVITY_TOUCH_INTERVAL_SECONDS)
        with self.session_scope() as session:
            session.execute(
                update(UserORM)
                .where(
                    UserORM.id == user_id,
                    or_(UserORM.last_seen_at.is_(None), UserORM.last_seen_at < stale_before),
                )
                .values(**values)
            )

    @staticmethod
    def _admin_users_select():
        """Users with their research count, usage totals and latest session, in one
        statement: each aggregate is a LATERAL subquery, so sorting by one of them orders
        the whole filtered set in SQL before LIMIT/OFFSET (not just the current page)."""
        research_stats = (
            select(func.count().label("researches_count"))
            .where(ResearchORM.user_id == UserORM.id)
            .lateral("research_stats")
        )
        usage_stats = (
            select(
                func.coalesce(func.sum(LLMUsageLogORM.total_tokens), 0).label("total_tokens"),
                func.coalesce(func.sum(LLMUsageLogORM.estimated_cost_usd), 0.0).label("total_cost"),
            )
            .where(LLMUsageLogORM.user_id == UserORM.id)
            .lateral("usage_stats")
        )
        last_session = (
            select(
                UserSessionORM.ip_address,
                UserSessionORM.device_type,
                UserSessionORM.browser,
                UserSessionORM.os,
            )
            .where(UserSessionORM.user_id == UserORM.id)
            .order_by(UserSessionORM.last_active_at.desc(), UserSessionORM.id.desc())
            .limit(1)
            .lateral("last_session")
        )
        statement = (
            select(
                UserORM.id,
                UserORM.email,
                UserORM.name,
                UserORM.avatar_url,
                UserORM.created_at,
                UserORM.last_seen_at,
                UserORM.last_ip,
                UserORM.last_device,
                research_stats.c.researches_count,
                usage_stats.c.total_tokens,
                usage_stats.c.total_cost,
                last_session.c.ip_address.label("session_ip"),
                last_session.c.device_type.label("session_device"),
                last_session.c.browser.label("session_browser"),
                last_session.c.os.label("session_os"),
            )
            .select_from(UserORM)
            .join(research_stats, true())
            .join(usage_stats, true())
            .outerjoin(last_session, true())
        )
        return statement, research_stats, usage_stats

    @staticmethod
    def _admin_user_item(row, admin_emails: set[str], online_threshold: datetime) -> AdminUserListItem:
        return AdminUserListItem(
            id=row.id,
            email=row.email,
            name=row.name,
            avatar_url=row.avatar_url,
            is_admin=bool(row.email and row.email.lower() in admin_emails),
            created_at=row.created_at.isoformat(),
            last_seen_at=row.last_seen_at.isoformat() if row.last_seen_at else None,
            is_online=bool(row.last_seen_at and row.last_seen_at >= online_threshold),
            last_ip=row.last_ip or row.session_ip,
            last_device=row.last_device or row.session_device,
            last_browser=row.session_browser,
            last_os=row.session_os,
            researches_count=int(row.researches_count),
            total_tokens=int(row.total_tokens),
            total_cost_usd=round(float(row.total_cost), 4),
        )

    def get_admin_users_list(
        self,
        page: int = 1,
        page_size: int = 20,
        search: str | None = None,
        role: str | None = None,
        online_only: bool = False,
        sort_by: str = "last_seen",
    ) -> AdminUserListResponse:
        now = datetime.now(timezone.utc)
        online_threshold = now - timedelta(minutes=2)
        admin_emails = _parse_admin_emails()

        filters = []
        term = (search or "").strip()
        if term:
            pattern = _contains_pattern(term)
            filters.append(
                or_(
                    UserORM.email.ilike(pattern, escape="\\"),
                    UserORM.name.ilike(pattern, escape="\\"),
                    UserORM.last_ip.ilike(pattern, escape="\\"),
                )
            )
        if role == "admin":
            filters.append(func.lower(UserORM.email).in_(admin_emails) if admin_emails else false())
        elif role == "user" and admin_emails:
            filters.append(~func.lower(UserORM.email).in_(admin_emails))
        if online_only:
            filters.append(UserORM.last_seen_at >= online_threshold)

        statement, research_stats, usage_stats = self._admin_users_select()
        newest_first = (UserORM.created_at.desc(), UserORM.id.desc())
        order_by = {
            "tokens": (usage_stats.c.total_tokens.desc(), *newest_first),
            "cost": (usage_stats.c.total_cost.desc(), *newest_first),
            "researches": (research_stats.c.researches_count.desc(), *newest_first),
            "registered": newest_first,
        }.get(sort_by, (UserORM.last_seen_at.desc().nulls_last(), *newest_first))

        with self.session_scope() as session:
            total_users = session.execute(
                select(func.count()).select_from(UserORM).where(*filters)
            ).scalar_one()
            online_users = session.execute(
                select(func.count()).select_from(UserORM).where(UserORM.last_seen_at >= online_threshold)
            ).scalar_one()
            rows = session.execute(
                statement.where(*filters)
                .order_by(*order_by)
                .offset((page - 1) * page_size)
                .limit(page_size)
            ).all()
            return AdminUserListResponse(
                users=[self._admin_user_item(row, admin_emails, online_threshold) for row in rows],
                total_users=total_users,
                online_users=online_users,
                page=page,
                page_size=page_size,
            )

    def get_admin_user_detail(self, user_id: str) -> AdminUserDetailResponse | None:
        online_threshold = datetime.now(timezone.utc) - timedelta(minutes=2)
        admin_emails = _parse_admin_emails()
        statement, _research_stats, _usage_stats = self._admin_users_select()

        with self.session_scope() as session:
            row = session.execute(statement.where(UserORM.id == user_id)).one_or_none()
            if row is None:
                return None
            user_item = self._admin_user_item(row, admin_emails, online_threshold)

            sessions = session.execute(
                select(UserSessionORM)
                .where(UserSessionORM.user_id == user_id)
                .order_by(UserSessionORM.last_active_at.desc(), UserSessionORM.id.desc())
                .limit(10)
            ).scalars().all()
            sessions_data = [
                {
                    "id": s.id,
                    "session_id": s.session_id,
                    "ip_address": s.ip_address,
                    "device_type": s.device_type,
                    "browser": s.browser,
                    "os": s.os,
                    "screen_res": s.screen_res,
                    "language": s.language,
                    "timezone": s.timezone,
                    "country": s.country,
                    "city": s.city,
                    "started_at": s.started_at.isoformat(),
                    "last_active_at": s.last_active_at.isoformat(),
                }
                for s in sessions
            ]

            # Recent researches: listing columns only, never the report or graph_state.
            research_rows = session.execute(
                select(
                    ResearchORM.id,
                    ResearchORM.prompt,
                    ResearchORM.depth,
                    ResearchORM.status,
                    ResearchORM.created_at,
                )
                .where(ResearchORM.user_id == user_id)
                .order_by(ResearchORM.created_at.desc(), ResearchORM.id.desc())
                .limit(20)
            ).all()
            usage_by_research = self._usage_by_research(session, [r.id for r in research_rows])
            researches_data = [
                {
                    "id": r.id,
                    "prompt": r.prompt,
                    "depth": r.depth,
                    "status": r.status,
                    "total_tokens": usage_by_research.get(r.id, (0, 0.0, 0))[0],
                    "cost_usd": round(usage_by_research.get(r.id, (0, 0.0, 0))[1], 4),
                    "created_at": r.created_at.isoformat(),
                }
                for r in research_rows
            ]

            event_rows = session.execute(
                select(
                    UserEventORM.id,
                    UserEventORM.event_name,
                    UserEventORM.event_category,
                    UserEventORM.details,
                    UserEventORM.created_at,
                )
                .where(UserEventORM.user_id == user_id)
                .order_by(UserEventORM.created_at.desc(), UserEventORM.id.desc())
                .limit(20)
            ).all()
            events_data = [
                {
                    "id": e.id,
                    "event_name": e.event_name,
                    "event_category": e.event_category,
                    "details": e.details or {},
                    "created_at": e.created_at.isoformat(),
                }
                for e in event_rows
            ]

            model_rows = session.execute(
                select(
                    LLMUsageLogORM.model,
                    func.sum(LLMUsageLogORM.total_tokens),
                    func.sum(LLMUsageLogORM.estimated_cost_usd),
                )
                .where(LLMUsageLogORM.user_id == user_id)
                .group_by(LLMUsageLogORM.model)
                .order_by(LLMUsageLogORM.model)
            ).all()
            by_model = {row[0]: {"tokens": int(row[1]), "cost_usd": round(float(row[2]), 4)} for row in model_rows}

            return AdminUserDetailResponse(
                user=user_item,
                sessions=sessions_data,
                researches=researches_data,
                recent_researches=researches_data,
                recent_events=events_data,
                token_breakdown={"by_model": by_model},
            )

    def get_admin_telemetry_summary(self) -> AdminTelemetrySummaryResponse:
        now = datetime.now(timezone.utc)
        online_threshold = now - timedelta(minutes=2)
        dau_threshold = now - timedelta(days=1)
        wau_threshold = now - timedelta(days=7)
        mau_threshold = now - timedelta(days=30)

        with self.session_scope() as session:
            total_users = session.execute(select(func.count()).select_from(UserORM)).scalar_one()
            online_now = session.execute(
                select(func.count()).select_from(UserORM).where(UserORM.last_seen_at >= online_threshold)
            ).scalar_one()

            dau = session.execute(
                select(func.count()).select_from(UserORM).where(UserORM.last_seen_at >= dau_threshold)
            ).scalar_one()

            wau = session.execute(
                select(func.count()).select_from(UserORM).where(UserORM.last_seen_at >= wau_threshold)
            ).scalar_one()

            mau = session.execute(
                select(func.count()).select_from(UserORM).where(UserORM.last_seen_at >= mau_threshold)
            ).scalar_one()

            total_researches = session.execute(select(func.count()).select_from(ResearchORM)).scalar_one()

            usage_row = session.execute(
                select(
                    func.coalesce(func.sum(LLMUsageLogORM.total_tokens), 0),
                    func.coalesce(func.sum(LLMUsageLogORM.estimated_cost_usd), 0.0),
                )
            ).one()
            total_tokens = int(usage_row[0])
            total_cost = float(usage_row[1])

            # Breakdowns from UserSessionORM; ties are ordered by name, so the top 10 is stable.
            os_rows = session.execute(
                select(UserSessionORM.os, func.count())
                .where(UserSessionORM.os.isnot(None))
                .group_by(UserSessionORM.os)
                .order_by(func.count().desc(), UserSessionORM.os)
                .limit(10)
            ).all()

            browser_rows = session.execute(
                select(UserSessionORM.browser, func.count())
                .where(UserSessionORM.browser.isnot(None))
                .group_by(UserSessionORM.browser)
                .order_by(func.count().desc(), UserSessionORM.browser)
                .limit(10)
            ).all()

            device_rows = session.execute(
                select(UserSessionORM.device_type, func.count())
                .where(UserSessionORM.device_type.isnot(None))
                .group_by(UserSessionORM.device_type)
                .order_by(func.count().desc(), UserSessionORM.device_type)
                .limit(10)
            ).all()

            country_rows = session.execute(
                select(UserSessionORM.country, func.count())
                .where(UserSessionORM.country.isnot(None))
                .group_by(UserSessionORM.country)
                .order_by(func.count().desc(), UserSessionORM.country)
                .limit(10)
            ).all()

            depth_rows = session.execute(
                select(ResearchORM.depth, func.count())
                .group_by(ResearchORM.depth)
                .order_by(func.count().desc(), ResearchORM.depth)
            ).all()

            model_rows = session.execute(
                select(LLMUsageLogORM.model, func.count())
                .group_by(LLMUsageLogORM.model)
                .order_by(func.count().desc(), LLMUsageLogORM.model)
                .limit(10)
            ).all()

            avg_len_row = session.execute(
                select(func.coalesce(func.avg(func.length(ResearchORM.prompt)), 0.0))
            ).scalar_one()

            os_dict = {r[0]: int(r[1]) for r in os_rows if r[0]}
            browser_dict = {r[0]: int(r[1]) for r in browser_rows if r[0]}
            device_dict = {r[0]: int(r[1]) for r in device_rows if r[0]}
            depth_dict = {r[0]: int(r[1]) for r in depth_rows if r[0]}

            return AdminTelemetrySummaryResponse(
                total_users=total_users,
                online_now=online_now,
                online_users_now=online_now,
                dau=dau,
                dau_today=dau,
                wau=wau,
                wau_7d=wau,
                mau=mau,
                mau_30d=mau,
                total_researches=total_researches,
                total_tokens=total_tokens,
                total_cost_usd=round(total_cost, 4),
                by_os=[{"name": r[0], "count": r[1]} for r in os_rows],
                by_browser=[{"name": r[0], "count": r[1]} for r in browser_rows],
                by_device=[{"name": r[0], "count": r[1]} for r in device_rows],
                by_country=[{"name": r[0], "count": r[1]} for r in country_rows],
                os_breakdown=os_dict,
                browser_breakdown=browser_dict,
                device_breakdown=device_dict,
                depth_distribution=depth_dict,
                popular_depths=[{"depth": r[0], "count": r[1]} for r in depth_rows],
                popular_models=[{"model": r[0], "count": r[1]} for r in model_rows],
                avg_prompt_len=round(float(avg_len_row), 1),
            )

    def get_admin_event_logs(
        self,
        limit: int = 50,
        offset: int = 0,
        category: str | None = None,
        event_name: str | None = None,
        user_id: str | None = None,
    ) -> AdminEventLogResponse:
        with self.session_scope() as session:
            query = select(UserEventORM, UserORM.email).outerjoin(UserORM, UserEventORM.user_id == UserORM.id)

            if category:
                query = query.where(UserEventORM.event_category == category)
            if event_name:
                query = query.where(UserEventORM.event_name == event_name)
            if user_id:
                query = query.where(UserEventORM.user_id == user_id)

            count_stmt = select(func.count()).select_from(query.subquery())
            total_count = session.execute(count_stmt).scalar_one()

            # id breaks created_at ties, so LIMIT/OFFSET pages neither repeat nor skip rows.
            query = (
                query.order_by(UserEventORM.created_at.desc(), UserEventORM.id.desc())
                .offset(offset)
                .limit(limit)
            )
            rows = session.execute(query).all()

            items = [
                AdminEventLogItem(
                    id=e.id,
                    user_id=e.user_id,
                    user_email=email,
                    session_id=e.session_id,
                    event_name=e.event_name,
                    event_category=e.event_category,
                    details=e.details or {},
                    ip_address=e.ip_address,
                    user_agent=e.user_agent,
                    created_at=e.created_at.isoformat(),
                )
                for e, email in rows
            ]

            return AdminEventLogResponse(
                events=items,
                total_count=total_count,
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
        """Research prompts and chat follow-up prompts (chat_prompt events) as one list,
        newest first. Paginated in SQL: a UNION ALL of the two sources ordered and sliced
        by the database, a separate COUNT, and usage summed for the page's researches only."""
        term = (search or "").strip()
        pattern = _contains_pattern(term) if term else None
        branches = []
        if not prompt_type or prompt_type in ("all", "research"):
            research_branch = (
                select(
                    literal("research").label("prompt_type"),
                    func.concat("res_", ResearchORM.id).label("item_id"),
                    ResearchORM.prompt.label("prompt"),
                    ResearchORM.id.label("research_id"),
                    ResearchORM.user_id.label("user_id"),
                    UserORM.email.label("user_email"),
                    UserORM.name.label("user_name"),
                    ResearchORM.depth.label("depth"),
                    ResearchORM.status.label("status"),
                    ResearchORM.created_at.label("created_at"),
                )
                .select_from(ResearchORM)
                .outerjoin(UserORM, ResearchORM.user_id == UserORM.id)
            )
            if user_id:
                research_branch = research_branch.where(ResearchORM.user_id == user_id)
            if pattern:
                research_branch = research_branch.where(
                    or_(
                        ResearchORM.prompt.ilike(pattern, escape="\\"),
                        UserORM.email.ilike(pattern, escape="\\"),
                        UserORM.name.ilike(pattern, escape="\\"),
                    )
                )
            branches.append(research_branch)
        if not prompt_type or prompt_type in ("all", "chat"):
            chat_prompt = func.coalesce(UserEventORM.details["prompt"].astext, "")
            chat_branch = (
                select(
                    literal("chat").label("prompt_type"),
                    func.concat("chat_", UserEventORM.id).label("item_id"),
                    chat_prompt.label("prompt"),
                    func.coalesce(UserEventORM.details["research_id"].astext, "").label("research_id"),
                    UserEventORM.user_id.label("user_id"),
                    UserORM.email.label("user_email"),
                    UserORM.name.label("user_name"),
                    null().label("depth"),
                    null().label("status"),
                    UserEventORM.created_at.label("created_at"),
                )
                .select_from(UserEventORM)
                .outerjoin(UserORM, UserEventORM.user_id == UserORM.id)
                .where(UserEventORM.event_name == "chat_prompt")
            )
            if user_id:
                chat_branch = chat_branch.where(UserEventORM.user_id == user_id)
            if pattern:
                chat_branch = chat_branch.where(
                    or_(
                        chat_prompt.ilike(pattern, escape="\\"),
                        UserORM.email.ilike(pattern, escape="\\"),
                        UserORM.name.ilike(pattern, escape="\\"),
                    )
                )
            branches.append(chat_branch)
        if not branches:  # an unknown prompt_type matches nothing
            return AdminPromptsResponse(prompts=[], total_count=0, page=page, page_size=page_size)

        prompts = (union_all(*branches) if len(branches) > 1 else branches[0]).subquery("prompts")
        with self.session_scope() as session:
            total_count = session.execute(select(func.count()).select_from(prompts)).scalar_one()
            rows = session.execute(
                select(prompts)
                .order_by(prompts.c.created_at.desc(), prompts.c.item_id.desc())
                .offset(max(0, (page - 1) * page_size))
                .limit(page_size)
            ).all()
            usage_by_research = self._usage_by_research(
                session, [row.research_id for row in rows if row.prompt_type == "research"]
            )

        items: list[AdminPromptItem] = []
        for row in rows:
            tokens, cost = 0, 0.0
            if row.prompt_type == "research":
                tokens, cost, _calls = usage_by_research.get(row.research_id, (0, 0.0, 0))
            items.append(
                AdminPromptItem(
                    id=row.item_id,
                    prompt_type=row.prompt_type,
                    prompt=row.prompt,
                    research_id=row.research_id,
                    user_id=row.user_id,
                    user_email=row.user_email,
                    user_name=row.user_name,
                    depth=row.depth,
                    status=row.status,
                    total_tokens=tokens,
                    cost_usd=round(cost, 4),
                    created_at=row.created_at.isoformat(),
                )
            )
        return AdminPromptsResponse(
            prompts=items,
            total_count=total_count,
            page=page,
            page_size=page_size,
        )

    def get_user_token_analytics(self, user_id: str) -> dict:
        with self.session_scope() as session:
            tot_stmt = select(
                func.coalesce(func.sum(LLMUsageLogORM.prompt_tokens), 0),
                func.coalesce(func.sum(LLMUsageLogORM.completion_tokens), 0),
                func.coalesce(func.sum(LLMUsageLogORM.total_tokens), 0),
                func.coalesce(func.sum(LLMUsageLogORM.estimated_cost_usd), 0.0),
                func.count(LLMUsageLogORM.id),
            ).where(LLMUsageLogORM.user_id == user_id)
            tot_row = session.execute(tot_stmt).one()
            total_prompt = int(tot_row[0])
            total_comp = int(tot_row[1])
            total_tok = int(tot_row[2])
            total_cost = round(float(tot_row[3]), 4)
            calls_count = int(tot_row[4])

            res_stmt = select(func.count(ResearchORM.id)).where(ResearchORM.user_id == user_id)
            researches_count = int(session.execute(res_stmt).scalar_one())

            model_stmt = (
                select(
                    LLMUsageLogORM.model,
                    func.coalesce(func.sum(LLMUsageLogORM.prompt_tokens), 0),
                    func.coalesce(func.sum(LLMUsageLogORM.completion_tokens), 0),
                    func.coalesce(func.sum(LLMUsageLogORM.total_tokens), 0),
                    func.coalesce(func.sum(LLMUsageLogORM.estimated_cost_usd), 0.0),
                    func.count(LLMUsageLogORM.id),
                )
                .where(LLMUsageLogORM.user_id == user_id)
                .group_by(LLMUsageLogORM.model)
                .order_by(func.sum(LLMUsageLogORM.total_tokens).desc(), LLMUsageLogORM.model)
            )
            by_model = [
                {
                    "model": row[0],
                    "prompt_tokens": int(row[1]),
                    "completion_tokens": int(row[2]),
                    "total_tokens": int(row[3]),
                    "estimated_cost_usd": round(float(row[4]), 4),
                    "calls_count": int(row[5]),
                }
                for row in session.execute(model_stmt).all()
            ]

            recent_stmt = (
                select(
                    ResearchORM.id,
                    ResearchORM.prompt,
                    ResearchORM.depth,
                    ResearchORM.status,
                    func.coalesce(func.sum(LLMUsageLogORM.total_tokens), 0),
                    func.coalesce(func.sum(LLMUsageLogORM.estimated_cost_usd), 0.0),
                    ResearchORM.created_at,
                )
                .outerjoin(LLMUsageLogORM, LLMUsageLogORM.research_id == ResearchORM.id)
                .where(ResearchORM.user_id == user_id)
                .group_by(ResearchORM.id, ResearchORM.prompt, ResearchORM.depth, ResearchORM.status, ResearchORM.created_at)
                .order_by(ResearchORM.created_at.desc(), ResearchORM.id.desc())
                .limit(10)
            )
            recent = [
                {
                    "id": row[0],
                    "prompt": row[1],
                    "depth": row[2],
                    "status": row[3],
                    "total_tokens": int(row[4]),
                    "estimated_cost_usd": round(float(row[5]), 4),
                    "created_at": row[6].isoformat() if row[6] else None,
                }
                for row in session.execute(recent_stmt).all()
            ]

            return {
                "total_tokens": total_tok,
                "prompt_tokens": total_prompt,
                "completion_tokens": total_comp,
                "estimated_cost_usd": total_cost,
                "calls_count": calls_count,
                "researches_count": researches_count,
                "by_model": by_model,
                "recent": recent,
            }
